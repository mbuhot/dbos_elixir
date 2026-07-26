defmodule Dbos.Compiler.State do
  @moduledoc """
  ETS-backed collection state for `Mix.Tasks.Compile.Dbos`, and its on-disk manifest.

  Three tables, all keyed by module so that one `:ets.delete/2` invalidates everything recorded
  about it:

  | Table | Row |
  |---|---|
  | calls | `{from_module, %{from:, to:, file:, line:, mode:}}` |
  | entries | `{module, %{kind:, ...}}` — workflow, step, transaction, repo and trusted declarations |
  | seen | `{module}` — modules whose first trace event of this run has arrived |

  The first trace event for a module in a run wipes that module's rows loaded from the manifest,
  so after the run each module's rows are either freshly traced or untouched from the previous
  compile. Collection is incremental; the analysis is re-run in full over the union.

  Rows are maps, and the manifest carries a format number. Unknown keys are ignored on read, so
  a row written by an older `dbos` still loads.
  """

  use GenServer

  @manifest_format 1

  @doc """
  Starts (or reuses) the table owner and prepares it for a compile run. `:force` starts from
  empty tables; `:app` and `:manifest_path` default to the project being compiled.
  """
  def start_run(opts \\ []) do
    app = Keyword.get_lazy(opts, :app, fn -> Mix.Project.config()[:app] || :dbos_no_app end)
    :persistent_term.put({__MODULE__, :app}, app)
    opts = Keyword.put(opts, :app, app)

    case GenServer.start_link(__MODULE__, opts, name: server_name(app)) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> GenServer.call(pid, {:start_run, opts}, :infinity)
    end
  end

  @doc "Whether collection is active — false when the Mix compiler is not installed."
  def enabled?, do: :ets.whereis(table(:calls)) != :undefined

  @doc """
  Wipes `module`'s rows the first time it is seen in this run. Called from every trace event,
  including the ones whose edge is discarded, so that a module recompiled with no recordable
  calls still drops its stale rows.
  """
  def initialize_module(module) do
    if :ets.insert_new(table(:seen), {module}) do
      :ets.delete(table(:calls), module)
      :ets.delete(table(:entries), module)
    end

    :ok
  end

  @doc "Records one caller → callee edge."
  def add_call(from_module, row) do
    :ets.insert(table(:calls), {from_module, row})
    :ok
  end

  @doc "Records one workflow, step, transaction, repo or trusted-function declaration."
  def add_entry(module, row) do
    if enabled?() do
      initialize_module(module)
      :ets.insert(table(:entries), {module, row})
    end

    :ok
  end

  @doc "Every recorded edge."
  def calls, do: rows(:calls)

  @doc "Every recorded declaration."
  def entries, do: rows(:entries)

  @doc "Modules whose first trace event arrived in this run."
  def recompiled_modules, do: table(:seen) |> :ets.tab2list() |> Enum.map(&elem(&1, 0))

  @doc """
  Whether the manifest read at the start of this run was missing or written in another format.
  A stale manifest under-reports, because a Mix compiler cannot make `compile.elixir` re-trace
  files it considers up to date.
  """
  def manifest_stale?, do: call(:manifest_stale?)

  @doc """
  Drops rows for modules absent from `app_modules`, clears the seen table, and rewrites the
  manifest when anything changed.
  """
  def flush(app_modules), do: call({:flush, app_modules})

  @doc "The manifest path for the project being compiled."
  def manifest_path do
    Path.join(Mix.Project.manifest_path(Mix.Project.config()), "compile.dbos_determinism")
  end

  @impl GenServer
  def init(opts) do
    app = Keyword.fetch!(opts, :app)

    for kind <- [:calls, :entries] do
      :ets.new(table_name(app, kind), [
        :named_table,
        :public,
        :duplicate_bag,
        write_concurrency: true
      ])
    end

    :ets.new(table_name(app, :seen), [:named_table, :public, :set, write_concurrency: true])

    state = %{
      app: app,
      path: Keyword.get_lazy(opts, :manifest_path, &manifest_path/0),
      stale?: false
    }

    {:ok, load(state, opts)}
  end

  @impl GenServer
  def handle_call({:start_run, opts}, _from, state) do
    :ets.delete_all_objects(table_name(state.app, :seen))

    state =
      if Keyword.get(opts, :force, false) do
        :ets.delete_all_objects(table_name(state.app, :calls))
        :ets.delete_all_objects(table_name(state.app, :entries))
        %{state | stale?: false}
      else
        state
      end

    {:reply, {:ok, self()}, state}
  end

  def handle_call(:manifest_stale?, _from, state), do: {:reply, state.stale?, state}

  def handle_call({:flush, app_modules}, _from, state) do
    known = MapSet.new(app_modules)
    dropped_calls? = drop_unknown(state.app, :calls, known)
    dropped_entries? = drop_unknown(state.app, :entries, known)
    recompiled = :ets.info(table_name(state.app, :seen), :size)
    :ets.delete_all_objects(table_name(state.app, :seen))

    if dropped_calls? or dropped_entries? or recompiled > 0 do
      write_manifest(state)
      {:reply, :ok, %{state | stale?: false}}
    else
      {:reply, :ok, state}
    end
  end

  defp drop_unknown(app, kind, known) do
    stale =
      app
      |> table_name(kind)
      |> :ets.tab2list()
      |> Enum.map(&elem(&1, 0))
      |> Enum.uniq()
      |> Enum.reject(&MapSet.member?(known, &1))

    Enum.each(stale, &:ets.delete(table_name(app, kind), &1))
    stale != []
  end

  defp write_manifest(state) do
    payload = %{
      format: @manifest_format,
      calls: uniq_rows(state.app, :calls),
      entries: uniq_rows(state.app, :entries)
    }

    File.mkdir_p!(Path.dirname(state.path))
    File.write!(state.path, :erlang.term_to_binary(payload, [:compressed]))
  end

  defp uniq_rows(app, kind), do: app |> table_name(kind) |> :ets.tab2list() |> Enum.uniq()

  defp load(state, opts) do
    if Keyword.get(opts, :force, false), do: state, else: read_manifest(state)
  end

  defp read_manifest(state) do
    with {:ok, binary} <- File.read(state.path),
         {:ok, %{format: @manifest_format} = payload} <- decode(binary) do
      :ets.insert(table_name(state.app, :calls), Map.get(payload, :calls, []))
      :ets.insert(table_name(state.app, :entries), Map.get(payload, :entries, []))
      state
    else
      _other -> %{state | stale?: true}
    end
  end

  defp decode(binary) do
    {:ok, :erlang.binary_to_term(binary)}
  rescue
    _error -> :error
  end

  defp rows(kind), do: kind |> table() |> :ets.tab2list() |> Enum.map(&elem(&1, 1))

  defp call(message), do: GenServer.call(server_name(app()), message, :infinity)

  defp app do
    :persistent_term.get({__MODULE__, :app}, nil) || Mix.Project.config()[:app] || :dbos_no_app
  end

  defp server_name(app), do: Module.concat(__MODULE__, to_string(app))

  defp table_name(app, kind), do: Module.concat([__MODULE__, to_string(app), to_string(kind)])

  defp table(kind), do: table_name(app(), kind)
end
