# An ETS-backed name to {{module, function, arity}, version} map for one engine's registered
# workflows. The table is owned by this GenServer, so it dies with the engine, and is namespaced
# under the engine's name so multiple engines can coexist in one BEAM.
defmodule Dbos.Registry do
  @moduledoc false

  use GenServer

  @doc """
  Starts the registry for the engine named `opts[:name]`, pre-registering `opts[:workflows]` —
  `{name, mfa}` or `{name, mfa, version}` entries.
  """
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    workflows = Keyword.get(opts, :workflows, [])
    GenServer.start_link(__MODULE__, {engine_name, workflows}, name: process_name(engine_name))
  end

  @doc "Registers `name` as `mfa` at `version`. Raises if `name` is already registered differently."
  def register(engine_name, name, mfa, version \\ nil) do
    case GenServer.call(process_name(engine_name), {:register, name, mfa, version}) do
      :ok ->
        :ok

      {:error, {:conflict, existing}} ->
        raise "workflow #{inspect(name)} is already registered as #{inspect(existing)}, " <>
                "cannot re-register as #{inspect({mfa, version})}"
    end
  end

  @doc "Looks up the `mfa` registered under `name`."
  def lookup(engine_name, name) do
    case :ets.lookup(table_name(engine_name), name) do
      [{^name, mfa, _version}] -> {:ok, mfa}
      [] -> :error
    end
  end

  @doc """
  The declared version of the workflow registered under `name`: `nil` when it declares none, and
  also when this engine has no registry at all (a bare `Dbos.Config` built for a migration) or
  does not register that name.
  """
  def version(engine_name, name) do
    table = table_name(engine_name)

    with false <- :ets.whereis(table) == :undefined,
         [{^name, _mfa, version}] <- :ets.lookup(table, name) do
      version
    else
      _other -> nil
    end
  end

  @doc "Every registered workflow name for this engine."
  def registered_names(engine_name) do
    engine_name
    |> entries()
    |> Enum.map(fn {name, _mfa, _version} -> name end)
  end

  @doc """
  What this engine can run, as `{name, version}` pairs: the identity both reclaim and the
  fleet-wide orphan report match a `workflow_status` row against.
  """
  def capabilities(engine_name) do
    engine_name
    |> entries()
    |> Enum.map(fn {name, _mfa, version} -> {name, version} end)
  end

  @doc "Every distinct module backing a registered workflow for this engine."
  def modules(engine_name) do
    engine_name
    |> entries()
    |> Enum.map(fn {_name, {module, _fun, _arity}, _version} -> module end)
    |> Enum.uniq()
  end

  @doc "The workflow name registered to `mfa`, if any."
  def name_for_mfa(engine_name, mfa) do
    engine_name
    |> entries()
    |> Enum.find(fn {_name, entry_mfa, _version} -> entry_mfa == mfa end)
    |> case do
      {name, ^mfa, _version} -> {:ok, name}
      nil -> :error
    end
  end

  @impl true
  def init({engine_name, workflows}) do
    table = table_name(engine_name)
    :ets.new(table, [:named_table, :protected, :set])

    Enum.each(workflows, fn entry ->
      {name, mfa, version} = normalize(entry)

      case insert_new(table, name, mfa, version) do
        :ok ->
          :ok

        {:error, {:conflict, existing}} ->
          raise "workflow #{inspect(name)} is already registered as #{inspect(existing)}, " <>
                  "cannot re-register as #{inspect({mfa, version})}"
      end
    end)

    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:register, name, mfa, version}, _from, state) do
    {:reply, insert_new(state.table, name, mfa, version), state}
  end

  defp normalize({name, mfa, version}), do: {name, mfa, version}
  defp normalize({name, mfa}), do: {name, mfa, nil}

  # An engine that never started a registry — a bare `Dbos.Config` driving `Dbos.SystemDb`
  # directly — registers nothing rather than raising on the missing table.
  defp entries(engine_name) do
    table = table_name(engine_name)

    case :ets.whereis(table) do
      :undefined -> []
      _reference -> :ets.tab2list(table)
    end
  end

  defp insert_new(table, name, mfa, version) do
    case :ets.lookup(table, name) do
      [] ->
        :ets.insert(table, {name, mfa, version})
        :ok

      [{^name, ^mfa, ^version}] ->
        :ok

      [{^name, existing_mfa, existing_version}] ->
        {:error, {:conflict, {existing_mfa, existing_version}}}
    end
  end

  defp process_name(engine_name), do: Module.concat(engine_name, Registry)
  defp table_name(engine_name), do: Module.concat(engine_name, RegistryTable)
end
