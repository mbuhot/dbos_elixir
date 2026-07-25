defmodule Dbos.Registry do
  @moduledoc """
  An ETS-backed name to `{module, function, arity}` map for one engine's registered workflows.
  The table is owned by this GenServer, so it dies with the engine, and is namespaced under the
  engine's name so multiple engines can coexist in one BEAM.
  """

  use GenServer

  @doc "Starts the registry for the engine named `opts[:name]`, pre-registering `opts[:workflows]`."
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    workflows = Keyword.get(opts, :workflows, [])
    GenServer.start_link(__MODULE__, {engine_name, workflows}, name: process_name(engine_name))
  end

  @doc "Registers `name` as `mfa`. Raises if `name` is already registered to a different `mfa`."
  def register(engine_name, name, mfa) do
    case GenServer.call(process_name(engine_name), {:register, name, mfa}) do
      :ok ->
        :ok

      {:error, {:conflict, existing}} ->
        raise "workflow #{inspect(name)} is already registered as #{inspect(existing)}, " <>
                "cannot re-register as #{inspect(mfa)}"
    end
  end

  @doc "Looks up the `mfa` registered under `name`."
  def lookup(engine_name, name) do
    case :ets.lookup(table_name(engine_name), name) do
      [{^name, mfa}] -> {:ok, mfa}
      [] -> :error
    end
  end

  @doc "Every registered workflow name for this engine."
  def registered_names(engine_name) do
    engine_name
    |> table_name()
    |> :ets.tab2list()
    |> Enum.map(fn {name, _mfa} -> name end)
  end

  @doc "Every distinct module backing a registered workflow for this engine."
  def modules(engine_name) do
    engine_name
    |> table_name()
    |> :ets.tab2list()
    |> Enum.map(fn {_name, {module, _fun, _arity}} -> module end)
    |> Enum.uniq()
  end

  @doc "The workflow name registered to `mfa`, if any."
  def name_for_mfa(engine_name, mfa) do
    engine_name
    |> table_name()
    |> :ets.tab2list()
    |> Enum.find(fn {_name, entry_mfa} -> entry_mfa == mfa end)
    |> case do
      {name, ^mfa} -> {:ok, name}
      nil -> :error
    end
  end

  @impl true
  def init({engine_name, workflows}) do
    table = table_name(engine_name)
    :ets.new(table, [:named_table, :protected, :set])

    Enum.each(workflows, fn {name, mfa} ->
      case insert_new(table, name, mfa) do
        :ok ->
          :ok

        {:error, {:conflict, existing}} ->
          raise "workflow #{inspect(name)} is already registered as #{inspect(existing)}, " <>
                  "cannot re-register as #{inspect(mfa)}"
      end
    end)

    {:ok, %{table: table}}
  end

  @impl true
  def handle_call({:register, name, mfa}, _from, state) do
    {:reply, insert_new(state.table, name, mfa), state}
  end

  defp insert_new(table, name, mfa) do
    case :ets.lookup(table, name) do
      [] ->
        :ets.insert(table, {name, mfa})
        :ok

      [{^name, ^mfa}] ->
        :ok

      [{^name, existing}] ->
        {:error, {:conflict, existing}}
    end
  end

  defp process_name(engine_name), do: Module.concat(engine_name, Registry)
  defp table_name(engine_name), do: Module.concat(engine_name, RegistryTable)
end
