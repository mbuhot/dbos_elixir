defmodule Dbos.Cluster do
  @moduledoc """
  Which `{node, executor_id}` pairs are live in this deployment.

  Every engine started with `cluster: [enabled: true]` joins one deployment-wide `:pg` group. One
  BEAM node may host several engines with distinct executor ids, so the roster is a set of pairs.

  `roster/1` reports current membership. `executor_ids_for_node/2` answers from a monotonic
  history of every executor id ever seen on a node, so it still resolves ids for a node that has
  already departed.

  Without distributed Erlang (`Node.alive?/0` is `false`) or `:pg`, the roster degrades to this
  engine alone and logs once at `info`.
  """

  use GenServer

  require Logger

  @scope Dbos.Cluster.Scope

  defstruct [
    :engine_name,
    :executor_id,
    :group,
    :monitor_ref,
    :cluster_enabled,
    members: %{},
    seen_by_node: %{}
  ]

  @doc "Starts the cluster roster for the engine named `opts[:name]`."
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, engine_name, name: process_name(engine_name))
  end

  @doc "The currently known live `{node, executor_id}` pairs for this engine, always including itself."
  def roster(engine_name), do: GenServer.call(process_name(engine_name), :roster)

  @doc """
  The executor ids this engine has ever seen on `node`, live or departed. Answers from a
  monotonic history rather than `roster/1`'s live membership, so a node that has just gone down
  still resolves correctly regardless of whether `:pg`'s own leave processing has already run.
  """
  def executor_ids_for_node(engine_name, node) do
    GenServer.call(process_name(engine_name), {:executor_ids_for_node, node})
  end

  @doc "The name this engine's cluster roster process is registered under."
  def process_name(engine_name), do: Module.concat(engine_name, Cluster)

  @impl true
  def init(engine_name) do
    config = Dbos.config(engine_name)

    state = %__MODULE__{
      engine_name: engine_name,
      executor_id: config.executor_id,
      group: config.cluster_group,
      cluster_enabled: false
    }

    {:ok, join_cluster(state)}
  end

  @impl true
  def handle_call(:roster, _from, state), do: {:reply, roster_from_state(state), state}

  @impl true
  def handle_call({:executor_ids_for_node, node}, _from, state) do
    executor_ids = state.seen_by_node |> Map.get(node, MapSet.new()) |> MapSet.to_list()
    {:reply, executor_ids, state}
  end

  @impl true
  def handle_call(:whoami, _from, state), do: {:reply, {node(), state.executor_id}, state}

  @impl true
  def handle_info({ref, :join, group, pids}, %{monitor_ref: ref, group: group} = state) do
    {:noreply, add_members(state, pids)}
  end

  def handle_info({ref, :leave, group, pids}, %{monitor_ref: ref, group: group} = state) do
    {:noreply, remove_members(state, pids)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp join_cluster(state) do
    if Node.alive?() do
      try_join_pg(state)
    else
      degrade(state, "distributed Erlang is not enabled")
    end
  end

  defp try_join_pg(state) do
    ensure_scope_started()
    :ok = :pg.join(@scope, state.group, self())
    {ref, initial_members} = :pg.monitor(@scope, state.group)

    %{state | cluster_enabled: true, monitor_ref: ref}
    |> add_member(self(), {node(), state.executor_id})
    |> add_members(initial_members)
  rescue
    error -> degrade(state, Exception.format_banner(:error, error, []))
  catch
    kind, reason -> degrade(state, Exception.format_banner(kind, reason, []))
  end

  defp ensure_scope_started do
    case :pg.start_link(@scope) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp degrade(state, reason) do
    Logger.info(
      "dbos: Dbos.Cluster for #{inspect(state.engine_name)} is running with a single-entry " <>
        "roster (#{reason}); cross-node dead-executor reclaim is disabled"
    )

    %{state | cluster_enabled: false, seen_by_node: %{node() => MapSet.new([state.executor_id])}}
  end

  defp add_members(state, pids) do
    Enum.reduce(pids, state, fn pid, state ->
      if Map.has_key?(state.members, pid), do: state, else: resolve_and_add_member(state, pid)
    end)
  end

  defp resolve_and_add_member(state, pid) do
    case safe_whoami(pid) do
      {:ok, node_and_executor_id} -> add_member(state, pid, node_and_executor_id)
      :error -> state
    end
  end

  defp add_member(state, pid, {node, executor_id} = node_and_executor_id) do
    seen_for_node = state.seen_by_node |> Map.get(node, MapSet.new()) |> MapSet.put(executor_id)

    %{
      state
      | members: Map.put(state.members, pid, node_and_executor_id),
        seen_by_node: Map.put(state.seen_by_node, node, seen_for_node)
    }
  end

  defp safe_whoami(pid) do
    {:ok, GenServer.call(pid, :whoami, 5_000)}
  catch
    :exit, _reason -> :error
  end

  defp remove_members(state, pids), do: %{state | members: Map.drop(state.members, pids)}

  defp roster_from_state(%__MODULE__{cluster_enabled: false, executor_id: executor_id}),
    do: MapSet.new([{node(), executor_id}])

  defp roster_from_state(%__MODULE__{members: members}), do: MapSet.new(Map.values(members))
end
