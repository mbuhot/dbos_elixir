defmodule Dbos.Cluster.NodeWatcher do
  @moduledoc """
  Watches for departed nodes via `:net_kernel.monitor_nodes/1` and reclaims a departed node's
  `PENDING` workflows through `Dbos.Cluster`'s cached roster and `Dbos.Recovery.reclaim/3`.
  Started only when the owning `Dbos.Supervisor` is given `cluster: [enabled: true]`.

  Reclaim runs in an unsupervised `Task` so a slow or failing reclaim pass never blocks this
  process (and so `:nodedown` messages keep being handled promptly).
  """

  use GenServer

  alias Dbos.Cluster
  alias Dbos.Recovery

  @doc "Starts the node watcher for the engine named `opts[:name]`."
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, engine_name, name: process_name(engine_name))
  end

  @doc "The name this engine's node watcher process is registered under."
  def process_name(engine_name), do: Module.concat(engine_name, Cluster.NodeWatcher)

  @impl true
  def init(engine_name) do
    :net_kernel.monitor_nodes(true)
    {:ok, engine_name}
  end

  @impl true
  def handle_info({:nodedown, node}, engine_name) do
    reclaim_node(engine_name, node)
    {:noreply, engine_name}
  end

  def handle_info({:nodeup, _node}, engine_name), do: {:noreply, engine_name}

  defp reclaim_node(engine_name, node) do
    case Cluster.executor_ids_for_node(engine_name, node) do
      [] ->
        :ok

      executor_ids ->
        config = Dbos.config(engine_name)
        batch_size = config.reclaim_batch_size
        Task.start(fn -> Recovery.reclaim(engine_name, executor_ids, batch_size: batch_size) end)
    end
  end
end
