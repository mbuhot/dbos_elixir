defmodule Dbos.Cluster.NodeWatcher do
  @moduledoc """
  Watches for departed nodes via `:net_kernel.monitor_nodes/1` and reclaims a departed node's
  `PENDING` workflows through `Dbos.Cluster`'s cached roster and `Dbos.Recovery.reclaim/3`.
  Started only when the owning `Dbos.Supervisor` is given `cluster: [enabled: true]`.

  Reclaim runs in an unsupervised `Task` so a slow or failing reclaim pass never blocks this
  process (and so `:nodedown` messages keep being handled promptly). A reclaim that raises is
  caught there and retried on a bounded exponential backoff (`@max_reclaim_attempts` attempts,
  capped at `@reclaim_max_backoff_ms`) rather than dropped: a single `:nodedown` is the only
  signal this departed node ever gets, unlike `Dbos.Cluster.OrphanSweep`'s recurring pass, so a
  failure here needs its own retry rather than a later sweep to fall back on. Attempts exhausted,
  it gives up and logs, leaving the node's workflows to `Dbos.Cluster.OrphanSweep` (if enabled) or
  a future engine restart's boot recovery scan.
  """

  use GenServer
  require Logger

  alias Dbos.Cluster
  alias Dbos.Recovery

  @max_reclaim_attempts 5
  @reclaim_base_backoff_ms 500
  @reclaim_max_backoff_ms 30_000

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
    reclaim_node(engine_name, node, 1)
    {:noreply, engine_name}
  end

  def handle_info({:nodeup, _node}, engine_name), do: {:noreply, engine_name}

  def handle_info({:retry_reclaim, node, attempt}, engine_name) do
    reclaim_node(engine_name, node, attempt)
    {:noreply, engine_name}
  end

  defp reclaim_node(engine_name, node, attempt) do
    case Cluster.executor_ids_for_node(engine_name, node) do
      [] ->
        :ok

      executor_ids ->
        config = Dbos.config(engine_name)
        batch_size = config.reclaim_batch_size
        watcher = self()

        Task.start(fn ->
          reclaim_or_retry(engine_name, executor_ids, batch_size, watcher, node, attempt)
        end)
    end
  end

  defp reclaim_or_retry(engine_name, executor_ids, batch_size, watcher, node, attempt) do
    Recovery.reclaim(engine_name, executor_ids, batch_size: batch_size)
  rescue
    error ->
      Logger.error(
        "dbos: reclaim for departed node #{inspect(node)} failed (attempt #{attempt}): " <>
          Exception.format_banner(:error, error, __STACKTRACE__)
      )

      schedule_retry(watcher, node, attempt)
  end

  defp schedule_retry(watcher, node, attempt) when attempt < @max_reclaim_attempts do
    delay =
      min(@reclaim_base_backoff_ms * Integer.pow(2, attempt - 1), @reclaim_max_backoff_ms)

    Process.send_after(watcher, {:retry_reclaim, node, attempt + 1}, delay)
  end

  defp schedule_retry(_watcher, node, _attempt) do
    Logger.error(
      "dbos: giving up reclaiming departed node #{inspect(node)} after " <>
        "#{@max_reclaim_attempts} attempts; its workflows are left PENDING for the orphan " <>
        "sweep or a future boot recovery scan to pick up"
    )
  end
end
