defmodule Dbos.Cluster.OrphanSweep do
  @moduledoc """
  Periodically reclaims `PENDING` workflows whose `executor_id` is absent from `Dbos.Cluster`'s
  live roster and whose `updated_at` is at least `config.orphan_sweep_threshold_ms` old, per the
  Phase 3b dead-executor reclaim design in `DECISIONS.md`. Covers executors no live node ever saw
  depart — a whole-cluster restart, or a pod that is permanently gone — which
  `Dbos.Cluster.NodeWatcher`'s `:nodedown` detection cannot see.

  Off by default. Started only when the owning `Dbos.Supervisor` is given `cluster: [enabled:
  true, orphan_sweep: [enabled: true]]`.
  """

  use GenServer

  alias Dbos.Cluster
  alias Dbos.Recovery
  alias Dbos.SystemDb

  @doc "Starts the orphan sweep for the engine named `opts[:name]`."
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, engine_name, name: process_name(engine_name))
  end

  @doc "The name this engine's orphan sweep process is registered under."
  def process_name(engine_name), do: Module.concat(engine_name, Cluster.OrphanSweep)

  @doc "Runs one sweep pass immediately, outside the timer. Tests use this to avoid waiting on the interval."
  def sweep_now(engine_name), do: sweep(engine_name)

  @impl true
  def init(engine_name) do
    schedule_sweep(engine_name)
    {:ok, engine_name}
  end

  @impl true
  def handle_info(:sweep, engine_name) do
    sweep(engine_name)
    schedule_sweep(engine_name)
    {:noreply, engine_name}
  end

  defp schedule_sweep(engine_name) do
    interval_ms = Dbos.config(engine_name).orphan_sweep_interval_ms
    Process.send_after(self(), :sweep, interval_ms)
  end

  defp sweep(engine_name) do
    config = Dbos.config(engine_name)
    live_executor_ids = live_executor_ids(engine_name)

    config
    |> SystemDb.list_stale_pending_executor_ids(config.orphan_sweep_threshold_ms)
    |> Enum.reject(&(&1 in live_executor_ids))
    |> reclaim_orphans(engine_name, config.reclaim_batch_size)
  end

  defp reclaim_orphans([], _engine_name, _batch_size), do: :ok

  defp reclaim_orphans(orphaned_executor_ids, engine_name, batch_size),
    do: Recovery.reclaim(engine_name, orphaned_executor_ids, batch_size: batch_size)

  defp live_executor_ids(engine_name) do
    engine_name
    |> Cluster.roster()
    |> Enum.map(fn {_node, executor_id} -> executor_id end)
  end
end
