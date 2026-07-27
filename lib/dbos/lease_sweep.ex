# Periodically reclaims PENDING workflows whose executor's lease (dbos.executor_leases) has
# expired, or who never renewed one at all. A lease is the sole authority consulted: it is renewed
# over the same connection an executor needs to checkpoint, so an executor that cannot renew also
# cannot write conflicting checkpoints, making a false positive self-limiting. This is why
# updated_at staleness plays no part — a workflow legitimately parked for days in Dbos.sleep/1 or
# recv_message/2 says nothing about whether its executor is alive.
#
# Needs only the system database, so it covers a single-node-per-deployment setup (a Kubernetes
# pod whose identity changes every deploy) exactly as well as a multi-node one. On by default
# (lease_sweep: [enabled: true]).
defmodule Dbos.LeaseSweep do
  @moduledoc false

  use GenServer

  require Logger

  alias Dbos.Recovery
  alias Dbos.SystemDb

  @doc "Starts the lease sweep for the engine named `opts[:name]`."
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, engine_name, name: process_name(engine_name))
  end

  @doc "The name this engine's lease sweep process is registered under."
  def process_name(engine_name), do: Module.concat(engine_name, LeaseSweep)

  @doc "Runs one sweep pass immediately, outside the timer. Tests use this to avoid waiting on the interval."
  def sweep_now(engine_name), do: sweep(engine_name)

  @impl true
  def init(engine_name) do
    Process.flag(:trap_exit, true)
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
    interval_ms = Dbos.config(engine_name).lease_sweep_interval_ms
    Process.send_after(self(), :sweep, interval_ms)
  end

  defp sweep(engine_name) do
    config = Dbos.config(engine_name)

    config
    |> SystemDb.list_expired_lease_pending_executor_ids()
    |> reclaim_orphans(engine_name, config.reclaim_batch_size)

    finish_abandoned_cancellations(config)
  end

  # A workflow cancelled while running unwinds itself, and its process commits CANCELLED with the
  # unwind. If that process died first the row sits CANCELLING with nothing to finish it, so the
  # sweep does — the same lease authority, applied to the cancellation path.
  defp finish_abandoned_cancellations(config) do
    config
    |> SystemDb.list_stale_cancelling_workflow_ids()
    |> Enum.each(&finish_one(config, &1))
  end

  defp finish_one(config, workflow_id) do
    SystemDb.finish_cancelling(config, workflow_id)
  rescue
    error ->
      Logger.error(
        "dbos: could not finish cancelling workflow #{workflow_id}; the sweep continues: " <>
          Exception.format_banner(:error, error, __STACKTRACE__)
      )
  end

  defp reclaim_orphans([], _engine_name, _batch_size), do: :ok

  defp reclaim_orphans(orphaned_executor_ids, engine_name, batch_size),
    do: Recovery.reclaim(engine_name, orphaned_executor_ids, batch_size: batch_size)
end
