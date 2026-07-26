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
  end

  defp reclaim_orphans([], _engine_name, _batch_size), do: :ok

  defp reclaim_orphans(orphaned_executor_ids, engine_name, batch_size),
    do: Recovery.reclaim(engine_name, orphaned_executor_ids, batch_size: batch_size)
end
