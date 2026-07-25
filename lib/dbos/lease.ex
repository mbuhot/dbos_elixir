# Per-engine heartbeat that keeps this executor's row in dbos.executor_leases fresh, renewed on
# Dbos.Config.lease_renew_interval_ms — a small fraction of Dbos.Config.lease_ttl_ms, so a couple
# of missed renewals are survivable. Renewed over the same connection this executor checkpoints
# through, so a node that cannot renew also cannot write conflicting checkpoints: this is the
# property that makes the lease Dbos.Cluster.OrphanSweep's sole authority for automatic reclaim.
#
# Writes the first lease synchronously in init/1, before Dbos.Recovery starts, so this engine's
# own PENDING rows are never briefly lease-less while it boots. A renewal failure is logged and
# retried on the next tick rather than crashing this process — Dbos.DB.Retry already absorbs
# transient errors underneath it. Graceful shutdown expires the lease immediately, via
# terminate/2, so a replacement executor doesn't have to wait out the TTL; this is best-effort,
# since a non-graceful stop (a killed node, :brutal_kill) never runs it.
defmodule Dbos.Lease do
  @moduledoc false

  use GenServer
  require Logger

  alias Dbos.SystemDb

  @doc "Starts the lease heartbeat for the engine named `opts[:name]`."
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, engine_name, name: process_name(engine_name))
  end

  @doc "The name this engine's lease heartbeat process is registered under."
  def process_name(engine_name), do: Module.concat(engine_name, Lease)

  @impl true
  def init(engine_name) do
    Process.flag(:trap_exit, true)
    renew(engine_name)
    schedule_renew(engine_name)
    {:ok, engine_name}
  end

  @impl true
  def handle_info(:renew, engine_name) do
    renew(engine_name)
    schedule_renew(engine_name)
    {:noreply, engine_name}
  end

  @impl true
  def terminate(_reason, engine_name) do
    expire(engine_name)
    :ok
  end

  defp renew(engine_name) do
    config = Dbos.config(engine_name)
    SystemDb.renew_lease(config, config.lease_ttl_ms)
  rescue
    error ->
      Logger.error(
        "dbos: renewing the executor lease for #{inspect(engine_name)} failed; retrying on " <>
          "the next tick: " <> Exception.format_banner(:error, error, __STACKTRACE__)
      )
  end

  defp expire(engine_name) do
    config = Dbos.config(engine_name)
    SystemDb.expire_lease(config)
  rescue
    error ->
      Logger.error(
        "dbos: expiring the executor lease for #{inspect(engine_name)} on shutdown failed " <>
          "(best-effort): " <> Exception.format_banner(:error, error, __STACKTRACE__)
      )
  end

  defp schedule_renew(engine_name) do
    interval_ms = Dbos.config(engine_name).lease_renew_interval_ms
    Process.send_after(self(), :renew, interval_ms)
  end
end
