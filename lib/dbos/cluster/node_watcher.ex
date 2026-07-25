defmodule Dbos.Cluster.NodeWatcher do
  @moduledoc """
  Watches for departed nodes via `:net_kernel.monitor_nodes/1` and schedules a
  `Dbos.Cluster.OrphanSweep` pass for the moment the departed executors' leases expire.

  A node that dies renewed its lease moments earlier, so at `:nodedown` that lease still has
  nearly its full term left and a sweep run right then reclaims nothing. Scheduling the pass for
  the expiry instant brings reclaim forward to roughly the lease TTL, from the TTL plus however
  much of the sweep interval remains.

  The lease stays the sole authority. A `:nodedown` never shortens a lease: it reports that two
  BEAM nodes cannot see each other, which says nothing about whether either can still reach
  Postgres. Letting it expire a peer's lease would let both sides of a netsplit evict each other
  and run the same workflows at once.

  Started only when the owning `Dbos.Supervisor` is given `cluster: [enabled: true]`.
  """

  use GenServer
  require Logger

  alias Dbos.Cluster
  alias Dbos.Cluster.OrphanSweep
  alias Dbos.SystemDb

  @expiry_margin_ms 250

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
    schedule_sweep_at_lease_expiry(engine_name, node)
    {:noreply, engine_name}
  end

  def handle_info({:nodeup, _node}, engine_name), do: {:noreply, engine_name}

  def handle_info({:sweep, node}, engine_name) do
    run_sweep(engine_name, node)
    {:noreply, engine_name}
  end

  @doc """
  Milliseconds until the earliest of `executor_ids`' leases expires, `0` when any has none.
  """
  def sweep_delay_ms(engine_name, executor_ids) do
    config = Dbos.config(engine_name)
    now = System.os_time(:millisecond)
    expiries = Enum.map(executor_ids, &lease_expiry(config, &1))

    cond do
      expiries == [] -> 0
      Enum.any?(expiries, &is_nil/1) -> 0
      true -> max(Enum.min(expiries) - now + @expiry_margin_ms, 0)
    end
  end

  defp schedule_sweep_at_lease_expiry(engine_name, node) do
    delay_ms =
      engine_name
      |> Cluster.executor_ids_for_node(node)
      |> then(&sweep_delay_ms(engine_name, &1))

    Process.send_after(self(), {:sweep, node}, delay_ms)
  rescue
    error -> log_failure(node, error, __STACKTRACE__)
  end

  defp lease_expiry(config, executor_id) do
    case SystemDb.get_executor_lease(config, executor_id) do
      %{lease_expires_epoch_ms: expires_at} -> expires_at
      nil -> nil
    end
  end

  defp run_sweep(engine_name, node) do
    case Process.whereis(OrphanSweep.process_name(engine_name)) do
      nil -> :ok
      _pid -> OrphanSweep.sweep_now(engine_name)
    end
  rescue
    error -> log_failure(node, error, __STACKTRACE__)
  catch
    kind, reason -> log_failure(node, {kind, reason}, __STACKTRACE__)
  end

  defp log_failure(context, error, stacktrace) do
    Logger.error(
      "dbos: the sweep pass triggered by :nodedown from #{inspect(context)} failed; the " <>
        "periodic sweep will " <>
        "retry: " <> Exception.format_banner(:error, error, stacktrace)
    )
  end
end
