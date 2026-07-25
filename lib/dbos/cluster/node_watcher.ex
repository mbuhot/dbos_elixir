defmodule Dbos.Cluster.NodeWatcher do
  @moduledoc """
  Watches for departed nodes via `:net_kernel.monitor_nodes/1` and triggers an immediate
  `Dbos.Cluster.OrphanSweep` pass on `:nodedown` — a latency optimisation, nothing more. The
  sweep itself is the sole authority: it still requires the departed node's executors to have an
  expired lease before reclaiming anything, so a BEAM netsplit (both sides see the other as
  `:nodedown` while Postgres is reachable to both) degrades to waiting out the lease TTL instead
  of reclaiming, and possibly double-executing, a node that is merely unreachable, not dead.

  Started only when the owning `Dbos.Supervisor` is given `cluster: [enabled: true]`. Runs the
  triggered sweep in an unsupervised `Task` so a slow sweep pass never blocks `:nodedown` handling;
  a failure there is logged and left to the sweep's own recurring timer to retry.
  """

  use GenServer
  require Logger

  alias Dbos.Cluster.OrphanSweep

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
    trigger_sweep(engine_name, node)
    {:noreply, engine_name}
  end

  def handle_info({:nodeup, _node}, engine_name), do: {:noreply, engine_name}

  defp trigger_sweep(engine_name, node) do
    Task.start(fn -> safe_sweep(engine_name, node) end)
  end

  defp safe_sweep(engine_name, node) do
    case Process.whereis(OrphanSweep.process_name(engine_name)) do
      nil -> :ok
      _pid -> OrphanSweep.sweep_now(engine_name)
    end
  rescue
    error ->
      Logger.error(
        "dbos: the sweep pass triggered by :nodedown from #{inspect(node)} failed; the " <>
          "periodic sweep will retry: " <> Exception.format_banner(:error, error, __STACKTRACE__)
      )
  catch
    kind, reason ->
      Logger.error(
        "dbos: the sweep pass triggered by :nodedown from #{inspect(node)} failed; the " <>
          "periodic sweep will retry: " <>
          Exception.format_banner(kind, reason, __STACKTRACE__)
      )
  end
end
