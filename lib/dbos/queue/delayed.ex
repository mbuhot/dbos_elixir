# Promotes DELAYED workflows to ENQUEUED when their wake time arrives, one process per engine.
#
# Sleeps until the earliest wake time rather than polling for it: the set of delayed workflows only
# changes when one is written, bounced, or promoted, and the queue trigger notifies on all three
# (Dbos.Notifications.subscribe_delayed/1), so the timer is re-armed on the change instead of
# rediscovered on a tick. Each wake reads the earliest wake time and writes only when something is
# actually due, so a wake with nothing to promote costs one index scan of
# idx_workflow_status_delayed.
#
# Where no LISTEN connection is available the notification never arrives, so the ceiling on how
# long this process will sleep drops to the notification module's own polling cadence — the same
# fallback Dbos.Queue.Runner makes, for the same reason.
#
# Traps exits so a shutdown arriving mid-sweep is handled after it returns rather than killing the
# process while it holds a database connection.
defmodule Dbos.Queue.Delayed do
  @moduledoc false

  use GenServer

  require Logger

  alias Dbos.Notifications
  alias Dbos.SystemDb

  @min_arm_ms 50

  defstruct [:engine, :config, :timer_ref, :fallback_ms]

  @doc "Starts the delayed-workflow timer for the engine named `opts[:name]`."
  def start_link(opts) do
    engine = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, engine, name: process_name(engine))
  end

  @doc "The name this engine's delayed-workflow timer is registered under."
  def process_name(engine), do: Module.concat(engine, Queue.Delayed)

  @doc "Runs one promotion pass immediately, outside the timer. Tests use this to avoid waiting."
  def sweep_now(engine), do: GenServer.call(process_name(engine), :sweep)

  @impl true
  def init(engine) do
    Process.flag(:trap_exit, true)
    config = Dbos.config(engine)
    Notifications.subscribe_delayed(engine)

    state = %__MODULE__{
      engine: engine,
      config: config,
      fallback_ms: config.delayed_fallback_interval_ms
    }

    {:ok, tick(state)}
  end

  @impl true
  def handle_call(:sweep, _from, state), do: {:reply, :ok, tick(state)}

  @impl true
  def handle_info(:wake, state), do: {:noreply, tick(state)}

  @impl true
  def handle_info({:dbos_notify, :delayed, _payload}, state), do: {:noreply, tick(state)}

  @impl true
  def handle_info({:dbos_notify, :reconnect, _payload}, state), do: {:noreply, tick(state)}

  @impl true
  def handle_info(_message, state), do: {:noreply, state}

  defp tick(state, promoted? \\ false) do
    now = System.os_time(:millisecond)

    case SystemDb.next_delayed_wake_epoch_ms(state.config) do
      nil ->
        arm(state, ceiling(state))

      wake_at when wake_at <= now and not promoted? ->
        SystemDb.transition_delayed_workflows(state.config)
        tick(state, true)

      wake_at ->
        arm(state, max(wake_at - now, @min_arm_ms))
    end
  rescue
    error ->
      Logger.error(
        "dbos: promoting delayed workflows failed; retrying on the fallback interval: " <>
          Exception.format_banner(:error, error, __STACKTRACE__)
      )

      arm(state, state.fallback_ms)
  end

  # How long this process is willing to sleep without hearing anything. A notification is what
  # normally ends the sleep early, so where there is none to hear, the ceiling has to be short
  # enough for this process to notice a newly delayed workflow by itself.
  defp ceiling(%__MODULE__{engine: engine, fallback_ms: fallback_ms}) do
    if Notifications.mode(engine) == :listen,
      do: fallback_ms,
      else: min(Notifications.poll_interval_ms(), fallback_ms)
  end

  defp arm(state, delay_ms) do
    cancel(state.timer_ref)
    delay_ms = round(min(delay_ms, ceiling(state)))
    %{state | timer_ref: Process.send_after(self(), :wake, delay_ms)}
  end

  defp cancel(nil), do: :ok
  defp cancel(timer_ref), do: Process.cancel_timer(timer_ref)
end
