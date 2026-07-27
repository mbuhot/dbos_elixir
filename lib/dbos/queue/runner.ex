# One runner per declared queue. Each tick dequeues from every live partition (or the queue itself,
# if unpartitioned) and dispatches every claimed workflow into Dbos.WorkflowSup.
#
# A ticking loop is the fallback, not the mechanism: the runner subscribes to its queue's
# notifications, so an enqueue anywhere in the fleet wakes it within a debounce window, and a tick
# that claims nothing backs the interval off toward max_polling_interval_ms. Where no LISTEN
# connection is available (Dbos.Notifications.mode/1 is :poll) the interval stays at the base,
# since the tick is then the only thing that will notice new work. Lock contention backs off the
# same way, and multiplicative jitter is applied every tick.
#
# Traps exits so a shutdown arriving mid-tick is handled after the tick returns, rather than
# killing the process while it holds a database connection.
defmodule Dbos.Queue.Runner do
  @moduledoc false

  use GenServer

  require Logger

  alias Dbos.Notifications
  alias Dbos.Queue
  alias Dbos.Registry, as: WorkflowRegistry
  alias Dbos.SystemDb
  alias Dbos.Telemetry
  alias Dbos.WorkflowSup

  @backoff_factor 2.0
  @idle_backoff_factor 1.5
  @scaleback_factor 0.9
  @jitter_min 0.95
  @jitter_max 1.05
  @notify_debounce_ms 25

  defstruct [:engine, :queue, :config, :polling_interval_ms, :timer_ref, :poll_due_at_ms]

  @doc "Starts the runner for `opts[:queue]` (a `Dbos.Queue`) on the engine named `opts[:engine]`."
  def start_link(opts) do
    engine = Keyword.fetch!(opts, :engine)
    queue = Keyword.fetch!(opts, :queue)
    GenServer.start_link(__MODULE__, {engine, queue}, name: process_name(engine, queue.name))
  end

  @doc "The `:via` name this engine's runner for `queue_name` is registered under."
  def process_name(engine, queue_name),
    do: {:via, Elixir.Registry, {registry_name(engine), queue_name}}

  @doc "The name of this engine's queue-runner `Registry`, keyed by queue name."
  def registry_name(engine), do: Module.concat(engine, Queue.Runner.Registry)

  @impl true
  def init({engine, %Queue{} = queue}) do
    Process.flag(:trap_exit, true)

    Notifications.subscribe_queue(engine, queue.name)

    state = %__MODULE__{
      engine: engine,
      queue: queue,
      config: Dbos.config(engine),
      polling_interval_ms: queue.base_polling_interval_ms
    }

    {:ok, schedule_poll(state, 0)}
  end

  @impl true
  def handle_info(:poll, state) do
    outcome = poll_once(state)
    new_interval = adjust_interval(state.polling_interval_ms, state, outcome)

    {:noreply,
     state
     |> Map.put(:polling_interval_ms, new_interval)
     |> schedule_poll(jittered(new_interval))}
  end

  # An enqueue anywhere in the fleet. Debounced, so a burst of them costs one poll rather than one
  # per workflow, and ignored outright when a poll is already due sooner than the debounce window.
  @impl true
  def handle_info({:dbos_notify, :queue, _payload}, state) do
    {:noreply, poll_soon(state)}
  end

  @impl true
  def handle_info({:dbos_notify, :reconnect, _payload}, state) do
    {:noreply, poll_soon(state)}
  end

  @impl true
  def handle_info(_message, state), do: {:noreply, state}

  defp poll_soon(state) do
    if due_within?(state, @notify_debounce_ms) do
      state
    else
      schedule_poll(state, @notify_debounce_ms)
    end
  end

  defp due_within?(%__MODULE__{poll_due_at_ms: nil}, _ms), do: false

  defp due_within?(%__MODULE__{poll_due_at_ms: due_at}, ms),
    do: due_at - System.monotonic_time(:millisecond) <= ms

  defp schedule_poll(state, delay_ms) do
    cancel_timer(state.timer_ref)
    delay_ms = round(delay_ms)

    %{
      state
      | timer_ref: Process.send_after(self(), :poll, delay_ms),
        poll_due_at_ms: System.monotonic_time(:millisecond) + delay_ms
    }
  end

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer_ref), do: Process.cancel_timer(timer_ref)

  defp poll_once(%__MODULE__{config: config, queue: queue, engine: engine}) do
    queue
    |> partition_keys(config)
    |> Enum.map(&dequeue_and_dispatch(engine, config, queue, &1))
    |> summarise_tick()
  end

  defp summarise_tick(outcomes) do
    cond do
      Enum.any?(outcomes, &(&1 == :contention)) -> :contention
      Enum.any?(outcomes, &(&1 == :claimed)) -> :claimed
      true -> :idle
    end
  end

  defp partition_keys(%Queue{partition_queue: false}, _config), do: [nil]

  defp partition_keys(%Queue{partition_queue: true} = queue, config),
    do: SystemDb.get_queue_partitions(config, queue.name)

  defp dequeue_and_dispatch(engine, config, queue, partition_key) do
    local_running_count = WorkflowSup.count_running(engine, queue.name, partition_key)
    metadata = %{engine: engine, queue_name: queue.name, partition_key: partition_key}

    claimed =
      Telemetry.span_dequeue(metadata, fn ->
        SystemDb.dequeue_workflows(config, queue,
          partition_key: partition_key,
          local_running_count: local_running_count
        )
      end)

    dispatch_all(engine, claimed, queue.name, partition_key)
    if claimed == [], do: :idle, else: :claimed
  rescue
    error ->
      if SystemDb.contention_error?(error) do
        :contention
      else
        Logger.error(
          "dbos: queue #{inspect(queue.name)} dequeue failed: " <>
            Exception.format_banner(:error, error, __STACKTRACE__)
        )

        :idle
      end
  end

  @doc false
  def dispatch_all(engine, claimed, queue_name, partition_key) do
    Enum.each(claimed, &dispatch_one(engine, &1, queue_name, partition_key))
  end

  defp dispatch_one(engine, workflow, queue_name, partition_key) do
    dispatch(engine, workflow, queue_name, partition_key)
  rescue
    error ->
      Logger.error(
        "dbos: queue #{inspect(queue_name)}: dispatch failed for workflow " <>
          "#{inspect(Map.get(workflow, :workflow_id))}: " <>
          Exception.format_banner(:error, error, __STACKTRACE__)
      )
  end

  defp dispatch(engine, %{workflow_id: id, name: name, inputs: inputs}, queue_name, partition_key) do
    case WorkflowRegistry.lookup(engine, name) do
      {:ok, mfa} ->
        WorkflowSup.start_workflow(engine, id, mfa, inputs,
          queue_name: queue_name,
          partition_key: partition_key
        )

      :error ->
        Logger.error(
          "dbos: queue #{inspect(queue_name)}: workflow #{inspect(name)} (#{id}) is not " <>
            "registered on this executor; skipping dispatch"
        )
    end
  end

  defp adjust_interval(current_ms, %__MODULE__{queue: queue}, :contention),
    do: min(current_ms * @backoff_factor, queue.max_polling_interval_ms)

  defp adjust_interval(_current_ms, %__MODULE__{queue: queue}, :claimed),
    do: queue.base_polling_interval_ms

  # An idle tick backs off only where a notification will bring the runner back sooner than the
  # interval would. Under the polling fallback the tick is the only thing that notices new work, so
  # the base interval is the floor.
  defp adjust_interval(current_ms, %__MODULE__{engine: engine, queue: queue}, :idle) do
    if Notifications.mode(engine) == :listen do
      min(current_ms * @idle_backoff_factor, queue.max_polling_interval_ms)
    else
      max(current_ms * @scaleback_factor, queue.base_polling_interval_ms)
    end
  end

  defp jittered(interval_ms),
    do: interval_ms * (@jitter_min + :rand.uniform() * (@jitter_max - @jitter_min))
end
