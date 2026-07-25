# One polling loop per declared queue. Each tick: sweeps DELAYED workflows globally
# (Dbos.SystemDb.transition_delayed_workflows/1), dequeues from every live partition (or the queue
# itself, if unpartitioned), and dispatches every claimed workflow into Dbos.WorkflowSup. The
# polling interval backs off exponentially on lock contention and scales back toward the base
# interval otherwise, with multiplicative jitter applied every tick.
defmodule Dbos.Queue.Runner do
  @moduledoc false

  use GenServer

  require Logger

  alias Dbos.Queue
  alias Dbos.Registry, as: WorkflowRegistry
  alias Dbos.SystemDb
  alias Dbos.Telemetry
  alias Dbos.WorkflowSup

  @backoff_factor 2.0
  @scaleback_factor 0.9
  @jitter_min 0.95
  @jitter_max 1.05

  defstruct [:engine, :queue, :config, :polling_interval_ms]

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
    state = %__MODULE__{
      engine: engine,
      queue: queue,
      config: Dbos.config(engine),
      polling_interval_ms: queue.base_polling_interval_ms
    }

    schedule_poll(0)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    backoff? = poll_once(state)
    new_interval = adjust_interval(state.polling_interval_ms, state.queue, backoff?)
    schedule_poll(jittered(new_interval))
    {:noreply, %{state | polling_interval_ms: new_interval}}
  end

  defp schedule_poll(delay_ms), do: Process.send_after(self(), :poll, round(delay_ms))

  defp poll_once(%__MODULE__{config: config, queue: queue, engine: engine}) do
    SystemDb.transition_delayed_workflows(config)

    queue
    |> partition_keys(config)
    |> Enum.map(&dequeue_and_dispatch(engine, config, queue, &1))
    |> Enum.any?(&(&1 == :contention))
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
    :ok
  rescue
    error ->
      if SystemDb.contention_error?(error) do
        :contention
      else
        Logger.error(
          "dbos: queue #{inspect(queue.name)} dequeue failed: " <>
            Exception.format_banner(:error, error, __STACKTRACE__)
        )

        :ok
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

  defp adjust_interval(current_ms, %Queue{max_polling_interval_ms: max_ms}, true),
    do: min(current_ms * @backoff_factor, max_ms)

  defp adjust_interval(current_ms, %Queue{base_polling_interval_ms: base_ms}, false),
    do: max(current_ms * @scaleback_factor, base_ms)

  defp jittered(interval_ms),
    do: interval_ms * (@jitter_min + :rand.uniform() * (@jitter_max - @jitter_min))
end
