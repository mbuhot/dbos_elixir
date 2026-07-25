defmodule Dbos.Scheduler do
  @moduledoc """
  Fires cron-scheduled workflows, backed by `workflow_schedules`. Registers this engine's own
  `schedule:`-declared workflows into the table at boot (idempotent), then every
  `poll_interval_ms` reconciles the full `ACTIVE` set read fresh from the database — so several
  engines sharing one database, or a schedule declared by only one of them, all converge on the
  same set.

  Each due occurrence enqueues under a deterministic id (`"sched-<schedule_name>-<scheduled_time_ms>"`),
  so two engines independently computing the same occurrence collapse onto one `workflow_status`
  row; the queue's `FOR UPDATE SKIP LOCKED` dequeue is what actually guarantees the workflow body
  runs exactly once, not the reconcile timing. The fired workflow receives the scheduled time
  (epoch ms) and the schedule's static context as its two arguments.

  Catch-up: a schedule with `automatic_backfill: false` (the default) starts counting from the
  moment *this process* first sees it — any ticks missed while no engine was running are silently
  skipped. `automatic_backfill: true` instead seeds its starting floor from the schedule's
  persisted `last_fired_at`, so every tick missed since the last time any engine ran is enqueued
  on the next reconcile.

  `deactivate/1` (the `/deactivate` admin route) stops firing new ticks; already-enqueued work is
  unaffected.
  """

  use GenServer

  alias Dbos.Cron
  alias Dbos.Queue
  alias Dbos.Serialization
  alias Dbos.SystemDb

  defstruct [:engine, :config, :poll_interval_ms, schedules: %{}, deactivated: false]

  @doc "Starts the scheduler for the engine named `opts[:name]`, registering `opts[:schedules]` (from `Module.__dbos_schedules__/0`)."
  def start_link(opts) do
    engine = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: process_name(engine))
  end

  @doc "The `Dbos.Scheduler` process name for `engine`."
  def process_name(engine), do: Module.concat(engine, Scheduler)

  @doc "Stops firing new ticks for this engine. Idempotent; does not interrupt in-flight work."
  def deactivate(engine), do: GenServer.cast(process_name(engine), :deactivate)

  @impl true
  def init(opts) do
    engine = Keyword.fetch!(opts, :name)
    config = Dbos.config(engine)
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, config.scheduler_poll_interval_ms)

    Enum.each(Keyword.get(opts, :schedules, []), &register_schedule_def(config, &1))

    schedule_tick(0)
    {:ok, %__MODULE__{engine: engine, config: config, poll_interval_ms: poll_interval_ms}}
  end

  @impl true
  def handle_cast(:deactivate, state), do: {:noreply, %{state | deactivated: true}}

  @impl true
  def handle_info(:tick, %__MODULE__{deactivated: true} = state) do
    schedule_tick(state.poll_interval_ms)
    {:noreply, state}
  end

  def handle_info(:tick, %__MODULE__{} = state) do
    now = System.os_time(:millisecond)
    active = SystemDb.list_active_schedules(state.config)
    schedules = Enum.reduce(active, state.schedules, &reconcile_one(state, now, &1, &2))
    schedule_tick(state.poll_interval_ms)
    {:noreply, %{state | schedules: schedules}}
  end

  defp register_schedule_def(config, meta) do
    SystemDb.register_schedule(config, %{
      schedule_name: meta.schedule_name,
      workflow_name: meta.workflow_name,
      schedule: meta.cron,
      context: Serialization.encode(Map.get(meta, :context)),
      automatic_backfill: Map.get(meta, :automatic_backfill, false),
      cron_timezone: Map.get(meta, :cron_timezone),
      queue_name: Map.get(meta, :queue_name)
    })
  end

  defp reconcile_one(state, now, %{schedule_name: name} = definition, schedules) do
    {cron, floor_ms} =
      case Map.fetch(schedules, name) do
        {:ok, {cron, floor_ms}} -> {cron, floor_ms}
        :error -> {Cron.parse!(definition.cron), initial_floor(definition, now)}
      end

    cron
    |> Cron.due_between(floor_ms, now)
    |> Enum.each(&fire(state, definition, &1))

    Map.put(schedules, name, {cron, now})
  end

  defp initial_floor(%{automatic_backfill: true, last_fired_at: last_fired_at}, _now)
       when is_integer(last_fired_at),
       do: last_fired_at

  defp initial_floor(_definition, now), do: now

  defp fire(state, definition, scheduled_time_ms) do
    workflow_id = "sched-#{definition.schedule_name}-#{scheduled_time_ms}"
    queue_name = definition.queue_name || Queue.internal_queue_name()

    Dbos.enqueue(definition.workflow_name, [scheduled_time_ms, definition.context],
      queue_name: queue_name,
      workflow_id: workflow_id,
      engine: state.engine
    )

    SystemDb.update_schedule_last_fired_at(
      state.config,
      definition.schedule_name,
      scheduled_time_ms
    )
  end

  defp schedule_tick(delay_ms), do: Process.send_after(self(), :tick, round(delay_ms))
end
