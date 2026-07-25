defmodule Dbos.Waits do
  @moduledoc """
  Registry of parked durable waits. A workflow blocked in `sleep`/`recv_message`/`get_event`
  with more wait remaining than `Dbos.Config.park_exit_threshold_ms` gives up its process,
  leaving one ETS row and a timer in its place — a few tens of bytes against the few KB a live
  `Dbos.WorkflowProcess` holds. A timer firing at the wait's deadline, or a `Dbos.Notifications`
  wake for the topic or event key it was parked on, re-dispatches the workflow through
  `Dbos.WorkflowSup.start_workflow/5`, exactly like `Dbos.Recovery` does, replaying it back to the
  wait site from its checkpoints.

  Node affinity falls out of `workflow_status.executor_id`, unchanged by parking: a parked wait's
  row is `PENDING` under this engine's own executor id, so a node that dies with parked waits
  outstanding leaves behind ordinary `PENDING` rows that `Dbos.Recovery`'s boot scan and
  `Dbos.Cluster`'s dead-executor reclaim already know how to pick up — parking invents no new
  recovery path. That path only runs on a full engine restart or a live dead-executor reclaim,
  though; it does not fire when only this GenServer crashes and its supervisor restarts it in
  place, so the table itself is owned by `Dbos.Waits.Table`, a separate, sibling process that
  outlives a crash here. `init/1` re-adopts whatever is already in that table — re-arming a fresh
  timer for each entry and re-subscribing it with `Dbos.Notifications` (a restarted process is a
  new pid, so the crashed process's own timers and subscriptions are unreachable) — rather than
  starting from an empty table.

  A single wait's wake or redispatch failing is isolated to that wait: `wake/2` runs under rescue
  so one workflow's database error cannot take this process, and every other parked wait, down
  with it.
  """

  use GenServer
  require Logger

  alias Dbos.Notifications
  alias Dbos.Registry, as: WorkflowRegistry
  alias Dbos.SystemDb
  alias Dbos.WorkflowStatus
  alias Dbos.WorkflowSup

  defstruct [:engine]

  defmodule Table do
    @moduledoc """
    Long-lived owner of an engine's parked-waits ETS table, kept alive independently of
    `Dbos.Waits` so a crash and restart of that GenServer never destroys the table, its parked
    entries, or forces them to be rebuilt from the database.
    """

    use GenServer

    @doc "Starts the table owner for the engine named `opts[:name]`."
    def start_link(opts) do
      engine = Keyword.fetch!(opts, :name)
      GenServer.start_link(__MODULE__, engine, name: Dbos.Waits.table_owner_name(engine))
    end

    @impl true
    def init(engine) do
      Dbos.Waits.ensure_table(engine)
      {:ok, engine}
    end
  end

  @doc "Starts the wait registry for the engine named `opts[:name]`."
  def start_link(opts) do
    engine = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, engine, name: process_name(engine))
  end

  @doc "This engine's `Dbos.Waits` GenServer name."
  def process_name(engine), do: Module.concat(engine, Waits)

  @doc "This engine's parked-waits ETS table owner name."
  def table_owner_name(engine), do: Module.concat(engine, Waits.Table)

  @doc "This engine's parked-waits ETS table name."
  def table_name(engine), do: Module.concat(engine, Waits.Table.Ets)

  @doc "Creates this engine's parked-waits ETS table if it does not already exist."
  def ensure_table(engine) do
    table = table_name(engine)

    case :ets.whereis(table) do
      :undefined -> :ets.new(table, [:named_table, :public, :set, read_concurrency: true])
      _tid -> table
    end
  end

  @doc """
  Whether a wait with `remaining_ms` left, `completed_steps` steps into this run, should park
  instead of staying resident: `remaining_ms` must exceed `config.park_exit_threshold_ms` and
  `completed_steps` must be under `config.park_replay_ceiling` — a workflow already deep into a
  run would pay for a parked wait with a proportionally expensive rehydration replay.
  """
  def should_park?(%Dbos.Config{} = config, remaining_ms, completed_steps) do
    exceeds_threshold?(remaining_ms, config.park_exit_threshold_ms) and
      completed_steps < config.park_replay_ceiling
  end

  defp exceeds_threshold?(_remaining_ms, :infinity), do: false
  defp exceeds_threshold?(remaining_ms, threshold_ms), do: remaining_ms > threshold_ms

  @doc """
  Parks `workflow_id`, waiting on `waiting_for` (`:sleep`, `{:recv, topic}`, or `{:event,
  target_workflow_id, key}`) until `deadline_ms`, then raises `Dbos.Waits.Parked` to unwind the
  calling workflow process. Registers with `Dbos.Notifications` and rechecks whether the wait is
  already resolved before parking, closing the race where it resolved between the caller's own
  last check and this call.
  """
  def park(%Dbos.Config{} = config, workflow_id, waiting_for, deadline_ms) do
    GenServer.call(
      process_name(config.name),
      {:park, config, workflow_id, waiting_for, deadline_ms}
    )

    raise Dbos.Waits.Parked, workflow_id: workflow_id
  end

  @doc "How many waits are currently parked for `engine`."
  def count(engine), do: :ets.info(table_name(engine), :size)

  @impl true
  def init(engine) do
    table = ensure_table(engine)
    readopt_entries(engine, table)
    {:ok, %__MODULE__{engine: engine}}
  end

  defp readopt_entries(engine, table) do
    config = Dbos.config(engine)
    Enum.each(:ets.tab2list(table), &readopt_entry(engine, config, table, &1))
  end

  defp readopt_entry(
         engine,
         config,
         table,
         {workflow_id, waiting_for, stale_timer_ref, deadline_ms}
       ) do
    :erlang.cancel_timer(stale_timer_ref)
    subscribe(engine, workflow_id, waiting_for)

    if resolved?(config, workflow_id, waiting_for) do
      :ets.delete(table, workflow_id)
      send(self(), {:wake, workflow_id})
    else
      timer_ref = schedule_wake(workflow_id, deadline_ms)
      :ets.insert(table, {workflow_id, waiting_for, timer_ref, deadline_ms})
    end
  end

  @impl true
  def handle_call({:park, config, workflow_id, waiting_for, deadline_ms}, _from, state) do
    subscribe(state.engine, workflow_id, waiting_for)

    if resolved?(config, workflow_id, waiting_for) do
      send(self(), {:wake, workflow_id})
    else
      timer_ref = schedule_wake(workflow_id, deadline_ms)
      :ets.insert(table_name(state.engine), {workflow_id, waiting_for, timer_ref, deadline_ms})
    end

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:wake, workflow_id}, state) do
    safely(workflow_id, fn -> wake(state.engine, workflow_id) end)
    {:noreply, state}
  end

  def handle_info({:dbos_notify, _keyspace, payload}, state) when is_binary(payload) do
    case workflow_id_from_payload(payload) do
      nil -> :ok
      workflow_id -> safely(workflow_id, fn -> wake(state.engine, workflow_id) end)
    end

    {:noreply, state}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp safely(workflow_id, fun) do
    fun.()
  rescue
    error ->
      Logger.error(
        "dbos: waking parked workflow #{workflow_id} failed; the rest of the parked waits are " <>
          "unaffected: " <> Exception.format_banner(:error, error, __STACKTRACE__)
      )
  end

  defp workflow_id_from_payload(payload) do
    case String.split(payload, "::", parts: 2) do
      [workflow_id, _topic_or_key] -> workflow_id
      _other -> nil
    end
  end

  defp schedule_wake(workflow_id, deadline_ms) do
    remaining_ms = max(deadline_ms - System.os_time(:millisecond), 0)
    :erlang.send_after(remaining_ms, self(), {:wake, workflow_id})
  end

  defp subscribe(_engine, _workflow_id, :sleep), do: :ok

  defp subscribe(engine, workflow_id, {:recv, topic}),
    do: Notifications.subscribe_recv(engine, workflow_id, topic)

  defp subscribe(engine, _workflow_id, {:event, target_workflow_id, key}),
    do: Notifications.subscribe_event(engine, target_workflow_id, key)

  defp unsubscribe(_engine, _workflow_id, :sleep), do: :ok

  defp unsubscribe(engine, workflow_id, {:recv, topic}),
    do: Notifications.unsubscribe_recv(engine, workflow_id, topic)

  defp unsubscribe(engine, _workflow_id, {:event, target_workflow_id, key}),
    do: Notifications.unsubscribe_event(engine, target_workflow_id, key)

  defp resolved?(_config, _workflow_id, :sleep), do: false

  defp resolved?(config, workflow_id, {:recv, topic}),
    do: SystemDb.notification_pending?(config, workflow_id, topic)

  defp resolved?(config, _workflow_id, {:event, target_workflow_id, key}) do
    match?({:ok, _value}, SystemDb.get_event_value(config, target_workflow_id, key))
  end

  defp wake(engine, workflow_id) do
    case :ets.take(table_name(engine), workflow_id) do
      [{^workflow_id, waiting_for, timer_ref, _deadline_ms}] ->
        :erlang.cancel_timer(timer_ref)
        unsubscribe(engine, workflow_id, waiting_for)
        redispatch(engine, workflow_id)

      [] ->
        :ok
    end
  end

  defp redispatch(engine, workflow_id) do
    config = Dbos.config(engine)

    case SystemDb.get_workflow_status(config, workflow_id) do
      {:ok, %WorkflowStatus{status: :pending} = workflow} ->
        redispatch_pending(engine, workflow)

      {:ok, %WorkflowStatus{}} ->
        :ok

      {:error, :not_found} ->
        :ok
    end
  end

  defp redispatch_pending(engine, %WorkflowStatus{
         name: name,
         inputs: inputs,
         workflow_uuid: workflow_id
       }) do
    case WorkflowRegistry.lookup(engine, name) do
      {:ok, mfa} ->
        WorkflowSup.start_workflow(engine, workflow_id, mfa, inputs, replay: true)

      :error ->
        Logger.warning(
          "dbos: workflow #{inspect(name)} (#{workflow_id}) is not registered on this " <>
            "executor; skipping wake"
        )
    end
  end
end
