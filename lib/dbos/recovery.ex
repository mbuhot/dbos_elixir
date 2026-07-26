defmodule Dbos.Recovery do
  @moduledoc """
  Bringing interrupted workflows back to life.

  `recover_pending/1` re-dispatches this executor's own `PENDING` workflows; the engine runs it
  once at boot, and it is callable directly to force a pass. `reclaim/3` takes over the `PENDING`
  workflows of executors named as dead, for an operator driving failover by hand.
  `await_boot_recovery/1` blocks until the boot pass has finished, which tests need before
  asserting on recovered work.

  Recovery is capability-aware: only workflows whose name this engine has registered are ever
  reassigned. A deployment is normally heterogeneous, so a name this engine doesn't implement is
  left exactly where it is for whichever peer does implement it to claim.

  A recovered workflow replays from its checkpoints. A queued one is handed back to its queue
  rather than re-invoked directly. One workflow failing to redispatch is isolated and logged; the
  pass continues with the rest of the batch.

  ## Declined workflows

  A reclaim pass that drains its batch reports what it left behind: every `PENDING` row still
  owned by a dead executor, grouped by name, the row's `application_version` and a
  `t:decline_reason/0`. Each group logs one warning and emits one `[:dbos, :recovery, :declined]`
  event measuring `%{count: n}`, so a population no live executor can claim is a steady signal
  rather than silence.
  """

  use GenServer

  require Logger

  alias Dbos.Registry
  alias Dbos.SystemDb
  alias Dbos.Telemetry
  alias Dbos.WorkflowSup

  @rescan_passes 5
  @rescan_base_delay_ms 50

  @typedoc """
  Why a reclaim pass left a `PENDING` row where it was: this executor has no workflow registered
  under that name, the row's `application_version` differs from this executor's, or the row was
  claimable and another transaction held it.
  """
  @type decline_reason :: :name_not_registered | :version_mismatch | :locked_elsewhere

  @doc "Starts recovery for the engine named `opts[:name]`, scanning once in a `handle_continue` so `start_link` returns promptly."
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, engine_name, name: process_name(engine_name))
  end

  @doc """
  Synchronously recovers every `PENDING` workflow owned by this executor and application
  version: `reclaim/3` reclaiming its own executor id, with an unbounded batch.
  """
  def recover_pending(engine_name) do
    config = Dbos.config(engine_name)
    reclaim(engine_name, [config.executor_id])
  end

  @doc """
  Reclaims every `PENDING` workflow owned by any of `dead_executor_ids` to this engine's own
  executor and redispatches it. A queued `PENDING` workflow is cleared back to `ENQUEUED`
  instead, so the queue redistributes it, and is not itself redispatched here.

  Every survivor may call this concurrently for the same dead ids: the single `UPDATE` that
  reassigns the batch is the serialization point, so a call that loses the race simply
  redispatches nothing. `opts[:batch_size]` bounds how many non-queued rows one call claims; the
  default is unbounded.

  Returns every workflow id this call actually acted on (queue-cleared or reclaimed-and-redispatched).
  """
  def reclaim(engine_name, dead_executor_ids, opts \\ []) do
    config = Dbos.config(engine_name)
    registered_names = Registry.registered_names(engine_name)
    metadata = %{engine: engine_name, executor_ids: dead_executor_ids}

    Telemetry.span_recovery(metadata, fn ->
      queued_ids = SystemDb.list_queued_pending_workflow_ids(config, dead_executor_ids)
      Enum.each(queued_ids, &clear_one(config, &1))

      {ids, handled, outcome} =
        reclaim_passes(
          engine_name,
          config,
          dead_executor_ids,
          registered_names,
          opts,
          MapSet.new(),
          1
        )

      report_declined(outcome, engine_name, config, dead_executor_ids, registered_names, handled)

      queued_ids ++ ids
    end)
  end

  @doc """
  Blocks until this engine's at-boot recovery pass (scheduled via `handle_continue` so
  `start_link` itself doesn't block) has finished. Tests use this to avoid racing that pass with
  their own freshly started workflows.
  """
  def await_boot_recovery(engine_name) do
    :sys.get_state(process_name(engine_name))
    :ok
  end

  @impl true
  def init(engine_name), do: {:ok, engine_name, {:continue, :recover}}

  @impl true
  def handle_continue(:recover, engine_name) do
    recover_pending(engine_name)
    {:noreply, engine_name}
  end

  defp reclaim_passes(
         engine_name,
         config,
         dead_executor_ids,
         registered_names,
         opts,
         handled,
         pass
       ) do
    reclaimed =
      SystemDb.reclaim_pending_workflows(config, dead_executor_ids, registered_names, opts)

    Enum.each(reclaimed, &recover_one(engine_name, config, &1))

    ids = Enum.map(reclaimed, & &1.workflow_uuid)
    handled = Enum.into(ids, handled)

    cond do
      batch_exhausted?(opts, reclaimed) ->
        {ids, handled, :batch_exhausted}

      rescan?(config, dead_executor_ids, registered_names, handled, pass) ->
        Process.sleep(@rescan_base_delay_ms * Integer.pow(2, pass - 1))

        {rest, handled, outcome} =
          reclaim_passes(
            engine_name,
            config,
            dead_executor_ids,
            registered_names,
            opts,
            handled,
            pass + 1
          )

        {ids ++ rest, handled, outcome}

      true ->
        {ids, handled, :drained}
    end
  end

  defp rescan?(config, dead_executor_ids, registered_names, handled, pass) do
    pass < @rescan_passes and
      Enum.any?(
        SystemDb.list_reclaimable_pending_workflow_ids(
          config,
          dead_executor_ids,
          registered_names
        ),
        &(not MapSet.member?(handled, &1))
      )
  end

  defp batch_exhausted?(opts, reclaimed) do
    case Keyword.get(opts, :batch_size) do
      nil -> false
      batch_size -> length(reclaimed) >= batch_size
    end
  end

  defp report_declined(:batch_exhausted, _engine, _config, _dead_ids, _names, _handled), do: :ok

  defp report_declined(:drained, engine_name, config, dead_executor_ids, names, handled) do
    config
    |> SystemDb.list_unclaimed_pending_workflow_groups(
      dead_executor_ids,
      names,
      MapSet.to_list(handled)
    )
    |> Enum.each(&report_declined_group(engine_name, config, names, &1))
  end

  defp report_declined_group(engine_name, config, registered_names, group) do
    reason = decline_reason(group, registered_names)

    Logger.warning(
      "dbos: leaving #{group.count} PENDING workflow(s) named #{inspect(group.name)} " <>
        "(version #{inspect(group.application_version)}, this executor " <>
        "#{inspect(config.application_version)}) unclaimed: #{reason}; " <>
        "for example #{group.example_workflow_id}"
    )

    Telemetry.declined_reclaim(
      %{
        engine: engine_name,
        name: group.name,
        row_version: group.application_version,
        executor_version: config.application_version,
        reason: reason
      },
      group.count
    )
  end

  defp decline_reason(%{claimable: true}, _registered_names), do: :locked_elsewhere

  defp decline_reason(%{name: name}, registered_names) do
    if name in registered_names, do: :version_mismatch, else: :name_not_registered
  end

  defp clear_one(config, workflow_id) do
    SystemDb.clear_queue_assignment(config, workflow_id)
  rescue
    error ->
      Logger.error(
        "dbos: could not return queued workflow #{workflow_id} to its queue: " <>
          Exception.format_banner(:error, error, __STACKTRACE__)
      )
  end

  defp recover_one(engine_name, config, workflow) do
    case Registry.lookup(engine_name, workflow.name) do
      :error ->
        Logger.warning(
          "dbos: workflow #{inspect(workflow.name)} (#{workflow.workflow_uuid}) is not " <>
            "registered on this executor; skipping recovery"
        )

      {:ok, mfa} ->
        redispatch(engine_name, config, workflow, mfa)
    end
  rescue
    error ->
      Logger.error(
        "dbos: recovery failed for workflow #{workflow.workflow_uuid}; the rest of the batch " <>
          "continues: " <> Exception.format_banner(:error, error, __STACKTRACE__)
      )
  end

  defp redispatch(engine_name, config, workflow, mfa) do
    SystemDb.insert_workflow_status(
      config,
      %{
        workflow_id: workflow.workflow_uuid,
        status: :pending,
        name: workflow.name,
        inputs: workflow.inputs,
        parent_workflow_id: workflow.parent_workflow_id
      },
      increment_attempts: true,
      max_retries: config.max_recovery_attempts
    )

    WorkflowSup.start_workflow(engine_name, workflow.workflow_uuid, mfa, workflow.inputs,
      replay: true
    )
  rescue
    error in Dbos.MaxRecoveryAttemptsExceededError ->
      Logger.warning("dbos: " <> Exception.message(error))
  end

  defp process_name(engine_name), do: Module.concat(engine_name, Recovery)
end
