defmodule Dbos.Recovery do
  @moduledoc """
  Re-dispatches `PENDING` workflows on engine start, and reclaims a dead executor's `PENDING`
  workflows on demand. A queued `PENDING` workflow is handed back to its queue rather than
  re-invoked directly. An unregistered workflow name is logged and skipped; the pass continues
  with the rest of the batch. A workflow whose redispatch itself fails is isolated the same way:
  reclaiming moves the whole batch to this executor in one statement, so a failure that escaped
  would leave every workflow behind it owned by a live executor and invisible to any later
  recovery pass.

  Reclaiming takes `FOR UPDATE SKIP LOCKED`, so a row whose lock is still held — by a peer
  executor, or by a backend that has not finished being torn down after its client was killed —
  is passed over. A pass that skipped a row it has not already handled re-scans, on a short
  bounded backoff, so a lock released milliseconds later still gets the workflow recovered.
  """

  use GenServer

  require Logger

  alias Dbos.Registry
  alias Dbos.SystemDb
  alias Dbos.Telemetry
  alias Dbos.WorkflowSup

  @rescan_passes 5
  @rescan_base_delay_ms 50

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

  Every survivor may call this concurrently for the same dead ids: the reassigning `UPDATE`
  inside `Dbos.SystemDb.reclaim_pending_workflows/3` is the serialization point, so a call that
  loses the race simply redispatches nothing. `opts[:batch_size]` bounds how many non-queued rows
  one call claims; the default is unbounded. Callers driven by cluster membership
  (`Dbos.Cluster.NodeWatcher`, `Dbos.Cluster.OrphanSweep`) pass an explicit
  `config.reclaim_batch_size` instead.

  Returns every workflow id this call actually acted on (queue-cleared or reclaimed-and-redispatched).
  """
  def reclaim(engine_name, dead_executor_ids, opts \\ []) do
    config = Dbos.config(engine_name)
    metadata = %{engine: engine_name, executor_ids: dead_executor_ids}

    Telemetry.span_recovery(metadata, fn ->
      queued_ids = SystemDb.list_queued_pending_workflow_ids(config, dead_executor_ids)
      Enum.each(queued_ids, &clear_one(config, &1))

      queued_ids ++
        reclaim_passes(engine_name, config, dead_executor_ids, opts, MapSet.new(), 1)
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

  defp reclaim_passes(engine_name, config, dead_executor_ids, opts, handled, pass) do
    reclaimed = SystemDb.reclaim_pending_workflows(config, dead_executor_ids, opts)
    Enum.each(reclaimed, &recover_one(engine_name, config, &1))

    ids = Enum.map(reclaimed, & &1.workflow_uuid)
    handled = Enum.into(ids, handled)

    if rescan?(config, dead_executor_ids, opts, handled, reclaimed, pass) do
      Process.sleep(@rescan_base_delay_ms * Integer.pow(2, pass - 1))
      ids ++ reclaim_passes(engine_name, config, dead_executor_ids, opts, handled, pass + 1)
    else
      ids
    end
  end

  defp rescan?(config, dead_executor_ids, opts, handled, reclaimed, pass) do
    pass < @rescan_passes and not batch_exhausted?(opts, reclaimed) and
      Enum.any?(
        SystemDb.list_reclaimable_pending_workflow_ids(config, dead_executor_ids),
        &(not MapSet.member?(handled, &1))
      )
  end

  defp batch_exhausted?(opts, reclaimed) do
    case Keyword.get(opts, :batch_size) do
      nil -> false
      batch_size -> length(reclaimed) >= batch_size
    end
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
