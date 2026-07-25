defmodule Dbos.Recovery do
  @moduledoc """
  Re-dispatches `PENDING` workflows on engine start, and reclaims a dead executor's `PENDING`
  workflows on demand. A queued `PENDING` workflow is handed back to its queue rather than
  re-invoked directly. An unregistered workflow name is logged and skipped; the pass continues
  with the rest of the batch.
  """

  use GenServer

  require Logger

  alias Dbos.Registry
  alias Dbos.SystemDb
  alias Dbos.Telemetry
  alias Dbos.WorkflowSup

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
      Enum.each(queued_ids, &SystemDb.clear_queue_assignment(config, &1))

      reclaimed = SystemDb.reclaim_pending_workflows(config, dead_executor_ids, opts)
      Enum.each(reclaimed, &recover_one(engine_name, config, &1))

      queued_ids ++ Enum.map(reclaimed, & &1.workflow_uuid)
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
