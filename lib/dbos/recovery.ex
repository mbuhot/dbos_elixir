defmodule Dbos.Recovery do
  @moduledoc """
  Re-dispatches this executor's `PENDING` workflows on engine start, per `notes/recovery.md` §1.
  A queued `PENDING` workflow is handed back to its queue rather than re-invoked directly, since
  Phase 3 owns dequeueing. An unregistered workflow name is logged and skipped; the pass
  continues with the rest of the batch.
  """

  use GenServer

  require Logger

  alias Dbos.Registry
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  @doc "Starts recovery for the engine named `opts[:name]`, scanning once in a `handle_continue` so `start_link` returns promptly."
  def start_link(opts) do
    engine_name = Keyword.fetch!(opts, :name)
    GenServer.start_link(__MODULE__, engine_name, name: process_name(engine_name))
  end

  @doc "Synchronously recovers every `PENDING` workflow owned by this executor and application version. The entry point tests call directly."
  def recover_pending(engine_name) do
    config = Dbos.config(engine_name)

    filters =
      [status: :pending, executor_id: config.executor_id] ++
        application_version_filter(config)

    {:ok, workflows} = SystemDb.list_workflows(config, filters)
    Enum.each(workflows, &recover_one(engine_name, config, &1))
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

  defp application_version_filter(%{application_version: nil}), do: []

  defp application_version_filter(%{application_version: version}),
    do: [application_version: version]

  defp recover_one(_engine_name, config, %{queue_name: queue_name, workflow_uuid: workflow_id})
       when is_binary(queue_name) do
    SystemDb.clear_queue_assignment(config, workflow_id)
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
