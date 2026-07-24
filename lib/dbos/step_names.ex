defmodule Dbos.StepNames do
  @moduledoc """
  The reserved `function_name` strings upstream writes for its own built-in operations
  (`notes/step-ids.md` §3), verbatim, so no call site spells one by hand.
  """

  @doc "The `function_name` for `RunAsStep`-wrapped calls to a handle's `GetStatus`."
  def get_status, do: "DBOS.getStatus"

  @doc "The `function_name` for awaiting a workflow's result from within another workflow."
  def get_result, do: "DBOS.getResult"

  @doc "The `function_name` for enqueueing a workflow from within a workflow."
  def enqueue, do: "DBOS.enqueue"

  @doc "The `function_name` for a `Select` step."
  def select, do: "DBOS.select"

  @doc "The `function_name` for `Send` calls made from within a workflow."
  def send_message, do: "DBOS.send"

  @doc "The `function_name` for the `Recv` step."
  def recv, do: "DBOS.recv"

  @doc "The `function_name` shared by the standalone `Sleep` step and the internal recv/getEvent timeout step."
  def sleep, do: "DBOS.sleep"

  @doc "The `function_name` for `SetEvent`."
  def set_event, do: "DBOS.setEvent"

  @doc "The `function_name` for `GetEvent`."
  def get_event, do: "DBOS.getEvent"

  @doc "The `function_name` for `WriteStream`."
  def write_stream, do: "DBOS.writeStream"

  @doc "The `function_name` for `CloseStream`."
  def close_stream, do: "DBOS.closeStream"

  @doc "The `function_name` for retrieving a workflow handle from within a workflow."
  def retrieve_workflow, do: "DBOS.retrieveWorkflow"

  @doc "The `function_name` for cancelling a workflow from within a workflow."
  def cancel_workflow, do: "DBOS.cancelWorkflow"

  @doc "The `function_name` for updating a workflow's attributes from within a workflow."
  def update_workflow_attributes, do: "DBOS.updateWorkflowAttributes"

  @doc "The `function_name` for a bulk cancel from within a workflow."
  def cancel_workflows, do: "DBOS.cancelWorkflows"

  @doc "The `function_name` for setting a workflow's delay from within a workflow."
  def set_workflow_delay, do: "DBOS.setWorkflowDelay"

  @doc "The `function_name` for deleting workflows from within a workflow."
  def delete_workflows, do: "DBOS.deleteWorkflows"

  @doc "The `function_name` for resuming a workflow from within a workflow."
  def resume_workflow, do: "DBOS.resumeWorkflow"

  @doc "The `function_name` for forking a workflow from within a workflow."
  def fork_workflow, do: "DBOS.forkWorkflow"

  @doc "The `function_name` for listing workflows from within a workflow."
  def list_workflows, do: "DBOS.listWorkflows"

  @doc "The `function_name` for reading a workflow's steps from within a workflow."
  def get_workflow_steps, do: "DBOS.getWorkflowSteps"

  @doc "The `function_name` for reading workflow aggregates from within a workflow."
  def get_workflow_aggregates, do: "DBOS.getWorkflowAggregates"

  @doc "The `function_name` for reading step aggregates from within a workflow."
  def get_step_aggregates, do: "DBOS.getStepAggregates"

  @doc "The `function_name` for creating a schedule from within a workflow."
  def create_schedule, do: "DBOS.createSchedule"

  @doc "The `function_name` for pausing a schedule from within a workflow."
  def pause_schedule, do: "DBOS.pauseSchedule"

  @doc "The `function_name` for resuming a schedule from within a workflow."
  def resume_schedule, do: "DBOS.resumeSchedule"

  @doc "The `function_name` for deleting a schedule from within a workflow."
  def delete_schedule, do: "DBOS.deleteSchedule"

  @doc "The `function_name` for reading a schedule from within a workflow."
  def get_schedule, do: "DBOS.getSchedule"

  @doc "The `function_name` for listing schedules from within a workflow."
  def list_schedules, do: "DBOS.listSchedules"

  @doc "The `function_name` for a `Patch`/`DeprecatePatch` checkpoint at the given patch name."
  def patch(patch_name), do: "DBOS.patch-" <> patch_name
end
