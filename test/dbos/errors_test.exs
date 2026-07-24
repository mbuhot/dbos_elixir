defmodule Dbos.ErrorsTest do
  use ExUnit.Case, async: true

  test "WorkflowCancelledError names the workflow" do
    error = %Dbos.WorkflowCancelledError{workflow_id: "wf-1"}
    assert Exception.message(error) =~ "wf-1"
    assert Exception.message(error) =~ "cancelled"
  end

  test "UnexpectedStepError names the workflow, step, and both step names" do
    error = %Dbos.UnexpectedStepError{
      workflow_id: "wf-1",
      function_id: 2,
      expected: "charge_card/2",
      recorded: "reserve_stock/1"
    }

    message = Exception.message(error)
    assert message =~ "wf-1"
    assert message =~ "charge_card/2"
    assert message =~ "reserve_stock/1"
  end

  test "NonExistentWorkflowError names the workflow" do
    error = %Dbos.NonExistentWorkflowError{workflow_id: "wf-1"}
    assert Exception.message(error) =~ "wf-1"
  end

  test "MaxRecoveryAttemptsExceededError names the workflow and attempts" do
    error = %Dbos.MaxRecoveryAttemptsExceededError{workflow_id: "wf-1", attempts: 5}
    message = Exception.message(error)
    assert message =~ "wf-1"
    assert message =~ "5"
  end

  test "NotStartedError explains what to do" do
    assert Exception.message(%Dbos.NotStartedError{}) =~ "start"
  end

  test "NotInWorkflowError explains what to do" do
    assert Exception.message(%Dbos.NotInWorkflowError{}) =~ "workflow"
  end
end
