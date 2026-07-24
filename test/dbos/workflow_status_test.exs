defmodule Dbos.WorkflowStatusTest do
  use ExUnit.Case, async: true

  alias Dbos.WorkflowStatus

  test "columns/0 lists every workflow_status column in select order" do
    assert length(WorkflowStatus.columns()) == 37
    assert List.first(WorkflowStatus.columns()) == :workflow_uuid
    assert List.last(WorkflowStatus.columns()) == :is_debounced
  end

  test "from_row/1 builds a struct with the status as an atom and inputs decoded" do
    inputs = Dbos.Serialization.encode(["ord_1", 4999])

    row =
      WorkflowStatus.columns()
      |> Enum.map(fn
        :workflow_uuid -> "wf-1"
        :status -> "ENQUEUED"
        :inputs -> inputs
        :priority -> 0
        :recovery_attempts -> 0
        :was_forked_from -> false
        :rate_limited -> false
        :is_debounced -> false
        _ -> nil
      end)

    status = WorkflowStatus.from_row(row)

    assert status.workflow_uuid == "wf-1"
    assert status.status == :enqueued
    assert status.inputs == ["ord_1", 4999]
  end
end
