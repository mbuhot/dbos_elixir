defmodule Dbos.StepInfoTest do
  use ExUnit.Case, async: true

  alias Dbos.StepInfo

  test "columns/0 lists every operation_outputs column in select order" do
    assert StepInfo.columns() == [
             :workflow_uuid,
             :function_id,
             :function_name,
             :output,
             :error,
             :child_workflow_id,
             :started_at_epoch_ms,
             :completed_at_epoch_ms,
             :serialization
           ]
  end

  test "from_row/1 decodes the output" do
    output = Dbos.Serialization.encode(%{charged: true})

    row = ["wf-1", 0, "charge_card/2", output, nil, nil, 1000, 1200, "erl_etf"]

    step = StepInfo.from_row(row)

    assert step.function_id == 0
    assert step.output == %{charged: true}
  end
end
