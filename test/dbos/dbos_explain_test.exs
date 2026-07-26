defmodule Mix.Tasks.Dbos.ExplainTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  test "prints the step sequence for a straight-line workflow" do
    output =
      capture_io(fn ->
        Mix.Tasks.Dbos.Explain.run(["Dbos.CheckoutWorkflow.process_order/2"])
      end)

    assert output =~ ~s(workflow "Dbos.CheckoutWorkflow.process_order")
    assert output =~ "id 0: step charge_card/2"
    assert output =~ "id 1: step record_receipt/2"
  end

  test "flags a case whose branches allocate ids unevenly" do
    output =
      capture_io(fn ->
        Mix.Tasks.Dbos.Explain.run(["Dbos.CheckoutWorkflow.uneven_branches/2"])
      end)

    assert output =~ "UNEVEN ID ALLOCATION ACROSS BRANCHES"
    assert output =~ "branch [:approve]: consumes 2 id(s)"
    assert output =~ "branch [_]: consumes 1 id(s)"
  end

  test "does not flag a case whose branches allocate ids evenly" do
    output =
      capture_io(fn ->
        Mix.Tasks.Dbos.Explain.run(["Dbos.CheckoutWorkflow.even_branches/2"])
      end)

    refute output =~ "UNEVEN"
    assert output =~ "branch [:approve]: consumes 1 id(s)"
    assert output =~ "branch [_]: consumes 1 id(s)"
  end

  test "a bare workflow call is reported as a child workflow step" do
    output =
      capture_io(fn ->
        Mix.Tasks.Dbos.Explain.run(["Dbos.CheckoutWorkflow.parent_flow/1"])
      end)

    assert output =~ "child workflow child_flow/1"
    assert output =~ ~s("Dbos.CheckoutWorkflow.child_flow")
  end

  test "Dbos.enqueue, Dbos.fork, and Dbos.status are each reported as consuming one id" do
    output =
      capture_io(fn ->
        Mix.Tasks.Dbos.Explain.run(["Dbos.CheckoutWorkflow.inspects_other_workflow/2"])
      end)

    assert output =~ "id 0: Dbos.enqueue"
    assert output =~ "id 1: Dbos.fork (DBOS.forkWorkflow)"
    assert output =~ "id 2: Dbos.status (DBOS.getStatus)"
  end

  test "Dbos.cancel and Dbos.resume are each reported as consuming one id" do
    output =
      capture_io(fn ->
        Mix.Tasks.Dbos.Explain.run(["Dbos.CheckoutWorkflow.cancels_and_resumes_other_workflow/1"])
      end)

    assert output =~ "id 0: Dbos.cancel (DBOS.cancelWorkflow)"
    assert output =~ "id 1: Dbos.resume (DBOS.resumeWorkflow)"
  end

  test "Dbos.retry is reported as consuming one id" do
    output =
      capture_io(fn ->
        Mix.Tasks.Dbos.Explain.run(["Dbos.CheckoutWorkflow.retries_other_workflow/1"])
      end)

    assert output =~ "id 0: Dbos.retry (DBOS.retryWorkflow)"
  end

  test "Dbos.patch is reported as consuming 0 or 1 id, conditionally, and analysis stops there" do
    output =
      capture_io(fn ->
        Mix.Tasks.Dbos.Explain.run(["Dbos.CheckoutWorkflow.uses_a_patch/1"])
      end)

    assert output =~ "id 1: Dbos.patch(\"fraud-check\") consumes 0 or 1 id, CONDITIONAL"
    refute output =~ "CANNOT BE STATICALLY DETERMINED"
  end

  test "an unrecognized target raises with a usage message" do
    assert_raise Mix.Error, ~r/could not load|does not `use Dbos`|has no defworkflow/, fn ->
      capture_io(fn ->
        Mix.Tasks.Dbos.Explain.run(["Dbos.CheckoutWorkflow.no_such_workflow/1"])
      end)
    end
  end

  test "an invalid target format raises explaining the expected shape" do
    assert_raise Mix.Error, ~r/MODULE.function\/arity/, fn ->
      capture_io(fn -> Mix.Tasks.Dbos.Explain.run(["not-a-valid-target"]) end)
    end
  end
end
