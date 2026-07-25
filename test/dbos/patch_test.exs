defmodule Dbos.PatchTest do
  use Dbos.Case, async: false

  alias Dbos.Runtime
  alias Dbos.SystemDb

  setup %{conn: conn} do
    config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}
    {:ok, config: config}
  end

  defp start_workflow(config, workflow_id) do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "TestWorkflow"
    })

    workflow_id
  end

  test "a workflow reaching a patch for the first time takes it, records the checkpoint, and consumes exactly one id",
       %{config: config} do
    workflow_id = start_workflow(config, "wf-patch-first")

    result =
      Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
        Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end)
        patched? = Dbos.patch("fraud-check")
        Runtime.run_step("ship_order/1", [], fn -> %{shipped: true} end)
        patched?
      end)

    assert result == true

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

    assert Enum.map(steps, &{&1.function_id, &1.function_name}) == [
             {0, "reserve_stock/1"},
             {1, "DBOS.patch-fraud-check"},
             {2, "ship_order/1"}
           ]
  end

  test "replaying a taken patch returns the recorded decision and consumes ids identically", %{
    config: config
  } do
    workflow_id = start_workflow(config, "wf-patch-replay")

    Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
      Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end)
      Dbos.patch("fraud-check")
      Runtime.run_step("ship_order/1", [], fn -> %{shipped: true} end)
    end)

    result =
      Runtime.with_context([config: config, workflow_id: workflow_id, replay: true], fn ->
        Runtime.run_step("reserve_stock/1", [], fn -> raise "must not run" end)
        patched? = Dbos.patch("fraud-check")
        Runtime.run_step("ship_order/1", [], fn -> raise "must not run" end)
        patched?
      end)

    assert result == true

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

    assert Enum.map(steps, &{&1.function_id, &1.function_name}) == [
             {0, "reserve_stock/1"},
             {1, "DBOS.patch-fraud-check"},
             {2, "ship_order/1"}
           ]
  end

  test "a workflow that ran to ship_order under code without a patch replays under code with the patch inserted, sees false, skips the new step, and completes with its original sequence intact",
       %{config: config} do
    workflow_id = start_workflow(config, "wf-patch-upgrade")

    Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
      Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end)
      Runtime.run_step("ship_order/1", [], fn -> %{shipped: true} end)
    end)

    result =
      Runtime.with_context([config: config, workflow_id: workflow_id, replay: true], fn ->
        Runtime.run_step("reserve_stock/1", [], fn -> raise "must not run" end)
        patched? = Dbos.patch("fraud-check")

        if patched? do
          Runtime.run_step("fraud_check/1", [], fn ->
            raise "must not run: new code must be skipped for this old execution"
          end)
        end

        shipped = Runtime.run_step("ship_order/1", [], fn -> raise "must not run" end)
        {patched?, shipped}
      end)

    assert result == {false, %{shipped: true}}

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

    assert Enum.map(steps, &{&1.function_id, &1.function_name}) == [
             {0, "reserve_stock/1"},
             {1, "ship_order/1"}
           ]
  end

  test "a patch inside a branch that is not taken consumes no id", %{config: config} do
    workflow_id = start_workflow(config, "wf-patch-branch-skip")

    Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
      Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end)

      if false do
        Dbos.patch("unreachable")
      end

      Runtime.run_step("ship_order/1", [], fn -> %{shipped: true} end)
    end)

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

    assert Enum.map(steps, &{&1.function_id, &1.function_name}) == [
             {0, "reserve_stock/1"},
             {1, "ship_order/1"}
           ]
  end

  test "outside a workflow, Dbos.patch raises Dbos.NotInWorkflowError" do
    assert_raise Dbos.NotInWorkflowError, fn ->
      Dbos.patch("fraud-check")
    end
  end

  test "the exact (function_id, function_name) sequence for a workflow using a patch alongside plain steps",
       %{config: config} do
    workflow_id = start_workflow(config, "wf-patch-sequence")

    Runtime.with_context([config: config, workflow_id: workflow_id], fn ->
      Runtime.run_step("reserve_stock/1", [], fn -> %{reserved: true} end)
      Runtime.run_step("charge_card/1", [], fn -> %{charged: true} end)
      patched? = Dbos.patch("fraud-check")

      if patched? do
        Runtime.run_step("fraud_check/1", [], fn -> %{fraud_checked: true} end)
      end

      Runtime.run_step("ship_order/1", [], fn -> %{shipped: true} end)
    end)

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

    assert Enum.map(steps, &{&1.function_id, &1.function_name}) == [
             {0, "reserve_stock/1"},
             {1, "charge_card/1"},
             {2, "DBOS.patch-fraud-check"},
             {3, "fraud_check/1"},
             {4, "ship_order/1"}
           ]
  end
end
