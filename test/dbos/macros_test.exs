defmodule Dbos.MacrosTest do
  use Dbos.Case, async: false

  alias Dbos.CheckoutWorkflow
  alias Dbos.SystemDb

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      Keyword.merge(
        [
          name: name,
          db: {Dbos.DB.Postgrex, Dbos.TestConn},
          executor_id: "exec-#{System.unique_integer([:positive])}",
          workflows: workflows,
          migrations: :skip
        ],
        extra_opts
      )

    engine_name = Keyword.fetch!(opts, :name)
    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(engine_name)
    engine_name
  end

  test "a module using the macros compiles, registers, and runs end to end" do
    engine = start_engine([CheckoutWorkflow])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("process_order", ["ord_1", 4999], engine: engine)

    assert {:ok, %{charge: %{charge_id: "ch_ord_1"}, receipt: {"ord_1", "ch_ord_1"}}} =
             Dbos.await(handle)

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_id) == [0, 1]
    assert Enum.map(steps, & &1.function_name) == ["charge_card/2", "record_receipt/2"]
  end

  test "step names exclude the module, and a name: override works" do
    engine = start_engine([CheckoutWorkflow])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("process_order", ["ord_2", 100], engine: engine)
    assert {:ok, _} = Dbos.await(handle)

    {:ok, [charge_step, _receipt_step]} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert charge_step.function_name == "charge_card/2"
    refute charge_step.function_name =~ "CheckoutWorkflow"
  end

  test "defstep's name: option overrides the default step name" do
    _engine = start_engine([CheckoutWorkflow])

    assert CheckoutWorkflow.reserve_stock("ord_3") == "ord_3"
  end

  test "a bare workflow call inside a workflow becomes a child workflow with a derived id" do
    engine = start_engine([CheckoutWorkflow])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("parent_flow", ["ord_4"], engine: engine)
    assert {:ok, "ord_4"} = Dbos.await(handle)

    expected_child_id = "#{handle.workflow_id}-0"
    {:ok, [start_step, get_result_step]} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert start_step.child_workflow_id == expected_child_id
    assert start_step.function_name == "child_flow"

    assert get_result_step.function_name == "DBOS.getResult"
    assert get_result_step.child_workflow_id == expected_child_id

    {:ok, child_status} = SystemDb.get_workflow_status(config, expected_child_id)
    assert child_status.status == :success
    assert child_status.output == "ord_4"
  end

  test "a bare workflow call outside a workflow starts and awaits a root workflow" do
    _engine = start_engine([CheckoutWorkflow], name: Dbos)

    assert CheckoutWorkflow.child_flow("ord_5") == "ord_5"
  end

  test "a bare workflow call with no engine started raises Dbos.NotStartedError" do
    :persistent_term.erase({Dbos, :config, Dbos})

    assert_raise Dbos.NotStartedError, fn ->
      CheckoutWorkflow.child_flow("ord_6")
    end
  end

  test "defworkflow supports a default argument" do
    _engine = start_engine([CheckoutWorkflow], name: Dbos)

    assert CheckoutWorkflow.greet() == "hello, world"
    assert CheckoutWorkflow.greet("Mike") == "hello, Mike"
  end
end
