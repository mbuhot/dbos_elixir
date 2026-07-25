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

  test "a bare workflow call outside a workflow starts a root workflow and returns a handle without awaiting it" do
    _engine = start_engine([CheckoutWorkflow], name: Dbos)

    assert {:ok, %Dbos.WorkflowHandle{} = handle} = CheckoutWorkflow.child_flow("ord_5")
    assert {:ok, "ord_5"} = Dbos.await(handle)
  end

  test "a bare workflow call in a controller-like context outside a workflow returns immediately without blocking for the workflow's runtime" do
    _engine = start_engine([CheckoutWorkflow], name: Dbos)

    {elapsed_us, {:ok, handle}} = :timer.tc(fn -> CheckoutWorkflow.slow_flow("ord_slow") end)

    assert elapsed_us < 1_000_000
    assert {:ok, "ord_slow"} = Dbos.await(handle, timeout_ms: 10_000)
  end

  test "a bare workflow call with no engine started raises Dbos.NotStartedError" do
    :persistent_term.erase({Dbos, :config, Dbos})

    assert_raise Dbos.NotStartedError, fn ->
      CheckoutWorkflow.child_flow("ord_6")
    end
  end

  test "defworkflow supports a default argument" do
    _engine = start_engine([CheckoutWorkflow], name: Dbos)

    {:ok, handle_1} = CheckoutWorkflow.greet()
    assert {:ok, "hello, world"} = Dbos.await(handle_1)

    {:ok, handle_2} = CheckoutWorkflow.greet("Mike")
    assert {:ok, "hello, Mike"} = Dbos.await(handle_2)
  end

  test "a capture of a defworkflow's public function resolves and starts the workflow via Dbos.start" do
    engine = start_engine([CheckoutWorkflow])

    {:ok, handle} =
      Dbos.start(&CheckoutWorkflow.process_order/2, ["ord_cap", 100], engine: engine)

    assert {:ok, %{charge: %{charge_id: "ch_ord_cap"}}} = Dbos.await(handle)
  end

  test "a capture of a defworkflow's public function resolves via Dbos.enqueue" do
    engine = start_engine([CheckoutWorkflow])

    {:ok, handle} =
      Dbos.enqueue(&CheckoutWorkflow.process_order/2, ["ord_cap2", 50],
        queue_name: Dbos.Queue.internal_queue_name(),
        engine: engine
      )

    assert {:ok, %{charge: %{charge_id: "ch_ord_cap2"}}} = Dbos.await(handle)
  end

  test "a capture of a function that is not a registered workflow raises clearly" do
    engine = start_engine([CheckoutWorkflow])

    assert_raise RuntimeError, ~r/not registered/, fn ->
      Dbos.start(&CheckoutWorkflow.reserve_stock/1, ["ord_x"], engine: engine)
    end
  end

  test "a pinned workflow_id passed through the generated options dispatcher makes a repeated start idempotent" do
    _engine = start_engine([CheckoutWorkflow], name: Dbos)

    {:ok, handle_1} =
      CheckoutWorkflow.process_order("ord_pin", 100, workflow_id: "pinned-order")

    {:ok, handle_2} =
      CheckoutWorkflow.process_order("ord_pin", 100, workflow_id: "pinned-order")

    assert handle_1.workflow_id == "pinned-order"
    assert handle_2.workflow_id == "pinned-order"
    assert {:ok, %{charge: %{charge_id: "ch_ord_pin"}}} = Dbos.await(handle_1)
  end

  test "the options dispatcher works alongside a default argument, requiring the argument explicitly" do
    _engine = start_engine([CheckoutWorkflow], name: Dbos)

    {:ok, handle} = CheckoutWorkflow.greet("Mike", workflow_id: "greet-pinned")

    assert handle.workflow_id == "greet-pinned"
    assert {:ok, "hello, Mike"} = Dbos.await(handle)
  end

  test "a pinned workflow_id for a child call inside a workflow is honored" do
    engine = start_engine([CheckoutWorkflow])

    {:ok, handle} =
      Dbos.start("parent_flow_with_opts", ["ord_child_pin", "pinned-child"], engine: engine)

    assert {:ok, "ord_child_pin"} = Dbos.await(handle)

    {:ok, child_status} = Dbos.SystemDb.get_workflow_status(Dbos.config(engine), "pinned-child")
    assert child_status.status == :success
    assert child_status.output == "ord_child_pin"
  end

  test "an opts-dispatcher arity that would collide with another declared defworkflow is a compile error" do
    module =
      Module.concat(__MODULE__, :"AmbiguousArityFixture#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module)} do
      use Dbos

      defworkflow ambiguous(a), name: "ambiguous_a" do
        a
      end

      defworkflow ambiguous(a, b), name: "ambiguous_ab" do
        {a, b}
      end
    end
    """

    error =
      assert_raise CompileError, fn ->
        Code.compile_string(source, "test/fixture.ex")
      end

    assert error.description =~ "ambiguous/1"
    assert error.description =~ "ambiguous/2"
    assert error.description =~ "collides"
  end
end
