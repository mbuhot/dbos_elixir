defmodule Dbos.MacrosTest do
  use Dbos.Case, async: false

  alias Dbos.CheckoutWorkflow
  alias Dbos.Queue
  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  defp children_queue, do: %Queue{name: "children", base_polling_interval_ms: 20}

  defp wait_until(fun, attempts \\ 300)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  defp count_workflows_named(config, name) do
    {:ok, %{rows: [[count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT count(*) FROM dbos.workflow_status WHERE name = $1",
        [name]
      )

    count
  end

  defp step_layout(config, workflow_id) do
    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)
    Enum.map(steps, &{&1.function_id, &1.function_name})
  end

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
    unless opts[:testing], do: Recovery.await_boot_recovery(engine_name)
    engine_name
  end

  test "a module using the macros compiles, registers, and runs end to end" do
    engine = start_engine([CheckoutWorkflow])
    config = Dbos.config(engine)

    {:ok, handle} =
      Dbos.start("Dbos.CheckoutWorkflow.process_order", ["ord_1", 4999], engine: engine)

    assert {:ok, %{charge: %{charge_id: "ch_ord_1"}, receipt: {"ord_1", "ch_ord_1"}}} =
             Dbos.await(handle)

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_id) == [0, 1]
    assert Enum.map(steps, & &1.function_name) == ["charge_card/2", "record_receipt/2"]
  end

  test "step names exclude the module, and a name: override works" do
    engine = start_engine([CheckoutWorkflow])
    config = Dbos.config(engine)

    {:ok, handle} =
      Dbos.start("Dbos.CheckoutWorkflow.process_order", ["ord_2", 100], engine: engine)

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

    {:ok, handle} = Dbos.start("Dbos.CheckoutWorkflow.parent_flow", ["ord_4"], engine: engine)
    assert {:ok, "ord_4"} = Dbos.await(handle)

    expected_child_id = "#{handle.workflow_id}-0"
    {:ok, [start_step, get_result_step]} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert start_step.child_workflow_id == expected_child_id
    assert start_step.function_name == "Dbos.CheckoutWorkflow.child_flow"

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
      Dbos.start("Dbos.CheckoutWorkflow.parent_flow_with_opts", ["ord_child_pin", "pinned-child"],
        engine: engine
      )

    assert {:ok, "ord_child_pin"} = Dbos.await(handle)

    {:ok, child_status} = Dbos.SystemDb.get_workflow_status(Dbos.config(engine), "pinned-child")
    assert child_status.status == :success
    assert child_status.output == "ord_child_pin"
  end

  test "a workflow call naming a queue waits its turn on that queue instead of running straight away" do
    engine = start_engine([CheckoutWorkflow], queues: [children_queue()])
    config = Dbos.config(engine)

    {:ok, handle} =
      CheckoutWorkflow.child_flow("ord_queued", queue_name: "children", engine: engine)

    assert {:ok, "ord_queued"} = Dbos.await(handle, timeout_ms: 10_000)

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.queue_name == "children"
  end

  test "a workflow call naming no queue runs straight away" do
    engine = start_engine([CheckoutWorkflow], queues: [children_queue()])
    config = Dbos.config(engine)

    {:ok, handle} = CheckoutWorkflow.child_flow("ord_direct", engine: engine)
    assert {:ok, "ord_direct"} = Dbos.await(handle)

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.queue_name == nil
  end

  test "a workflow call asking for a delay is held back until the delay elapses" do
    engine =
      start_engine([CheckoutWorkflow], queues: [children_queue()], testing: :manual)

    config = Dbos.config(engine)

    {:ok, handle} =
      CheckoutWorkflow.child_flow("ord_delayed",
        queue_name: "children",
        delay_ms: 60_000,
        engine: engine
      )

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :delayed
  end

  test "a misspelled option on a workflow call is refused, naming the option" do
    error =
      assert_raise Dbos.InvalidWorkflowOptionError, fn ->
        CheckoutWorkflow.child_flow("ord_typo", queue: "children")
      end

    message = Exception.message(error)
    assert message =~ "Dbos.CheckoutWorkflow.child_flow"
    assert message =~ ":queue"
    assert message =~ ":queue_name"
  end

  test "a delay with no queue to hold it is refused" do
    error =
      assert_raise Dbos.InvalidWorkflowOptionError, fn ->
        CheckoutWorkflow.child_flow("ord_undelayable", delay_ms: 60_000)
      end

    message = Exception.message(error)
    assert message =~ ":delay_ms"
    assert message =~ "queue_name"
  end

  test "a partition key alongside a deduplication id is refused" do
    error =
      assert_raise Dbos.InvalidWorkflowOptionError, fn ->
        CheckoutWorkflow.child_flow("ord_both",
          queue_name: "children",
          deduplication_id: "dedup",
          partition_key: "tenant-a"
        )
      end

    message = Exception.message(error)
    assert message =~ ":partition_key"
    assert message =~ ":deduplication_id"
  end

  test "asking Dbos.start for a queue is refused, pointing at Dbos.enqueue" do
    error =
      assert_raise Dbos.InvalidWorkflowOptionError, fn ->
        Dbos.start("Dbos.CheckoutWorkflow.child_flow", ["ord_start_queue"],
          queue_name: "children"
        )
      end

    message = Exception.message(error)
    assert message =~ ":queue_name"
    assert message =~ "Dbos.enqueue/3"
  end

  test "a queued workflow call inside a workflow runs the child on the queue and returns its result" do
    engine = start_engine([CheckoutWorkflow], queues: [children_queue()])
    config = Dbos.config(engine)

    {:ok, handle} =
      Dbos.start("Dbos.CheckoutWorkflow.parent_flow_queueing_child", ["ord_qchild"],
        engine: engine
      )

    assert {:ok, %{charge_id: "ch_ord_qchild"}} = Dbos.await(handle, timeout_ms: 10_000)

    assert step_layout(config, handle.workflow_id) == [
             {0, "DBOS.enqueue"},
             {1, "DBOS.getResult"},
             {2, "charge_card/2"}
           ]

    assert count_workflows_named(config, "Dbos.CheckoutWorkflow.child_flow") == 1
  end

  test "a workflow that queues a child and crashes before completing recovers to exactly one child" do
    engine =
      start_engine(
        [
          CheckoutWorkflow,
          {"queue_child_and_die/2", {SampleWorkflows, :queue_child_and_die, 2}}
        ],
        queues: [children_queue()]
      )

    config = Dbos.config(engine)
    table = :"macros_queue_child_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])
    parent_id = "wf-queue-child-dies-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(config, %{
      workflow_id: parent_id,
      status: :pending,
      name: "queue_child_and_die/2",
      inputs: [table, "ord_replayed"]
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(
        engine,
        parent_id,
        {SampleWorkflows, :queue_child_and_die, 2},
        [table, "ord_replayed"]
      )

    wait_until(fn -> WorkflowSup.whereis(engine, parent_id) == :error end)

    Recovery.recover_pending(engine)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, parent_id)
      status.status == :success
    end)

    {:ok, status} = SystemDb.get_workflow_status(config, parent_id)
    assert status.output == "ord_replayed"
    assert count_workflows_named(config, "Dbos.CheckoutWorkflow.child_flow") == 1

    assert step_layout(config, parent_id) == [{0, "DBOS.enqueue"}, {1, "DBOS.getResult"}]
  end

  test "a @doc above defworkflow, defstep, and deftransaction attaches to the generated public function" do
    module = Module.concat(__MODULE__, :"DocFixture#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module)} do
      use Dbos

      @doc "Processes the order end to end."
      defworkflow doc_flow(id), name: "doc_flow" do
        id
      end

      @doc "Charges the card for the order."
      defstep doc_step(id) do
        id
      end

      @doc "Records the transaction in the ledger."
      deftransaction doc_transaction(id) do
        id
      end
    end
    """

    Code.compiler_options(docs: true)
    compiled = Code.compile_string(source, "test/fixture.ex")
    {^module, binary} = List.keyfind(compiled, module, 0)
    beam_path = Path.join(System.tmp_dir!(), "#{module}.beam")
    File.write!(beam_path, binary)

    {:docs_v1, _, :elixir, _, _, _, function_docs} = Code.fetch_docs(beam_path)

    doc_for = fn name, arity ->
      Enum.find_value(function_docs, fn
        {{:function, ^name, ^arity}, _, _, doc, _} -> doc
        _ -> nil
      end)
    end

    assert %{"en" => "Processes the order end to end."} = doc_for.(:doc_flow, 1)
    assert %{"en" => "Charges the card for the order."} = doc_for.(:doc_step, 1)
    assert %{"en" => "Records the transaction in the ledger."} = doc_for.(:doc_transaction, 1)
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
