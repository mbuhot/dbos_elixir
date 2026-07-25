defmodule Dbos.QueueAcceptanceTest do
  use Dbos.Case, async: false

  alias Dbos.Queue
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

  defp start_engine(workflows, extra_opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        workflows: workflows,
        migrations: :skip
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp new_table(hold_ms) do
    table = :"queue_acceptance_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])
    :ets.insert(table, {:hold_ms, hold_ms})
    table
  end

  defp wait_until(fun, attempts \\ 200)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  defp success_count(config, prefix) do
    {:ok, %{rows: [[count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT count(*) FROM dbos.workflow_status WHERE status = 'SUCCESS' AND workflow_uuid LIKE $1",
        ["#{prefix}%"]
      )

    count
  end

  defp duplicate_operation_output_count(config) do
    {:ok, %{rows: [[count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        """
        SELECT count(*) FROM (
          SELECT workflow_uuid, function_id FROM dbos.operation_outputs
          GROUP BY workflow_uuid, function_id HAVING count(*) > 1
        ) duplicates
        """,
        []
      )

    count
  end

  test "1. N enqueued workflows, one runner: each executes exactly once, no duplicate steps" do
    engine =
      start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}],
        queues: [Queue.new("orders", base_polling_interval_ms: 10)]
      )

    config = Dbos.config(engine)
    prefix = "wf-single-#{System.unique_integer([:positive])}-"

    handles =
      for i <- 1..20 do
        {:ok, handle} =
          Dbos.enqueue("three_steps/1", ["ord_#{i}"],
            queue_name: "orders",
            engine: engine,
            workflow_id: "#{prefix}#{i}"
          )

        handle
      end

    Enum.each(handles, fn handle ->
      assert {:ok, %{shipped: _}} = Dbos.await(handle, timeout_ms: 5_000)
    end)

    assert success_count(config, prefix) == 20
    assert duplicate_operation_output_count(config) == 0
  end

  test "2. two engines, one queue, one database: exactly N executions, zero duplicates" do
    workflows = [{"three_steps/1", {SampleWorkflows, :three_steps, 1}}]
    queues = [Queue.new("orders", base_polling_interval_ms: 10)]

    engine_a = start_engine(workflows, queues: queues)
    _engine_b = start_engine(workflows, queues: queues)
    config = Dbos.config(engine_a)

    prefix = "wf-comp-#{System.unique_integer([:positive])}-"

    for i <- 1..50 do
      Dbos.enqueue("three_steps/1", ["o"],
        queue_name: "orders",
        engine: engine_a,
        workflow_id: "#{prefix}#{i}"
      )
    end

    wait_until(fn -> success_count(config, prefix) == 50 end, 500)

    assert success_count(config, prefix) == 50
    assert duplicate_operation_output_count(config) == 0
  end

  test "3. worker_concurrency caps simultaneous execution for one engine" do
    engine =
      start_engine([{"instrumented/1", {SampleWorkflows, :instrumented, 1}}],
        queues: [Queue.new("orders", worker_concurrency: 2, base_polling_interval_ms: 10)]
      )

    table = new_table(60)

    for i <- 1..10 do
      Dbos.enqueue("instrumented/1", [table],
        queue_name: "orders",
        engine: engine,
        workflow_id: "wf-wc-#{i}"
      )
    end

    wait_until(fn -> :ets.lookup_element(table, :seq, 2, 0) == 10 end)

    assert :ets.lookup_element(table, :max, 2) <= 2
  end

  test "4. global_concurrency caps simultaneous execution across two engines" do
    workflows = [{"instrumented/1", {SampleWorkflows, :instrumented, 1}}]
    queues = [Queue.new("orders", global_concurrency: 3, base_polling_interval_ms: 10)]

    engine_a = start_engine(workflows, queues: queues)
    engine_b = start_engine(workflows, queues: queues)

    table = new_table(80)

    for i <- 1..14 do
      engine = if rem(i, 2) == 0, do: engine_a, else: engine_b

      Dbos.enqueue("instrumented/1", [table],
        queue_name: "orders",
        engine: engine,
        workflow_id: "wf-gc-#{i}"
      )
    end

    wait_until(fn -> :ets.lookup_element(table, :seq, 2, 0) == 14 end, 500)

    assert :ets.lookup_element(table, :max, 2) <= 3
  end

  test "5. rate limiting caps starts within a rolling window" do
    engine =
      start_engine([{"instrumented/1", {SampleWorkflows, :instrumented, 1}}],
        queues: [
          Queue.new("orders",
            rate_limit: %{limit: 2, period_ms: 300},
            base_polling_interval_ms: 10
          )
        ]
      )

    table = new_table(10)

    for i <- 1..6 do
      Dbos.enqueue("instrumented/1", [table],
        queue_name: "orders",
        engine: engine,
        workflow_id: "wf-rl-#{i}"
      )
    end

    Process.sleep(150)
    assert :ets.lookup_element(table, :seq, 2, 0) <= 2

    wait_until(fn -> :ets.lookup_element(table, :seq, 2, 0) == 6 end, 500)
  end

  test "6. priority ordering: lower number first, created_at breaks ties" do
    raw_config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: Dbos.TestConn}
    table = new_table(20)

    {:ok, low_priority} =
      SystemDb.insert_enqueued_workflow(raw_config, %{
        name: "instrumented/1",
        queue_name: "orders",
        inputs: [table],
        priority: 5
      })

    Process.sleep(5)

    {:ok, high_priority} =
      SystemDb.insert_enqueued_workflow(raw_config, %{
        name: "instrumented/1",
        queue_name: "orders",
        inputs: [table],
        priority: 1
      })

    Process.sleep(5)

    {:ok, tied_priority_older} =
      SystemDb.insert_enqueued_workflow(raw_config, %{
        name: "instrumented/1",
        queue_name: "orders",
        inputs: [table],
        priority: 1
      })

    start_engine([{"instrumented/1", {SampleWorkflows, :instrumented, 1}}],
      queues: [Queue.new("orders", worker_concurrency: 1, base_polling_interval_ms: 20)]
    )

    wait_until(fn -> :ets.lookup_element(table, :seq, 2, 0) == 3 end)

    executed_order =
      1..3
      |> Enum.map(fn seq -> :ets.lookup_element(table, {:event, seq}, 2) end)

    assert executed_order == [high_priority, tied_priority_older, low_priority]
  end

  test "7. partition keys are dequeued and counted independently" do
    engine =
      start_engine([{"instrumented/1", {SampleWorkflows, :instrumented, 1}}],
        queues: [
          Queue.new("orders",
            partition_queue: true,
            worker_concurrency: 1,
            base_polling_interval_ms: 10
          )
        ]
      )

    table_a = new_table(30)
    table_b = new_table(30)

    for i <- 1..4 do
      Dbos.enqueue("instrumented/1", [table_a],
        queue_name: "orders",
        engine: engine,
        partition_key: "tenant-a",
        workflow_id: "wf-pa-#{i}"
      )

      Dbos.enqueue("instrumented/1", [table_b],
        queue_name: "orders",
        engine: engine,
        partition_key: "tenant-b",
        workflow_id: "wf-pb-#{i}"
      )
    end

    wait_until(fn ->
      :ets.lookup_element(table_a, :seq, 2, 0) == 4 and
        :ets.lookup_element(table_b, :seq, 2, 0) == 4
    end)

    assert :ets.lookup_element(table_a, :max, 2) <= 1
    assert :ets.lookup_element(table_b, :max, 2) <= 1
  end

  test "8. a DELAYED workflow does not run before delay_until, and does run after" do
    engine =
      start_engine([{"add/2", {SampleWorkflows, :add, 2}}],
        queues: [Queue.new("orders", base_polling_interval_ms: 10)]
      )

    config = Dbos.config(engine)

    {:ok, handle} =
      Dbos.enqueue("add/2", [1, 2], queue_name: "orders", engine: engine, delay_ms: 300)

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :delayed

    Process.sleep(100)
    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :delayed

    assert {:ok, 3} = Dbos.await(handle, timeout_ms: 3_000)
  end

  test "9. deduplication: a second enqueue with the same id raises Dbos.QueueDeduplicatedError" do
    engine =
      start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}],
        queues: [Queue.new("orders", base_polling_interval_ms: 10)]
      )

    {:ok, first} =
      Dbos.enqueue("three_steps/1", ["a"],
        queue_name: "orders",
        engine: engine,
        deduplication_id: "order-42"
      )

    assert_raise Dbos.QueueDeduplicatedError, fn ->
      Dbos.enqueue("three_steps/1", ["b"],
        queue_name: "orders",
        engine: engine,
        deduplication_id: "order-42"
      )
    end

    assert {:ok, %{shipped: "a"}} = Dbos.await(first, timeout_ms: 3_000)
  end

  test "10. application version: NULL is picked up by the latest worker; a mismatched version is not" do
    workflows = [{"instrumented/1", {SampleWorkflows, :instrumented, 1}}]

    engine_old =
      start_engine(workflows,
        application_version: "v-old",
        queues: [Queue.new("orders", base_polling_interval_ms: 10)]
      )

    engine_latest =
      start_engine(workflows,
        application_version: "v-latest",
        queues: [Queue.new("orders", base_polling_interval_ms: 10)]
      )

    config_old = Dbos.config(engine_old)
    config_latest = Dbos.config(engine_latest)

    table = new_table(10)

    {:ok, _} =
      SystemDb.insert_enqueued_workflow(config_old, %{
        workflow_id: "wf-nullver",
        name: "instrumented/1",
        queue_name: "orders",
        inputs: [table],
        application_version: nil
      })

    {:ok, _} =
      SystemDb.insert_enqueued_workflow(config_old, %{
        workflow_id: "wf-otherver",
        name: "instrumented/1",
        queue_name: "orders",
        inputs: [table],
        application_version: "v-unrelated"
      })

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config_old, "wf-nullver")
      status.status == :success
    end)

    {:ok, null_version_status} = SystemDb.get_workflow_status(config_old, "wf-nullver")
    assert null_version_status.executor_id == config_latest.executor_id

    Process.sleep(150)

    {:ok, other_version_status} = SystemDb.get_workflow_status(config_old, "wf-otherver")
    assert other_version_status.status == :enqueued
  end
end
