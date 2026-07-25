defmodule Dbos.DequeueTest do
  use Dbos.Case, async: false

  alias Dbos.Queue
  alias Dbos.SystemDb

  setup %{conn: conn} do
    config = %Dbos.Config{
      db: Dbos.DB.Postgrex,
      conn: conn,
      executor_id: "exec-1",
      application_version: "v1"
    }

    {:ok, config: config}
  end

  defp enqueue(config, opts) do
    {:ok, id} =
      SystemDb.insert_enqueued_workflow(
        config,
        Map.new(opts) |> Map.put_new(:name, "W") |> Map.put_new(:queue_name, "q")
      )

    id
  end

  test "an unbounded queue claims every ENQUEUED row, ordered by priority then created_at", %{
    config: config
  } do
    id_low_priority = enqueue(config, inputs: [1], priority: 5)
    Process.sleep(2)
    id_high_priority = enqueue(config, inputs: [2], priority: 1)
    Process.sleep(2)
    id_same_priority_older = enqueue(config, inputs: [3], priority: 1)

    claimed = SystemDb.dequeue_workflows(config, Queue.new("q"))

    assert Enum.map(claimed, & &1.workflow_id) == [
             id_high_priority,
             id_same_priority_older,
             id_low_priority
           ]

    for id <- [id_low_priority, id_high_priority, id_same_priority_older] do
      {:ok, status} = SystemDb.get_workflow_status(config, id)
      assert status.status == :pending
      assert status.executor_id == "exec-1"
    end
  end

  test "worker_concurrency minus local_running_count bounds the claim", %{config: config} do
    enqueue(config, inputs: [1])
    enqueue(config, inputs: [2])
    enqueue(config, inputs: [3])

    queue = Queue.new("q", worker_concurrency: 2)

    claimed = SystemDb.dequeue_workflows(config, queue, local_running_count: 1)
    assert length(claimed) == 1
  end

  test "worker_concurrency already met by local_running_count claims nothing", %{config: config} do
    enqueue(config, inputs: [1])
    queue = Queue.new("q", worker_concurrency: 1)

    assert SystemDb.dequeue_workflows(config, queue, local_running_count: 1) == []
  end

  test "global_concurrency bounds the claim by counting PENDING rows for the queue", %{
    config: config
  } do
    already_pending = enqueue(config, inputs: [0])
    SystemDb.dequeue_workflows(config, Queue.new("q"))
    {:ok, status} = SystemDb.get_workflow_status(config, already_pending)
    assert status.status == :pending

    enqueue(config, inputs: [1])
    enqueue(config, inputs: [2])

    queue = Queue.new("q", global_concurrency: 2)
    claimed = SystemDb.dequeue_workflows(config, queue)
    assert length(claimed) == 1
  end

  test "the application-version predicate: NULL version rows only go to the latest-version worker",
       %{config: config} do
    SystemDb.create_application_version(config, "v1")

    null_version_id =
      enqueue(config, inputs: [1], application_version: nil)

    old_worker_config = %{config | application_version: "v0"}
    assert SystemDb.dequeue_workflows(old_worker_config, Queue.new("q")) == []

    claimed = SystemDb.dequeue_workflows(config, Queue.new("q"))
    assert Enum.map(claimed, & &1.workflow_id) == [null_version_id]
  end

  test "a non-matching application version is not claimed by a different version's worker", %{
    config: config
  } do
    SystemDb.create_application_version(config, "v1")
    enqueue(config, inputs: [1], application_version: "v2")

    other_config = %{config | application_version: "v1"}
    assert SystemDb.dequeue_workflows(other_config, Queue.new("q")) == []
  end

  test "rate limiting caps starts within the rolling window", %{config: config} do
    enqueue(config, inputs: [1])
    enqueue(config, inputs: [2])
    enqueue(config, inputs: [3])

    queue = Queue.new("q", rate_limit: %{limit: 2, period_ms: 60_000})

    claimed = SystemDb.dequeue_workflows(config, queue)
    assert length(claimed) == 2

    enqueue(config, inputs: [4])
    assert SystemDb.dequeue_workflows(config, queue) == []
  end

  test "partition keys are dequeued and counted independently", %{config: config} do
    enqueue(config, inputs: [1], queue_partition_key: "a")
    enqueue(config, inputs: [2], queue_partition_key: "b")

    queue = Queue.new("q", partition_queue: true, worker_concurrency: 1)

    claimed_a = SystemDb.dequeue_workflows(config, queue, partition_key: "a")
    claimed_b = SystemDb.dequeue_workflows(config, queue, partition_key: "b")

    assert length(claimed_a) == 1
    assert length(claimed_b) == 1
  end

  test "claiming is idempotent under a concurrent claim (row already ENQUEUED->PENDING is skipped)",
       %{config: config} do
    id = enqueue(config, inputs: [1])

    {:ok, %{rows: [_]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "UPDATE dbos.workflow_status SET status = 'PENDING' WHERE workflow_uuid = $1 RETURNING workflow_uuid",
        [id]
      )

    assert SystemDb.dequeue_workflows(config, Queue.new("q")) == []
  end
end
