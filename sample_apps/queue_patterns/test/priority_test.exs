defmodule QueuePatterns.PriorityTest do
  use ExUnit.Case, async: false

  alias Dbos.SystemDb
  alias Dbos.WorkflowHandle
  alias QueuePatterns.Priority

  test "a lower priority number runs before requests already waiting, ties broken by arrival" do
    raw_config = %Dbos.Config{db: Dbos.DB.Ecto, conn: QueuePatterns.Repo}

    {:ok, normal_1_id} =
      SystemDb.insert_enqueued_workflow(raw_config, %{
        name: "handle_request",
        queue_name: "priority_queue",
        inputs: ["normal-1"],
        priority: 10
      })

    Process.sleep(5)

    {:ok, normal_2_id} =
      SystemDb.insert_enqueued_workflow(raw_config, %{
        name: "handle_request",
        queue_name: "priority_queue",
        inputs: ["normal-2"],
        priority: 10
      })

    Process.sleep(5)

    {:ok, urgent_id} =
      SystemDb.insert_enqueued_workflow(raw_config, %{
        name: "handle_request",
        queue_name: "priority_queue",
        inputs: ["urgent"],
        priority: 0
      })

    start_supervised!(
      {Dbos.Supervisor,
       db: {Dbos.DB.Ecto, QueuePatterns.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Priority],
       queues: [
         Dbos.Queue.new("priority_queue",
           priority_enabled: true,
           worker_concurrency: 1,
           base_polling_interval_ms: 20
         )
       ],
       migrations: :verify}
    )

    Dbos.Recovery.await_boot_recovery(Dbos)

    normal_1 = %WorkflowHandle{engine: Dbos, workflow_id: normal_1_id}
    normal_2 = %WorkflowHandle{engine: Dbos, workflow_id: normal_2_id}
    urgent = %WorkflowHandle{engine: Dbos, workflow_id: urgent_id}

    for handle <- [normal_1, normal_2, urgent] do
      assert {:ok, _result} = Dbos.await(handle, timeout_ms: 10_000)
    end

    config = Dbos.config()

    started_at = fn handle ->
      {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
      status.started_at_epoch_ms
    end

    assert started_at.(urgent) < started_at.(normal_1)
    assert started_at.(normal_1) < started_at.(normal_2)
  end
end
