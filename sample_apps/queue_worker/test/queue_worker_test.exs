defmodule QueueWorkerTest do
  use ExUnit.Case, async: false

  alias Dbos.SystemDb
  alias QueueWorker.Tasks

  setup do
    start_supervised!(
      {Dbos.Supervisor,
       db: {Dbos.DB.Ecto, QueueWorker.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Tasks],
       queues: [Dbos.Queue.new("tasks", worker_concurrency: 2)],
       migrations: :verify}
    )

    Dbos.Recovery.await_boot_recovery(Dbos)
    :ok
  end

  test "killing an in-flight worker still lets every enqueued task run exactly once" do
    batch_id = "kill-test-#{System.unique_integer([:positive])}"
    count = 6

    handles =
      for task_number <- 1..count do
        {:ok, handle} =
          Dbos.enqueue(&Tasks.process_task/2, [batch_id, task_number],
            queue_name: "tasks",
            workflow_id: "#{batch_id}-#{task_number}"
          )

        handle
      end

    {_killed_handle, pid} = wait_for_any_running(handles)
    Process.exit(pid, :kill)

    Dbos.Recovery.recover_pending(Dbos)

    results =
      Enum.map(handles, fn handle ->
        assert {:ok, result} = Dbos.await(handle, timeout_ms: 10_000)
        result
      end)

    assert length(results) == count
    assert Enum.map(results, & &1.task_number) |> Enum.sort() == Enum.to_list(1..count)

    config = Dbos.config()

    %{rows: [[success_count]]} =
      QueueWorker.Repo.query!(
        "SELECT count(*) FROM \"dbos\".workflow_status WHERE status = 'SUCCESS' AND workflow_uuid LIKE $1",
        ["#{batch_id}-%"]
      )

    assert success_count == count

    for handle <- handles do
      {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
      assert Enum.map(steps, & &1.function_id) == [0, 1]
    end
  end

  defp wait_for_any_running(handles, attempts \\ 200)

  defp wait_for_any_running(_handles, 0), do: flunk("no workflow started running")

  defp wait_for_any_running(handles, attempts) do
    handles
    |> Enum.find_value(fn handle ->
      case Dbos.WorkflowSup.whereis(Dbos, handle.workflow_id) do
        {:ok, pid} -> {handle, pid}
        :error -> nil
      end
    end)
    |> case do
      nil ->
        Process.sleep(10)
        wait_for_any_running(handles, attempts - 1)

      found ->
        found
    end
  end
end
