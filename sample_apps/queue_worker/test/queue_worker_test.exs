defmodule QueueWorkerTest do
  use ExUnit.Case, async: false

  alias Dbos.SystemDb
  alias QueueWorker.Tasks

  setup do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Postgrex, QueueWorker.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Tasks],
       queues: [Dbos.Queue.new("tasks", worker_concurrency: 2)],
       migrations: :skip},
      id: name
    )

    Dbos.Recovery.await_boot_recovery(name)
    {:ok, engine: name}
  end

  test "killing an in-flight worker still lets every enqueued task run exactly once", %{
    engine: engine
  } do
    batch_id = "kill-test-#{System.unique_integer([:positive])}"
    count = 6

    handles =
      for task_number <- 1..count do
        {:ok, handle} =
          Dbos.enqueue("process_task", [batch_id, task_number],
            queue_name: "tasks",
            workflow_id: "#{batch_id}-#{task_number}",
            engine: engine
          )

        handle
      end

    {_killed_handle, pid} = wait_for_any_running(engine, handles)
    Process.exit(pid, :kill)

    Dbos.Recovery.recover_pending(engine)

    results =
      Enum.map(handles, fn handle ->
        assert {:ok, result} = Dbos.await(handle, timeout_ms: 10_000)
        result
      end)

    assert length(results) == count
    assert Enum.map(results, & &1.task_number) |> Enum.sort() == Enum.to_list(1..count)

    config = Dbos.config(engine)

    %{rows: [[success_count]]} =
      Postgrex.query!(
        QueueWorker.Repo,
        "SELECT count(*) FROM \"dbos\".workflow_status WHERE status = 'SUCCESS' AND workflow_uuid LIKE $1",
        ["#{batch_id}-%"]
      )

    assert success_count == count

    for handle <- handles do
      {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
      assert Enum.map(steps, & &1.function_id) == [0, 1]
    end
  end

  defp wait_for_any_running(engine, handles, attempts \\ 200)

  defp wait_for_any_running(_engine, _handles, 0), do: flunk("no workflow started running")

  defp wait_for_any_running(engine, handles, attempts) do
    handles
    |> Enum.find_value(fn handle ->
      case Dbos.WorkflowSup.whereis(engine, handle.workflow_id) do
        {:ok, pid} -> {handle, pid}
        :error -> nil
      end
    end)
    |> case do
      nil ->
        Process.sleep(10)
        wait_for_any_running(engine, handles, attempts - 1)

      found ->
        found
    end
  end
end
