defmodule Dbos.WorkflowSupTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows
  alias Dbos.WorkflowSup

  setup %{conn: conn} do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")
    config = %Dbos.Config{name: name, db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}
    Dbos.put_config(config)

    start_supervised!({Registry, keys: :unique, name: WorkflowSup.process_registry_name(name)},
      id: :process_registry
    )

    start_supervised!({WorkflowSup, name: name})

    insert_pending(config, "wf-sup-1")

    {:ok, config: config, name: name}
  end

  defp insert_pending(config, workflow_id) do
    Dbos.SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "sleep_forever/1"
    })
  end

  test "start_workflow starts a process reachable via whereis/2", %{name: name} do
    {:ok, pid} =
      WorkflowSup.start_workflow(name, "wf-sup-1", {SampleWorkflows, :sleep_forever, 1}, [
        :ignored
      ])

    assert Process.alive?(pid)
    assert WorkflowSup.whereis(name, "wf-sup-1") == {:ok, pid}
  end

  test "count_running/3 counts live workflow processes for a queue/partition key", %{name: name} do
    assert WorkflowSup.count_running(name, nil, nil) == 0

    {:ok, _pid} =
      WorkflowSup.start_workflow(name, "wf-sup-1", {SampleWorkflows, :sleep_forever, 1}, [
        :ignored
      ])

    assert WorkflowSup.count_running(name, nil, nil) == 1
    assert WorkflowSup.count_running(name, "some_queue", nil) == 0
  end

  test "a successful workflow records SUCCESS with the decoded output", %{
    config: config,
    name: name
  } do
    Dbos.SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-sup-add",
      status: :pending,
      name: "add/2"
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(name, "wf-sup-add", {SampleWorkflows, :add, 2}, [1, 2])

    wait_until(fn ->
      {:ok, status} = Dbos.SystemDb.get_workflow_status(config, "wf-sup-add")
      status.status == :success
    end)

    {:ok, status} = Dbos.SystemDb.get_workflow_status(config, "wf-sup-add")
    assert status.output == 3
  end

  test "a workflow that raises records ERROR with the encoded failure", %{
    config: config,
    name: name
  } do
    Dbos.SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-sup-boom",
      status: :pending,
      name: "boom/1"
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(name, "wf-sup-boom", {SampleWorkflows, :boom, 1}, [:ignored])

    wait_until(fn ->
      {:ok, status} = Dbos.SystemDb.get_workflow_status(config, "wf-sup-boom")
      status.status == :error
    end)

    {:ok, status} = Dbos.SystemDb.get_workflow_status(config, "wf-sup-boom")
    assert %RuntimeError{message: "boom"} = status.error.value
  end

  defp wait_until(fun, attempts \\ 50)

  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end
end
