defmodule Dbos.RecoveryTest do
  use Dbos.Case, async: false

  require Logger

  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  setup %{conn: conn} do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    config = %Dbos.Config{
      name: name,
      db: Dbos.DB.Postgrex,
      conn: conn,
      executor_id: "exec-1",
      max_recovery_attempts: 2
    }

    Dbos.put_config(config)

    start_supervised!(
      {Dbos.Registry, name: name, workflows: [{"add/2", {SampleWorkflows, :add, 2}}]}
    )

    start_supervised!({Registry, keys: :unique, name: WorkflowSup.process_registry_name(name)},
      id: :process_registry
    )

    start_supervised!({WorkflowSup, name: name})

    {:ok, config: config, name: name}
  end

  test "recovers a PENDING workflow for this executor, bumping recovery_attempts", %{
    config: config,
    name: name
  } do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-recover-add",
      status: :pending,
      name: "add/2",
      inputs: [1, 2]
    })

    Recovery.recover_pending(name)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, "wf-recover-add")
      status.status == :success
    end)

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-recover-add")
    assert status.output == 3
    assert status.recovery_attempts == 2
  end

  test "skips a workflow whose name is not registered, and still recovers a registered one", %{
    config: config,
    name: name
  } do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-unregistered",
      status: :pending,
      name: "unknown_workflow/1",
      inputs: [1]
    })

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-recover-registered",
      status: :pending,
      name: "add/2",
      inputs: [4, 5]
    })

    Recovery.recover_pending(name)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, "wf-recover-registered")
      status.status == :success
    end)

    {:ok, unregistered_status} = SystemDb.get_workflow_status(config, "wf-unregistered")
    assert unregistered_status.status == :pending

    {:ok, registered_status} = SystemDb.get_workflow_status(config, "wf-recover-registered")
    assert registered_status.output == 9
  end

  test "a queued PENDING workflow is cleared back to ENQUEUED, not re-invoked", %{
    config: config,
    name: name
  } do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-recover-queued",
      status: :pending,
      name: "add/2",
      inputs: [1, 2],
      queue_name: "orders"
    })

    Recovery.recover_pending(name)

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-recover-queued")
    assert status.status == :enqueued
    assert status.started_at_epoch_ms == nil
  end

  test "repeated recovery of a workflow whose process keeps crashing flips it to MAX_RECOVERY_ATTEMPTS_EXCEEDED",
       %{config: config, name: name} do
    Dbos.Registry.register(name, "crash_self/1", {SampleWorkflows, :crash_self, 1})

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-recover-dlq",
      status: :pending,
      name: "crash_self/1",
      inputs: [1]
    })

    Recovery.recover_pending(name)
    Process.sleep(20)
    assert still_pending?(config, "wf-recover-dlq")

    Recovery.recover_pending(name)
    Process.sleep(20)
    assert still_pending?(config, "wf-recover-dlq")

    Recovery.recover_pending(name)

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-recover-dlq")
    assert status.status == :max_recovery_attempts_exceeded
  end

  defp still_pending?(config, workflow_id) do
    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    status.status == :pending
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
