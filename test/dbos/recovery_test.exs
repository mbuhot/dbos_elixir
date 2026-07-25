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

  test "reclaim/2 moves another executor's PENDING rows to this executor and redispatches them, leaving a third executor's rows untouched",
       %{config: config, name: name} do
    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-owned-by-dead",
      status: :pending,
      name: "add/2",
      inputs: [2, 3]
    })

    insert_owned_by(config, "exec-other", %{
      workflow_id: "wf-owned-by-other",
      status: :pending,
      name: "add/2",
      inputs: [5, 5]
    })

    Recovery.reclaim(name, ["exec-dead"])

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, "wf-owned-by-dead")
      status.status == :success
    end)

    {:ok, reclaimed} = SystemDb.get_workflow_status(config, "wf-owned-by-dead")
    assert reclaimed.output == 5
    assert reclaimed.executor_id == "exec-1"

    {:ok, untouched} = SystemDb.get_workflow_status(config, "wf-owned-by-other")
    assert untouched.status == :pending
    assert untouched.executor_id == "exec-other"
  end

  test "two engines reclaiming the same dead executor concurrently redispatch every workflow exactly once",
       %{config: config, name: name} do
    other_name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")
    other_config = %{config | name: other_name, executor_id: "exec-2"}
    Dbos.put_config(other_config)

    start_supervised!(
      {Dbos.Registry, name: other_name, workflows: [{"add/2", {SampleWorkflows, :add, 2}}]},
      id: :other_registry
    )

    start_supervised!(
      {Registry, keys: :unique, name: WorkflowSup.process_registry_name(other_name)},
      id: :other_process_registry
    )

    start_supervised!({WorkflowSup, name: other_name}, id: :other_workflow_sup)

    workflow_ids = for i <- 1..6, do: "wf-concurrent-#{i}"

    Enum.each(workflow_ids, fn workflow_id ->
      insert_owned_by(config, "exec-dead", %{
        workflow_id: workflow_id,
        status: :pending,
        name: "add/2",
        inputs: [1, 1]
      })
    end)

    task_1 = Task.async(fn -> Recovery.reclaim(name, ["exec-dead"]) end)
    task_2 = Task.async(fn -> Recovery.reclaim(other_name, ["exec-dead"]) end)
    Task.await(task_1)
    Task.await(task_2)

    Enum.each(workflow_ids, fn workflow_id ->
      wait_until(fn ->
        {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
        status.status == :success
      end)
    end)

    Enum.each(workflow_ids, fn workflow_id ->
      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      assert status.recovery_attempts == 2
    end)
  end

  test "reclaim/2 respects the batch size, claiming only that many rows per call", %{
    config: config,
    name: name
  } do
    workflow_ids = for i <- 1..5, do: "wf-batch-#{i}"

    Enum.each(workflow_ids, fn workflow_id ->
      insert_owned_by(config, "exec-dead", %{
        workflow_id: workflow_id,
        status: :pending,
        name: "add/2",
        inputs: [1, 1]
      })
    end)

    Recovery.reclaim(name, ["exec-dead"], batch_size: 2)

    claimed_count =
      workflow_ids
      |> Enum.map(fn workflow_id ->
        {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
        status.executor_id
      end)
      |> Enum.count(&(&1 == "exec-1"))

    assert claimed_count == 2
  end

  test "a queued PENDING workflow owned by a dead executor is cleared to ENQUEUED, not redispatched",
       %{config: config, name: name} do
    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-dead-queued",
      status: :pending,
      name: "add/2",
      inputs: [1, 2],
      queue_name: "orders"
    })

    Recovery.reclaim(name, ["exec-dead"])

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-dead-queued")
    assert status.status == :enqueued
    assert status.started_at_epoch_ms == nil
  end

  test "reclaim/2 leaves a workflow whose name isn't registered on this executor untouched, letting a peer that has it claim it instead",
       %{config: config, name: name} do
    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-dead-unregistered",
      status: :pending,
      name: "unknown_workflow/1",
      inputs: [1]
    })

    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-dead-registered",
      status: :pending,
      name: "add/2",
      inputs: [3, 4]
    })

    Recovery.reclaim(name, ["exec-dead"])

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, "wf-dead-registered")
      status.status == :success
    end)

    {:ok, unregistered_status} = SystemDb.get_workflow_status(config, "wf-dead-unregistered")
    assert unregistered_status.status == :pending
    assert unregistered_status.executor_id == "exec-dead"
  end

  test "reclaim/2 is capability-aware: a peer registering a different workflow claims only what it can run, leaving the rest for a third engine that implements it",
       %{config: config} do
    beta_name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")
    beta_config = %{config | name: beta_name, executor_id: "exec-beta"}
    Dbos.put_config(beta_config)

    start_supervised!(
      {Dbos.Registry, name: beta_name, workflows: [{"beta/1", {SampleWorkflows, :sleeper, 1}}]},
      id: :beta_registry
    )

    start_supervised!(
      {Registry, keys: :unique, name: WorkflowSup.process_registry_name(beta_name)},
      id: :beta_process_registry
    )

    start_supervised!({WorkflowSup, name: beta_name}, id: :beta_workflow_sup)

    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-alpha-owned",
      status: :pending,
      name: "add/2",
      inputs: [1, 2]
    })

    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-beta-owned",
      status: :pending,
      name: "beta/1",
      inputs: [10]
    })

    Recovery.reclaim(beta_name, ["exec-dead"])

    {:ok, alpha_status} = SystemDb.get_workflow_status(config, "wf-alpha-owned")
    assert alpha_status.executor_id == "exec-dead"

    {:ok, beta_status} = SystemDb.get_workflow_status(config, "wf-beta-owned")
    assert beta_status.executor_id == "exec-beta"

    gamma_name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")
    gamma_config = %{config | name: gamma_name, executor_id: "exec-gamma"}
    Dbos.put_config(gamma_config)

    start_supervised!(
      {Dbos.Registry, name: gamma_name, workflows: [{"add/2", {SampleWorkflows, :add, 2}}]},
      id: :gamma_registry
    )

    start_supervised!(
      {Registry, keys: :unique, name: WorkflowSup.process_registry_name(gamma_name)},
      id: :gamma_process_registry
    )

    start_supervised!({WorkflowSup, name: gamma_name}, id: :gamma_workflow_sup)

    Recovery.reclaim(gamma_name, ["exec-dead"])

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, "wf-alpha-owned")
      status.status == :success
    end)

    {:ok, final_alpha_status} = SystemDb.get_workflow_status(config, "wf-alpha-owned")
    assert final_alpha_status.executor_id == "exec-gamma"
  end

  test "reclaimed workflows keep climbing recovery_attempts and eventually hit MAX_RECOVERY_ATTEMPTS_EXCEEDED",
       %{config: config, name: name} do
    Dbos.Registry.register(name, "crash_self/1", {SampleWorkflows, :crash_self, 1})

    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-dead-dlq",
      status: :pending,
      name: "crash_self/1",
      inputs: [1]
    })

    Recovery.reclaim(name, ["exec-dead"])
    Process.sleep(20)
    assert still_pending?(config, "wf-dead-dlq")

    Recovery.reclaim(name, ["exec-1"])
    Process.sleep(20)
    assert still_pending?(config, "wf-dead-dlq")

    Recovery.reclaim(name, ["exec-1"])

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-dead-dlq")
    assert status.status == :max_recovery_attempts_exceeded
  end

  defp insert_owned_by(config, executor_id, attrs) do
    SystemDb.insert_workflow_status(%{config | executor_id: executor_id}, attrs)
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
