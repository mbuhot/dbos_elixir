defmodule Dbos.RecoveryTest do
  use Dbos.Case, async: false

  import ExUnit.CaptureLog

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

  test "reports how many workflows were left behind because nothing here is registered under their name",
       %{config: config, name: name} do
    Enum.each(1..3, fn index ->
      insert_owned_by(config, "exec-dead", %{
        workflow_id: "wf-unknown-#{index}",
        status: :pending,
        name: "unknown_workflow/1",
        inputs: [index]
      })
    end)

    watch_declined()

    log = capture_log(fn -> Recovery.reclaim(name, ["exec-dead"]) end)

    assert log =~ "3 PENDING workflow(s) named \"unknown_workflow/1\""
    assert log =~ "name_not_registered"
    assert log =~ "wf-unknown-1"

    assert_received {:declined, %{count: 3}, metadata}
    assert metadata.name == "unknown_workflow/1"
    assert metadata.reason == :name_not_registered
    refute_received {:declined, _, _}
  end

  test "reports the workflows left behind by a workflow version this executor does not run",
       %{
         config: config,
         name: name
       } do
    Dbos.put_config(%{config | application_version: "v2"})

    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-old-version",
      status: :pending,
      name: "add/2",
      inputs: [1, 2],
      application_version: "v1",
      ex_workflow_version: "1"
    })

    watch_declined()

    log = capture_log(fn -> Recovery.reclaim(name, ["exec-dead"]) end)

    assert log =~
             "1 PENDING workflow(s) named \"add/2\" (workflow version \"1\", " <>
               "application version \"v1\")"

    assert log =~ "version_mismatch"
    assert log =~ "wf-old-version"

    assert_received {:declined, %{count: 1}, metadata}
    assert metadata.row_version == "1"
    assert metadata.executor_version == "v2"
    assert metadata.reason == :version_mismatch

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-old-version")
    assert status.executor_id == "exec-dead"
  end

  test "reports a workflow it could have run but another transaction was holding", %{
    config: config,
    name: name
  } do
    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-held",
      status: :pending,
      name: "add/2",
      inputs: [1, 2]
    })

    holder = hold_row_lock("wf-held")
    watch_declined()

    log = capture_log(fn -> Recovery.reclaim(name, ["exec-dead"]) end)

    release_row_lock(holder)

    assert log =~ "1 PENDING workflow(s) named \"add/2\""
    assert log =~ "locked_elsewhere"
    assert log =~ "wf-held"

    assert_received {:declined, %{count: 1}, metadata}
    assert metadata.reason == :locked_elsewhere
  end

  test "says nothing about workflows it reclaimed and redispatched itself", %{
    config: config,
    name: name
  } do
    insert_owned_by(config, "exec-dead", %{
      workflow_id: "wf-quiet",
      status: :pending,
      name: "add/2",
      inputs: [1, 2]
    })

    watch_declined()

    log = capture_log(fn -> Recovery.reclaim(name, ["exec-dead"]) end)

    refute log =~ "unclaimed"
    refute_received {:declined, _, _}
  end

  defp watch_declined do
    test_pid = self()
    handler_id = {__MODULE__, System.unique_integer([:positive])}

    :telemetry.attach(
      handler_id,
      [:dbos, :recovery, :declined],
      fn _event, measurements, metadata, _config ->
        send(test_pid, {:declined, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp hold_row_lock(workflow_id) do
    {:ok, conn} = Postgrex.start_link(database: Application.get_env(:dbos, :test_database))
    test_pid = self()

    task =
      Task.async(fn ->
        Postgrex.transaction(
          conn,
          fn tx ->
            Postgrex.query!(
              tx,
              "SELECT workflow_uuid FROM dbos.workflow_status WHERE workflow_uuid = $1 FOR UPDATE",
              [workflow_id]
            )

            send(test_pid, :row_locked)

            receive do
              :release -> :ok
            end
          end,
          timeout: 30_000
        )
      end)

    assert_receive :row_locked, 5_000
    task
  end

  defp release_row_lock(task) do
    send(task.pid, :release)
    Task.await(task)
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
