defmodule Dbos.SystemDbRetryTest do
  use Dbos.Case, async: false

  import ExUnit.CaptureLog

  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.Test.FaultyDB
  alias Dbos.WorkflowSup

  @dead_executor "exec-dead"

  setup do
    on_exit(fn ->
      FaultyDB.clear_injection()
      Application.delete_env(:dbos, :system_db_retry)
    end)

    :ok
  end

  defp fast_retries(max_attempts) do
    Application.put_env(:dbos, :system_db_retry,
      max_attempts: max_attempts,
      base_delay_ms: 1,
      max_delay_ms: 5
    )
  end

  defp config(conn, opts \\ []) do
    %Dbos.Config{
      name: Keyword.get(opts, :name, Dbos),
      db: Keyword.get(opts, :db, FaultyDB),
      conn: conn,
      schema: Keyword.get(opts, :schema, "dbos"),
      executor_id: Keyword.get(opts, :executor_id, "exec-1"),
      application_version: Keyword.get(opts, :application_version)
    }
  end

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      Keyword.merge(
        [
          name: name,
          db: {FaultyDB, Dbos.TestConn},
          executor_id: "exec-#{System.unique_integer([:positive])}",
          workflows: workflows,
          migrations: :skip
        ],
        extra_opts
      )

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Recovery.await_boot_recovery(name)
    name
  end

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

  defp seed_pending(config, workflow_id, inputs) do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "add/2",
      inputs: inputs
    })
  end

  defp dead_config(conn, engine, application_version) do
    config(conn,
      name: engine,
      executor_id: @dead_executor,
      application_version: application_version
    )
  end

  defp status_of(config, workflow_id) do
    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    status
  end

  test "a step checkpoint that hits a transient connection error is still recorded", %{conn: conn} do
    fast_retries(5)
    config = config(conn)
    seed_pending(config, "wf-checkpoint-retry", [1, 2])

    FaultyDB.inject_connection_error(["INSERT INTO", "operation_outputs"], times: 3)

    assert :ok =
             SystemDb.record_operation_result(config, %{
               workflow_id: "wf-checkpoint-retry",
               function_id: 1,
               function_name: "charge_card/1",
               output: Dbos.Serialization.encode(:charged),
               started_at: 1,
               completed_at: 2
             })

    assert FaultyDB.injected_count() == 3
    {:ok, [step]} = SystemDb.get_workflow_steps(config, "wf-checkpoint-retry")
    assert step.function_name == "charge_card/1"
  end

  test "a statement that never recovers raises after a bounded number of attempts", %{conn: conn} do
    fast_retries(3)
    config = config(conn)

    FaultyDB.inject_connection_error(["FROM \"dbos\".workflow_status"], times: 100)

    assert_raise Dbos.SystemDbError, fn ->
      SystemDb.get_workflow_status(config, "wf-never")
    end

    assert FaultyDB.injected_count() == 3
  end

  test "a genuine query error names the failing statement instead of raising MatchError", %{
    conn: conn
  } do
    config = config(conn, schema: "no_such_schema")

    error =
      assert_raise Dbos.SystemDbError, fn ->
        SystemDb.get_workflow_status(config, "wf-any")
      end

    message = Exception.message(error)
    assert message =~ "no_such_schema"
    assert message =~ "SELECT"
  end

  test "without the retry layer a transient error during recovery strands the workflow, while the rest of the batch continues",
       %{conn: conn} do
    fast_retries(1)

    engine =
      start_engine([{"add/2", {SampleWorkflows, :add, 2}}], application_version: "v-strand")

    live = config(conn, name: engine)
    dead = dead_config(conn, engine, "v-strand")

    seed_pending(dead, "wf-stranded", [1, 2])
    seed_pending(dead, "wf-survivor", [3, 4])

    FaultyDB.inject_connection_error(["recovery_attempts = CASE"],
      param: "wf-stranded",
      times: 1
    )

    capture_log(fn -> Recovery.reclaim(engine, [@dead_executor]) end)

    wait_until(fn -> status_of(live, "wf-survivor").status == :success end)

    stranded = status_of(live, "wf-stranded")
    assert stranded.status == :pending
    assert stranded.executor_id == Dbos.config(engine).executor_id

    FaultyDB.clear_injection()
    assert Recovery.reclaim(engine, [@dead_executor]) == []
    assert status_of(live, "wf-stranded").status == :pending
  end

  test "with the retry layer the same transient error during recovery recovers the workflow", %{
    conn: conn
  } do
    fast_retries(5)

    engine =
      start_engine([{"add/2", {SampleWorkflows, :add, 2}}], application_version: "v-retry")

    live = config(conn, name: engine)
    dead = dead_config(conn, engine, "v-retry")

    seed_pending(dead, "wf-retried", [1, 2])
    seed_pending(dead, "wf-companion", [3, 4])

    FaultyDB.inject_connection_error(["recovery_attempts = CASE"], param: "wf-retried", times: 2)

    Recovery.reclaim(engine, [@dead_executor])

    wait_until(fn -> status_of(live, "wf-retried").status == :success end)
    wait_until(fn -> status_of(live, "wf-companion").status == :success end)

    assert status_of(live, "wf-retried").output == 3
    assert FaultyDB.injected_count() == 2
  end

  test "the boot recovery scan absorbs a transient error on its reclaiming update", %{conn: conn} do
    fast_retries(5)
    seeding = config(conn, executor_id: "exec-boot-seed", application_version: "v-boot")
    seed_pending(seeding, "wf-boot-scan", [5, 6])

    FaultyDB.inject_connection_error(["SET executor_id = $1", "FOR UPDATE SKIP LOCKED"], times: 2)

    engine =
      start_engine([{"add/2", {SampleWorkflows, :add, 2}}],
        executor_id: "exec-boot-seed",
        application_version: "v-boot"
      )

    live = config(conn, name: engine)
    wait_until(fn -> status_of(live, "wf-boot-scan").status == :success end)
    assert FaultyDB.injected_count() == 2
  end

  test "a queued workflow is still dequeued when the claim query fails transiently", %{conn: conn} do
    fast_retries(5)
    queue = %Dbos.Queue{name: "retry-queue", base_polling_interval_ms: 20}
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}], queues: [queue])
    live = config(conn, name: engine)

    FaultyDB.inject_connection_error(["ORDER BY priority ASC"], times: 3)

    {:ok, handle} = Dbos.start("add/2", [7, 8], engine: engine, queue: "retry-queue")

    assert {:ok, 15} = Dbos.await(handle, timeout_ms: 10_000)
    assert status_of(live, handle.workflow_id).status == :success
  end

  test "a parked wait rehydrates when its status read fails transiently", %{conn: conn} do
    fast_retries(5)

    engine =
      start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}], park_exit_threshold_ms: 50)

    live = config(conn, name: engine)
    {:ok, handle} = Dbos.start("sleeper/1", [400], engine: engine)

    wait_until(fn -> WorkflowSup.whereis(engine, handle.workflow_id) == :error end)

    FaultyDB.inject_connection_error(["FROM \"dbos\".workflow_status WHERE workflow_uuid = $1"],
      param: handle.workflow_id,
      times: 2
    )

    assert {:ok, :woke} = Dbos.await(handle, timeout_ms: 10_000)
    assert status_of(live, handle.workflow_id).status == :success
  end

  test "a workflow whose status row is still locked when recovery scans is recovered by a later pass",
       %{conn: conn} do
    engine =
      start_engine([{"add/2", {SampleWorkflows, :add, 2}}],
        executor_id: "exec-locked",
        application_version: "v-locked"
      )

    seeding =
      config(conn, name: engine, executor_id: "exec-locked", application_version: "v-locked")

    seed_pending(seeding, "wf-locked", [9, 10])

    {:ok, holder} = Postgrex.start_link(database: Application.fetch_env!(:dbos, :test_database))
    released = hold_row_lock(holder, "wf-locked", 150)

    Recovery.recover_pending(engine)

    assert Task.await(released) == :released
    wait_until(fn -> status_of(seeding, "wf-locked").status == :success end)
    assert status_of(seeding, "wf-locked").output == 19
  end

  defp hold_row_lock(holder, workflow_id, hold_ms) do
    lock_taken = self()

    task =
      Task.async(fn ->
        Postgrex.transaction(holder, fn tx ->
          Postgrex.query!(
            tx,
            ~s(SELECT 1 FROM "dbos".workflow_status WHERE workflow_uuid = $1 FOR UPDATE),
            [workflow_id]
          )

          send(lock_taken, :locked)
          Process.sleep(hold_ms)
        end)

        :released
      end)

    assert_receive :locked, 5_000
    task
  end

  test "a workflow with steps completes when its next system-database statement fails five times",
       %{conn: conn} do
    fast_retries(10)
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    live = config(conn, name: engine)

    FaultyDB.inject_connection_error(["dbos"], times: 5)

    {:ok, handle} = Dbos.start("three_steps/1", ["order-1"], engine: engine)

    assert {:ok, %{shipped: "order-1"}} = Dbos.await(handle, timeout_ms: 10_000)
    assert status_of(live, handle.workflow_id).status == :success
    assert FaultyDB.injected_count() == 5
  end
end
