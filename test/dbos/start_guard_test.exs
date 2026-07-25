defmodule Dbos.StartGuardTest do
  use Dbos.Case, async: false

  import ExUnit.CaptureLog

  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      Keyword.merge(
        [
          name: name,
          db: {Dbos.DB.Postgrex, Dbos.TestConn},
          executor_id: "exec-#{System.unique_integer([:positive])}",
          workflows: workflows,
          migrations: :skip
        ],
        extra_opts
      )

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp new_table do
    table = :"start_guard_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])
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

  test "two concurrent starts for the same workflow id run the body exactly once and both awaits see the same outcome" do
    engine = start_engine([{"counting_workflow/1", {SampleWorkflows, :counting_workflow, 1}}])
    table = new_table()
    workflow_id = "wf-double-start-#{System.unique_integer([:positive])}"

    task_1 =
      Task.async(fn ->
        Dbos.start("counting_workflow/1", [table], workflow_id: workflow_id, engine: engine)
      end)

    task_2 =
      Task.async(fn ->
        Dbos.start("counting_workflow/1", [table], workflow_id: workflow_id, engine: engine)
      end)

    {:ok, handle_1} = Task.await(task_1)
    {:ok, handle_2} = Task.await(task_2)

    assert handle_1.workflow_id == workflow_id
    assert handle_2.workflow_id == workflow_id

    assert {:ok, 1} = Dbos.await(handle_1)
    assert {:ok, 1} = Dbos.await(handle_2)

    assert :ets.lookup_element(table, :count, 2) == 1
  end

  test "a second execution racing a step checkpoint never masks the winner's success as ERROR" do
    engine = start_engine([{"gated_racing_step/1", {SampleWorkflows, :gated_racing_step, 1}}])
    config = Dbos.config(engine)
    table = new_table()
    workflow_id = "wf-racing-checkpoint-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "gated_racing_step/1",
      inputs: [table]
    })

    {:ok, _pid_1} =
      WorkflowSup.start_workflow(
        engine,
        workflow_id,
        {SampleWorkflows, :gated_racing_step, 1},
        [table]
      )

    {:ok, _pid_2} =
      WorkflowSup.start_workflow(
        engine,
        workflow_id,
        {SampleWorkflows, :gated_racing_step, 1},
        [table]
      )

    wait_until(fn ->
      {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)
      length(steps) == 1
    end)

    Process.sleep(200)

    {:ok, [step]} = SystemDb.get_workflow_steps(config, workflow_id)
    winning_tag = step.output
    [{{:pid, ^winning_tag}, winner_pid}] = :ets.lookup(table, {:pid, winning_tag})

    send(winner_pid, :go)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      status.status in [:success, :error]
    end)

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    assert status.status == :success
  end

  test "a parent crashing right after starting a child recovers to exactly one child execution" do
    engine =
      start_engine([
        {"spawn_child_and_die/1", {SampleWorkflows, :spawn_child_and_die, 1}},
        {"counting_child/1", {SampleWorkflows, :counting_workflow, 1}}
      ])

    config = Dbos.config(engine)
    table = new_table()
    parent_id = "wf-parent-dies-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(config, %{
      workflow_id: parent_id,
      status: :pending,
      name: "spawn_child_and_die/1",
      inputs: [table]
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(
        engine,
        parent_id,
        {SampleWorkflows, :spawn_child_and_die, 1},
        [table]
      )

    wait_until(fn -> WorkflowSup.whereis(engine, parent_id) == :error end)

    Recovery.recover_pending(engine)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, parent_id)
      status.status == :success
    end)

    {:ok, %{rows: rows}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT workflow_uuid FROM dbos.workflow_status WHERE parent_workflow_id = $1",
        [parent_id]
      )

    assert length(rows) == 1
    assert :ets.lookup_element(table, :count, 2) == 1
  end

  test "the child insert and the parent's step record commit or roll back together" do
    engine =
      start_engine(
        [
          {"spawn_child/1", {SampleWorkflows, :spawn_child, 1}},
          {"add/2", {SampleWorkflows, :add, 2}}
        ],
        db: {Dbos.Test.FaultyDB, Dbos.TestConn}
      )

    config = Dbos.config(engine)
    parent_id = "wf-atomic-parent-#{System.unique_integer([:positive])}"
    child_id = "#{parent_id}-0"

    Dbos.Test.FaultyDB.set_fault_target(parent_id)

    on_exit(fn -> Dbos.Test.FaultyDB.clear_fault_target() end)

    SystemDb.insert_workflow_status(config, %{
      workflow_id: parent_id,
      status: :pending,
      name: "spawn_child/1"
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(engine, parent_id, {SampleWorkflows, :spawn_child, 1}, [
        :ignored
      ])

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, parent_id)
      status.status == :error
    end)

    assert {:error, :not_found} = SystemDb.get_workflow_status(config, child_id)
  end

  test "recovery still dispatches a reclaimed workflow normally" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)
    workflow_id = "wf-recovered-normal-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(%{config | executor_id: "exec-dead"}, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "add/2",
      inputs: [3, 4]
    })

    Recovery.reclaim(engine, ["exec-dead"])

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      status.status == :success
    end)

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    assert status.output == 7
  end

  test "stopping an engine with a deadline timer pending produces no crash report" do
    engine = start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}])
    {:ok, _handle} = Dbos.start("sleeper/1", [60_000], timeout_ms: 100, engine: engine)
    Process.sleep(20)

    log =
      capture_log(fn ->
        stop_supervised!(engine)
        Process.sleep(200)
      end)

    refute log =~ "CRASH REPORT"
    refute log =~ "ArgumentError"
  end
end
