defmodule Dbos.InWorkflowCheckpointTest do
  use Dbos.Case, async: false

  alias Dbos.CheckoutWorkflow
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
    table = :"in_workflow_checkpoint_#{System.unique_integer([:positive])}"
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

  defp count_workflows_named(config, name) do
    {:ok, %{rows: [[count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT count(*) FROM dbos.workflow_status WHERE name = $1",
        [name]
      )

    count
  end

  defp count_forks_of(config, original_workflow_id) do
    {:ok, %{rows: [[count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT count(*) FROM dbos.workflow_status WHERE forked_from = $1",
        [original_workflow_id]
      )

    count
  end

  test "a workflow that enqueues a job and crashes before completing recovers to exactly one extra enqueued workflow" do
    engine =
      start_engine([
        {"enqueue_and_die/1", {SampleWorkflows, :enqueue_and_die, 1}},
        {"counting_child/1", {SampleWorkflows, :counting_workflow, 1}}
      ])

    config = Dbos.config(engine)
    table = new_table()
    parent_id = "wf-enqueue-dies-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(config, %{
      workflow_id: parent_id,
      status: :pending,
      name: "enqueue_and_die/1",
      inputs: [table]
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(engine, parent_id, {SampleWorkflows, :enqueue_and_die, 1}, [
        table
      ])

    wait_until(fn -> WorkflowSup.whereis(engine, parent_id) == :error end)

    Recovery.recover_pending(engine)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, parent_id)
      status.status == :success
    end)

    assert count_workflows_named(config, "counting_child/1") == 1
  end

  test "a workflow that forks another workflow and crashes before completing recovers to exactly one extra fork" do
    engine =
      start_engine([
        {"fork_and_die/3", {SampleWorkflows, :fork_and_die, 3}},
        {"add/2", {SampleWorkflows, :add, 2}}
      ])

    config = Dbos.config(engine)
    table = new_table()

    {:ok, target_handle} = Dbos.start("add/2", [1, 2], engine: engine)
    assert {:ok, 3} = Dbos.await(target_handle)

    parent_id = "wf-fork-dies-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(config, %{
      workflow_id: parent_id,
      status: :pending,
      name: "fork_and_die/3",
      inputs: [table, target_handle.workflow_id, 0]
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(
        engine,
        parent_id,
        {SampleWorkflows, :fork_and_die, 3},
        [table, target_handle.workflow_id, 0]
      )

    wait_until(fn -> WorkflowSup.whereis(engine, parent_id) == :error end)

    Recovery.recover_pending(engine)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, parent_id)
      status.status == :success
    end)

    assert count_forks_of(config, target_handle.workflow_id) == 1
  end

  test "a workflow reading another workflow's status replays the recorded status across recovery, even after the target changes" do
    engine =
      start_engine([
        {"status_reader_then_die/2", {SampleWorkflows, :status_reader_then_die, 2}},
        {"add/2", {SampleWorkflows, :add, 2}}
      ])

    config = Dbos.config(engine)
    table = new_table()

    {:ok, target_handle} = Dbos.start("add/2", [1, 2], engine: engine)
    assert {:ok, 3} = Dbos.await(target_handle)

    parent_id = "wf-status-dies-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(config, %{
      workflow_id: parent_id,
      status: :pending,
      name: "status_reader_then_die/2",
      inputs: [table, target_handle.workflow_id]
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(
        engine,
        parent_id,
        {SampleWorkflows, :status_reader_then_die, 2},
        [table, target_handle.workflow_id]
      )

    wait_until(fn -> :ets.lookup(table, :status_result) != [] end)
    wait_until(fn -> WorkflowSup.whereis(engine, parent_id) == :error end)

    [{:status_result, {:ok, original_recorded_status}}] = :ets.lookup(table, :status_result)
    assert original_recorded_status.output == 3

    Dbos.DB.Postgrex.query!(
      config.conn,
      "UPDATE dbos.workflow_status SET output = $1 WHERE workflow_uuid = $2",
      [Dbos.Serialization.encode(999), target_handle.workflow_id]
    )

    :ets.delete(table, :status_result)

    Recovery.recover_pending(engine)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, parent_id)
      status.status == :success
    end)

    [{:status_result, {:ok, replayed_status}}] = :ets.lookup(table, :status_result)
    assert replayed_status.status == :success
    assert replayed_status.output == 3
  end

  test "the exact (function_id, function_name) sequence for a workflow using enqueue, fork, status, and a plain step" do
    engine =
      start_engine([
        {"enqueue_fork_status_layout/1",
         {SampleWorkflows, :enqueue_fork_status_layout_workflow, 1}},
        {"add/2", {SampleWorkflows, :add, 2}}
      ])

    config = Dbos.config(engine)

    {:ok, target_handle} = Dbos.start("add/2", [1, 2], engine: engine)
    assert {:ok, 3} = Dbos.await(target_handle)

    {:ok, handle} =
      Dbos.start("enqueue_fork_status_layout/1", [target_handle.workflow_id], engine: engine)

    assert {:ok, _} = Dbos.await(handle)

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)

    assert Enum.map(steps, &{&1.function_id, &1.function_name}) == [
             {0, "DBOS.enqueue"},
             {1, "DBOS.forkWorkflow"},
             {2, "DBOS.getStatus"},
             {3, "plain_step/0"}
           ]
  end

  test "a workflow that cancels another workflow and crashes before completing recovers to exactly one DBOS.cancelWorkflow checkpoint" do
    engine =
      start_engine([
        {"cancel_and_die/2", {SampleWorkflows, :cancel_and_die, 2}},
        {"sleep_forever/1", {SampleWorkflows, :sleep_forever, 1}}
      ])

    config = Dbos.config(engine)
    table = new_table()

    {:ok, target_handle} = Dbos.start("sleep_forever/1", [nil], engine: engine)
    wait_until(fn -> match?({:ok, _}, WorkflowSup.whereis(engine, target_handle.workflow_id)) end)

    parent_id = "wf-cancel-dies-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(config, %{
      workflow_id: parent_id,
      status: :pending,
      name: "cancel_and_die/2",
      inputs: [table, target_handle.workflow_id]
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(
        engine,
        parent_id,
        {SampleWorkflows, :cancel_and_die, 2},
        [table, target_handle.workflow_id]
      )

    wait_until(fn -> WorkflowSup.whereis(engine, parent_id) == :error end)

    Recovery.recover_pending(engine)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, parent_id)
      status.status == :success
    end)

    {:ok, steps} = SystemDb.get_workflow_steps(config, parent_id)
    assert Enum.map(steps, & &1.function_name) == ["DBOS.cancelWorkflow"]

    {:ok, target_status} = SystemDb.get_workflow_status(config, target_handle.workflow_id)
    assert target_status.status == :cancelled
  end

  test "a workflow that resumes another workflow and crashes before completing recovers to exactly one DBOS.resumeWorkflow checkpoint" do
    engine =
      start_engine([
        {"resume_and_die/2", {SampleWorkflows, :resume_and_die, 2}},
        {"three_steps/1", {SampleWorkflows, :three_steps, 1}}
      ])

    config = Dbos.config(engine)
    table = new_table()

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-resume-target",
      status: :cancelled,
      name: "three_steps/1",
      inputs: ["ord_1"]
    })

    parent_id = "wf-resume-dies-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(config, %{
      workflow_id: parent_id,
      status: :pending,
      name: "resume_and_die/2",
      inputs: [table, "wf-resume-target"]
    })

    {:ok, _pid} =
      WorkflowSup.start_workflow(
        engine,
        parent_id,
        {SampleWorkflows, :resume_and_die, 2},
        [table, "wf-resume-target"]
      )

    wait_until(fn -> WorkflowSup.whereis(engine, parent_id) == :error end)

    Recovery.recover_pending(engine)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, parent_id)
      status.status == :success
    end)

    {:ok, steps} = SystemDb.get_workflow_steps(config, parent_id)
    assert Enum.map(steps, & &1.function_name) == ["DBOS.resumeWorkflow"]

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, "wf-resume-target")
      status.status == :success
    end)
  end

  test "the exact (function_id, function_name) sequence for a workflow using cancel and resume" do
    engine =
      start_engine([
        CheckoutWorkflow,
        {"sleep_forever/1", {SampleWorkflows, :sleep_forever, 1}}
      ])

    config = Dbos.config(engine)

    {:ok, target_handle} = Dbos.start("sleep_forever/1", [nil], engine: engine)
    wait_until(fn -> match?({:ok, _}, WorkflowSup.whereis(engine, target_handle.workflow_id)) end)

    {:ok, handle} =
      Dbos.start("cancels_and_resumes_other_workflow", [target_handle.workflow_id],
        engine: engine
      )

    assert {:ok, :ok} = Dbos.await(handle)

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)

    assert Enum.map(steps, &{&1.function_id, &1.function_name}) == [
             {0, "DBOS.cancelWorkflow"},
             {1, "DBOS.resumeWorkflow"}
           ]
  end

  test "outside a workflow, cancel and resume still consume no ids and write no checkpoint" do
    engine =
      start_engine([
        {"sleep_forever/1", {SampleWorkflows, :sleep_forever, 1}}
      ])

    config = Dbos.config(engine)

    {:ok, target_handle} = Dbos.start("sleep_forever/1", [nil], engine: engine)
    wait_until(fn -> match?({:ok, _}, WorkflowSup.whereis(engine, target_handle.workflow_id)) end)

    :ok = Dbos.cancel(target_handle.workflow_id, engine: engine)
    :ok = Dbos.resume(target_handle.workflow_id, engine: engine)

    {:ok, %{rows: [[reserved_checkpoint_count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT count(*) FROM dbos.operation_outputs WHERE function_name IN ($1, $2)",
        ["DBOS.cancelWorkflow", "DBOS.resumeWorkflow"]
      )

    assert reserved_checkpoint_count == 0
  end

  test "outside a workflow, enqueue, fork, and status still consume no ids and write no checkpoint" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)

    {:ok, target_handle} = Dbos.start("add/2", [1, 2], engine: engine)
    assert {:ok, 3} = Dbos.await(target_handle)

    {:ok, _enqueue_handle} =
      Dbos.enqueue("add/2", [4, 5], queue_name: Dbos.Queue.internal_queue_name(), engine: engine)

    {:ok, _fork_handle} = Dbos.fork(target_handle.workflow_id, 0, engine: engine)
    assert {:ok, %{status: :success}} = Dbos.status(target_handle.workflow_id, engine: engine)

    {:ok, %{rows: [[reserved_checkpoint_count]]}} =
      Dbos.DB.Postgrex.query(
        config.conn,
        "SELECT count(*) FROM dbos.operation_outputs WHERE function_name IN ($1, $2, $3)",
        ["DBOS.enqueue", "DBOS.forkWorkflow", "DBOS.getStatus"]
      )

    assert reserved_checkpoint_count == 0

    {:ok, target_steps} = SystemDb.get_workflow_steps(config, target_handle.workflow_id)
    assert target_steps == []
  end
end
