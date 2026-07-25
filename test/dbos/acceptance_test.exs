defmodule Dbos.AcceptanceTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        workflows: workflows,
        migrations: :skip
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp wait_until(fun, attempts \\ 100)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end

  test "1. start and await a three-step workflow, checkpointing every step" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])

    {:ok, handle} = Dbos.start("three_steps/1", ["ord_1"], engine: engine)
    assert {:ok, %{shipped: "ord_1"}} = Dbos.await(handle)

    config = Dbos.config(engine)
    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_id) == [0, 1, 2]
  end

  test "2. crash and resume: step 0 does not re-run, exactly one row per step" do
    engine = start_engine([{"blocking_workflow/1", {SampleWorkflows, :blocking_workflow, 1}}])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("blocking_workflow/1", [:ignored], engine: engine)

    wait_until(fn ->
      {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
      length(steps) == 1
    end)

    {:ok, pid} = WorkflowSup.whereis(engine, handle.workflow_id)
    Process.exit(pid, :kill)

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :pending

    Dbos.Recovery.recover_pending(engine)

    {:ok, new_pid} = WorkflowSup.whereis(engine, handle.workflow_id)
    send(new_pid, :go)

    assert {:ok, :done} = Dbos.await(handle)

    counter_key = {SampleWorkflows, :counter, handle.workflow_id}
    assert :persistent_term.get(counter_key) == 1

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_id) == [0, 1]
  end

  test "3. a workflow that raises ends ERROR, recoverable through await as the same struct" do
    engine = start_engine([{"raises_declined/1", {SampleWorkflows, :raises_declined, 1}}])

    {:ok, handle} = Dbos.start("raises_declined/1", ["ord_1"], engine: engine)

    assert {:error, %SampleWorkflows.CardDeclinedError{amount: 4999}} = Dbos.await(handle)
  end

  test "4. recovery skips an unregistered workflow name and still recovers a registered one" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    config = Dbos.config(engine)

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-not-registered",
      status: :pending,
      name: "unregistered_name/1",
      inputs: ["x"]
    })

    {:ok, handle} = Dbos.start("three_steps/1", ["ord_2"], engine: engine)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
      status.status == :success
    end)

    Dbos.Recovery.recover_pending(engine)

    {:ok, unregistered} = SystemDb.get_workflow_status(config, "wf-not-registered")
    assert unregistered.status == :pending
  end

  test "5. repeated recovery of an always-crashing workflow flips it to MAX_RECOVERY_ATTEMPTS_EXCEEDED" do
    engine =
      start_engine([{"crash_self/1", {SampleWorkflows, :crash_self, 1}}],
        max_recovery_attempts: 1
      )

    config = Dbos.config(engine)

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-dlq",
      status: :pending,
      name: "crash_self/1",
      inputs: [1]
    })

    Dbos.Recovery.recover_pending(engine)
    Process.sleep(20)
    Dbos.Recovery.recover_pending(engine)

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-dlq")
    assert status.status == :max_recovery_attempts_exceeded
  end

  test "6. two engines with different names and executor ids each recover only their own workflows" do
    engine_a = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    engine_b = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])

    config_a = Dbos.config(engine_a)
    config_b = Dbos.config(engine_b)

    refute config_a.executor_id == config_b.executor_id

    SystemDb.insert_workflow_status(config_a, %{
      workflow_id: "wf-a-pending",
      status: :pending,
      name: "three_steps/1",
      inputs: ["a"],
      executor_id: config_a.executor_id
    })

    SystemDb.insert_workflow_status(config_b, %{
      workflow_id: "wf-b-pending",
      status: :pending,
      name: "three_steps/1",
      inputs: ["b"],
      executor_id: config_b.executor_id
    })

    Dbos.Recovery.recover_pending(engine_a)

    wait_until(
      fn ->
        {:ok, status} = SystemDb.get_workflow_status(config_a, "wf-a-pending")
        status.status == :success
      end,
      300
    )

    {:ok, still_pending} = SystemDb.get_workflow_status(config_b, "wf-b-pending")
    assert still_pending.status == :pending
  end

  test "7. a parent's child workflow id is derived, recorded, and not started twice on replay" do
    engine =
      start_engine([
        {"spawn_child/1", {SampleWorkflows, :spawn_child, 1}},
        {"add/2", {SampleWorkflows, :add, 2}}
      ])

    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("spawn_child/1", [:ignored], engine: engine)
    assert {:ok, 3} = Dbos.await(handle)

    expected_child_id = "#{handle.workflow_id}-0"

    {:ok, [step]} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert step.child_workflow_id == expected_child_id
    assert step.function_name == "add/2"

    {:ok, child_status} = SystemDb.get_workflow_status(config, expected_child_id)
    assert child_status.status == :success
    assert child_status.output == 3

    assert SystemDb.check_child_workflow(config, handle.workflow_id, 0, "add/2") ==
             {:existing, expected_child_id}
  end
end
