defmodule Dbos.TimeoutTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

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

  test "a workflow exceeding workflow_timeout_ms ends CANCELLED" do
    engine = start_engine([{"timeout_workflow/1", {SampleWorkflows, :timeout_workflow, 1}}])
    config = Dbos.config(engine)

    {:ok, handle} =
      Dbos.start("timeout_workflow/1", [30_000], timeout_ms: 100, engine: engine)

    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle, timeout_ms: 5_000)

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :cancelled
  end

  test "a workflow that finishes within its timeout is unaffected" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])

    {:ok, handle} = Dbos.start("add/2", [2, 2], timeout_ms: 60_000, engine: engine)
    assert {:ok, 4} = Dbos.await(handle)
  end

  test "a child workflow inherits its parent's deadline" do
    engine =
      start_engine([
        {"parent_with_timeout_child/1", {SampleWorkflows, :parent_with_timeout_child, 1}},
        {"child_deadline_probe/0", {SampleWorkflows, :child_deadline_probe, 0}}
      ])

    started_at = System.os_time(:millisecond)

    {:ok, handle} =
      Dbos.start("parent_with_timeout_child/1", [:ignored], timeout_ms: 10_000, engine: engine)

    assert {:ok, child_deadline_ms} = Dbos.await(handle, timeout_ms: 5_000)

    expected_deadline_ms = started_at + 10_000
    assert_in_delta child_deadline_ms, expected_deadline_ms, 2_000
  end
end
