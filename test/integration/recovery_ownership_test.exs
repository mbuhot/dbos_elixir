Code.require_file("support/harness.ex", __DIR__)

defmodule Dbos.Integration.RecoveryOwnershipTest do
  @moduledoc """
  Node1 dies while holding two in-flight workflows, and only one of them is a workflow node2 can
  run: `"exclusive_workflow/1"` is registered on node1 alone (`DBOS_HOST_EXCLUSIVE_WORKFLOW` in
  `docker-compose.yml`), standing in for the heterogeneous deployment where not every executor
  implements every workflow.

  Reclaim is capability-filtered, so node2's `Dbos.LeaseSweep` takes over the shared workflow and
  leaves the node1-only one exactly where it is — still `PENDING`, still owned by node1, its
  recovery attempts untouched — waiting for an executor that implements it. This test makes that
  boundary observable end to end across two real nodes.
  """

  use ExUnit.Case, async: false

  alias Dbos.Integration.Harness

  @moduletag :integration
  @moduletag timeout: 180_000

  setup_all do
    Harness.up!()
    {:ok, conn: Harness.conn()}
  end

  setup do
    on_exit(fn -> Harness.start!(:node1) end)
    :ok
  end

  test "the surviving node takes over the dead node's workflow it can run and leaves the one it cannot",
       %{conn: conn} do
    shared_id = "ownership-shared-#{System.unique_integer([:positive])}"
    exclusive_id = "ownership-exclusive-#{System.unique_integer([:positive])}"

    {:ok, _shared_handle} =
      Harness.rpc(:node1, Dbos, :start, [
        "hard_kill_workflow/1",
        [:ignored],
        [workflow_id: shared_id]
      ])

    {:ok, _exclusive_handle} =
      Harness.rpc(:node1, Dbos, :start, [
        "exclusive_workflow/1",
        [:ignored],
        [workflow_id: exclusive_id]
      ])

    Harness.wait_until(fn -> Harness.execution_count(conn, shared_id, "reserve/1") == 1 end)
    Harness.wait_until(fn -> Harness.execution_count(conn, exclusive_id, "reserve/1") == 1 end)

    exclusive_before = Harness.owner_and_status(conn, exclusive_id)
    assert exclusive_before.status == "PENDING"
    assert exclusive_before.executor_id == "node1"

    Harness.kill!(:node1)
    Harness.release!(conn, shared_id)

    Harness.wait_until(
      fn ->
        match?({:ok, %{status: :success}}, Harness.rpc(:node2, Dbos, :status, [shared_id]))
      end,
      600
    )

    assert Harness.owner_and_status(conn, shared_id).executor_id == "node2"
    assert Harness.execution_count(conn, shared_id, "reserve/1") == 1
    assert Harness.execution_count(conn, shared_id, "await_release/1") == 1
    assert Harness.attempt_count(conn, shared_id, "reserve/1") == 1
    assert Harness.attempt_count(conn, shared_id, "await_release/1") == 1

    exclusive_after = Harness.owner_and_status(conn, exclusive_id)
    assert exclusive_after.status == "PENDING"
    assert exclusive_after.executor_id == "node1"
    assert exclusive_after.recovery_attempts == exclusive_before.recovery_attempts

    assert Harness.execution_count(conn, exclusive_id, "reserve/1") == 1
    assert Harness.attempt_count(conn, exclusive_id, "reserve/1") == 1
    assert Harness.execution_count(conn, exclusive_id, "await_release/1") == 0

    Harness.release!(conn, exclusive_id)
  end
end
