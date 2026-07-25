Code.require_file("support/harness.ex", __DIR__)

defmodule Dbos.Integration.RecoveryOwnershipTest do
  @moduledoc """
  Node1 dies while holding two in-flight workflows. Only one of them gets reassigned to node2 —
  the ops action a real deployment would take to hand a dead executor's rows to a live one (see
  `Dbos.Integration.Harness.reassign_executor!/3`). Node2's own `Dbos.Recovery.recover_pending/1`
  then recovers exactly the workflow reassigned to it, and never touches its still-node1-owned
  peer: `Dbos.Recovery.recover_pending/1` only ever scans for rows matching its own
  `config.executor_id`, so an un-reassigned row is invisible to it by construction. This test
  makes that boundary observable end to end.
  """

  use ExUnit.Case, async: false

  alias Dbos.Integration.Harness

  @moduletag :integration
  @moduletag timeout: 120_000

  setup_all do
    Harness.up!()
    {:ok, conn: Harness.conn(), system_db: Harness.system_db_config()}
  end

  test "node2 recovers only the workflow reassigned to it, leaving node1's other row untouched",
       %{conn: conn, system_db: system_db} do
    reassigned_id = "ownership-reassigned-#{System.unique_integer([:positive])}"
    untouched_id = "ownership-untouched-#{System.unique_integer([:positive])}"

    {:ok, _handle} =
      Harness.rpc(:node1, Dbos, :start, [
        "hard_kill_workflow/1",
        [:ignored],
        [workflow_id: reassigned_id]
      ])

    {:ok, _handle} =
      Harness.rpc(:node1, Dbos, :start, [
        "hard_kill_workflow/1",
        [:ignored],
        [workflow_id: untouched_id]
      ])

    Harness.wait_until(fn -> Harness.execution_count(conn, reassigned_id, "reserve/1") == 1 end)
    Harness.wait_until(fn -> Harness.execution_count(conn, untouched_id, "reserve/1") == 1 end)

    {:ok, untouched_before} = Dbos.SystemDb.get_workflow_status(system_db, untouched_id)
    assert untouched_before.status == :pending
    assert untouched_before.executor_id == "node1"

    Harness.kill!(:node1)

    Harness.reassign_executor!(conn, reassigned_id, "node2")
    :ok = Harness.rpc(:node2, Dbos.Recovery, :recover_pending, [Dbos])

    Harness.release!(conn, reassigned_id)

    Harness.wait_until(fn ->
      match?({:ok, %{status: :success}}, Harness.rpc(:node2, Dbos, :status, [reassigned_id]))
    end)

    assert Harness.execution_count(conn, reassigned_id, "reserve/1") == 1
    assert Harness.execution_count(conn, reassigned_id, "await_release/1") == 1
    assert Harness.attempt_count(conn, reassigned_id, "reserve/1") == 1
    assert Harness.attempt_count(conn, reassigned_id, "await_release/1") == 1

    {:ok, untouched_after} = Dbos.SystemDb.get_workflow_status(system_db, untouched_id)
    assert untouched_after.status == :pending
    assert untouched_after.executor_id == "node1"
    assert untouched_after.recovery_attempts == untouched_before.recovery_attempts

    assert Harness.execution_count(conn, untouched_id, "reserve/1") == 1
    assert Harness.attempt_count(conn, untouched_id, "reserve/1") == 1
    assert Harness.execution_count(conn, untouched_id, "await_release/1") == 0

    Harness.start!(:node1)
  end
end
