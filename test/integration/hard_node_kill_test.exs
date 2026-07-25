Code.require_file("support/harness.ex", __DIR__)

defmodule Dbos.Integration.HardNodeKillTest do
  @moduledoc """
  A whole node dies mid-workflow, holding an in-flight execution; a second node recovers it
  without duplicating any step's execution.
  """

  use ExUnit.Case, async: false

  alias Dbos.Integration.Harness

  @moduletag :integration
  @moduletag timeout: 120_000

  setup_all do
    Harness.up!()
    {:ok, conn: Harness.conn()}
  end

  test "node1 is SIGKILLed mid-workflow; node2 recovers it with every step run exactly once", %{
    conn: conn
  } do
    workflow_id = "hard-kill-#{System.unique_integer([:positive])}"

    {:ok, _handle} =
      Harness.rpc(:node1, Dbos, :start, [
        "hard_kill_workflow/1",
        [:ignored],
        [workflow_id: workflow_id]
      ])

    Harness.wait_until(fn -> Harness.execution_count(conn, workflow_id, "reserve/1") == 1 end)

    Harness.wait_until(fn ->
      Harness.operation_output_function_ids(conn, workflow_id) == [0]
    end)

    Harness.kill!(:node1)

    Harness.reassign_executor!(conn, workflow_id, "node2")
    :ok = Harness.rpc(:node2, Dbos.Recovery, :recover_pending, [Dbos])

    Harness.release!(conn, workflow_id)

    Harness.wait_until(fn ->
      match?({:ok, %{status: :success}}, Harness.rpc(:node2, Dbos, :status, [workflow_id]))
    end)

    assert {:ok, status} = Harness.rpc(:node2, Dbos, :status, [workflow_id])
    assert status.status == :success

    assert Harness.operation_output_function_ids(conn, workflow_id) == [0, 1]
    assert Harness.execution_count(conn, workflow_id, "reserve/1") == 1
    assert Harness.execution_count(conn, workflow_id, "await_release/1") == 1
    assert Harness.attempt_count(conn, workflow_id, "reserve/1") == 1
    assert Harness.attempt_count(conn, workflow_id, "await_release/1") == 1

    Harness.start!(:node1)
  end
end
