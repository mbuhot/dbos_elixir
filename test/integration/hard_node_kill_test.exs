Code.require_file("support/harness.ex", __DIR__)

defmodule Dbos.Integration.HardNodeKillTest do
  @moduledoc """
  A whole node dies mid-workflow, holding an in-flight execution; the surviving node picks it up
  on its own and finishes it without duplicating any step's execution.

  Nothing in the test reassigns the row. An expired executor lease is the only automatic
  dead-executor signal the engine has: `Dbos.Lease` stops renewing the instant the node dies, and
  once the lease lapses the surviving node's `Dbos.LeaseSweep` reclaims and redispatches what it
  owned. `test/integration/node_runtime.exs` runs both nodes on a 5s lease TTL and a 1s sweep
  interval so that handover lands in seconds rather than the production default's minute.
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

  test "a SIGKILLed node's in-flight workflow finishes on the surviving node with every step run exactly once",
       %{conn: conn} do
    workflow_id = "hard-kill-#{System.unique_integer([:positive])}"

    {:ok, _handle} =
      Harness.rpc(:node1, Dbos, :start, [
        "hard_kill_workflow/1",
        [:ignored],
        [workflow_id: workflow_id]
      ])

    Harness.wait_until(fn -> Harness.execution_count(conn, workflow_id, "reserve/1") == 1 end)
    Harness.wait_until(fn -> Harness.operation_output_function_ids(conn, workflow_id) == [0] end)

    Harness.kill!(:node1)
    Harness.release!(conn, workflow_id)

    Harness.wait_until(
      fn ->
        match?({:ok, %{status: :success}}, Harness.rpc(:node2, Dbos, :status, [workflow_id]))
      end,
      600
    )

    assert Harness.owner_and_status(conn, workflow_id).executor_id == "node2"

    assert Harness.operation_output_function_ids(conn, workflow_id) == [0, 1]
    assert Harness.execution_count(conn, workflow_id, "reserve/1") == 1
    assert Harness.execution_count(conn, workflow_id, "await_release/1") == 1
    assert Harness.attempt_count(conn, workflow_id, "reserve/1") == 1
    assert Harness.attempt_count(conn, workflow_id, "await_release/1") == 1
  end
end
