Code.require_file("support/harness.ex", __DIR__)

defmodule Dbos.Integration.ConcurrentStartTest do
  @moduledoc """
  Both nodes call `Dbos.start/3` with the same explicit workflow id at (as close to) the same
  time. `workflow_status` has a primary key on `workflow_uuid` and `operation_outputs` a unique
  constraint on `(workflow_uuid, function_id)`, so exactly one status row and one checkpointed
  step row survive regardless of which node's insert lands first — that part is a straightforward
  consequence of the schema.

  What is not guaranteed by anything in `lib/` today: `Dbos.Runtime.run_step/3` checks whether a
  step already ran and then, separately, executes and checkpoints it — those two things are not
  one atomic operation. If both nodes' processes pass the "not yet run" check before either has
  checkpointed, the step body itself executes on both nodes; only one checkpoint write wins, but
  the side effect the loser's execution performed already happened. This test records every
  actual invocation append-only (`execution_attempts`, no unique constraint) alongside the
  deduplicated count (`execution_log`, unique on `(workflow_id, step_name)`) so that race is
  visible rather than silently hidden by the storage layer's own deduplication.
  """

  use ExUnit.Case, async: false

  alias Dbos.Integration.Harness

  @moduletag :integration
  @moduletag timeout: 60_000

  setup_all do
    Harness.up!()
    {:ok, conn: Harness.conn()}
  end

  test "starting the same workflow id from both nodes concurrently leaves one status row and one checkpointed step",
       %{conn: conn} do
    workflow_id = "concurrent-start-#{System.unique_integer([:positive])}"

    task1 =
      Task.async(fn ->
        Harness.rpc(:node1, Dbos, :start, [
          "concurrent_start_workflow/1",
          [1],
          [workflow_id: workflow_id]
        ])
      end)

    task2 =
      Task.async(fn ->
        Harness.rpc(:node2, Dbos, :start, [
          "concurrent_start_workflow/1",
          [1],
          [workflow_id: workflow_id]
        ])
      end)

    assert {:ok, %{workflow_id: ^workflow_id}} = Task.await(task1)
    assert {:ok, %{workflow_id: ^workflow_id}} = Task.await(task2)

    Harness.wait_until(fn ->
      match?({:ok, %{status: :success}}, Harness.rpc(:node1, Dbos, :status, [workflow_id]))
    end)

    %{rows: [[status_row_count]]} =
      Postgrex.query!(
        conn,
        "SELECT count(*) FROM \"dbos\".workflow_status WHERE workflow_uuid = $1",
        [workflow_id]
      )

    assert status_row_count == 1
    assert Harness.operation_output_function_ids(conn, workflow_id) == [0]
    assert Harness.execution_count(conn, workflow_id, "record/1") == 1

    attempts = Harness.attempt_count(conn, workflow_id, "record/1")
    assert attempts in [1, 2]
  end
end
