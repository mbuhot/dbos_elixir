Code.require_file("support/harness.ex", __DIR__)

defmodule Dbos.Integration.QueueCompetitionTest do
  @moduledoc """
  Queue-backed competition between two nodes sharing one database and one queue.

  Both `node1` and `node2` boot with a `"queue-competition"` queue registered and a dequeue loop
  already polling it (`test/integration/node_runtime.exs`). This test enqueues `N` workflows onto
  that queue, then lets both nodes race to claim and run them via `Dbos.SystemDb.dequeue_workflows/3`'s
  `FOR UPDATE SKIP LOCKED`/`NOWAIT` claim. Once every workflow reaches `SUCCESS`, it asserts:

  - `execution_log` (the durable, deduplicated table also used by `concurrent_start_test.exs`) has
    exactly one row per workflow's single step.
  - `execution_attempts` (the append-only table) also has exactly one row per workflow's step —
    no workflow's step body ran twice, i.e. the dequeue claim itself (not just the checkpoint) is
    race-free.
  - Every workflow's `execution_log` row records the `executor_id` of whichever node actually
    dequeued and ran it, and every recorded `executor_id` is one of the two live nodes — no
    workflow was claimed by both.
  """

  use ExUnit.Case, async: false

  alias Dbos.Integration.Harness

  @moduletag :integration
  @moduletag timeout: 60_000

  setup_all do
    Harness.up!()
    {:ok, conn: Harness.conn()}
  end

  test "N enqueued workflows, two nodes competing, exactly N executions, zero duplicates", %{
    conn: conn
  } do
    n = 50
    prefix = "queue-competition-#{System.unique_integer([:positive])}-"

    workflow_ids =
      for i <- 1..n do
        workflow_id = "#{prefix}#{i}"

        {:ok, %{workflow_id: ^workflow_id}} =
          Harness.rpc(:node1, Dbos, :enqueue, [
            "queue_competition_workflow/1",
            [i],
            [queue_name: "queue-competition", workflow_id: workflow_id]
          ])

        workflow_id
      end

    Harness.wait_until(
      fn ->
        Enum.all?(workflow_ids, fn workflow_id ->
          match?({:ok, %{status: :success}}, Harness.rpc(:node1, Dbos, :status, [workflow_id]))
        end)
      end,
      300
    )

    %{rows: [[success_count]]} =
      Postgrex.query!(
        conn,
        "SELECT count(*) FROM \"dbos\".workflow_status WHERE status = 'SUCCESS' AND workflow_uuid LIKE $1",
        ["#{prefix}%"]
      )

    assert success_count == n

    executor_ids =
      for workflow_id <- workflow_ids do
        assert Harness.execution_count(conn, workflow_id, "record/1") == 1
        assert Harness.attempt_count(conn, workflow_id, "record/1") == 1
        assert Harness.operation_output_function_ids(conn, workflow_id) == [0]

        %{rows: [[executor_id]]} =
          Postgrex.query!(
            conn,
            "SELECT executor_id FROM execution_log WHERE workflow_id = $1 AND step_name = 'record/1'",
            [workflow_id]
          )

        executor_id
      end

    assert MapSet.new(executor_ids) |> MapSet.subset?(MapSet.new(["node1", "node2"]))
  end
end
