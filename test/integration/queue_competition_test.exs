defmodule Dbos.Integration.QueueCompetitionTest do
  @moduledoc """
  Phase 3 placeholder: queue-backed competition between two nodes.

  Scenario to implement once Phase 3 lands dequeueing:

  1. Enqueue `N` workflows onto a shared named queue (`insert_enqueued_workflow`/`ENQUEUED`
     status) before either node's dequeue loop is running, so both nodes start out competing
     for the same backlog.
  2. Start both `node1` and `node2` with their dequeue loops running against the same queue.
  3. Each node repeatedly claims and runs whatever workflows it successfully dequeues, racing
     the other node for the rest.
  4. Once every workflow reaches a terminal status, assert:
     - `execution_log` (the durable, deduplicated table already used by the other scenarios in
       this suite) has exactly `N` rows — one per workflow's single step.
     - `execution_attempts` (the append-only table) also has exactly `N` rows — no workflow's
       step body ran twice, i.e. the dequeue claim itself (not just the checkpoint) is race-free.
     - No workflow was claimed by both nodes: each workflow's `operation_outputs` row records
       only the `executor_id` of whichever node actually dequeued it.

  This depends on the queue's `FOR UPDATE SKIP LOCKED`-style dequeue claim existing in `lib/`,
  which Phase 2b does not yet implement (`Dbos.Recovery`'s `@moduledoc` explicitly defers
  dequeueing to Phase 3). Until then this test is skipped rather than deleted, so the scenario
  stays written down where Phase 3 will find it.
  """

  use ExUnit.Case, async: false

  @moduletag :integration

  @tag :skip
  test "N enqueued workflows, two nodes competing, exactly N executions, zero duplicates" do
    flunk("Phase 3: no queue dequeue exists yet in lib/ to exercise")
  end
end
