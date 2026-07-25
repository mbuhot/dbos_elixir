defmodule QueueWorker.Tasks do
  @moduledoc """
  A durable worker: `process_task/2` runs one task through two checkpointed steps. Enqueue a
  batch of tasks onto the `"tasks"` queue and every one of them reaches `SUCCESS` exactly once, no
  matter how many worker processes crash and get recovered along the way — a step whose
  checkpoint already committed is never re-run; only the step that was actually in flight when a
  worker died runs again, picking up from there.

  `execution_count/3` reads a `:persistent_term` counter each step bumps before doing its work —
  informational instrumentation the demo task prints, not a correctness guarantee: a step killed
  before its own checkpoint commits can bump its counter more than once, which is expected. What
  `Dbos` guarantees is that `dbos.operation_outputs` ends up with exactly one row per step per
  workflow, however many attempts it took to get there.
  """

  use Dbos

  defworkflow process_task(batch_id, task_number), name: "process_task" do
    claim_task(batch_id, task_number)
    do_work(batch_id, task_number)
  end

  defstep claim_task(batch_id, task_number) do
    bump_execution_count(:claim_task, batch_id, task_number)
    %{batch_id: batch_id, task_number: task_number, status: :claimed}
  end

  defstep do_work(batch_id, task_number) do
    bump_execution_count(:do_work, batch_id, task_number)
    Process.sleep(200)
    %{batch_id: batch_id, task_number: task_number, processed_at: System.os_time(:millisecond)}
  end

  @doc "How many times `step`'s body has actually executed for `batch_id`/`task_number`."
  def execution_count(step, batch_id, task_number) when step in [:claim_task, :do_work] do
    :persistent_term.get({__MODULE__, step, batch_id, task_number}, 0)
  end

  defp bump_execution_count(step, batch_id, task_number) do
    key = {__MODULE__, step, batch_id, task_number}
    :persistent_term.put(key, :persistent_term.get(key, 0) + 1)
  end
end
