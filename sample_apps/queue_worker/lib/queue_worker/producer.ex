defmodule QueueWorker.Producer do
  @moduledoc """
  Enqueues a batch of tasks onto the `"tasks"` queue and reports on their progress and results —
  the "one place enqueues, workers elsewhere execute" side of the sample.
  """

  alias Dbos.WorkflowHandle

  @queue_name "tasks"

  @doc "Enqueues `count` tasks under `batch_id`, returning one handle per task in submission order."
  def enqueue_batch(batch_id, count) do
    for task_number <- 1..count do
      {:ok, handle} =
        Dbos.enqueue("process_task", [batch_id, task_number],
          queue_name: @queue_name,
          workflow_id: "#{batch_id}-#{task_number}"
        )

      handle
    end
  end

  @doc "Fetches each handle's current status, without blocking for completion."
  def statuses(handles) do
    Enum.map(handles, fn %WorkflowHandle{workflow_id: workflow_id} ->
      {:ok, status} = Dbos.status(workflow_id)
      status
    end)
  end

  @doc "How many of `handles` have reached a terminal SUCCESS status."
  def completed_count(handles) do
    handles
    |> statuses()
    |> Enum.count(&(&1.status == :success))
  end

  @doc "Awaits every handle, in order, returning the list of unwrapped results."
  def await_all(handles) do
    Enum.map(handles, fn %WorkflowHandle{} = handle ->
      {:ok, result} = Dbos.await(handle, timeout_ms: 30_000)
      result
    end)
  end
end
