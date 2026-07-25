defmodule QueuePatterns.FanOut do
  @moduledoc """
  Fan-out / fan-in: a parent workflow enqueues N children onto a shared queue, then awaits every
  child's result and collects them back in submission order.

  **Problem**: a batch of independent items (rows, files, API calls) needs to run concurrently,
  bounded by the queue's `worker_concurrency`, with the caller getting one combined result back.

  **Observe**: `process_batch/2`'s result is a list the same length and order as `items`, each
  entry the corresponding child's squared value — fan-in reassembles fanned-out work in the order
  it was requested, not the order children happen to finish.
  """

  use Dbos

  defworkflow process_batch(batch_id, items), name: "process_batch" do
    handles =
      Enum.map(items, fn item ->
        {:ok, handle} = Dbos.enqueue(&process_item/2, [batch_id, item], queue_name: "fan_out")
        handle
      end)

    Enum.map(handles, fn handle ->
      {:ok, result} = Dbos.await(handle)
      result
    end)
  end

  defworkflow process_item(batch_id, item), name: "process_item" do
    square(batch_id, item)
  end

  defstep square(batch_id, item) do
    %{batch_id: batch_id, item: item, result: item * item}
  end
end
