defmodule QueuePatterns.Priority do
  @moduledoc """
  Priority: urgent work jumps the queue.

  **Problem**: most enqueued work should run in submission order, but some requests (a paying
  customer, an on-call alert) need to run before whatever is already waiting.

  **Ordering rule**: a queue declared `priority_enabled: true` claims candidates in
  `priority ASC, created_at ASC` order — a lower `:priority` number runs first, and rows with
  equal priority run in the order they were enqueued. `priority: 0` (the default) sorts before
  anything positive, so most callers only need to set it on the rows that should jump the queue.

  **Observe**: enqueue several normal-priority requests, then one with a lower `:priority` number
  — on a queue with `worker_concurrency: 1` (so only one request runs at a time), the low-number
  request's `started_at_epoch_ms` comes before the ones already waiting when it arrived.
  """

  use Dbos

  defworkflow handle_request(request_id), name: "handle_request" do
    do_handle(request_id)
  end

  defstep do_handle(request_id) do
    %{request_id: request_id, handled_at: System.os_time(:millisecond)}
  end
end
