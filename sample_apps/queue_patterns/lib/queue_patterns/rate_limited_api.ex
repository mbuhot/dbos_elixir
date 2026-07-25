defmodule QueuePatterns.RateLimitedApi do
  @moduledoc """
  Rate-limited external API: a queue caps how many workflow starts leave it within a sliding
  window, so a downstream service that enforces its own quota never sees more traffic than it
  allows, no matter how many callers enqueue work at once.

  **Problem**: a burst of callers all want to hit a third-party API with a hard rate limit (say,
  an LLM provider billed and throttled per minute).

  **Observe**: enqueue far more calls at once than the limit allows — `queue_patterns_dev`'s
  `"rate_limited_api"` queue is declared `rate_limit: %{limit: 2, period_ms: 1_000}` — and every
  `call_api/1`'s recorded `started_at_epoch_ms` still respects at most 2 starts per second, however
  many requests were enqueued in the same instant.
  """

  use Dbos

  defworkflow call_api(request_id), name: "call_api" do
    do_call(request_id)
  end

  defstep do_call(request_id) do
    %{request_id: request_id, called_at: System.os_time(:millisecond)}
  end
end
