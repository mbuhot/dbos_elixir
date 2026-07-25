defmodule QueuePatterns.Deduplication do
  @moduledoc """
  Deduplication: collapsing duplicate submissions.

  **Problem**: the same logical request (say, "generate this report") might get submitted more
  than once — a retried HTTP request, a double click — and should only ever run once while the
  first submission is still enqueued or in flight.

  **Solution**: `:deduplication_id` reserves a single slot per `(queue_name, deduplication_id)`
  pair. A second `Dbos.enqueue/3` call with the same pair, while the first is still enqueued or
  running, raises `Dbos.QueueDeduplicatedError` (carrying the id of whichever workflow already
  holds the slot) instead of inserting a second row.

  **Observe**: enqueue `generate_report/1` for a given `report_id` with
  `deduplication_id: "report-\#{report_id}"`, then immediately try to enqueue it again with the
  same id — the second call raises, and only one workflow ever runs for that report.
  """

  use Dbos

  defworkflow generate_report(report_id), name: "generate_report" do
    do_generate(report_id)
  end

  defstep do_generate(report_id) do
    %{report_id: report_id, generated_at: System.os_time(:millisecond)}
  end
end
