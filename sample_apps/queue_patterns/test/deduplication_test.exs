defmodule QueuePatterns.DeduplicationTest do
  use ExUnit.Case, async: false

  alias QueuePatterns.Deduplication

  setup do
    start_supervised!(
      {Dbos.Supervisor,
       db: {Dbos.DB.Postgrex, QueuePatterns.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Deduplication],
       queues: [Dbos.Queue.new("dedup_queue", base_polling_interval_ms: 20)],
       migrations: :skip}
    )

    Dbos.Recovery.await_boot_recovery(Dbos)
    :ok
  end

  test "a second submission for the same report is rejected while the first is in flight" do
    {:ok, first} =
      Dbos.enqueue(&Deduplication.generate_report/1, ["report-42"],
        queue_name: "dedup_queue",
        deduplication_id: "report-42"
      )

    assert_raise Dbos.QueueDeduplicatedError, fn ->
      Dbos.enqueue(&Deduplication.generate_report/1, ["report-42"],
        queue_name: "dedup_queue",
        deduplication_id: "report-42"
      )
    end

    assert {:ok, %{report_id: "report-42"}} = Dbos.await(first, timeout_ms: 10_000)
  end
end
