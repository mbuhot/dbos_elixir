defmodule QueuePatterns.DeduplicationTest do
  use ExUnit.Case, async: false

  alias QueuePatterns.Deduplication

  setup do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Postgrex, QueuePatterns.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Deduplication],
       queues: [Dbos.Queue.new("dedup_queue", base_polling_interval_ms: 20)],
       migrations: :skip},
      id: name
    )

    Dbos.Recovery.await_boot_recovery(name)
    {:ok, engine: name}
  end

  test "a second submission for the same report is rejected while the first is in flight", %{
    engine: engine
  } do
    {:ok, first} =
      Dbos.enqueue(&Deduplication.generate_report/1, ["report-42"],
        queue_name: "dedup_queue",
        deduplication_id: "report-42",
        engine: engine
      )

    assert_raise Dbos.QueueDeduplicatedError, fn ->
      Dbos.enqueue(&Deduplication.generate_report/1, ["report-42"],
        queue_name: "dedup_queue",
        deduplication_id: "report-42",
        engine: engine
      )
    end

    assert {:ok, %{report_id: "report-42"}} = Dbos.await(first, timeout_ms: 10_000)
  end
end
