defmodule QueuePatterns.FanOutTest do
  use ExUnit.Case, async: false

  alias QueuePatterns.FanOut

  setup do
    start_supervised!(
      {Dbos.Supervisor,
       db: {Dbos.DB.Ecto, QueuePatterns.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [FanOut],
       queues: [Dbos.Queue.new("fan_out", worker_concurrency: 5, base_polling_interval_ms: 20)],
       migrations: :verify}
    )

    Dbos.Recovery.await_boot_recovery(Dbos)
    :ok
  end

  test "fans out N children onto the queue and fans their results back in, in submission order" do
    batch_id = "batch-#{System.unique_integer([:positive])}"

    {:ok, handle} = FanOut.process_batch(batch_id, [1, 2, 3, 4, 5])

    assert {:ok, results} = Dbos.await(handle, timeout_ms: 10_000)

    assert Enum.map(results, & &1.item) == [1, 2, 3, 4, 5]
    assert Enum.map(results, & &1.result) == [1, 4, 9, 16, 25]
    assert Enum.all?(results, &(&1.batch_id == batch_id))
  end
end
