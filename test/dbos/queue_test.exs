defmodule Dbos.QueueTest do
  use ExUnit.Case, async: true

  alias Dbos.Queue

  test "defaults match upstream: unbounded concurrency, no rate limit, priority/partition off" do
    queue = Queue.new("orders")

    assert queue.name == "orders"
    assert queue.worker_concurrency == nil
    assert queue.global_concurrency == nil
    assert queue.rate_limit == nil
    assert queue.priority_enabled == false
    assert queue.partition_queue == false
    assert queue.base_polling_interval_ms == 1_000
    assert queue.max_polling_interval_ms == 120_000
  end

  test "every option is settable" do
    queue =
      Queue.new("orders",
        worker_concurrency: 5,
        global_concurrency: 10,
        rate_limit: %{limit: 3, period_ms: 60_000},
        priority_enabled: true,
        partition_queue: true,
        base_polling_interval_ms: 250
      )

    assert queue.worker_concurrency == 5
    assert queue.global_concurrency == 10
    assert queue.rate_limit == %{limit: 3, period_ms: 60_000}
    assert queue.priority_enabled == true
    assert queue.partition_queue == true
    assert queue.base_polling_interval_ms == 250
  end

  test "worker_concurrency greater than global_concurrency is rejected" do
    assert_raise Dbos.InvalidQueueOptionError, fn ->
      Queue.new("orders", worker_concurrency: 10, global_concurrency: 5)
    end
  end

  test "a non-positive base_polling_interval_ms is rejected" do
    assert_raise Dbos.InvalidQueueOptionError, fn ->
      Queue.new("orders", base_polling_interval_ms: 0)
    end
  end

  test "a non-positive rate limit is rejected" do
    assert_raise Dbos.InvalidQueueOptionError, fn ->
      Queue.new("orders", rate_limit: %{limit: 0, period_ms: 1_000})
    end

    assert_raise Dbos.InvalidQueueOptionError, fn ->
      Queue.new("orders", rate_limit: %{limit: 1, period_ms: 0})
    end
  end

  test "the reserved internal queue name is rejected" do
    assert_raise Dbos.InvalidQueueOptionError, fn ->
      Queue.new(Queue.internal_queue_name())
    end
  end
end
