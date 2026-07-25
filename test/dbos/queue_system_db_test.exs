defmodule Dbos.QueueSystemDbTest do
  use Dbos.Case, async: false

  alias Dbos.Queue
  alias Dbos.SystemDb

  setup %{conn: conn} do
    config = %Dbos.Config{
      db: Dbos.DB.Postgrex,
      conn: conn,
      executor_id: "exec-1",
      application_version: "v1"
    }

    {:ok, config: config}
  end

  describe "insert_enqueued_workflow/2 extended options" do
    test "priority and queue_partition_key round-trip", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{
          name: "W",
          queue_name: "q",
          inputs: [1],
          priority: 5,
          queue_partition_key: "tenant-a"
        })

      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      assert status.priority == 5
      assert status.queue_partition_key == "tenant-a"
    end

    test "a positive delay_ms enqueues DELAYED with delay_until_epoch_ms in the future", %{
      config: config
    } do
      now = System.os_time(:millisecond)

      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{
          name: "W",
          queue_name: "q",
          inputs: [1],
          delay_ms: 60_000
        })

      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      assert status.status == :delayed
      assert status.delay_until_epoch_ms >= now + 59_000
    end

    test "no delay_ms enqueues ENQUEUED", %{config: config} do
      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      assert status.status == :enqueued
      assert status.delay_until_epoch_ms == nil
    end

    test "a second enqueue with the same deduplication_id raises Dbos.QueueDeduplicatedError", %{
      config: config
    } do
      {:ok, _workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{
          name: "W",
          queue_name: "q",
          inputs: [1],
          deduplication_id: "dedup-1"
        })

      assert_raise Dbos.QueueDeduplicatedError, fn ->
        SystemDb.insert_enqueued_workflow(config, %{
          name: "W",
          queue_name: "q",
          inputs: [2],
          deduplication_id: "dedup-1"
        })
      end
    end

    test "the same deduplication_id is reusable on a different queue", %{config: config} do
      {:ok, _} =
        SystemDb.insert_enqueued_workflow(config, %{
          name: "W",
          queue_name: "q1",
          inputs: [1],
          deduplication_id: "dedup-1"
        })

      {:ok, _} =
        SystemDb.insert_enqueued_workflow(config, %{
          name: "W",
          queue_name: "q2",
          inputs: [1],
          deduplication_id: "dedup-1"
        })
    end
  end

  describe "get_deduplicated_workflow/3" do
    test "returns the holder's workflow id, or nil if the slot is free", %{config: config} do
      assert SystemDb.get_deduplicated_workflow(config, "q", "dedup-1") == nil

      {:ok, workflow_id} =
        SystemDb.insert_enqueued_workflow(config, %{
          name: "W",
          queue_name: "q",
          inputs: [1],
          deduplication_id: "dedup-1"
        })

      assert SystemDb.get_deduplicated_workflow(config, "q", "dedup-1") == workflow_id
    end
  end

  describe "application versions" do
    test "get_latest_application_version returns :none when nothing is registered", %{
      config: config
    } do
      assert SystemDb.get_latest_application_version(config) == :none
    end

    test "create_application_version is idempotent and get_latest reflects the newest timestamp",
         %{config: config} do
      SystemDb.create_application_version(config, "v1")
      assert SystemDb.get_latest_application_version(config) == {:ok, "v1"}

      SystemDb.create_application_version(config, "v1")
      assert SystemDb.get_latest_application_version(config) == {:ok, "v1"}
    end
  end

  describe "transition_delayed_workflows/1" do
    test "promotes DELAYED rows whose delay has expired to ENQUEUED", %{config: config} do
      past = System.os_time(:millisecond) - 1_000
      future = System.os_time(:millisecond) + 60_000

      SystemDb.insert_workflow_status(config, %{
        workflow_id: "wf-due",
        status: :delayed,
        name: "W",
        queue_name: "q",
        inputs: [1],
        delay_until_epoch_ms: past
      })

      SystemDb.insert_workflow_status(config, %{
        workflow_id: "wf-not-due",
        status: :delayed,
        name: "W",
        queue_name: "q",
        inputs: [1],
        delay_until_epoch_ms: future
      })

      SystemDb.transition_delayed_workflows(config)

      {:ok, due_status} = SystemDb.get_workflow_status(config, "wf-due")
      assert due_status.status == :enqueued

      {:ok, not_due_status} = SystemDb.get_workflow_status(config, "wf-not-due")
      assert not_due_status.status == :delayed
    end
  end

  describe "queue registration" do
    test "register_queue persists a new queue and get_queue reads it back", %{config: config} do
      queue =
        Queue.new("orders",
          worker_concurrency: 3,
          global_concurrency: 10,
          rate_limit: %{limit: 5, period_ms: 60_000},
          priority_enabled: true,
          partition_queue: true,
          base_polling_interval_ms: 500
        )

      {:ok, persisted} = SystemDb.register_queue(config, queue)

      assert persisted.name == "orders"
      assert persisted.worker_concurrency == 3
      assert persisted.global_concurrency == 10
      assert persisted.rate_limit == %{limit: 5, period_ms: 60_000}
      assert persisted.priority_enabled == true
      assert persisted.partition_queue == true
      assert persisted.base_polling_interval_ms == 500

      {:ok, fetched} = SystemDb.get_queue(config, "orders")
      assert fetched == persisted
    end

    test "get_queue returns :not_found for an unregistered queue", %{config: config} do
      assert SystemDb.get_queue(config, "does-not-exist") == :not_found
    end

    test "list_queues returns every registered queue", %{config: config} do
      SystemDb.register_queue(config, Queue.new("a"))
      SystemDb.register_queue(config, Queue.new("b"))

      {:ok, queues} = SystemDb.list_queues(config)
      assert Enum.map(queues, & &1.name) |> Enum.sort() == ["a", "b"]
    end

    test "re-registering as the latest application version overwrites the existing row", %{
      config: config
    } do
      SystemDb.create_application_version(config, "v1")
      SystemDb.register_queue(config, Queue.new("orders", worker_concurrency: 1))
      {:ok, updated} = SystemDb.register_queue(config, Queue.new("orders", worker_concurrency: 2))
      assert updated.worker_concurrency == 2
    end

    test "registering as an older application version does not overwrite an existing row", %{
      config: config
    } do
      SystemDb.create_application_version(config, "v1")
      SystemDb.register_queue(config, Queue.new("orders", worker_concurrency: 1))

      older_config = %{config | application_version: "older"}
      SystemDb.create_application_version(older_config, "newer")

      {:ok, unchanged} =
        SystemDb.register_queue(older_config, Queue.new("orders", worker_concurrency: 2))

      assert unchanged.worker_concurrency == 1
    end
  end

  describe "get_queue_partitions/2" do
    test "returns the distinct, non-null partition keys among ENQUEUED workflows", %{
      config: config
    } do
      SystemDb.insert_enqueued_workflow(config, %{
        name: "W",
        queue_name: "q",
        inputs: [1],
        queue_partition_key: "a"
      })

      SystemDb.insert_enqueued_workflow(config, %{
        name: "W",
        queue_name: "q",
        inputs: [1],
        queue_partition_key: "b"
      })

      SystemDb.insert_enqueued_workflow(config, %{name: "W", queue_name: "q", inputs: [1]})

      assert SystemDb.get_queue_partitions(config, "q") |> Enum.sort() == ["a", "b"]
    end
  end
end
