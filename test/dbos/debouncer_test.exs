defmodule Dbos.DebouncerTest do
  use Dbos.Case, async: false

  alias Dbos.Debouncer
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

  test "the first debounce call enqueues a DELAYED, is_debounced workflow keyed by debounce_key",
       %{config: config} do
    now = System.os_time(:millisecond)

    {:ok, workflow_id} =
      Debouncer.debounce(config, "job/1", ["a"],
        queue_name: "jobs",
        debounce_key: "cust-1",
        period_ms: 200
      )

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    assert status.status == :delayed
    assert status.is_debounced == true
    assert status.deduplication_id == "cust-1"
    assert status.inputs == ["a"]
    assert_in_delta status.delay_until_epoch_ms, now + 200, 100
  end

  test "a bounce within the window collapses onto the same workflow, replacing inputs and extending delay_until",
       %{config: config} do
    {:ok, first_id} =
      Debouncer.debounce(config, "job/1", ["a"],
        queue_name: "jobs",
        debounce_key: "cust-2",
        period_ms: 200
      )

    Process.sleep(20)
    {:ok, first_status} = SystemDb.get_workflow_status(config, first_id)

    {:ok, second_id} =
      Debouncer.debounce(config, "job/1", ["b"],
        queue_name: "jobs",
        debounce_key: "cust-2",
        period_ms: 200
      )

    assert second_id == first_id

    {:ok, status} = SystemDb.get_workflow_status(config, second_id)
    assert status.inputs == ["b"]
    assert status.delay_until_epoch_ms > first_status.delay_until_epoch_ms
  end

  test "a debounce_deadline_ms caps how far repeated bounces can push delay_until", %{
    config: config
  } do
    now = System.os_time(:millisecond)

    {:ok, first_id} =
      Debouncer.debounce(config, "job/1", ["a"],
        queue_name: "jobs",
        debounce_key: "cust-3",
        period_ms: 100,
        deadline_ms: 150
      )

    {:ok, deadline_status} = SystemDb.get_workflow_status(config, first_id)
    assert_in_delta deadline_status.debounce_deadline_epoch_ms, now + 150, 100

    {:ok, second_id} =
      Debouncer.debounce(config, "job/1", ["b"],
        queue_name: "jobs",
        debounce_key: "cust-3",
        period_ms: 100,
        deadline_ms: 150
      )

    assert second_id == first_id
    {:ok, status} = SystemDb.get_workflow_status(config, second_id)
    assert status.delay_until_epoch_ms <= deadline_status.debounce_deadline_epoch_ms
  end

  test "once the key is released (no longer DELAYED and no longer is_debounced), a fresh debounce starts a new workflow",
       %{config: config} do
    {:ok, first_id} =
      Debouncer.debounce(config, "job/1", ["a"],
        queue_name: "jobs",
        debounce_key: "cust-4",
        period_ms: 100
      )

    SystemDb.transition_delayed_workflows(config)

    sql = "UPDATE dbos.workflow_status SET deduplication_id = NULL WHERE workflow_uuid = $1"
    {:ok, _} = Dbos.DB.Postgrex.query(config.conn, sql, [first_id])

    {:ok, second_id} =
      Debouncer.debounce(config, "job/1", ["b"],
        queue_name: "jobs",
        debounce_key: "cust-4",
        period_ms: 100
      )

    assert second_id != first_id
  end

  test "a debounce key already held by a plain (non-debounced) dedup enqueue raises Dbos.QueueDeduplicatedError",
       %{config: config} do
    {:ok, _held_id} =
      SystemDb.insert_enqueued_workflow(config, %{
        name: "job/1",
        queue_name: "jobs",
        inputs: ["x"],
        deduplication_id: "cust-5"
      })

    assert_raise Dbos.QueueDeduplicatedError, fn ->
      Debouncer.debounce(config, "job/1", ["a"],
        queue_name: "jobs",
        debounce_key: "cust-5",
        period_ms: 100
      )
    end
  end
end
