defmodule Dbos.GarbageCollectTest do
  use Dbos.Case, async: false

  alias Dbos.SystemDb

  setup %{conn: conn} do
    config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}
    {:ok, config: config}
  end

  test "deletes terminal workflows older than the cutoff, leaves PENDING/ENQUEUED/DELAYED and newer rows",
       %{config: config} do
    old_success = insert(config, "old-success", :success, 1_000)
    old_pending = insert(config, "old-pending", :pending, 1_000)
    old_enqueued = insert(config, "old-enqueued", :enqueued, 1_000)
    old_delayed = insert(config, "old-delayed", :delayed, 1_000)
    new_success = insert(config, "new-success", :success, 1_000_000_000)

    deleted = SystemDb.garbage_collect_workflows(config, cutoff_epoch_timestamp_ms: 500_000)
    assert deleted == 1

    assert SystemDb.get_workflow_status(config, old_success) == {:error, :not_found}
    assert {:ok, _} = SystemDb.get_workflow_status(config, old_pending)
    assert {:ok, _} = SystemDb.get_workflow_status(config, old_enqueued)
    assert {:ok, _} = SystemDb.get_workflow_status(config, old_delayed)
    assert {:ok, _} = SystemDb.get_workflow_status(config, new_success)
  end

  test "rows_threshold keeps only the newest N rows, deleting older terminal ones", %{
    config: config
  } do
    insert(config, "keep-newest-1", :success, 3_000)
    insert(config, "keep-newest-2", :success, 2_000)
    older = insert(config, "drop-oldest", :success, 1_000)

    deleted = SystemDb.garbage_collect_workflows(config, rows_threshold: 2)
    assert deleted == 1
    assert SystemDb.get_workflow_status(config, older) == {:error, :not_found}
  end

  test "with neither cutoff nor rows_threshold, deletes nothing", %{config: config} do
    insert(config, "untouched", :success, 1_000)
    assert SystemDb.garbage_collect_workflows(config) == 0
  end

  defp insert(config, workflow_id, status, created_at) do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: status,
      name: "add/2",
      inputs: [1, 2],
      created_at: created_at
    })

    workflow_id
  end
end
