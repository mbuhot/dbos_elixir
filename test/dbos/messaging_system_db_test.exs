defmodule Dbos.MessagingSystemDbTest do
  use Dbos.Case, async: false

  alias Dbos.SystemDb

  defmodule Money do
    defstruct [:currency, :amount]
  end

  setup %{conn: conn} do
    config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}
    {:ok, config: config}
  end

  defp insert_workflow(config, workflow_id) do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "W",
      inputs: []
    })

    workflow_id
  end

  describe "send_notification/4" do
    test "stores a no-topic send under the null topic sentinel", %{config: config} do
      workflow_id = insert_workflow(config, "wf-send-1")

      assert :ok = SystemDb.send_notification(config, workflow_id, SystemDb.null_topic(), :hi)

      {:ok, %{rows: [[topic]]}} =
        config.db.query(
          config.conn,
          "SELECT topic FROM dbos.notifications WHERE destination_uuid = $1",
          [workflow_id]
        )

      assert topic == "__null__topic__"
    end

    test "raises NonExistentWorkflowError when the destination workflow does not exist", %{
      config: config
    } do
      assert_raise Dbos.NonExistentWorkflowError, fn ->
        SystemDb.send_notification(config, "does-not-exist", "topic", :hi)
      end
    end
  end

  describe "notification_pending?/3" do
    test "is false with nothing sent and true after a send", %{config: config} do
      workflow_id = insert_workflow(config, "wf-pending-1")

      refute SystemDb.notification_pending?(config, workflow_id, "topic")

      SystemDb.send_notification(config, workflow_id, "topic", :hi)

      assert SystemDb.notification_pending?(config, workflow_id, "topic")
    end
  end

  describe "consume_notification/3" do
    test "returns :none when nothing is pending", %{config: config} do
      workflow_id = insert_workflow(config, "wf-consume-1")
      assert SystemDb.consume_notification(config, workflow_id, "topic") == :none
    end

    test "consumes the oldest unconsumed message first and marks it consumed rather than deleting",
         %{config: config} do
      workflow_id = insert_workflow(config, "wf-consume-2")

      SystemDb.send_notification(config, workflow_id, "topic", :first)
      Process.sleep(2)
      SystemDb.send_notification(config, workflow_id, "topic", :second)

      assert {:ok, :first} = SystemDb.consume_notification(config, workflow_id, "topic")
      assert {:ok, :second} = SystemDb.consume_notification(config, workflow_id, "topic")
      assert SystemDb.consume_notification(config, workflow_id, "topic") == :none

      {:ok, %{rows: rows}} =
        config.db.query(
          config.conn,
          "SELECT consumed FROM dbos.notifications WHERE destination_uuid = $1",
          [workflow_id]
        )

      assert Enum.all?(rows, fn [consumed] -> consumed end)
      assert length(rows) == 2
    end

    test "round-trips an Elixir term with atoms and a nested struct", %{config: config} do
      workflow_id = insert_workflow(config, "wf-consume-3")

      message = %Dbos.MessagingSystemDbTest.Money{currency: :aud, amount: 4999}
      SystemDb.send_notification(config, workflow_id, "topic", message)

      assert {:ok, ^message} = SystemDb.consume_notification(config, workflow_id, "topic")
    end
  end

  describe "set_event_value/5 and get_event_value/3" do
    test "an event set is readable and upserts on a second set for the same key", %{
      config: config
    } do
      workflow_id = insert_workflow(config, "wf-event-1")

      SystemDb.set_event_value(config, workflow_id, 0, "status", :started)
      assert {:ok, :started} = SystemDb.get_event_value(config, workflow_id, "status")

      SystemDb.set_event_value(config, workflow_id, 3, "status", :finished)
      assert {:ok, :finished} = SystemDb.get_event_value(config, workflow_id, "status")
    end

    test "get_event_value returns :none for an unset key", %{config: config} do
      workflow_id = insert_workflow(config, "wf-event-2")
      assert SystemDb.get_event_value(config, workflow_id, "status") == :none
    end

    test "writes a workflow_events_history row per step", %{config: config} do
      workflow_id = insert_workflow(config, "wf-event-3")

      SystemDb.set_event_value(config, workflow_id, 0, "status", :started)

      {:ok, %{rows: [[value]]}} =
        config.db.query(
          config.conn,
          "SELECT value FROM dbos.workflow_events_history WHERE workflow_uuid = $1 AND function_id = $2 AND key = $3",
          [workflow_id, 0, "status"]
        )

      assert Dbos.Serialization.decode(value) == :started
    end
  end

  describe "write_stream/5, close_stream, and read_stream_page/4" do
    test "assigns sequential offsets and reads them back in order", %{config: config} do
      workflow_id = insert_workflow(config, "wf-stream-1")

      assert :ok = SystemDb.write_stream(config, workflow_id, 0, "log", :a)
      assert :ok = SystemDb.write_stream(config, workflow_id, 1, "log", :b)

      assert {[:a, :b], 2, false} = SystemDb.read_stream_page(config, workflow_id, "log", 0)
    end

    test "close_stream writes the sentinel, reported as closed and excluded from values", %{
      config: config
    } do
      workflow_id = insert_workflow(config, "wf-stream-2")

      SystemDb.write_stream(config, workflow_id, 0, "log", :a)
      SystemDb.close_stream(config, workflow_id, 1, "log")

      assert {[:a], 2, true} = SystemDb.read_stream_page(config, workflow_id, "log", 0)
    end

    test "writing after close is rejected, not silently appended", %{config: config} do
      workflow_id = insert_workflow(config, "wf-stream-3")

      SystemDb.close_stream(config, workflow_id, 0, "log")

      assert {:error, :stream_closed} =
               SystemDb.write_stream(config, workflow_id, 1, "log", :too_late)
    end
  end
end
