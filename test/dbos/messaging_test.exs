defmodule Dbos.MessagingTest do
  use Dbos.Case, async: false

  alias Dbos.Messaging
  alias Dbos.Notifications
  alias Dbos.Runtime
  alias Dbos.SystemDb

  defmodule Money do
    defstruct [:currency, :amount]
  end

  setup %{conn: conn} do
    engine = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!({Registry, keys: :unique, name: Notifications.recv_registry_name(engine)},
      id: {:recv, engine}
    )

    start_supervised!(
      {Registry, keys: :duplicate, name: Notifications.wait_registry_name(engine)},
      id: {:wait, engine}
    )

    config = %Dbos.Config{name: engine, db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}
    {:ok, config: config}
  end

  defp start_workflow(config, workflow_id) do
    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "TestWorkflow"
    })

    workflow_id
  end

  defp run_in_workflow(config, workflow_id, fun) do
    Runtime.with_context([config: config, workflow_id: workflow_id], fun)
  end

  describe "send_message/4" do
    test "inside a workflow, consumes one step id and checkpoints the send", %{config: config} do
      sender_id = start_workflow(config, "wf-sender")
      dest_id = start_workflow(config, "wf-dest")

      run_in_workflow(config, sender_id, fn ->
        Messaging.send_message(config, dest_id, "topic", :hello)
      end)

      {:ok, [step]} = SystemDb.get_workflow_steps(config, sender_id)
      assert step.function_id == 0
      assert step.function_name == "DBOS.send"

      assert {:ok, :hello} = SystemDb.consume_notification(config, dest_id, "topic")
    end

    test "outside a workflow, allocates no step id", %{config: config} do
      dest_id = start_workflow(config, "wf-dest-2")
      assert :ok = Messaging.send_message(config, dest_id, "topic", :hello)
      assert {:ok, :hello} = SystemDb.consume_notification(config, dest_id, "topic")
    end

    test "a nil topic sends and is received on the null topic", %{config: config} do
      dest_id = start_workflow(config, "wf-dest-3")
      Messaging.send_message(config, dest_id, nil, :hello)

      assert {:ok, :hello} =
               SystemDb.consume_notification(config, dest_id, Messaging.null_topic())
    end
  end

  describe "recv_message/3" do
    test "receives a message sent before recv is called (the race)", %{config: config} do
      workflow_id = start_workflow(config, "wf-recv-race")
      Messaging.send_message(config, workflow_id, "topic", %Money{currency: :aud, amount: 4999})

      result =
        run_in_workflow(config, workflow_id, fn ->
          Messaging.recv_message(config, "topic", 1_000)
        end)

      assert result == %Money{currency: :aud, amount: 4999}
    end

    test "receives a message sent after recv starts waiting", %{config: config} do
      workflow_id = start_workflow(config, "wf-recv-after")
      test_pid = self()

      task =
        Task.async(fn ->
          run_in_workflow(config, workflow_id, fn ->
            send(test_pid, :waiting)
            Messaging.recv_message(config, "topic", 2_000)
          end)
        end)

      assert_receive :waiting, 500
      Process.sleep(50)
      Messaging.send_message(config, workflow_id, "topic", :late)

      assert Task.await(task, 2_000) == :late
    end

    test "two messages on the same topic are consumed oldest-first, one per recv", %{
      config: config
    } do
      workflow_id = start_workflow(config, "wf-recv-order")
      Messaging.send_message(config, workflow_id, "topic", :first)
      Process.sleep(2)
      Messaging.send_message(config, workflow_id, "topic", :second)

      run_in_workflow(config, workflow_id, fn ->
        assert Messaging.recv_message(config, "topic", 1_000) == :first
        assert Messaging.recv_message(config, "topic", 1_000) == :second
      end)
    end

    test "a timeout with no message raises RecvTimeoutError and still allocates the sleep id", %{
      config: config
    } do
      workflow_id = start_workflow(config, "wf-recv-timeout")

      assert_raise Dbos.RecvTimeoutError, fn ->
        run_in_workflow(config, workflow_id, fn ->
          Messaging.recv_message(config, "topic", 50)
        end)
      end

      {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)
      assert Enum.map(steps, & &1.function_id) == [0, 1]
      assert Enum.map(steps, & &1.function_name) == ["DBOS.recv", "DBOS.sleep"]
    end

    test "the sleep id is allocated even when the message is already pending (no row written for it)",
         %{config: config} do
      workflow_id = start_workflow(config, "wf-recv-pending-ids")
      Messaging.send_message(config, workflow_id, "topic", :ready)

      run_in_workflow(config, workflow_id, fn ->
        Messaging.recv_message(config, "topic", 1_000)
        assert Runtime.run_step("after/0", [], fn -> :done end) == :done
      end)

      {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)
      assert Enum.map(steps, & &1.function_id) == [0, 2]
      assert Enum.map(steps, & &1.function_name) == ["DBOS.recv", "after/0"]
    end
  end

  describe "set_event/3 and get_event/4" do
    test "set_event then get_event from another workflow returns the value", %{config: config} do
      setter_id = start_workflow(config, "wf-setter")
      getter_id = start_workflow(config, "wf-getter")

      run_in_workflow(config, setter_id, fn ->
        Messaging.set_event(config, "status", :ready)
      end)

      result =
        run_in_workflow(config, getter_id, fn ->
          Messaging.get_event(config, setter_id, "status", 1_000)
        end)

      assert result == :ready
    end

    test "get_event works from outside a workflow", %{config: config} do
      setter_id = start_workflow(config, "wf-setter-2")

      run_in_workflow(config, setter_id, fn ->
        Messaging.set_event(config, "status", :ready)
      end)

      assert Messaging.get_event(config, setter_id, "status", 1_000) == :ready
    end

    test "get_event times out to nil when the key is never set", %{config: config} do
      setter_id = start_workflow(config, "wf-setter-3")
      assert Messaging.get_event(config, setter_id, "missing", 50) == nil
    end
  end

  describe "streams" do
    test "writes several items, closes, and reads them back in order, terminating at the close",
         %{config: config} do
      workflow_id = start_workflow(config, "wf-stream")

      run_in_workflow(config, workflow_id, fn ->
        Messaging.write_stream(config, "log", :a)
        Messaging.write_stream(config, "log", :b)
        Messaging.write_stream(config, "log", :c)
        Messaging.close_stream(config, "log")
      end)

      values = Messaging.read_stream(config, workflow_id, "log") |> Enum.to_list()
      assert values == [:a, :b, :c]
    end
  end

  describe "durable sleep" do
    test "checkpoints the absolute wake time under one step id", %{config: config} do
      workflow_id = start_workflow(config, "wf-sleep")

      run_in_workflow(config, workflow_id, fn ->
        Messaging.sleep(config, 30)
      end)

      {:ok, [step]} = SystemDb.get_workflow_steps(config, workflow_id)
      assert step.function_name == "DBOS.sleep"
      assert is_integer(step.output)
    end
  end
end
