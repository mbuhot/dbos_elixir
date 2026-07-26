defmodule Dbos.NotificationsTest do
  use Dbos.Case, async: false

  alias Dbos.Notifications
  alias Dbos.SampleWorkflows

  defp start_engine(extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      Keyword.merge(
        [
          name: name,
          db: {Dbos.DB.Postgrex, Dbos.TestConn},
          executor_id: "exec-#{System.unique_integer([:positive])}",
          migrations: :skip,
          workflows: []
        ],
        extra_opts
      )

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    name
  end

  defp start_listening_engine(extra_opts \\ []) do
    engine =
      start_engine(
        Keyword.merge(
          [
            notifications: :listen,
            notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
          ],
          extra_opts
        )
      )

    wait_until(fn -> Notifications.mode(engine) == :listen end)
    engine
  end

  test "subscribe_recv succeeds once and conflicts for a second concurrent receiver" do
    engine = start_engine()

    assert :ok = Notifications.subscribe_recv(engine, "wf-1", "topic")
    assert {:error, :conflict} = Notifications.subscribe_recv(engine, "wf-1", "topic")

    Notifications.unsubscribe_recv(engine, "wf-1", "topic")
    assert :ok = Notifications.subscribe_recv(engine, "wf-1", "topic")
  end

  test "subscribe_event allows more than one concurrent waiter on the same key" do
    engine = start_engine()

    assert :ok = Notifications.subscribe_event(engine, "wf-1", "key")
    assert :ok = Notifications.subscribe_event(engine, "wf-1", "key")
  end

  test "wait_until returns :found immediately when the recheck is already true" do
    engine = start_engine()
    assert Notifications.wait_until(engine, nil, fn -> true end) == :found
  end

  test "wait_until in :poll mode wakes and rechecks without any notification arriving" do
    engine = start_engine(notifications: :poll)
    assert Notifications.mode(engine) == :poll

    test_pid = self()
    counter = :counters.new(1, [])

    task =
      Task.async(fn ->
        Notifications.wait_until(engine, nil, fn ->
          :counters.add(counter, 1, 1)
          count = :counters.get(counter, 1)
          if count >= 2, do: send(test_pid, :ready)
          count >= 2
        end)
      end)

    assert_receive :ready, 3000
    assert Task.await(task) == :found
  end

  test "wait_until returns :timeout once the deadline passes without the recheck becoming true" do
    engine = start_engine()
    deadline_ms = System.os_time(:millisecond) + 50
    assert Notifications.wait_until(engine, deadline_ms, fn -> false end) == :timeout
  end

  test "a real pg NOTIFY on the notifications channel wakes a subscribed recv waiter in :listen mode" do
    engine =
      start_engine(
        notifications: :listen,
        notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
      )

    wait_until(fn -> Notifications.mode(engine) == :listen end)

    counter = :counters.new(1, [])
    test_pid = self()

    task =
      Task.async(fn ->
        assert :ok = Notifications.subscribe_recv(engine, "wf-notify", "topic")
        send(test_pid, :subscribed)

        Notifications.wait_until(engine, nil, fn ->
          :counters.add(counter, 1, 1)
          :counters.get(counter, 1) >= 2
        end)
      end)

    assert_receive :subscribed, 500
    assert :counters.get(counter, 1) == 1

    Postgrex.query!(
      Dbos.TestConn,
      "SELECT pg_notify('dbos_notifications_channel', 'wf-notify::topic')",
      []
    )

    assert Task.await(task, 2000) == :found
  end

  test "a waiter parked with no deadline is woken to re-probe after the listener connection drops and reconnects" do
    engine =
      start_engine(
        notifications: :listen,
        notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
      )

    wait_until(fn -> Notifications.mode(engine) == :listen end)

    test_pid = self()
    counter = :counters.new(1, [])

    task =
      Task.async(fn ->
        assert :ok = Notifications.subscribe_event(engine, "wf-reconnect", "key")
        send(test_pid, :subscribed)

        Notifications.wait_until(engine, nil, fn ->
          :counters.add(counter, 1, 1)
          :counters.get(counter, 1) >= 2
        end)
      end)

    assert_receive :subscribed, 500
    wait_until(fn -> :counters.get(counter, 1) == 1 end)

    listener_pid = :sys.get_state(Notifications.process_name(engine)).listener
    Process.exit(listener_pid, :kill)

    wait_until(fn -> Notifications.mode(engine) == :listen end, 300)

    assert Task.await(task, 5000) == :found
    assert :counters.get(counter, 1) >= 2
  end

  test "an engine-wide subscriber learns which workflow finished without naming it in advance" do
    engine =
      start_engine(
        notifications: :poll,
        workflows: [{"add/2", {SampleWorkflows, :add, 2}}]
      )

    :ok = Notifications.subscribe_all(engine, [:status])
    {:ok, handle} = Dbos.start("add/2", [1, 2], engine: engine)
    assert {:ok, 3} = Dbos.await(handle)

    workflow_id = handle.workflow_id
    assert_receive {:dbos_notification, :status, ^workflow_id, nil}, 2_000
  end

  test "an engine-wide subscriber receives the events, stream writes and messages of workflows it never named" do
    engine =
      start_listening_engine(workflows: [{"announcer/2", {SampleWorkflows, :announcer, 2}}])

    :ok = Notifications.subscribe_all(engine)
    {:ok, handle} = Dbos.start("announcer/2", ["progress", :ready], engine: engine)
    assert {:ok, :ok} = Dbos.await(handle)

    workflow_id = handle.workflow_id
    assert_receive {:dbos_notification, :event, ^workflow_id, "progress"}, 2_000
    assert_receive {:dbos_notification, :stream, ^workflow_id, "progress"}, 2_000

    :ok = Dbos.send_message(workflow_id, "decision", :approved, engine: engine)
    assert_receive {:dbos_notification, :recv, ^workflow_id, "decision"}, 2_000
  end

  test "an engine-wide subscription stops delivering once released" do
    engine =
      start_listening_engine(workflows: [{"announcer/2", {SampleWorkflows, :announcer, 2}}])

    :ok = Notifications.subscribe_all(engine)
    :ok = Notifications.unsubscribe_all(engine)

    {:ok, handle} = Dbos.start("announcer/2", ["progress", :ready], engine: engine)
    assert {:ok, :ok} = Dbos.await(handle)

    refute_receive {:dbos_notification, _kind, _workflow_id, _key}, 300
  end

  test "engine-wide subscription to messages, events or streams is refused on a polling engine" do
    engine = start_engine(notifications: :poll)

    assert_raise ArgumentError, ~r/requires the LISTEN transport/, fn ->
      Notifications.subscribe_all(engine, [:status, :event])
    end

    assert :ok = Notifications.subscribe_all(engine, [:status])
  end

  test "an unknown engine-wide notification kind is rejected" do
    engine = start_engine(notifications: :poll)

    assert_raise ArgumentError, ~r/unknown engine-wide notification kind/, fn ->
      Notifications.subscribe_all(engine, [:progress])
    end
  end

  test "an engine-wide subscriber is told to resync after the listener connection drops and comes back" do
    engine = start_listening_engine()

    :ok = Notifications.subscribe_all(engine)

    listener_pid = :sys.get_state(Notifications.process_name(engine)).listener
    Process.exit(listener_pid, :kill)

    assert_receive {:dbos_notification, :reconnect, nil, nil}, 5_000
    refute_receive {:dbos_notification, :reconnect, nil, nil}, 200
  end

  defp wait_until(fun, attempts \\ 100)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end
end
