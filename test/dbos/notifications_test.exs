defmodule Dbos.NotificationsTest do
  use Dbos.Case, async: false

  alias Dbos.Notifications

  defp start_engine(extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        migrations: :skip,
        workflows: []
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    name
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
      start_engine(notifications: :listen, notifications_conn_opts: [database: "dbos_test"])

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
      start_engine(notifications: :listen, notifications_conn_opts: [database: "dbos_test"])

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
