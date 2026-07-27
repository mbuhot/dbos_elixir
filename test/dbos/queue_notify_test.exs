defmodule Dbos.QueueNotifyTest do
  @moduledoc """
  A queue runner's primary wake-up is a notification, not its tick: these start engines with a real
  `LISTEN` connection and assert that work is picked up far sooner than any polling interval would
  allow, and that an idle runner stops polling at the base interval.
  """

  use Dbos.Case, async: false

  alias Dbos.Notifications
  alias Dbos.Queue
  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

  # Long enough that anything picked up promptly cannot have been the tick.
  @slow_poll_ms 30_000

  defp start_engine(opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    defaults = [
      name: name,
      db: {Dbos.DB.Postgrex, Dbos.TestConn},
      executor_id: "exec-#{System.unique_integer([:positive])}",
      workflows: [{"add/2", {SampleWorkflows, :add, 2}}],
      lease_sweep: [enabled: false],
      migrations: :skip,
      notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
    ]

    start_supervised!({Dbos.Supervisor, Keyword.merge(defaults, opts)}, id: name)
    Recovery.await_boot_recovery(name)
    name
  end

  defp slow_queue(name) do
    Queue.new(name,
      base_polling_interval_ms: @slow_poll_ms,
      max_polling_interval_ms: @slow_poll_ms
    )
  end

  defp await_listening(engine, attempts \\ 200)

  defp await_listening(engine, 0) do
    flunk("#{inspect(engine)} never established its LISTEN connection")
  end

  defp await_listening(engine, attempts) do
    if Notifications.mode(engine) == :listen do
      :ok
    else
      Process.sleep(10)
      await_listening(engine, attempts - 1)
    end
  end

  test "an enqueue wakes the runner rather than waiting for its next tick" do
    engine = start_engine(queues: [slow_queue("orders")])
    await_listening(engine)

    {:ok, handle} = Dbos.enqueue("add/2", [1, 2], queue_name: "orders", engine: engine)

    assert {:ok, 3} = Dbos.await(handle, timeout_ms: 2_000)
  end

  test "a delayed workflow is promoted at its wake time, not on a tick" do
    engine = start_engine(queues: [slow_queue("orders")])
    await_listening(engine)

    {:ok, handle} =
      Dbos.enqueue("add/2", [1, 2], queue_name: "orders", engine: engine, delay_ms: 200)

    config = Dbos.config(engine)
    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :delayed

    assert {:ok, 3} = Dbos.await(handle, timeout_ms: 3_000)
  end

  test "a workflow returned to its queue by recovery wakes the runner" do
    engine = start_engine(queues: [slow_queue("orders")])
    await_listening(engine)

    {:ok, handle} = Dbos.enqueue("add/2", [1, 2], queue_name: "orders", engine: engine)
    assert {:ok, 3} = Dbos.await(handle, timeout_ms: 2_000)

    Postgrex.query!(
      Dbos.TestConn,
      ~s(UPDATE "dbos".workflow_status SET status = 'PENDING', executor_id = 'exec-gone', output = NULL WHERE workflow_uuid = $1),
      [handle.workflow_id]
    )

    Recovery.reclaim(engine, ["exec-gone"])

    assert {:ok, 3} = Dbos.await(handle, timeout_ms: 3_000)
  end

  test "an idle runner backs its interval off past the base while it is listening" do
    engine = start_engine(queues: [Queue.new("orders", base_polling_interval_ms: 20)])
    await_listening(engine)

    interval = fn ->
      engine
      |> Queue.Runner.process_name("orders")
      |> GenServer.whereis()
      |> :sys.get_state()
      |> Map.fetch!(:polling_interval_ms)
    end

    wait_until(fn -> interval.() > 20 end)
    assert interval.() > 20
  end

  test "an idle runner under the polling fallback stays at its base interval" do
    engine =
      start_engine(
        queues: [Queue.new("orders", base_polling_interval_ms: 20)],
        notifications: :poll
      )

    assert Notifications.mode(engine) == :poll

    Process.sleep(200)

    interval =
      engine
      |> Queue.Runner.process_name("orders")
      |> GenServer.whereis()
      |> :sys.get_state()
      |> Map.fetch!(:polling_interval_ms)

    assert interval == 20
  end

  test "a runner that claims work resets to its base interval" do
    engine = start_engine(queues: [Queue.new("orders", base_polling_interval_ms: 50)])
    await_listening(engine)

    runner = engine |> Queue.Runner.process_name("orders") |> GenServer.whereis()
    wait_until(fn -> :sys.get_state(runner).polling_interval_ms > 50 end)

    {:ok, handle} = Dbos.enqueue("add/2", [1, 2], queue_name: "orders", engine: engine)
    assert {:ok, 3} = Dbos.await(handle, timeout_ms: 2_000)

    assert :sys.get_state(runner).polling_interval_ms == 50
  end

  defp wait_until(fun, attempts \\ 300)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end
end
