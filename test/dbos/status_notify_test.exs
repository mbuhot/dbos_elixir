defmodule Dbos.StatusNotifyTest do
  @moduledoc """
  Status transitions reach subscribers on every node, not only the one that recorded them. Each of
  these makes the transition with a bare SQL statement rather than through the engine, so nothing
  in-process could have delivered the wake — it can only have arrived over `LISTEN`.
  """

  use Dbos.Case, async: false

  alias Dbos.Notifications
  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.Waits
  alias Dbos.WorkflowSup

  @long_wait_ms 300_000

  defp start_engine(opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    defaults = [
      name: name,
      db: {Dbos.DB.Postgrex, Dbos.TestConn},
      executor_id: "exec-#{System.unique_integer([:positive])}",
      workflows: [{"receiver/2", {SampleWorkflows, :receiver, 2}}],
      lease_sweep: [enabled: false],
      migrations: :skip,
      park_exit_threshold_ms: 50,
      notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
    ]

    start_supervised!({Dbos.Supervisor, Keyword.merge(defaults, opts)}, id: name)
    Recovery.await_boot_recovery(name)
    await_listening(name)
    name
  end

  defp await_listening(engine, attempts \\ 200)
  defp await_listening(engine, 0), do: flunk("#{inspect(engine)} never began listening")

  defp await_listening(engine, attempts) do
    if Notifications.mode(engine) == :listen do
      :ok
    else
      Process.sleep(10)
      await_listening(engine, attempts - 1)
    end
  end

  # A transition written by nothing this engine runs, the way a peer node's would arrive.
  defp elsewhere(workflow_id, status) do
    Postgrex.query!(
      Dbos.TestConn,
      ~s(UPDATE "dbos".workflow_status SET status = $2 WHERE workflow_uuid = $1),
      [workflow_id, status]
    )
  end

  defp park_a_wait(engine) do
    {:ok, handle} = Dbos.start("receiver/2", ["topic", @long_wait_ms], engine: engine)
    wait_until(fn -> WorkflowSup.whereis(engine, handle.workflow_id) == :error end)
    assert Waits.count(engine) == 1
    handle
  end

  test "a per-workflow subscriber is woken by a transition recorded on another node" do
    engine = start_engine()

    {:ok, handle} =
      Dbos.enqueue("receiver/2", ["topic", 10], queue_name: "q-none", engine: engine)

    :ok = Notifications.subscribe_status(engine, handle.workflow_id)
    elsewhere(handle.workflow_id, "CANCELLED")

    assert_receive {:dbos_notify, :status, workflow_id}, 2_000
    assert workflow_id == handle.workflow_id
  end

  test "an engine-wide subscriber is woken by a transition recorded on another node" do
    engine = start_engine()

    {:ok, handle} =
      Dbos.enqueue("receiver/2", ["topic", 10], queue_name: "q-none", engine: engine)

    :ok = Notifications.subscribe_all(engine, [:status])
    elsewhere(handle.workflow_id, "CANCELLED")

    assert_receive {:dbos_notification, :status, workflow_id, nil}, 2_000
    assert workflow_id == handle.workflow_id
  end

  test "a parked wait is woken by a cancellation recorded on another node" do
    engine = start_engine()
    config = Dbos.config(engine)
    handle = park_a_wait(engine)

    elsewhere(handle.workflow_id, "CANCELLING")

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
      status.status == :cancelled
    end)

    assert Waits.count(engine) == 0
  end

  test "a parked wait is left parked by a transition that is not its own" do
    engine = start_engine()
    handle = park_a_wait(engine)

    {:ok, other} = Dbos.enqueue("receiver/2", ["topic", 10], queue_name: "q-none", engine: engine)
    elsewhere(other.workflow_id, "CANCELLED")

    Process.sleep(300)

    assert Waits.count(engine) == 1

    assert Dbos.status(handle.workflow_id, engine: engine) |> elem(1) |> Map.get(:status) ==
             :pending
  end

  test "an insert is announced too, so a dashboard sees a workflow appear" do
    engine = start_engine()
    :ok = Notifications.subscribe_all(engine, [:status])

    {:ok, handle} =
      Dbos.enqueue("receiver/2", ["topic", 10], queue_name: "q-none", engine: engine)

    assert_receive {:dbos_notification, :status, workflow_id, nil}, 2_000
    assert workflow_id == handle.workflow_id
  end

  defp wait_until(fun, attempts \\ 500)
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
