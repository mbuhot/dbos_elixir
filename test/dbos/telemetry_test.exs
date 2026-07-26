defmodule Dbos.TelemetryTest do
  use Dbos.Case, async: false

  alias Dbos.CheckoutWorkflow
  alias Dbos.SampleWorkflows

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        workflows: workflows,
        migrations: :skip
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp attach(events) do
    ref = :telemetry_test.attach_event_handlers(self(), events)
    on_exit(fn -> :telemetry.detach(ref) end)
    ref
  end

  test "workflow start/stop fire around a successful workflow, with duration and workflow_id/name metadata" do
    attach([[:dbos, :workflow, :start], [:dbos, :workflow, :stop]])

    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    {:ok, handle} = Dbos.start("add/2", [1, 2], engine: engine)
    assert {:ok, 3} = Dbos.await(handle)

    assert_received {[:dbos, :workflow, :start], _ref, %{system_time: _},
                     %{workflow_id: workflow_id, name: "add/2"}}

    assert_received {[:dbos, :workflow, :stop], _ref, %{duration: duration},
                     %{workflow_id: ^workflow_id, name: "add/2"}}

    assert is_integer(duration)
  end

  test "workflow exception fires when the body raises" do
    attach([[:dbos, :workflow, :exception]])

    engine = start_engine([{"boom/1", {SampleWorkflows, :boom, 1}}])
    {:ok, handle} = Dbos.start("boom/1", [nil], engine: engine)
    assert {:error, _exception} = Dbos.await(handle)

    assert_received {[:dbos, :workflow, :exception], _ref, %{duration: _},
                     %{workflow_id: _, name: "boom/1", kind: :error}}
  end

  test "step start/stop fire around a durable step's actual execution" do
    attach([[:dbos, :step, :start], [:dbos, :step, :stop]])

    engine = start_engine([CheckoutWorkflow])

    {:ok, handle} = Dbos.start("process_order", ["ord-1", 100], engine: engine)
    assert {:ok, _} = Dbos.await(handle)

    assert_received {[:dbos, :step, :start], _ref, %{system_time: _},
                     %{function_name: "charge_card/2"}}

    assert_received {[:dbos, :step, :stop], _ref, %{duration: _},
                     %{function_name: "charge_card/2"}}
  end

  test "queue dequeue span fires with queue_name and claimed count" do
    attach([[:dbos, :queue, :dequeue, :start], [:dbos, :queue, :dequeue, :stop]])

    engine =
      start_engine([{"add/2", {SampleWorkflows, :add, 2}}],
        queues: [Dbos.Queue.new("orders", base_polling_interval_ms: 20)]
      )

    {:ok, _handle} =
      Dbos.enqueue("add/2", [1, 2], queue_name: "orders", engine: engine)

    assert_receive {[:dbos, :queue, :dequeue, :stop], _ref, %{duration: _},
                    %{queue_name: "orders", count: count}},
                   2_000

    assert count >= 0
  end

  test "a durable sleep is spanned as a wait that resolves" do
    attach([[:dbos, :wait, :start], [:dbos, :wait, :stop]])

    engine = start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}])
    {:ok, handle} = Dbos.start("sleeper/1", [50], engine: engine)
    assert {:ok, :woke} = Dbos.await(handle, timeout_ms: 5_000)

    workflow_id = handle.workflow_id

    assert_receive {[:dbos, :wait, :start], _ref, %{system_time: _},
                    %{kind: :sleep, workflow_id: ^workflow_id, timeout_ms: 50}},
                   2_000

    assert_receive {[:dbos, :wait, :stop], _ref, %{duration: duration},
                    %{kind: :sleep, workflow_id: ^workflow_id, outcome: :resolved}},
                   2_000

    assert is_integer(duration)
  end

  test "a workflow waiting for a message is spanned until the message arrives" do
    attach([[:dbos, :wait, :stop]])

    engine =
      start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}],
        notifications: :listen,
        notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
      )

    {:ok, handle} = Dbos.start("receiver/2", ["decision", 10_000], engine: engine)
    Process.sleep(100)
    :ok = Dbos.send_message(handle.workflow_id, "decision", :approved, engine: engine)

    assert {:ok, :approved} = Dbos.await(handle, timeout_ms: 5_000)

    workflow_id = handle.workflow_id

    assert_receive {[:dbos, :wait, :stop], _ref, %{duration: _},
                    %{
                      kind: :recv,
                      workflow_id: ^workflow_id,
                      key: "decision",
                      timeout_ms: 10_000,
                      outcome: :resolved
                    }},
                   2_000
  end

  test "a message that never arrives is spanned as a wait that timed out" do
    attach([[:dbos, :wait, :stop]])

    engine = start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}])
    {:ok, handle} = Dbos.start("receiver/2", ["decision", 100], engine: engine)
    assert {:error, _exception} = Dbos.await(handle, timeout_ms: 5_000)

    assert_receive {[:dbos, :wait, :stop], _ref, %{duration: _},
                    %{kind: :recv, key: "decision", outcome: :timeout}},
                   2_000
  end

  test "waiting on another workflow's event names the workflow being watched" do
    attach([[:dbos, :wait, :stop]])

    engine =
      start_engine(
        [
          {"event_waiter/3", {SampleWorkflows, :event_waiter, 3}},
          {"announcer/2", {SampleWorkflows, :announcer, 2}}
        ],
        notifications: :listen,
        notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
      )

    {:ok, publisher} = Dbos.start("announcer/2", ["progress", :ready], engine: engine)
    assert {:ok, :ok} = Dbos.await(publisher)

    {:ok, waiter} =
      Dbos.start("event_waiter/3", [publisher.workflow_id, "progress", 5_000], engine: engine)

    assert {:ok, :ready} = Dbos.await(waiter, timeout_ms: 5_000)

    watched = publisher.workflow_id
    waiter_id = waiter.workflow_id

    assert_receive {[:dbos, :wait, :stop], _ref, %{duration: _},
                    %{
                      kind: :event,
                      workflow_id: ^waiter_id,
                      target_workflow_id: ^watched,
                      key: "progress",
                      outcome: :resolved
                    }},
                   2_000
  end

  test "a wait long enough to release the workflow process is spanned as parked, and its resumption starts a new wait" do
    attach([[:dbos, :wait, :start], [:dbos, :wait, :stop]])

    engine =
      start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}], park_exit_threshold_ms: 50)

    {:ok, handle} = Dbos.start("sleeper/1", [400], engine: engine)
    workflow_id = handle.workflow_id

    assert_receive {[:dbos, :wait, :start], _ref, %{system_time: _},
                    %{kind: :sleep, workflow_id: ^workflow_id}},
                   2_000

    assert_receive {[:dbos, :wait, :stop], _ref, %{duration: _},
                    %{kind: :sleep, workflow_id: ^workflow_id, outcome: :parked}},
                   2_000

    assert {:ok, :woke} = Dbos.await(handle, timeout_ms: 5_000)

    assert_receive {[:dbos, :wait, :start], _ref, %{system_time: _},
                    %{kind: :sleep, workflow_id: ^workflow_id}},
                   2_000
  end

  test "recovery span fires around a boot recovery pass" do
    attach([[:dbos, :recovery, :start], [:dbos, :recovery, :stop]])

    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    Dbos.Recovery.recover_pending(engine)

    assert_received {[:dbos, :recovery, :start], _ref, %{system_time: _}, %{engine: ^engine}}
    assert_received {[:dbos, :recovery, :stop], _ref, %{duration: _}, %{engine: ^engine}}
  end
end
