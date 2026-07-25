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

  test "recovery span fires around a boot recovery pass" do
    attach([[:dbos, :recovery, :start], [:dbos, :recovery, :stop]])

    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    Dbos.Recovery.recover_pending(engine)

    assert_received {[:dbos, :recovery, :start], _ref, %{system_time: _}, %{engine: ^engine}}
    assert_received {[:dbos, :recovery, :stop], _ref, %{duration: _}, %{engine: ^engine}}
  end
end
