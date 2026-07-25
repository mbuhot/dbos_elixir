defmodule AgentInbox.ApprovalsTest do
  use ExUnit.Case, async: false

  alias AgentInbox.Approvals
  alias Dbos.Recovery

  @tables ~w(
    workflow_status
    operation_outputs
    notifications
    workflow_events
    workflow_events_history
    streams
    event_dispatch_kv
    application_versions
    workflow_schedules
    queues
  )

  setup do
    truncate_tables()

    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Postgrex, AgentInbox.TestConn},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Approvals],
       migrations: :create_if_absent},
      id: name
    )

    Recovery.await_boot_recovery(name)

    {:ok, engine: name}
  end

  test "an approved request delivers the approver's note", %{engine: engine} do
    {:ok, handle} =
      Dbos.start("request_approval", ["req-1", "refund", "customer asked twice", 5_000, 0],
        engine: engine,
        workflow_id: "wf-approve"
      )

    wait_for_state(engine, "wf-approve", :awaiting_response)

    Dbos.send_message("wf-approve", Approvals.response_topic(), {:approved, "looks fine"},
      engine: engine
    )

    assert {:ok, {:ok, {:approved, "looks fine"}}} = Dbos.await(handle, timeout_ms: 5_000)
  end

  test "a rejected request delivers the rejecter's reason", %{engine: engine} do
    {:ok, handle} =
      Dbos.start("request_approval", ["req-2", "refund", "customer asked twice", 5_000, 0],
        engine: engine,
        workflow_id: "wf-reject"
      )

    wait_for_state(engine, "wf-reject", :awaiting_response)

    Dbos.send_message("wf-reject", Approvals.response_topic(), {:rejected, "not eligible"},
      engine: engine
    )

    assert {:ok, {:ok, {:rejected, "not eligible"}}} = Dbos.await(handle, timeout_ms: 5_000)
  end

  test "a request nobody answers expires and escalates", %{engine: engine} do
    {:ok, handle} =
      Dbos.start("request_approval", ["req-3", "refund", "customer asked twice", 100, 0],
        engine: engine,
        workflow_id: "wf-timeout"
      )

    assert {:ok, :expired} = Dbos.await(handle, timeout_ms: 5_000)
  end

  test "a response sent before the workflow reaches its wait is still delivered", %{
    engine: engine
  } do
    {:ok, handle} =
      Dbos.start(
        "request_approval",
        ["req-4", "refund", "customer asked twice", 5_000, 200],
        engine: engine,
        workflow_id: "wf-race"
      )

    Dbos.send_message("wf-race", Approvals.response_topic(), {:approved, "pre-approved"},
      engine: engine
    )

    assert {:ok, {:ok, {:approved, "pre-approved"}}} = Dbos.await(handle, timeout_ms: 5_000)
  end

  defp wait_for_state(engine, workflow_id, expected, attempts \\ 200)

  defp wait_for_state(_engine, workflow_id, expected, 0) do
    flunk("workflow #{workflow_id} never reached state #{inspect(expected)}")
  end

  defp wait_for_state(engine, workflow_id, expected, attempts) do
    config = Dbos.config(engine)

    if Dbos.SystemDb.get_event_value(config, workflow_id, "state") == {:ok, expected} do
      :ok
    else
      Process.sleep(10)
      wait_for_state(engine, workflow_id, expected, attempts - 1)
    end
  end

  defp truncate_tables do
    tables = Enum.map_join(@tables, ", ", &"dbos.#{&1}")
    Postgrex.query!(AgentInbox.TestConn, "TRUNCATE TABLE #{tables} CASCADE", [])
  rescue
    Postgrex.Error -> :ok
  end
end
