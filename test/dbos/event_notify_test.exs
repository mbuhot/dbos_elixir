defmodule Dbos.EventNotifyTest do
  @moduledoc """
  `workflow_events` is a mutable key-value store — `set_event` upserts on `(workflow_uuid, key)`, so
  every write after a key's first is an `UPDATE`. A subscriber has to hear all of them, not only the
  one that created the row.

  Every write here goes through `Dbos.SystemDb` while the assertions wait on a `LISTEN` connection, so
  a delivery can only have come from the trigger.
  """

  use Dbos.Case, async: false

  alias Dbos.Notifications
  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

  @key "stage"

  defp start_engine do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       [
         name: name,
         db: {Dbos.DB.Postgrex, Dbos.TestConn},
         executor_id: "exec-#{System.unique_integer([:positive])}",
         workflows: [{"add/2", {SampleWorkflows, :add, 2}}],
         lease_sweep: [enabled: false],
         migrations: :skip,
         notifications_conn_opts: [database: Application.fetch_env!(:dbos, :test_database)]
       ]},
      id: name
    )

    Recovery.await_boot_recovery(name)
    await_listening(name)
    name
  end

  defp await_listening(engine, attempts \\ 200)
  defp await_listening(_engine, 0), do: flunk("engine never established its LISTEN connection")

  defp await_listening(engine, attempts) do
    if Notifications.mode(engine) == :listen do
      :ok
    else
      Process.sleep(10)
      await_listening(engine, attempts - 1)
    end
  end

  defp set_event(config, workflow_id, function_id, value) do
    SystemDb.set_event_value(config, workflow_id, function_id, @key, value)
  end

  defp setter(config) do
    workflow_id = Dbos.Uuid.v4()

    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "add/2"
    })

    workflow_id
  end

  test "re-setting a key announces the new value" do
    engine = start_engine()
    config = Dbos.config(engine)
    workflow_id = setter(config)

    :ok = Notifications.subscribe_event(engine, workflow_id, @key)

    set_event(config, workflow_id, 0, :quoting)
    assert_receive {:dbos_notify, :event, _payload}, 2_000

    set_event(config, workflow_id, 1, :awaiting_confirmation)
    assert_receive {:dbos_notify, :event, payload}, 2_000
    assert payload == "#{workflow_id}::#{@key}"

    assert {:ok, :awaiting_confirmation} =
             SystemDb.get_event_value(config, workflow_id, @key)
  end

  test "an engine-wide subscriber hears every write, not only the first" do
    engine = start_engine()
    config = Dbos.config(engine)
    workflow_id = setter(config)

    :ok = Notifications.subscribe_all(engine, [:event])

    Enum.each([:quoting, :quoted, :awaiting_confirmation, :confirmed], fn value ->
      set_event(config, workflow_id, 0, value)
      assert_receive {:dbos_notification, :event, ^workflow_id, @key}, 2_000
    end)
  end

  test "a write that changes nothing is still announced" do
    engine = start_engine()
    config = Dbos.config(engine)
    workflow_id = setter(config)

    :ok = Notifications.subscribe_event(engine, workflow_id, @key)

    set_event(config, workflow_id, 0, :quoting)
    assert_receive {:dbos_notify, :event, _first}, 2_000

    set_event(config, workflow_id, 1, :quoting)
    assert_receive {:dbos_notify, :event, _second}, 2_000
  end
end
