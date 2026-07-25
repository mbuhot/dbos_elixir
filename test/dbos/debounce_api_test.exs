defmodule Dbos.DebounceApiTest do
  @moduledoc """
  Tests `Dbos.debounce/3`, the engine-level wrapper over `Dbos.Debouncer`: resolving a workflow
  the same way `Dbos.enqueue/3` does, collapsing repeated calls onto one delayed workflow, and
  running exactly once when the delay elapses.
  """

  use Dbos.Case, async: false

  alias Dbos.Queue
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.Testing

  @workflows [{"add/2", {SampleWorkflows, :add, 2}}]

  defp start_engine do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       [
         name: name,
         db: {Dbos.DB.Postgrex, Dbos.TestConn},
         executor_id: "exec-#{System.unique_integer([:positive])}",
         migrations: :skip,
         testing: :manual,
         workflows: @workflows,
         queues: [Queue.new("debounce_queue")]
       ]},
      id: name
    )

    name
  end

  defp config(engine), do: Dbos.config(engine)

  test "the first call schedules the work for one period from now" do
    engine = start_engine()
    now = System.os_time(:millisecond)

    {:ok, handle} =
      Dbos.debounce(&SampleWorkflows.add/2, [1, 2],
        engine: engine,
        queue_name: "debounce_queue",
        debounce_key: "cust-1",
        period_ms: 5_000
      )

    {:ok, status} = SystemDb.get_workflow_status(config(engine), handle.workflow_id)

    assert status.status == :delayed
    assert status.inputs == [1, 2]
    assert_in_delta status.delay_until_epoch_ms, now + 5_000, 500
  end

  test "a second call replaces the pending arguments instead of scheduling more work" do
    engine = start_engine()

    {:ok, first} =
      Dbos.debounce(&SampleWorkflows.add/2, [1, 2],
        engine: engine,
        queue_name: "debounce_queue",
        debounce_key: "cust-1",
        period_ms: 5_000
      )

    {:ok, second} =
      Dbos.debounce(&SampleWorkflows.add/2, [10, 20],
        engine: engine,
        queue_name: "debounce_queue",
        debounce_key: "cust-1",
        period_ms: 5_000
      )

    assert second.workflow_id == first.workflow_id

    {:ok, status} = SystemDb.get_workflow_status(config(engine), first.workflow_id)
    assert status.inputs == [10, 20]
  end

  test "two different keys are debounced independently" do
    engine = start_engine()

    {:ok, first} =
      Dbos.debounce(&SampleWorkflows.add/2, [1, 2],
        engine: engine,
        queue_name: "debounce_queue",
        debounce_key: "cust-1",
        period_ms: 5_000
      )

    {:ok, second} =
      Dbos.debounce(&SampleWorkflows.add/2, [3, 4],
        engine: engine,
        queue_name: "debounce_queue",
        debounce_key: "cust-2",
        period_ms: 5_000
      )

    assert second.workflow_id != first.workflow_id
  end

  test "the collapsed work runs once with the latest arguments after the delay elapses" do
    engine = start_engine()

    {:ok, _first} =
      Dbos.debounce(&SampleWorkflows.add/2, [1, 2],
        engine: engine,
        queue_name: "debounce_queue",
        debounce_key: "cust-1",
        period_ms: 50
      )

    {:ok, handle} =
      Dbos.debounce(&SampleWorkflows.add/2, [10, 20],
        engine: engine,
        queue_name: "debounce_queue",
        debounce_key: "cust-1",
        period_ms: 50
      )

    Process.sleep(120)

    assert Testing.drain_queue("debounce_queue", engine: engine) == 1
    assert {:ok, 30} = Dbos.await(handle)
  end

  test "the deadline caps how far repeated calls can push the work out" do
    engine = start_engine()
    now = System.os_time(:millisecond)

    {:ok, handle} =
      Dbos.debounce(&SampleWorkflows.add/2, [1, 2],
        engine: engine,
        queue_name: "debounce_queue",
        debounce_key: "cust-1",
        period_ms: 5_000,
        deadline_ms: 1_000
      )

    {:ok, ^handle} =
      Dbos.debounce(&SampleWorkflows.add/2, [3, 4],
        engine: engine,
        queue_name: "debounce_queue",
        debounce_key: "cust-1",
        period_ms: 5_000,
        deadline_ms: 1_000
      )

    {:ok, status} = SystemDb.get_workflow_status(config(engine), handle.workflow_id)
    assert_in_delta status.delay_until_epoch_ms, now + 1_000, 500
  end
end
