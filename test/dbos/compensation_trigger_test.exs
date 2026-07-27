defmodule Dbos.CompensationTriggerTest do
  use Dbos.Case, async: false

  alias Dbos.Compensation
  alias Dbos.Recovery
  alias Dbos.SystemDb

  defmodule Ledger do
    @moduledoc false

    def start, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def record(entry), do: Agent.update(__MODULE__, &[entry | &1])
    def entries, do: Agent.get(__MODULE__, &Enum.reverse(&1))
  end

  defmodule Sagas do
    @moduledoc false
    use Dbos

    defworkflow crash(trip), name: "trigger.crash" do
      reserve_seat(trip)
      raise "gateway down"
    end

    defworkflow decline(trip), name: "trigger.decline" do
      reserve_seat(trip)
      Dbos.abort("payment declined")
    end

    defworkflow abort_in_step(trip), name: "trigger.abort_in_step" do
      reserve_seat(trip)
      check_limit(trip)
    end

    defworkflow crash_uncompensated(trip), name: "trigger.uncompensated" do
      audit(trip)
      raise "gateway down"
    end

    defworkflow succeed(trip), name: "trigger.succeed" do
      reserve_seat(trip)
    end

    defstep reserve_seat(trip), compensate: &release_seat(trip, &1) do
      Ledger.record({:reserved, trip})
      "seat-#{trip}"
    end

    defstep release_seat(trip, seat) do
      Ledger.record({:released, trip, seat})
      :released
    end

    defstep check_limit(trip) do
      Dbos.abort({:over_limit, trip})
    end

    defstep audit(trip) do
      Ledger.record({:audited, trip})
      :audited
    end
  end

  setup do
    start_supervised!(%{id: Ledger, start: {Ledger, :start, []}})

    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Postgrex, Dbos.TestConn},
       executor_id: "exec-#{System.unique_integer([:positive])}",
       workflows: [Sagas],
       lease_sweep: [enabled: false],
       migrations: :skip},
      id: name
    )

    Recovery.await_boot_recovery(name)
    Dbos.put_engine(name)

    {:ok, engine: name, config: Dbos.config(name)}
  end

  defp await_unwound(workflow_id) do
    handle = %Dbos.WorkflowHandle{engine: Dbos.current_engine(), workflow_id: workflow_id}
    Dbos.await(handle)
  end

  defp unwind_status(config, workflow_id) do
    SystemDb.get_workflow_status(config, Compensation.workflow_id(workflow_id))
  end

  test "an uncaught exception unwinds the workflow without being asked", %{config: config} do
    {:ok, handle} = Sagas.crash("t1")
    assert {:error, error} = Dbos.await(handle)
    assert Exception.message(error) =~ "gateway down"

    assert {:ok, 1} = await_unwound(Compensation.workflow_id(handle.workflow_id))
    assert Ledger.entries() == [{:reserved, "t1"}, {:released, "t1", "seat-t1"}]

    {:ok, unwind} = unwind_status(config, handle.workflow_id)
    assert unwind.status == :success
    assert unwind.parent_workflow_id == handle.workflow_id
  end

  test "Dbos.abort/1 unwinds the workflow and records the reason as its error" do
    {:ok, handle} = Sagas.decline("t2")
    assert {:error, %Dbos.AbortError{} = error} = Dbos.await(handle)
    assert Exception.message(error) == "payment declined"

    assert {:ok, 1} = await_unwound(Compensation.workflow_id(handle.workflow_id))
    assert {:released, "t2", "seat-t2"} in Ledger.entries()
  end

  test "Dbos.abort/1 from inside a step aborts the workflow around it" do
    {:ok, handle} = Sagas.abort_in_step("t3")
    assert {:error, %Dbos.AbortError{reason: {:over_limit, "t3"}}} = Dbos.await(handle)

    assert {:ok, 1} = await_unwound(Compensation.workflow_id(handle.workflow_id))
    assert {:released, "t3", "seat-t3"} in Ledger.entries()
  end

  test "a failed workflow with nothing to compensate enqueues no unwind", %{config: config} do
    {:ok, handle} = Sagas.crash_uncompensated("t4")
    assert {:error, _error} = Dbos.await(handle)

    assert unwind_status(config, handle.workflow_id) == {:error, :not_found}
    assert Ledger.entries() == [{:audited, "t4"}]
  end

  test "a successful workflow enqueues no unwind", %{config: config} do
    {:ok, handle} = Sagas.succeed("t5")
    assert {:ok, "seat-t5"} = Dbos.await(handle)

    assert unwind_status(config, handle.workflow_id) == {:error, :not_found}
    refute Enum.any?(Ledger.entries(), &match?({:released, _, _}, &1))
  end

  test "an unwind that fails does not enqueue an unwind of its own", %{config: config} do
    {:ok, handle} = Sagas.crash("t6")
    assert {:error, _error} = Dbos.await(handle)
    assert {:ok, 1} = await_unwound(Compensation.workflow_id(handle.workflow_id))

    unwind_id = Compensation.workflow_id(handle.workflow_id)
    assert unwind_status(config, unwind_id) == {:error, :not_found}
  end

  test "the unwind is enqueued in the same transaction as the ERROR status", %{config: config} do
    {:ok, handle} = Sagas.crash("t7")
    assert {:error, _error} = Dbos.await(handle)

    {:ok, failed} = SystemDb.get_workflow_status(config, handle.workflow_id)
    {:ok, unwind} = unwind_status(config, handle.workflow_id)

    assert failed.status == :error
    assert unwind.name == Compensation.workflow_name()
    assert unwind.inputs == [handle.workflow_id]
  end
end
