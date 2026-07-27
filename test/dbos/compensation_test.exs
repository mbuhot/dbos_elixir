defmodule Dbos.CompensationTest do
  use Dbos.Case, async: false

  alias Dbos.Recovery
  alias Dbos.SystemDb

  defmodule Ledger do
    @moduledoc false

    def start, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def record(entry), do: Agent.update(__MODULE__, &[entry | &1])
    def entries, do: Agent.get(__MODULE__, &Enum.reverse(&1))

    def fail_next(step), do: Agent.update(__MODULE__, &[{:fail, step} | &1])

    def failing?(step) do
      Agent.get_and_update(__MODULE__, fn entries ->
        if {:fail, step} in entries,
          do: {true, List.delete(entries, {:fail, step})},
          else: {false, entries}
      end)
    end
  end

  defmodule Sagas do
    @moduledoc false
    use Dbos

    defworkflow book(trip), name: "saga.book" do
      reserve_seat(trip)
      charge_card(trip)
      issue_ticket(trip)
    end

    defworkflow partly(trip), name: "saga.partly" do
      reserve_seat(trip)
      audit(trip)
    end

    defstep reserve_seat(trip), compensate: &release_seat(trip, &1) do
      Ledger.record({:reserved, trip})
      "seat-#{trip}"
    end

    defstep release_seat(trip, seat) do
      Ledger.record({:released, trip, seat})
      :released
    end

    defstep charge_card(trip), compensate: &refund(trip, &1) do
      Ledger.record({:charged, trip})
      "charge-#{trip}"
    end

    defstep refund(trip, charge) do
      if Ledger.failing?(:refund), do: raise("refund declined")
      Ledger.record({:refunded, trip, charge})
      :refunded
    end

    defstep issue_ticket(trip), compensate: &void_ticket(trip, &1) do
      Ledger.record({:issued, trip})
      "ticket-#{trip}"
    end

    defstep void_ticket(trip, ticket) do
      Ledger.record({:voided, trip, ticket})
      :voided
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

  defp watch_stuck do
    handler_id = "compensation-stuck-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach(
      handler_id,
      [:dbos, :compensation, :stuck],
      fn _event, measurements, metadata, _cfg ->
        send(test_pid, {:stuck, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  defp unwind!(workflow_id) do
    {:ok, handle} = Dbos.unwind(workflow_id)
    {handle, Dbos.await(handle)}
  end

  test "the unwind runs every recorded compensation, newest first" do
    {:ok, booked} = Sagas.book("t1")
    assert {:ok, "ticket-t1"} = Dbos.await(booked)

    {_handle, {:ok, count}} = unwind!(booked.workflow_id)

    assert count == 3

    assert Ledger.entries() == [
             {:reserved, "t1"},
             {:charged, "t1"},
             {:issued, "t1"},
             {:voided, "t1", "ticket-t1"},
             {:refunded, "t1", "charge-t1"},
             {:released, "t1", "seat-t1"}
           ]
  end

  test "a step declaring no compensation is passed over" do
    {:ok, booked} = Sagas.partly("t2")
    assert {:ok, :audited} = Dbos.await(booked)

    {_handle, {:ok, count}} = unwind!(booked.workflow_id)

    assert count == 1
    assert Ledger.entries() == [{:reserved, "t2"}, {:audited, "t2"}, {:released, "t2", "seat-t2"}]
  end

  test "a workflow with nothing to unwind completes having run no undos" do
    {:ok, handle} = Dbos.start("saga.partly", ["t3"])
    assert {:ok, :audited} = Dbos.await(handle)

    Postgrex.query!(
      Dbos.TestConn,
      ~s(DELETE FROM "dbos".operation_outputs WHERE workflow_uuid = $1),
      [handle.workflow_id]
    )

    assert {_handle, {:ok, 0}} = unwind!(handle.workflow_id)
  end

  test "each undo is checkpointed as its own step of the compensator" do
    {:ok, booked} = Sagas.book("t4")
    assert {:ok, "ticket-t4"} = Dbos.await(booked)

    {handle, {:ok, _count}} = unwind!(booked.workflow_id)

    {:ok, steps} = SystemDb.get_workflow_steps(Dbos.config(), handle.workflow_id)

    assert Enum.map(steps, & &1.function_name) == [
             "void_ticket/2",
             "refund/2",
             "release_seat/2"
           ]
  end

  test "the compensator's id is derived from its target, so unwinding twice runs the undos once" do
    {:ok, booked} = Sagas.book("t5")
    assert {:ok, "ticket-t5"} = Dbos.await(booked)

    {first, {:ok, 3}} = unwind!(booked.workflow_id)
    {second, {:ok, 3}} = unwind!(booked.workflow_id)

    assert first.workflow_id == second.workflow_id
    assert first.workflow_id == booked.workflow_id <> "-compensate"

    assert Enum.count(Ledger.entries(), &match?({:released, _, _}, &1)) == 1
  end

  describe "when an undo fails" do
    test "the unwind stops there, leaving the earlier steps untouched" do
      {:ok, booked} = Sagas.book("t6")
      assert {:ok, "ticket-t6"} = Dbos.await(booked)

      Ledger.fail_next(:refund)
      {handle, {:error, error}} = unwind!(booked.workflow_id)

      assert Exception.message(error) =~ "refund declined"

      {:ok, status} = SystemDb.get_workflow_status(Dbos.config(), handle.workflow_id)
      assert status.status == :error

      entries = Ledger.entries()
      assert {:voided, "t6", "ticket-t6"} in entries
      refute Enum.any?(entries, &match?({:released, _, _}, &1))
    end

    test "forking at the reported step resumes there, keeping the completed undos done" do
      watch_stuck()

      {:ok, booked} = Sagas.book("t7")
      assert {:ok, "ticket-t7"} = Dbos.await(booked)

      Ledger.fail_next(:refund)
      {handle, {:error, _error}} = unwind!(booked.workflow_id)
      assert_receive {:stuck, _measurements, %{step_id: step_id}}

      {:ok, resumed} = Dbos.fork(handle.workflow_id, step_id)
      assert {:ok, 3} = Dbos.await(resumed)

      assert Enum.count(Ledger.entries(), &match?({:voided, _, _}, &1)) == 1
      assert Enum.count(Ledger.entries(), &match?({:refunded, _, _}, &1)) == 1
      assert Enum.count(Ledger.entries(), &match?({:released, _, _}, &1)) == 1
    end

    test "a retry replays the recorded failure rather than re-running the undo" do
      {:ok, booked} = Sagas.book("t9")
      assert {:ok, "ticket-t9"} = Dbos.await(booked)

      Ledger.fail_next(:refund)
      {handle, {:error, _error}} = unwind!(booked.workflow_id)

      assert :ok = Dbos.retry(handle.workflow_id)
      assert {:error, replayed} = Dbos.await(handle)
      assert Exception.message(replayed) =~ "refund declined"

      refute Enum.any?(Ledger.entries(), &match?({:released, _, _}, &1))
    end

    test "it reports how much of the history is still outstanding" do
      watch_stuck()

      {:ok, booked} = Sagas.book("t8")
      assert {:ok, "ticket-t8"} = Dbos.await(booked)

      Ledger.fail_next(:refund)
      {_handle, {:error, _error}} = unwind!(booked.workflow_id)

      assert_receive {:stuck, %{reversed: 1, outstanding: 2}, metadata}
      assert metadata.target_workflow_id == booked.workflow_id
      assert metadata.function_name == "charge_card/1"
      assert Exception.message(metadata.reason) =~ "refund declined"
    end
  end
end
