defmodule Dbos.CompensationDescendantTest do
  use Dbos.Case, async: false

  alias Dbos.Compensation
  alias Dbos.Recovery
  alias Dbos.SystemDb

  defmodule Ledger do
    @moduledoc false

    def start, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def record(entry), do: Agent.update(__MODULE__, &[entry | &1])
    def entries, do: Agent.get(__MODULE__, &Enum.reverse(&1))
    def await(entry, attempts \\ 300)
    def await(entry, 0), do: ExUnit.Assertions.flunk("#{inspect(entry)} never recorded")

    def await(entry, attempts) do
      if entry in entries() do
        :ok
      else
        Process.sleep(10)
        await(entry, attempts - 1)
      end
    end
  end

  defmodule Sagas do
    @moduledoc false
    use Dbos

    defworkflow parent(trip), name: "descendant.parent" do
      reserve_seat(trip)
      child(trip)
      :booked
    end

    defworkflow parent_enqueues(trip), name: "descendant.parent_enqueues" do
      reserve_seat(trip)
      Dbos.enqueue("descendant.child", [trip], queue_name: Dbos.Queue.internal_queue_name())
      :booked
    end

    defworkflow parent_of_blocked(trip), name: "descendant.parent_of_blocked" do
      reserve_seat(trip)
      Dbos.enqueue("descendant.blocked", [trip], queue_name: Dbos.Queue.internal_queue_name())
      :booked
    end

    defworkflow parent_of_plain(trip), name: "descendant.parent_of_plain" do
      reserve_seat(trip)
      Dbos.enqueue("descendant.plain", [trip], queue_name: Dbos.Queue.internal_queue_name())
      :booked
    end

    defworkflow child(trip), name: "descendant.child" do
      charge_card(trip)
    end

    defworkflow blocked(trip), name: "descendant.blocked" do
      charge_card(trip)
      Dbos.recv_message("go", 30_000)
    end

    defworkflow plain(trip), name: "descendant.plain" do
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
      Ledger.record({:refunded, trip, charge})
      :refunded
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

  defp unwind!(workflow_id, timeout_ms \\ 10_000) do
    {:ok, handle} = Dbos.unwind(workflow_id)
    {handle, Dbos.await(handle, timeout_ms: timeout_ms)}
  end

  defp status(config, workflow_id) do
    case SystemDb.get_workflow_status(config, workflow_id) do
      {:ok, status} -> status.status
      {:error, :not_found} -> :not_found
    end
  end

  test "an awaited child is unwound by its own compensator, before the parent's own steps" do
    {:ok, parent} = Sagas.parent("t1")
    assert {:ok, :booked} = Dbos.await(parent)

    {_handle, {:ok, 2}} = unwind!(parent.workflow_id)

    assert Ledger.entries() == [
             {:reserved, "t1"},
             {:charged, "t1"},
             {:refunded, "t1", "charge-t1"},
             {:released, "t1", "seat-t1"}
           ]
  end

  test "the child's undo runs in the child's own compensator, not the parent's", %{config: config} do
    {:ok, parent} = Sagas.parent("t2")
    assert {:ok, :booked} = Dbos.await(parent)

    {handle, {:ok, _count}} = unwind!(parent.workflow_id)

    {:ok, parent_steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    child_id = parent.workflow_id <> "-1"

    assert "refund/2" not in Enum.map(parent_steps, & &1.function_name)
    assert status(config, Compensation.workflow_id(child_id)) == :success

    {:ok, child_steps} =
      SystemDb.get_workflow_steps(config, Compensation.workflow_id(child_id))

    assert Enum.map(child_steps, & &1.function_name) == ["refund/2"]
  end

  test "an enqueued child that finished is unwound too" do
    {:ok, parent} = Sagas.parent_enqueues("t3")
    assert {:ok, :booked} = Dbos.await(parent)
    Ledger.await({:charged, "t3"})

    {_handle, {:ok, 2}} = unwind!(parent.workflow_id)

    assert {:refunded, "t3", "charge-t3"} in Ledger.entries()
    assert {:released, "t3", "seat-t3"} in Ledger.entries()
  end

  @tag timeout: 20_000
  test "a descendant still running is cancelled, and unwinds itself", %{config: config} do
    {:ok, parent} = Sagas.parent_of_blocked("t4")
    assert {:ok, :booked} = Dbos.await(parent)
    Ledger.await({:charged, "t4"})

    {_handle, {:ok, 2}} = unwind!(parent.workflow_id)

    assert {:refunded, "t4", "charge-t4"} in Ledger.entries()
    assert {:released, "t4", "seat-t4"} in Ledger.entries()

    [child_id] = descendant_ids(config, parent.workflow_id)
    assert status(config, child_id) == :cancelled
  end

  test "a descendant with nothing to reverse gets no compensator of its own", %{
    config: config
  } do
    {:ok, parent} = Sagas.parent_of_plain("t5")
    assert {:ok, :booked} = Dbos.await(parent)
    Ledger.await({:audited, "t5"})

    {_handle, {:ok, 2}} = unwind!(parent.workflow_id)

    [child_id] = descendant_ids(config, parent.workflow_id)
    assert status(config, Compensation.workflow_id(child_id)) == :not_found
    assert {:released, "t5", "seat-t5"} in Ledger.entries()
  end

  test "a descendant already unwound is skipped rather than unwound twice" do
    {:ok, parent} = Sagas.parent("t6")
    assert {:ok, :booked} = Dbos.await(parent)

    child_id = parent.workflow_id <> "-1"
    {_child_unwind, {:ok, 1}} = unwind!(child_id)
    assert {:refunded, "t6", "charge-t6"} in Ledger.entries()

    {_handle, {:ok, 2}} = unwind!(parent.workflow_id)

    assert Enum.count(Ledger.entries(), &match?({:refunded, _, _}, &1)) == 1
    assert {:released, "t6", "seat-t6"} in Ledger.entries()
  end

  test "the whole unwind is one workflow tree under the compensator", %{config: config} do
    {:ok, parent} = Sagas.parent("t7")
    assert {:ok, :booked} = Dbos.await(parent)

    {handle, {:ok, _count}} = unwind!(parent.workflow_id)

    child_unwind_id = Compensation.workflow_id(parent.workflow_id <> "-1")
    {:ok, child_unwind} = SystemDb.get_workflow_status(config, child_unwind_id)

    assert child_unwind.parent_workflow_id == handle.workflow_id
  end

  defp descendant_ids(config, workflow_id) do
    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

    steps
    |> Enum.filter(&(&1.function_name == "DBOS.enqueue"))
    |> Enum.map(& &1.output)
  end
end
