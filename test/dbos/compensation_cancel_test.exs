defmodule Dbos.CompensationCancelTest do
  use Dbos.Case, async: false

  alias Dbos.Compensation
  alias Dbos.LeaseSweep
  alias Dbos.Recovery
  alias Dbos.SystemDb

  defmodule Ledger do
    @moduledoc false

    def start, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def record(entry), do: Agent.update(__MODULE__, &[entry | &1])
    def entries, do: Agent.get(__MODULE__, &Enum.reverse(&1))
    def await(entry, attempts \\ 200)
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

    defworkflow long(trip), name: "cancel.long" do
      reserve_seat(trip)
      Dbos.recv_message("go", 30_000)
      charge(trip)
    end

    defworkflow plain(trip), name: "cancel.plain" do
      audit(trip)
      Dbos.recv_message("go", 30_000)
    end

    defstep reserve_seat(trip), compensate: &release_seat(trip, &1) do
      Ledger.record({:reserved, trip})
      "seat-#{trip}"
    end

    defstep release_seat(trip, seat) do
      Ledger.record({:released, trip, seat})
      :released
    end

    defstep charge(trip) do
      Ledger.record({:charged, trip})
      :charged
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

  defp status(config, workflow_id) do
    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    status.status
  end

  @tag timeout: 15_000
  test "cancelling a running workflow stops its blocked wait rather than waiting it out", %{
    config: config
  } do
    {:ok, handle} = Sagas.long("t1")
    Ledger.await({:reserved, "t1"})

    :ok = Dbos.cancel(handle.workflow_id)

    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle, timeout_ms: 5_000)
    assert status(config, handle.workflow_id) == :cancelled

    assert {:ok, 1} =
             Dbos.await(%Dbos.WorkflowHandle{
               engine: Dbos.current_engine(),
               workflow_id: Compensation.workflow_id(handle.workflow_id)
             })

    assert {:released, "t1", "seat-t1"} in Ledger.entries()
    refute Enum.any?(Ledger.entries(), &match?({:charged, _}, &1))
  end

  test "cancelling a workflow with nothing to compensate goes straight to CANCELLED", %{
    config: config
  } do
    {:ok, handle} = Sagas.plain("t2")
    Ledger.await({:audited, "t2"})

    :ok = Dbos.cancel(handle.workflow_id)

    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle)
    assert status(config, handle.workflow_id) == :cancelled

    assert SystemDb.get_workflow_status(config, Compensation.workflow_id(handle.workflow_id)) ==
             {:error, :not_found}
  end

  test "an ENQUEUED workflow is cancelled outright, having nothing recorded yet", %{
    config: config
  } do
    {:ok, handle} =
      Dbos.enqueue("cancel.long", ["t3"], queue_name: Dbos.Queue.internal_queue_name())

    Postgrex.query!(
      Dbos.TestConn,
      ~s(UPDATE "dbos".workflow_status SET status = 'ENQUEUED' WHERE workflow_uuid = $1),
      [handle.workflow_id]
    )

    :ok = Dbos.cancel(handle.workflow_id)

    assert status(config, handle.workflow_id) == :cancelled
  end

  describe "a workflow abandoned mid-cancellation" do
    test "is finished by the lease sweep, which enqueues its unwind", %{
      engine: engine,
      config: config
    } do
      {:ok, handle} = Sagas.long("t4")
      Ledger.await({:reserved, "t4"})

      Postgrex.query!(
        Dbos.TestConn,
        ~s(UPDATE "dbos".workflow_status SET status = 'CANCELLING', executor_id = 'exec-gone' WHERE workflow_uuid = $1),
        [handle.workflow_id]
      )

      assert status(config, handle.workflow_id) == :cancelling

      LeaseSweep.sweep_now(engine)

      assert status(config, handle.workflow_id) == :cancelled
      Ledger.await({:released, "t4", "seat-t4"})
    end

    test "is left alone while its executor's lease is live", %{engine: engine, config: config} do
      {:ok, handle} = Sagas.long("t5")
      Ledger.await({:reserved, "t5"})

      Postgrex.query!(
        Dbos.TestConn,
        ~s(UPDATE "dbos".workflow_status SET status = 'CANCELLING' WHERE workflow_uuid = $1),
        [handle.workflow_id]
      )

      LeaseSweep.sweep_now(engine)

      assert status(config, handle.workflow_id) == :cancelling
    end
  end

  test "CANCELLING is not terminal, so recovery does not replay it forward", %{config: config} do
    {:ok, handle} = Sagas.long("t6")
    Ledger.await({:reserved, "t6"})

    Postgrex.query!(
      Dbos.TestConn,
      ~s(UPDATE "dbos".workflow_status SET status = 'CANCELLING' WHERE workflow_uuid = $1),
      [handle.workflow_id]
    )

    Recovery.recover_pending(Dbos.current_engine())

    assert status(config, handle.workflow_id) == :cancelling
    refute Enum.any?(Ledger.entries(), &match?({:charged, _}, &1))
  end
end
