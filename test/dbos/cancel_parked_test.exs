defmodule Dbos.CancelParkedTest do
  @moduledoc """
  Cancelling a workflow that has parked its wait. A parked wait has no process to notify and its own
  timer is set for the deadline it parked on, so without a nudge into `Dbos.Waits` the cancellation
  lands only when that deadline fires — minutes or hours later.
  """

  use Dbos.Case, async: false

  alias Dbos.Compensation
  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.Waits
  alias Dbos.WorkflowSup

  # Far longer than any assertion here waits, so nothing can pass by the deadline arriving.
  @long_wait_ms 300_000

  defmodule Ledger do
    @moduledoc false

    def start, do: Agent.start_link(fn -> [] end, name: __MODULE__)
    def record(entry), do: Agent.update(__MODULE__, &[entry | &1])
    def entries, do: Agent.get(__MODULE__, &Enum.reverse(&1))
  end

  defmodule Sagas do
    @moduledoc false
    use Dbos

    defworkflow settle(payout), name: "parked.settle" do
      debit(payout)
      Dbos.recv_message("settlement", 300_000)
      :settled
    end

    defstep debit(payout), compensate: &reverse_debit(payout, &1) do
      Ledger.record({:debited, payout})
      "debit-#{payout}"
    end

    defstep reverse_debit(payout, debit) do
      Ledger.record({:reversed, payout, debit})
      :reversed
    end
  end

  defp start_engine(workflows, opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    defaults = [
      name: name,
      db: {Dbos.DB.Postgrex, Dbos.TestConn},
      executor_id: "exec-#{System.unique_integer([:positive])}",
      workflows: workflows,
      lease_sweep: [enabled: false],
      migrations: :skip,
      park_exit_threshold_ms: 50
    ]

    start_supervised!({Dbos.Supervisor, Keyword.merge(defaults, opts)}, id: name)
    Recovery.await_boot_recovery(name)
    name
  end

  defp await_parked(engine, workflow_id) do
    wait_until(fn -> WorkflowSup.whereis(engine, workflow_id) == :error end)
    assert Waits.count(engine) == 1
    :ok
  end

  defp status(config, workflow_id) do
    case SystemDb.get_workflow_status(config, workflow_id) do
      {:ok, status} -> status.status
      {:error, :not_found} -> :not_found
    end
  end

  test "cancelling a parked workflow with nothing to compensate settles it at once" do
    engine = start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}], [])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("receiver/2", ["topic", @long_wait_ms], engine: engine)
    await_parked(engine, handle.workflow_id)

    :ok = Dbos.cancel(handle.workflow_id, engine: engine)

    assert {:error, %Dbos.WorkflowCancelledError{}} =
             Dbos.await(handle, timeout_ms: 5_000)

    assert status(config, handle.workflow_id) == :cancelled
    assert Waits.count(engine) == 0
  end

  test "cancelling a parked workflow with effects unwinds it at once" do
    start_supervised!(%{id: Ledger, start: {Ledger, :start, []}})
    engine = start_engine([Sagas], [])
    config = Dbos.config(engine)
    Dbos.put_engine(engine)

    {:ok, handle} = Sagas.settle("p1")
    wait_until(fn -> {:debited, "p1"} in Ledger.entries() end)
    await_parked(engine, handle.workflow_id)

    :ok = Dbos.cancel(handle.workflow_id, engine: engine)

    assert {:error, %Dbos.WorkflowCancelledError{}} =
             Dbos.await(handle, timeout_ms: 5_000)

    assert {:ok, 1} =
             Dbos.await(
               %Dbos.WorkflowHandle{
                 engine: engine,
                 workflow_id: Compensation.workflow_id(handle.workflow_id)
               },
               timeout_ms: 5_000
             )

    assert Ledger.entries() == [{:debited, "p1"}, {:reversed, "p1", "debit-p1"}]
    assert status(config, handle.workflow_id) == :cancelled
  end

  test "the parked entry is released rather than left to fire against a terminal row" do
    engine = start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}], [])

    {:ok, handle} = Dbos.start("receiver/2", ["topic", @long_wait_ms], engine: engine)
    await_parked(engine, handle.workflow_id)

    :ok = Dbos.cancel(handle.workflow_id, engine: engine)
    assert {:error, _reason} = Dbos.await(handle, timeout_ms: 5_000)

    wait_until(fn -> Waits.count(engine) == 0 end)
    assert Waits.count(engine) == 0
  end

  test "a global timeout wakes a parked workflow too" do
    engine =
      start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}],
        admin_server: [enabled: true, port: 0]
      )

    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("receiver/2", ["topic", @long_wait_ms], engine: engine)
    await_parked(engine, handle.workflow_id)

    config
    |> SystemDb.cancel_all_before(System.os_time(:millisecond))
    |> then(&Dbos.wake_cancelled(engine, &1))

    assert {:error, %Dbos.WorkflowCancelledError{}} =
             Dbos.await(handle, timeout_ms: 5_000)
  end

  test "waking a workflow that is not parked here is a no-op" do
    engine = start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}], [])

    assert Waits.wake_parked(engine, "never-parked") == :ok
    assert Waits.count(engine) == 0
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
