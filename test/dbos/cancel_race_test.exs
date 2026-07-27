defmodule Dbos.CancelRaceTest do
  @moduledoc """
  Cancelling a workflow that is running somewhere else. `Dbos.cancel/2` records an intent and
  returns; the workflow notices at its next checkpoint. These pin down what happens in the window
  between the two, which is where a workflow owned by another engine always sits.
  """

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

    defworkflow settle(payout), name: "race.settle" do
      debit(payout)
      Dbos.recv_message("go", 30_000)
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

  test "a workflow that finishes after being cancelled is unwound, not reported successful", %{
    config: config
  } do
    {:ok, handle} = Sagas.settle("p1")
    wait_until(fn -> {:debited, "p1"} in Ledger.entries() end)

    # The window a peer's workflow always sits in: cancelled, but still running its last step.
    Postgrex.query!(
      Dbos.TestConn,
      ~s(UPDATE "dbos".workflow_status SET status = 'CANCELLING' WHERE workflow_uuid = $1),
      [handle.workflow_id]
    )

    :ok = Dbos.send_message(handle.workflow_id, "go", :ready)

    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle, timeout_ms: 5_000)
    assert status(config, handle.workflow_id) == :cancelled

    assert {:ok, 1} =
             Dbos.await(
               %Dbos.WorkflowHandle{
                 engine: Dbos.current_engine(),
                 workflow_id: Compensation.workflow_id(handle.workflow_id)
               },
               timeout_ms: 5_000
             )

    assert {:reversed, "p1", "debit-p1"} in Ledger.entries()
  end

  test "a success cannot overwrite a cancellation in progress", %{config: config} do
    {:ok, handle} = Sagas.settle("p2")
    wait_until(fn -> {:debited, "p2"} in Ledger.entries() end)

    Postgrex.query!(
      Dbos.TestConn,
      ~s(UPDATE "dbos".workflow_status SET status = 'CANCELLING' WHERE workflow_uuid = $1),
      [handle.workflow_id]
    )

    assert_raise Dbos.WorkflowCancellingError, fn ->
      SystemDb.update_workflow_outcome(config, handle.workflow_id, %{
        status: :success,
        output: Dbos.Serialization.encode(:settled)
      })
    end

    assert status(config, handle.workflow_id) == :cancelling
  end

  test "completing the cancellation itself is permitted from CANCELLING", %{config: config} do
    {:ok, handle} = Sagas.settle("p3")
    wait_until(fn -> {:debited, "p3"} in Ledger.entries() end)

    Postgrex.query!(
      Dbos.TestConn,
      ~s(UPDATE "dbos".workflow_status SET status = 'CANCELLING' WHERE workflow_uuid = $1),
      [handle.workflow_id]
    )

    assert SystemDb.update_workflow_outcome(config, handle.workflow_id, %{status: :cancelled}) ==
             :ok

    assert status(config, handle.workflow_id) == :cancelled
  end

  test "a step already in flight when the cancellation lands still checkpoints", %{config: config} do
    {:ok, handle} = Sagas.settle("p4")
    wait_until(fn -> {:debited, "p4"} in Ledger.entries() end)

    :ok = Dbos.cancel(handle.workflow_id)
    assert {:error, _reason} = Dbos.await(handle, timeout_ms: 5_000)

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert "debit/1" in Enum.map(steps, & &1.function_name)
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
