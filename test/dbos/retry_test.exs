defmodule Dbos.RetryTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

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

  defp new_table do
    table = :"retry_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])
    table
  end

  defp wait_until(fun, attempts \\ 200)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  defp await_status(config, workflow_id, expected) do
    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      status.status == expected
    end)

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    status
  end

  test "a workflow that failed runs to completion on retry, without re-running its completed steps" do
    engine =
      start_engine([{"fails_once_after_step/1", {SampleWorkflows, :fails_once_after_step, 1}}])

    config = Dbos.config(engine)
    table = new_table()

    {:ok, handle} = Dbos.start("fails_once_after_step/1", [table], engine: engine)
    assert {:error, %RuntimeError{message: "workflow body failed"}} = Dbos.await(handle)

    failed = await_status(config, handle.workflow_id, :error)
    assert failed.error.value == %RuntimeError{message: "workflow body failed"}

    :ok = Dbos.retry(handle.workflow_id, engine: engine)

    succeeded = await_status(config, handle.workflow_id, :success)
    assert succeeded.output == :recovered
    assert succeeded.error == nil
    assert :ets.lookup_element(table, :first_step_runs, 2) == 1

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_name) == ["first_step/0"]
  end

  test "the error a caller read is gone once the workflow is retried" do
    engine =
      start_engine([{"fails_once_after_step/1", {SampleWorkflows, :fails_once_after_step, 1}}])

    table = new_table()

    {:ok, handle} = Dbos.start("fails_once_after_step/1", [table], engine: engine)
    assert {:error, %RuntimeError{}} = Dbos.await(handle)

    assert {:error, %{value: %RuntimeError{}}} =
             Dbos.result(handle.workflow_id, engine: engine)

    :ok = Dbos.retry(handle.workflow_id, engine: engine)

    wait_until(fn -> Dbos.result(handle.workflow_id, engine: engine) == {:ok, :recovered} end)
  end

  test "retrying a successful workflow leaves its result untouched" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("add/2", [2, 3], engine: engine)
    assert {:ok, 5} = Dbos.await(handle)

    :ok = Dbos.retry(handle.workflow_id, engine: engine)
    Process.sleep(20)

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :success
    assert status.output == 5
  end

  test "a cancelled workflow continues from its last checkpoint when retried" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    config = Dbos.config(engine)

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-retry-cancelled",
      status: :pending,
      name: "three_steps/1",
      inputs: ["ord_1"]
    })

    SystemDb.record_operation_result(config, %{
      workflow_id: "wf-retry-cancelled",
      function_id: 0,
      function_name: "reserve_stock/1",
      output: Dbos.Serialization.encode(%{reserved: "ord_1"}),
      started_at: 1,
      completed_at: 1
    })

    :ok = Dbos.cancel("wf-retry-cancelled", engine: engine)
    :ok = Dbos.retry("wf-retry-cancelled", engine: engine)

    status = await_status(config, "wf-retry-cancelled", :success)
    assert status.output == %{shipped: "ord_1"}

    {:ok, steps} = SystemDb.get_workflow_steps(config, "wf-retry-cancelled")

    assert Enum.map(steps, & &1.function_name) == [
             "reserve_stock/1",
             "charge_card/1",
             "ship_order/1"
           ]
  end

  test "a step whose own failure was recorded fails the same way again on retry" do
    engine = start_engine([{"raises_declined/1", {SampleWorkflows, :raises_declined, 1}}])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("raises_declined/1", ["ord_1"], engine: engine)
    assert {:error, %SampleWorkflows.CardDeclinedError{}} = Dbos.await(handle)

    :ok = Dbos.retry(handle.workflow_id, engine: engine)

    status = await_status(config, handle.workflow_id, :error)
    assert status.error.value == %SampleWorkflows.CardDeclinedError{amount: 4999}

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_name) == ["charge_card/1"]
  end
end
