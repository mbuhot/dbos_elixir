defmodule Dbos.ForkTest do
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

  defp wait_until(fun, attempts \\ 100)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end

  test "forking from step 2 copies rows below 2, gives a new id, records was_forked_from, and resumes at step 2" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("three_steps/1", ["ord_1"], engine: engine)
    assert {:ok, %{shipped: "ord_1"}} = Dbos.await(handle)

    {:ok, fork_handle} = Dbos.fork(handle.workflow_id, 2, engine: engine)
    assert fork_handle.workflow_id != handle.workflow_id

    wait_until(fn ->
      case SystemDb.get_workflow_status(config, fork_handle.workflow_id) do
        {:ok, %{status: :success}} -> true
        _ -> false
      end
    end)

    {:ok, original} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert original.was_forked_from == true

    {:ok, forked} = SystemDb.get_workflow_status(config, fork_handle.workflow_id)
    assert forked.forked_from == handle.workflow_id
    assert forked.output == %{shipped: "ord_1"}

    {:ok, steps} = SystemDb.get_workflow_steps(config, fork_handle.workflow_id)
    step_names = Enum.map(steps, & &1.function_name)
    assert step_names == ["reserve_stock/1", "charge_card/1", "ship_order/1"]

    [reserve, charge, ship] = steps
    assert reserve.completed_at_epoch_ms != nil
    assert charge.completed_at_epoch_ms != nil
    assert ship.completed_at_epoch_ms != nil
  end

  test "forking from step 0 starts completely fresh, copying nothing" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("three_steps/1", ["ord_2"], engine: engine)
    assert {:ok, %{shipped: "ord_2"}} = Dbos.await(handle)

    {:ok, fork_handle} = Dbos.fork(handle.workflow_id, 0, engine: engine)

    wait_until(fn ->
      case SystemDb.get_workflow_status(config, fork_handle.workflow_id) do
        {:ok, %{status: :success}} -> true
        _ -> false
      end
    end)

    {:ok, forked} = SystemDb.get_workflow_status(config, fork_handle.workflow_id)
    assert forked.output == %{shipped: "ord_2"}

    {:ok, steps} = SystemDb.get_workflow_steps(config, fork_handle.workflow_id)

    assert Enum.map(steps, & &1.function_name) == [
             "reserve_stock/1",
             "charge_card/1",
             "ship_order/1"
           ]
  end

  test "fork accepts an explicit new_workflow_id" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("three_steps/1", ["ord_3"], engine: engine)
    assert {:ok, _} = Dbos.await(handle)

    {:ok, fork_handle} =
      Dbos.fork(handle.workflow_id, 1, new_workflow_id: "explicit-fork-id", engine: engine)

    assert fork_handle.workflow_id == "explicit-fork-id"

    wait_until(fn ->
      match?({:ok, %{status: :success}}, SystemDb.get_workflow_status(config, "explicit-fork-id"))
    end)
  end

  test "forking an unknown workflow id raises" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])

    assert_raise Dbos.NonExistentWorkflowError, fn ->
      Dbos.fork("does-not-exist", 0, engine: engine)
    end
  end
end
