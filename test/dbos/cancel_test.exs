defmodule Dbos.CancelTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

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

  test "cancelling a running workflow mid-flight stops it at the next step boundary" do
    engine =
      start_engine([{"multi_step_with_gate/1", {SampleWorkflows, :multi_step_with_gate, 1}}])

    config = Dbos.config(engine)
    table = :"gate_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])

    {:ok, handle} = Dbos.start("multi_step_with_gate/1", [table], engine: engine)

    wait_until(fn ->
      {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
      length(steps) == 1
    end)

    :ok = Dbos.cancel(handle.workflow_id, engine: engine)

    {:ok, pid} = WorkflowSup.whereis(engine, handle.workflow_id)
    send(pid, :go)

    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle)

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :cancelled

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_name) == ["first_step/0", "wait_for_gate/0"]
  end

  test "cancelling a workflow blocked in recv interrupts it promptly, not after the timeout" do
    engine = start_engine([{"cancellable_recv/2", {SampleWorkflows, :cancellable_recv, 2}}])
    config = Dbos.config(engine)

    {:ok, handle} =
      Dbos.start("cancellable_recv/2", ["topic", 30_000], engine: engine)

    wait_until(fn -> match?({:ok, _}, WorkflowSup.whereis(engine, handle.workflow_id)) end)

    started_at = System.monotonic_time(:millisecond)
    :ok = Dbos.cancel(handle.workflow_id, engine: engine)

    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle, timeout_ms: 5_000)
    elapsed_ms = System.monotonic_time(:millisecond) - started_at
    assert elapsed_ms < 5_000

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :cancelled
  end

  test "cancelling a workflow blocked in a durable sleep interrupts it promptly, not after the timeout" do
    engine = start_engine([{"cancellable_sleep/1", {SampleWorkflows, :cancellable_sleep, 1}}])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("cancellable_sleep/1", [30_000], engine: engine)

    wait_until(fn ->
      {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
      steps != []
    end)

    started_at = System.monotonic_time(:millisecond)
    :ok = Dbos.cancel(handle.workflow_id, engine: engine)

    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle, timeout_ms: 5_000)
    elapsed_ms = System.monotonic_time(:millisecond) - started_at
    assert elapsed_ms < 5_000

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :cancelled
  end

  test "cancelling an already-terminal workflow is a no-op" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("add/2", [1, 2], engine: engine)
    assert {:ok, 3} = Dbos.await(handle)

    :ok = Dbos.cancel(handle.workflow_id, engine: engine)

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :success
  end
end
