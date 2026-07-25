defmodule Dbos.ResumeTest do
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

  test "resuming a cancelled workflow continues from its last checkpoint and completes, without re-running completed steps" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    config = Dbos.config(engine)

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-resume-1",
      status: :pending,
      name: "three_steps/1",
      inputs: ["ord_1"]
    })

    SystemDb.record_operation_result(config, %{
      workflow_id: "wf-resume-1",
      function_id: 0,
      function_name: "reserve_stock/1",
      output: Dbos.Serialization.encode(%{reserved: "ord_1"}),
      started_at: 1,
      completed_at: 1
    })

    :ok = Dbos.cancel("wf-resume-1", engine: engine)
    {:ok, cancelled} = SystemDb.get_workflow_status(config, "wf-resume-1")
    assert cancelled.status == :cancelled

    :ok = Dbos.resume("wf-resume-1", engine: engine)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, "wf-resume-1")
      status.status == :success
    end)

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-resume-1")
    assert status.output == %{shipped: "ord_1"}

    {:ok, steps} = SystemDb.get_workflow_steps(config, "wf-resume-1")

    assert Enum.map(steps, & &1.function_name) == [
             "reserve_stock/1",
             "charge_card/1",
             "ship_order/1"
           ]
  end

  test "resuming an already-SUCCESS workflow is a silent no-op, matching upstream" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)

    {:ok, handle} = Dbos.start("add/2", [2, 3], engine: engine)
    assert {:ok, 5} = Dbos.await(handle)

    :ok = Dbos.resume(handle.workflow_id, engine: engine)
    Process.sleep(20)

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :success
    assert status.output == 5
  end

  test "resuming clears the queue assignment and deadline" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-resume-cleared",
      status: :cancelled,
      name: "add/2",
      inputs: [1, 1],
      workflow_deadline_epoch_ms: System.os_time(:millisecond) + 60_000
    })

    :ok = Dbos.resume("wf-resume-cleared", engine: engine)

    wait_until(fn ->
      case WorkflowSup.whereis(engine, "wf-resume-cleared") do
        {:ok, _pid} ->
          true

        :error ->
          match?(
            {:ok, %{status: :success}},
            SystemDb.get_workflow_status(config, "wf-resume-cleared")
          )
      end
    end)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, "wf-resume-cleared")
      status.status == :success
    end)

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-resume-cleared")
    assert status.workflow_deadline_epoch_ms == nil
  end
end
