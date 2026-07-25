defmodule Dbos.StepTest do
  use Dbos.Case, async: false

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

  test "Dbos.step/2 checkpoints an inline step under the given name" do
    engine =
      start_engine([{"inline_step_workflow/1", {Dbos.SampleWorkflows, :inline_step_workflow, 1}}])

    config = Dbos.config(engine)
    {:ok, handle} = Dbos.start("inline_step_workflow/1", ["ord_1"], engine: engine)

    assert {:ok, %{one_off: "ord_1"}} = Dbos.await(handle)

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_name) == ["one-off step"]
  end

  test "Dbos.step/2 does not re-run its body on replay" do
    engine =
      start_engine([{"counting_inline_step/1", {Dbos.SampleWorkflows, :counting_inline_step, 1}}])

    config = Dbos.config(engine)
    table = :"step_replay_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])

    workflow_id = "wf-step-replay-#{System.unique_integer([:positive])}"

    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: "counting_inline_step/1",
      inputs: [table]
    })

    {:ok, _pid} =
      Dbos.WorkflowSup.start_workflow(
        engine,
        workflow_id,
        {Dbos.SampleWorkflows, :counting_inline_step, 1},
        [table]
      )

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
      status.status == :success
    end)

    Dbos.WorkflowSup.start_workflow(
      engine,
      workflow_id,
      {Dbos.SampleWorkflows, :counting_inline_step, 1},
      [table],
      replay: true
    )

    Process.sleep(50)
    assert :ets.lookup_element(table, :count, 2) == 1
  end

  test "Dbos.step/3 honors max_retries, wrapping the final failure in Dbos.MaxStepRetriesExceededError" do
    engine =
      start_engine([
        {"always_fails_inline_step/1", {Dbos.SampleWorkflows, :always_fails_inline_step, 1}}
      ])

    config = Dbos.config(engine)
    {:ok, handle} = Dbos.start("always_fails_inline_step/1", [2], engine: engine)

    assert {:error, %Dbos.MaxStepRetriesExceededError{function_name: "flaky step"}} =
             Dbos.await(handle)

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_name) == ["flaky step"]
  end
end
