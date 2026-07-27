defmodule Dbos.NestedStepTest do
  @moduledoc """
  A step called from inside another step becomes part of the caller's execution rather than a
  checkpoint of its own, matching upstream DBOS. Dispatching a *workflow* from inside a step is
  refused: a step is one checkpoint and cannot carry another id.
  """

  use Dbos.Case, async: false

  alias Dbos.Recovery
  alias Dbos.SystemDb

  defp start_engine(workflows) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Postgrex, Dbos.TestConn},
       executor_id: "exec-#{System.unique_integer([:positive])}",
       migrations: :skip,
       testing: :inline,
       workflows: workflows},
      id: name
    )

    name
  end

  defp replay(config, engine, workflow_id) do
    Dbos.DB.Postgrex.query!(
      config.conn,
      "UPDATE dbos.workflow_status SET status = 'PENDING' WHERE workflow_uuid = $1",
      [workflow_id]
    )

    Recovery.recover_pending(engine)
  end

  def nested(table) do
    Dbos.Runtime.run_step("outer/1", [], fn ->
      Dbos.Runtime.run_step("charge/1", [], fn ->
        :ets.update_counter(table, :inner_runs, 1, {:inner_runs, 0})
        :nested_charge
      end)

      :outer
    end)

    Dbos.Runtime.run_step("charge/1", [], fn -> :real_charge end)
  end

  def starts_child_in_step(_arg) do
    Dbos.Runtime.run_step("outer/1", [], fn ->
      Dbos.start("child/1", [:x], workflow_id: "unreachable")
    end)
  end

  def child(_arg), do: :child

  def sends_in_step(_arg) do
    Dbos.Runtime.run_step("outer/1", [], fn ->
      Dbos.send_message("somewhere", "topic", :payload)
    end)
  end

  def sets_event_in_step(_arg) do
    Dbos.Runtime.run_step("outer/1", [], fn -> Dbos.set_event("key", :value) end)
  end

  test "a nested step is folded into its caller: no id of its own, no checkpoint" do
    table = :"nested_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])

    engine = start_engine([{"nested/1", {__MODULE__, :nested, 1}}])
    config = Dbos.config(engine)
    workflow_id = "nested-#{System.unique_integer([:positive])}"

    {:ok, handle} = Dbos.start("nested/1", [table], engine: engine, workflow_id: workflow_id)
    assert {:ok, :real_charge} = Dbos.await(handle, engine: engine)

    {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

    assert Enum.map(steps, &{&1.function_id, &1.function_name}) == [
             {0, "outer/1"},
             {1, "charge/1"}
           ]

    assert :ets.lookup_element(table, :inner_runs, 2) == 1
  end

  test "the folded step leaves replay returning the same result it first produced" do
    table = :"nested_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])

    engine = start_engine([{"nested/1", {__MODULE__, :nested, 1}}])
    config = Dbos.config(engine)
    workflow_id = "nested-replay-#{System.unique_integer([:positive])}"

    {:ok, handle} = Dbos.start("nested/1", [table], engine: engine, workflow_id: workflow_id)
    assert {:ok, :real_charge} = Dbos.await(handle, engine: engine)

    replay(config, engine, workflow_id)

    assert {:ok, :real_charge} = Dbos.result(workflow_id, engine: engine)
    assert :ets.lookup_element(table, :inner_runs, 2) == 1
  end

  test "starting a workflow from inside a step is refused, naming the step" do
    engine =
      start_engine([
        {"starts_child_in_step/1", {__MODULE__, :starts_child_in_step, 1}},
        {"child/1", {__MODULE__, :child, 1}}
      ])

    config = Dbos.config(engine)
    workflow_id = "child-in-step-#{System.unique_integer([:positive])}"

    {:ok, _} =
      Dbos.start("starts_child_in_step/1", [:x], engine: engine, workflow_id: workflow_id)

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    assert status.status == :error
    assert %Dbos.OperationInStepError{step: "outer/1"} = status.error.value
  end

  test "send_message from inside a step is refused" do
    engine = start_engine([{"sends_in_step/1", {__MODULE__, :sends_in_step, 1}}])
    config = Dbos.config(engine)
    workflow_id = "send-in-step-#{System.unique_integer([:positive])}"

    {:ok, _} = Dbos.start("sends_in_step/1", [:x], engine: engine, workflow_id: workflow_id)

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    assert status.status == :error

    assert %Dbos.OperationInStepError{step: "outer/1", operation: "call DBOS.send"} =
             status.error.value
  end

  test "set_event from inside a step is refused" do
    engine = start_engine([{"sets_event_in_step/1", {__MODULE__, :sets_event_in_step, 1}}])
    config = Dbos.config(engine)
    workflow_id = "event-in-step-#{System.unique_integer([:positive])}"

    {:ok, _} = Dbos.start("sets_event_in_step/1", [:x], engine: engine, workflow_id: workflow_id)

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    assert status.status == :error

    assert %Dbos.OperationInStepError{step: "outer/1", operation: "call DBOS.setEvent"} =
             status.error.value
  end
end
