defmodule Dbos.Queue.RunnerTest do
  use Dbos.Case, async: false

  alias Dbos.Queue.Runner

  defp start_engine(workflows) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts = [
      name: name,
      db: {Dbos.DB.Postgrex, Dbos.TestConn},
      executor_id: "exec-#{System.unique_integer([:positive])}",
      workflows: workflows,
      migrations: :skip
    ]

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    name
  end

  defp new_counter_table do
    table = :"runner_dispatch_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])
    table
  end

  defp wait_until(fun, attempts \\ 100)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  test "a dispatch that raises for one claimed workflow does not stop the rest of the batch" do
    engine =
      start_engine([{"counting_workflow/1", {Dbos.SampleWorkflows, :counting_workflow, 1}}])

    table = new_counter_table()

    claimed = [
      %{workflow_id: "bad-#{System.unique_integer([:positive])}", name: "counting_workflow/1"},
      %{
        workflow_id: "good-#{System.unique_integer([:positive])}",
        name: "counting_workflow/1",
        inputs: [table]
      }
    ]

    Runner.dispatch_all(engine, claimed, "orders", nil)

    wait_until(fn -> :ets.lookup(table, :count) == [{:count, 1}] end)
  end
end
