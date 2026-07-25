defmodule HackerNewsAgent.ResearchTest do
  use ExUnit.Case, async: false

  alias Dbos.SystemDb
  alias HackerNewsAgent.Research
  alias HackerNewsAgent.StubAgent
  alias HackerNewsAgent.StubHnClient

  setup do
    Application.put_env(:hacker_news_agent, :agent, StubAgent)
    Application.put_env(:hacker_news_agent, :hn_client, StubHnClient)

    engine = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: engine,
       db: {Dbos.DB.Postgrex, HackerNewsAgent.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Research],
       migrations: :skip},
      id: engine
    )

    Dbos.Recovery.await_boot_recovery(engine)
    {:ok, engine: engine}
  end

  test "researches a topic across iterations and synthesizes a report", %{engine: engine} do
    topic = "unique-topic-#{System.unique_integer([:positive])}"

    {:ok, handle} = Dbos.start("hn_research", [topic, 2], engine: engine)

    assert {:ok, report} = Dbos.await(handle, timeout_ms: 5_000)
    assert report =~ "Report on #{topic}"
    assert report =~ "2 rounds of findings"
  end

  test "a crash after the first search does not repeat the search or the LLM calls preceding it",
       %{engine: engine} do
    topic = "crash-topic-#{System.unique_integer([:positive])}"
    workflow_id = "hn-research-#{System.unique_integer([:positive])}"

    {:ok, handle} =
      Dbos.start("hn_research", [topic, 2], workflow_id: workflow_id, engine: engine)

    config = Dbos.config(engine)

    wait_until(fn ->
      {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)
      length(steps) >= 1
    end)

    {:ok, pid} = Dbos.WorkflowSup.whereis(engine, workflow_id)
    Process.exit(pid, :kill)
    wait_until(fn -> not Process.alive?(pid) end)

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    assert status.status == :pending

    Dbos.Recovery.recover_pending(engine)

    assert {:ok, report} = Dbos.await(handle, timeout_ms: 5_000)
    assert report =~ "Report on #{topic}"

    assert StubHnClient.call_count({:search_stories, topic}) == 1
  end

  defp wait_until(fun, attempts \\ 200)

  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end
end
