defmodule DeploySlackbot.NotifyTest.SlowSlack do
  @moduledoc "Delays only the completion post, so a test can reliably catch a workflow between its first and second Slack post."

  @behaviour DeploySlackbot.Slack

  alias DeploySlackbot.Slack.Logging

  @impl true
  def post_message(client, channel, text) do
    unless String.starts_with?(text, "Deploying"), do: Process.sleep(300)
    Logging.post_message(client, channel, text)
  end
end

defmodule DeploySlackbot.NotifyTest do
  use ExUnit.Case, async: false

  alias Dbos.SystemDb
  alias DeploySlackbot.DeploymentSource.InMemory
  alias DeploySlackbot.NotifyTest.SlowSlack
  alias DeploySlackbot.Slack.Logging
  alias DeploySlackbot.Workflows

  @tables ~w(
    workflow_status operation_outputs notifications workflow_events
    workflow_events_history streams event_dispatch_kv application_versions
    workflow_schedules queues
  )

  setup do
    truncate_dbos_tables()

    source_name = :"source_#{System.unique_integer([:positive])}"
    slack_name = :"slack_#{System.unique_integer([:positive])}"

    start_supervised!({InMemory, name: source_name})
    start_supervised!({Logging, name: slack_name})

    Application.put_env(:deploy_slackbot, :deployment_source_module, InMemory)
    Application.put_env(:deploy_slackbot, :deployment_source, source_name)
    Application.put_env(:deploy_slackbot, :slack_module, Logging)
    Application.put_env(:deploy_slackbot, :slack_client, slack_name)

    engine = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: engine,
       db: {Dbos.DB.Postgrex, DeploySlackbot.TestConn},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Workflows],
       migrations: :skip},
      id: engine
    )

    Dbos.Recovery.await_boot_recovery(engine)

    deployment = %{id: "d-1", app: "billing-api", version: "v1.2.3", environment: "production"}
    InMemory.push_deployment(source_name, deployment)

    {:ok, engine: engine, slack_name: slack_name, deployment: deployment}
  end

  test "a duplicate deployment event produces exactly one pair of Slack posts", %{
    engine: engine,
    slack_name: slack_name,
    deployment: deployment
  } do
    workflow_id = Workflows.workflow_id(deployment.id)

    {:ok, first} =
      Dbos.start("notify_deployment", [deployment], engine: engine, workflow_id: workflow_id)

    assert {:ok, :succeeded} = Dbos.await(first)

    {:ok, second} =
      Dbos.start("notify_deployment", [deployment], engine: engine, workflow_id: workflow_id)

    assert {:ok, :succeeded} = Dbos.await(second)
    assert first.workflow_id == second.workflow_id

    posts = Logging.posts(slack_name)
    assert length(posts) == 2

    assert [{"#deploys", started_text}, {"#deploys", finished_text}] = posts
    assert started_text =~ "Deploying billing-api v1.2.3"
    assert finished_text =~ "Deployed billing-api v1.2.3"
  end

  test "a crash between detecting a deployment and finishing its notification does not double-post",
       %{engine: engine, slack_name: slack_name, deployment: deployment} do
    Application.put_env(:deploy_slackbot, :slack_module, SlowSlack)

    workflow_id = Workflows.workflow_id(deployment.id)

    {:ok, handle} =
      Dbos.start("notify_deployment", [deployment], engine: engine, workflow_id: workflow_id)

    wait_until(fn ->
      {:ok, steps} = SystemDb.get_workflow_steps(Dbos.config(engine), workflow_id)
      length(steps) >= 2
    end)

    {:ok, pid} = Dbos.WorkflowSup.whereis(engine, workflow_id)
    Process.exit(pid, :kill)

    {:ok, status} = SystemDb.get_workflow_status(Dbos.config(engine), workflow_id)
    refute status.status in [:success, :error]

    assert length(Logging.posts(slack_name)) == 1

    Dbos.Recovery.recover_pending(engine)

    assert {:ok, :succeeded} = Dbos.await(handle, timeout_ms: 10_000)

    posts = Logging.posts(slack_name)
    assert length(posts) == 2
    assert [{_, started_text}, {_, finished_text}] = posts
    assert started_text =~ "Deploying"
    assert finished_text =~ "Deployed"
  end

  defp truncate_dbos_tables do
    tables = Enum.map_join(@tables, ", ", &"dbos.#{&1}")
    Postgrex.query!(DeploySlackbot.TestConn, "TRUNCATE TABLE #{tables} CASCADE", [])
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
end
