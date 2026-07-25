defmodule DeploySlackbot.Workflows do
  @moduledoc """
  Watches for deployments and reports them to Slack, exactly once per deployment no matter how
  many times it's detected or how many crashes happen along the way.

  `poll_deployments` is a scheduled workflow (fired every `@every 30s`, see `Dbos.Scheduler`)
  that lists every deployment the configured `DeploySlackbot.DeploymentSource` currently knows
  about and starts a `notify_deployment` workflow for each one, under a workflow id derived
  **deterministically** from the deployment's own id (`workflow_id/1`) — never a fresh random
  one. Two engines' schedulers firing at once, or the same deployment still present on the next
  poll before its workflow has finished, both start a workflow under the *same* id: `Dbos`
  collapses the second `Dbos.start` onto the row the first one created (`Dbos.Config`'s
  `insert_workflow_status` is `ON CONFLICT (workflow_uuid) DO UPDATE`, and a row already
  `SUCCESS`/`ERROR` is never restarted — see `Dbos.start/3`). That single property is what makes
  exactly-once posting and deduplication of repeated detections the same mechanism: there is
  only ever one `notify_deployment` workflow, and therefore only ever one pair of Slack posts,
  per deployment id — regardless of how many times it's detected, or whether a crash lands
  between detecting it and posting about it.
  """

  use Dbos

  @doc "The deterministic `notify_deployment` workflow id for a given deployment id."
  def workflow_id(deployment_id), do: "deploy-#{deployment_id}"

  defworkflow poll_deployments(_scheduled_time_ms, _context),
    name: "poll_deployments",
    schedule: "@every 30s" do
    deployments = list_deployments()

    Enum.each(deployments, fn deployment ->
      {:ok, _handle} =
        Dbos.start("notify_deployment", [deployment], workflow_id: workflow_id(deployment.id))
    end)

    length(deployments)
  end

  defworkflow notify_deployment(deployment), name: "notify_deployment" do
    post_deployment_message(deployment, :started)
    status = fetch_deploy_status(deployment)
    post_deployment_message(deployment, status)
    status
  end

  defstep list_deployments do
    deployment_source_module().list_deployments(deployment_source())
  end

  defstep fetch_deploy_status(deployment) do
    deployment_source_module().get_status(deployment_source(), deployment.id)
  end

  defstep post_deployment_message(deployment, phase) do
    slack_module().post_message(slack_client(), slack_channel(), message_text(deployment, phase))
  end

  defp message_text(deployment, :started) do
    "Deploying #{deployment.app} #{deployment.version} to #{deployment.environment}..."
  end

  defp message_text(deployment, :succeeded) do
    "Deployed #{deployment.app} #{deployment.version} to #{deployment.environment}."
  end

  defp message_text(deployment, :failed) do
    "Deploy of #{deployment.app} #{deployment.version} to #{deployment.environment} failed."
  end

  defp deployment_source_module do
    Application.get_env(
      :deploy_slackbot,
      :deployment_source_module,
      DeploySlackbot.DeploymentSource.InMemory
    )
  end

  defp deployment_source do
    Application.get_env(:deploy_slackbot, :deployment_source, DeploySlackbot.DeploymentSource.InMemory)
  end

  defp slack_module do
    Application.get_env(:deploy_slackbot, :slack_module, DeploySlackbot.Slack.Logging)
  end

  defp slack_client do
    Application.get_env(:deploy_slackbot, :slack_client, DeploySlackbot.Slack.Logging)
  end

  defp slack_channel do
    Application.get_env(:deploy_slackbot, :slack_channel, "#deploys")
  end
end
