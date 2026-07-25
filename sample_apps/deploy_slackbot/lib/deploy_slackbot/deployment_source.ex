defmodule DeploySlackbot.DeploymentSource do
  @moduledoc """
  Where deployments come from. `DeploySlackbot.DeploymentSource.InMemory` is the sample's only
  implementation — a fake CI/CD system to poll, so the sample runs with no external service.
  A real integration (a CI provider's deployments API, or a webhook receiver's own durable
  inbox) would implement the same behaviour; see the README for what changes.
  """

  @type source :: term
  @type deployment :: %{id: String.t(), app: String.t(), version: String.t(), environment: String.t()}

  @doc "Every deployment this source currently knows about."
  @callback list_deployments(source) :: [deployment]

  @doc "Whether `deployment_id` has finished, and how."
  @callback get_status(source, deployment_id :: String.t()) :: :succeeded | :failed
end
