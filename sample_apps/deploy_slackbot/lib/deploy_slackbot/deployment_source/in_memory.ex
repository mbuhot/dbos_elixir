defmodule DeploySlackbot.DeploymentSource.InMemory do
  @moduledoc """
  `DeploySlackbot.DeploymentSource` over an `Agent` seeded by hand — a stand-in CI/CD system.
  The `source` is the Agent's registered name.
  """

  @behaviour DeploySlackbot.DeploymentSource

  use Agent

  @doc "Starts the fake source under `opts[:name]`, holding no deployments and defaulting every status to `:succeeded`."
  def start_link(opts) do
    Agent.start_link(fn -> %{deployments: [], statuses: %{}} end, name: Keyword.fetch!(opts, :name))
  end

  @doc "Adds `deployment` to what `list_deployments/1` returns, with `status` (default `:succeeded`)."
  def push_deployment(source, deployment, status \\ :succeeded) do
    Agent.update(source, fn state ->
      %{
        deployments: state.deployments ++ [deployment],
        statuses: Map.put(state.statuses, deployment.id, status)
      }
    end)
  end

  @impl true
  def list_deployments(source), do: Agent.get(source, & &1.deployments)

  @impl true
  def get_status(source, deployment_id) do
    Agent.get(source, fn state -> Map.get(state.statuses, deployment_id, :succeeded) end)
  end
end
