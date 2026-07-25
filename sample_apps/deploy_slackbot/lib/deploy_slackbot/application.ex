defmodule DeploySlackbot.Application do
  @moduledoc "Starts a Postgrex pool, the default fakes, and the `Dbos` engine that runs `DeploySlackbot.Workflows`."

  use Application

  @impl true
  def start(_type, _args) do
    Supervisor.start_link(children(), strategy: :one_for_one, name: DeploySlackbot.Supervisor)
  end

  @doc false
  def children do
    if Mix.env() == :test do
      []
    else
      [
        {DeploySlackbot.DeploymentSource.InMemory, name: DeploySlackbot.DeploymentSource.InMemory},
        {DeploySlackbot.Slack.Logging, name: DeploySlackbot.Slack.Logging},
        {Postgrex, name: DeploySlackbot.Repo, database: database_name(), hostname: pg_hostname()},
        {Dbos.Supervisor,
         name: Dbos,
         db: {Dbos.DB.Postgrex, DeploySlackbot.Repo},
         workflows: [DeploySlackbot.Workflows],
         migrations: :create_if_absent}
      ]
    end
  end

  defp database_name, do: System.get_env("DEPLOY_SLACKBOT_DATABASE") || "deploy_slackbot_dev"
  defp pg_hostname, do: System.get_env("PGHOST") || "localhost"
end
