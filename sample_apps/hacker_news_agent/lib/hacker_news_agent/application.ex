defmodule HackerNewsAgent.Application do
  @moduledoc "Boots the Postgres pool and the Dbos engine that runs `HackerNewsAgent.Research`."

  use Application

  alias HackerNewsAgent.Research

  @impl true
  def start(_type, _args) do
    children = [
      {Postgrex, postgrex_opts()},
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Postgrex, HackerNewsAgent.Repo},
       workflows: [Research],
       migrations: :create_if_absent}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: HackerNewsAgent.Supervisor)
  end

  defp postgrex_opts do
    [
      name: HackerNewsAgent.Repo,
      hostname: System.get_env("PGHOST", "localhost"),
      port: System.get_env("PGPORT", "5432") |> String.to_integer(),
      username: System.get_env("PGUSER", System.get_env("USER", "postgres")),
      password: System.get_env("PGPASSWORD"),
      database: System.get_env("PGDATABASE", "hacker_news_agent_dev"),
      pool_size: 10
    ]
  end
end
