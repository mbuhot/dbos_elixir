defmodule QueueWorker.Application do
  @moduledoc "Boots the Postgres pool and the Dbos engine that runs `QueueWorker.Tasks`."

  use Application

  alias QueueWorker.Tasks

  @impl true
  def start(_type, _args) do
    children = [
      {Postgrex, postgrex_opts()},
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Postgrex, QueueWorker.Repo},
       workflows: [Tasks],
       queues: [
         Dbos.Queue.new("tasks",
           worker_concurrency: worker_concurrency(),
           base_polling_interval_ms: 200
         )
       ],
       migrations: :create_if_absent}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: QueueWorker.Supervisor)
  end

  defp postgrex_opts do
    [
      name: QueueWorker.Repo,
      hostname: System.get_env("PGHOST", "localhost"),
      port: System.get_env("PGPORT", "5432") |> String.to_integer(),
      username: System.get_env("PGUSER", System.get_env("USER", "postgres")),
      password: System.get_env("PGPASSWORD"),
      database: System.get_env("PGDATABASE", "queue_worker_dev"),
      pool_size: 10
    ]
  end

  defp worker_concurrency do
    "QUEUE_WORKER_CONCURRENCY" |> System.get_env("3") |> String.to_integer()
  end
end
