database = System.get_env("QUEUE_WORKER_DATABASE", "queue_worker_test")

{_output, 0} = System.cmd("dropdb", ["--if-exists", database])
{_output, 0} = System.cmd("createdb", [database])

Application.put_env(:queue_worker, QueueWorker.Repo,
  database: database,
  hostname: System.get_env("PGHOST", "localhost"),
  port: System.get_env("PGPORT", "5432") |> String.to_integer(),
  username: System.get_env("PGUSER", System.get_env("USER", "postgres")),
  password: System.get_env("PGPASSWORD"),
  pool_size: 5,
  log: false
)

{:ok, _pid} = QueueWorker.Repo.start_link()

migrations_path = Path.join([__DIR__, "..", "priv", "repo", "migrations"])
Ecto.Migrator.run(QueueWorker.Repo, migrations_path, :up, all: true)

ExUnit.start()
