database = System.get_env("OUTBOX_DATABASE", "outbox_test")

{_output, 0} = System.cmd("dropdb", ["--if-exists", database])
{_output, 0} = System.cmd("createdb", [database])

Application.put_env(:outbox, Outbox.Repo,
  database: database,
  hostname: System.get_env("PGHOST", "localhost"),
  username: System.get_env("PGUSER", "postgres"),
  password: System.get_env("PGPASSWORD", "postgres"),
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: 5,
  log: false
)

{:ok, _pid} = Outbox.Repo.start_link()

migrations_path = Path.join([__DIR__, "..", "priv", "repo", "migrations"])
Ecto.Migrator.run(Outbox.Repo, migrations_path, :up, all: true)

Dbos.Migrator.create!(%Dbos.Config{db: Dbos.DB.Ecto, conn: Outbox.Repo})

Ecto.Adapters.SQL.Sandbox.mode(Outbox.Repo, :manual)

ExUnit.start()
