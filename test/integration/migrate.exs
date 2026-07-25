{:ok, conn} =
  Postgrex.start_link(
    hostname: System.get_env("PGHOST", "postgres"),
    port: String.to_integer(System.get_env("PGPORT", "5432")),
    username: System.get_env("PGUSER", "postgres"),
    password: System.get_env("PGPASSWORD", "postgres"),
    database: System.get_env("PGDATABASE", "dbos_integration")
  )

config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: conn, executor_id: "migrate"}

try do
  Dbos.Migrator.verify!(config)
rescue
  _error ->
    Dbos.Migrator.create!(config)
    Dbos.Migrator.verify!(config)
end

Postgrex.query!(
  conn,
  """
  CREATE TABLE IF NOT EXISTS execution_log (
      id SERIAL PRIMARY KEY,
      workflow_id TEXT NOT NULL,
      step_name TEXT NOT NULL,
      executor_id TEXT NOT NULL,
      recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE (workflow_id, step_name)
  )
  """,
  []
)

Postgrex.query!(
  conn,
  """
  CREATE TABLE IF NOT EXISTS execution_attempts (
      id SERIAL PRIMARY KEY,
      workflow_id TEXT NOT NULL,
      step_name TEXT NOT NULL,
      executor_id TEXT NOT NULL,
      recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
  """,
  []
)

Postgrex.query!(
  conn,
  """
  CREATE TABLE IF NOT EXISTS release_signals (
      workflow_id TEXT PRIMARY KEY,
      released_at TIMESTAMPTZ NOT NULL DEFAULT now()
  )
  """,
  []
)
