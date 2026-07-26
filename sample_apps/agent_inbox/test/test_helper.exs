database = System.get_env("AGENT_INBOX_DATABASE", "agent_inbox_test")

{_output, 0} = System.cmd("dropdb", ["--if-exists", database])
{_output, 0} = System.cmd("createdb", [database])

Application.put_env(:agent_inbox, AgentInbox.Repo,
  database: database,
  hostname: System.get_env("PGHOST", "localhost"),
  username: System.get_env("PGUSER", "postgres"),
  password: System.get_env("PGPASSWORD", "postgres"),
  pool_size: 5,
  log: false
)

{:ok, _pid} = AgentInbox.Repo.start_link()

migrations_path = Path.join([__DIR__, "..", "priv", "repo", "migrations"])
Ecto.Migrator.run(AgentInbox.Repo, migrations_path, :up, all: true)

ExUnit.start()
