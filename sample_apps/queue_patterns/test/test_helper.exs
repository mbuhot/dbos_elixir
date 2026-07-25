schema_path = Application.app_dir(:dbos, "priv/schema/dbos_schema.sql")

{_output, 0} = System.cmd("dropdb", ["--if-exists", "queue_patterns_test"])
{_output, 0} = System.cmd("createdb", ["queue_patterns_test"])

{_output, 0} =
  System.cmd("psql", ["-v", "ON_ERROR_STOP=1", "-d", "queue_patterns_test", "-f", schema_path])

{:ok, _pid} = Postgrex.start_link(name: QueuePatterns.Repo, database: "queue_patterns_test")

ExUnit.start()
