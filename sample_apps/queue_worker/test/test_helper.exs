schema_path = Application.app_dir(:dbos, "priv/schema/dbos_schema.sql")

{_output, 0} = System.cmd("dropdb", ["--if-exists", "queue_worker_test"])
{_output, 0} = System.cmd("createdb", ["queue_worker_test"])
{_output, 0} = System.cmd("psql", ["-v", "ON_ERROR_STOP=1", "-d", "queue_worker_test", "-f", schema_path])

{:ok, _pid} = Postgrex.start_link(name: QueueWorker.Repo, database: "queue_worker_test")

ExUnit.start()
