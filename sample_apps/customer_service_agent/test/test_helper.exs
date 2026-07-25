schema_path = Application.app_dir(:dbos, "priv/schema/dbos_schema.sql")
database = System.get_env("PGDATABASE", "customer_service_agent_test")

{_output, 0} = System.cmd("dropdb", ["--if-exists", database])
{_output, 0} = System.cmd("createdb", [database])
{_output, 0} = System.cmd("psql", ["-v", "ON_ERROR_STOP=1", "-d", database, "-f", schema_path])

{:ok, _pid} = Postgrex.start_link(name: CustomerServiceAgent.Repo, database: database)

ExUnit.start()
