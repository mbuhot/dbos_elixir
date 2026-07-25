schema_path = Path.expand("../priv/schema/dbos_schema.sql", __DIR__)
database = System.get_env("DBOS_TEST_DATABASE", "dbos_test")

{_output, 0} = System.cmd("dropdb", ["--if-exists", database])
{_output, 0} = System.cmd("createdb", [database])
{_output, 0} = System.cmd("psql", ["-v", "ON_ERROR_STOP=1", "-d", database, "-f", schema_path])

{:ok, _pid} = Postgrex.start_link(name: Dbos.TestConn, database: database)

Application.put_env(:dbos, :test_database, database)

Application.put_env(:dbos, Dbos.TestRepo,
  database: database,
  pool_size: 5,
  log: false
)

{:ok, _pid} = Dbos.TestRepo.start_link()

ExUnit.start(exclude: [:integration])
