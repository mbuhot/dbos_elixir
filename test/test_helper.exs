schema_path = Path.expand("../priv/schema/dbos_schema.sql", __DIR__)

{_output, 0} = System.cmd("dropdb", ["--if-exists", "dbos_test"])
{_output, 0} = System.cmd("createdb", ["dbos_test"])
{_output, 0} = System.cmd("psql", ["-v", "ON_ERROR_STOP=1", "-d", "dbos_test", "-f", schema_path])

{:ok, _pid} = Postgrex.start_link(name: Dbos.TestConn, database: "dbos_test")

Application.put_env(:dbos, Dbos.TestRepo,
  database: "dbos_test",
  pool_size: 5,
  log: false
)

{:ok, _pid} = Dbos.TestRepo.start_link()

ExUnit.start(exclude: [:integration])
