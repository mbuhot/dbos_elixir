defmodule Dbos.MigratorTest do
  use Dbos.Case, async: false

  alias Dbos.Migrator

  setup %{conn: conn} do
    config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}
    {:ok, config: config}
  end

  test "verify! passes when the schema is at the expected version", %{config: config} do
    assert Migrator.verify!(config) == :ok
  end

  test "verify! raises a clear error when the schema is absent", %{config: config} do
    missing_config = %{config | schema: "no_such_schema"}

    error =
      assert_raise RuntimeError, fn ->
        Migrator.verify!(missing_config)
      end

    assert error.message =~ "no_such_schema"
    assert error.message =~ "42"
  end

  test "verify! checks the extension migration version independently of dbos_migrations.version",
       %{
         conn: conn,
         config: config
       } do
    assert Migrator.verify!(config) == :ok

    Postgrex.query!(conn, ~s(UPDATE "dbos".extension_migrations SET version = 999), [])

    error =
      assert_raise RuntimeError, fn ->
        Migrator.verify!(config)
      end

    assert error.message =~ "extension_migrations"
    assert error.message =~ "999"
    assert error.message =~ inspect(Migrator.expected_extension_version())

    Postgrex.query!(conn, ~s(UPDATE "dbos".extension_migrations SET version = 1), [])
    assert Migrator.verify!(config) == :ok
  end

  test "dbos_migrations.version is untouched by the extension migration", %{conn: conn} do
    {:ok, %{rows: [[version]]}} =
      Postgrex.query(conn, ~s(SELECT version FROM "dbos".dbos_migrations), [])

    assert version == 42
  end

  test "create! applies the schema fixture to an empty database" do
    {_output, 0} = System.cmd("dropdb", ["--if-exists", "dbos_migrator_test"])
    {_output, 0} = System.cmd("createdb", ["dbos_migrator_test"])
    {:ok, conn} = Postgrex.start_link(database: "dbos_migrator_test")

    config = %Dbos.Config{db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}

    assert Migrator.create!(config) == :ok
    assert Migrator.verify!(config) == :ok

    GenServer.stop(conn)
    System.cmd("dropdb", ["--if-exists", "dbos_migrator_test"])
  end
end
