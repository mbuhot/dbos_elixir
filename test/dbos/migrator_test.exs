defmodule Dbos.MigratorTest do
  use Dbos.Case, async: false

  alias Dbos.Migrator
  alias Dbos.SystemDb

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

    Postgrex.query!(
      conn,
      ~s(UPDATE "dbos".extension_migrations SET version = #{Migrator.expected_extension_version()}),
      []
    )

    assert Migrator.verify!(config) == :ok
  end

  test "dbos_migrations.version is untouched by the extension migration", %{conn: conn} do
    {:ok, %{rows: [[version]]}} =
      Postgrex.query(conn, ~s(SELECT version FROM "dbos".dbos_migrations), [])

    assert version == 42
  end

  test "a database populated under the previous extension version keeps its rows and moves forward" do
    {config, conn} = start_database("dbos_migrator_upgrade_test")

    apply_extension_sections_up_to(config, 1)

    assert Migrator.current_version(config, "extension_migrations") == {:ok, 1}

    Postgrex.query!(
      conn,
      """
      INSERT INTO "dbos".executor_leases
        (executor_id, application_version, node, lease_expires_epoch_ms, renewed_at_epoch_ms)
      VALUES ('exec-before-upgrade', 'v1', 'nonode@nohost', 1, 1)
      """,
      []
    )

    Postgrex.query!(
      conn,
      """
      INSERT INTO "dbos".workflow_status (workflow_uuid, status, name, application_version)
      VALUES ('wf-before-upgrade', 'PENDING', 'add/2', 'v1')
      """,
      []
    )

    assert Migrator.create!(config) == :ok
    assert Migrator.verify!(config) == :ok

    assert Migrator.current_version(config, "extension_migrations") ==
             {:ok, Migrator.expected_extension_version()}

    assert %{rows: [["exec-before-upgrade", "v1", nil]]} =
             Postgrex.query!(
               conn,
               ~s(SELECT executor_id, application_version, ex_capabilities FROM "dbos".executor_leases),
               []
             )

    assert %{rows: [["wf-before-upgrade", "PENDING", nil]]} =
             Postgrex.query!(
               conn,
               ~s(SELECT workflow_uuid, status, ex_workflow_version FROM "dbos".workflow_status),
               []
             )

    assert SystemDb.renew_lease(config, 60_000, capabilities: [{"add/2", "3"}]) == :ok

    assert SystemDb.get_executor_lease(config, config.executor_id).capabilities == [
             %{name: "add/2", version: "3"}
           ]

    stop_database(conn, "dbos_migrator_upgrade_test")
  end

  defp start_database(database) do
    {_output, 0} = System.cmd("dropdb", ["--if-exists", database])
    {_output, 0} = System.cmd("createdb", [database])
    {:ok, conn} = Postgrex.start_link(database: database)

    {%Dbos.Config{db: Dbos.DB.Postgrex, conn: conn, executor_id: "exec-1"}, conn}
  end

  defp stop_database(conn, database) do
    GenServer.stop(conn)
    System.cmd("dropdb", ["--if-exists", database])
  end

  defp apply_extension_sections_up_to(config, version) do
    {base_sql, extension_sql} = Migrator.schema_parts(config.schema)

    sections =
      extension_sql
      |> Migrator.extension_sections()
      |> Enum.filter(fn {section_version, _sql} -> section_version <= version end)

    Enum.each([base_sql | Enum.map(sections, &elem(&1, 1))], fn sql ->
      sql
      |> Migrator.statements()
      |> Enum.each(&Postgrex.query!(config.conn, &1, []))
    end)
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
