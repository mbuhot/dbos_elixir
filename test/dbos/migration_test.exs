defmodule Dbos.MigrationTest do
  use ExUnit.Case, async: false

  alias Dbos.MigrationFixture
  alias Dbos.MigrationTestRepo, as: Repo
  alias Dbos.Migrator

  setup_all do
    database = Application.fetch_env!(:dbos, :test_database) <> "_migration"

    {_output, 0} = System.cmd("dropdb", ["--if-exists", database])
    {_output, 0} = System.cmd("createdb", [database])

    Application.put_env(:dbos, Repo,
      database: database,
      pool_size: 2,
      log: false
    )

    {:ok, _pid} = Repo.start_link()

    :ok
  end

  setup do
    prefix = "dbos_mig_#{System.unique_integer([:positive, :monotonic])}"
    Application.put_env(:dbos, :migration_fixture_prefix, prefix)
    {:ok, prefix: prefix}
  end

  test "up/1 creates everything and both version markers read correctly on an empty schema", %{
    prefix: prefix
  } do
    migrate_up()

    assert base_version(prefix) == Migrator.expected_version()
    assert extension_version(prefix) == Migrator.expected_extension_version()
  end

  test "up/1 adds the extension tables and leaves the base version untouched when only the base schema is present",
       %{prefix: prefix} do
    seed_base_schema_only(prefix)

    migrate_up()

    assert base_version(prefix) == Migrator.expected_version()
    assert extension_version(prefix) == Migrator.expected_extension_version()
  end

  test "up/1 is a no-op the second time", %{prefix: prefix} do
    migrate_up()
    migrate_up()

    assert base_version(prefix) == Migrator.expected_version()
    assert extension_version(prefix) == Migrator.expected_extension_version()
  end

  test "down/1 then up/1 round-trips", %{prefix: prefix} do
    migrate_up()
    migrate_down()

    refute table_exists?(prefix, "dbos_migrations")
    refute table_exists?(prefix, "extension_migrations")
    refute table_exists?(prefix, "workflow_status")

    migrate_up()

    assert base_version(prefix) == Migrator.expected_version()
    assert extension_version(prefix) == Migrator.expected_extension_version()
  end

  test "a non-default prefix works", %{prefix: prefix} do
    assert prefix != "dbos"

    migrate_up()

    assert base_version(prefix) == Migrator.expected_version()
    assert extension_version(prefix) == Migrator.expected_extension_version()
  end

  test "raises a message naming ecto_sql when Ecto.Migration is not loaded" do
    error =
      assert_raise RuntimeError, fn ->
        Dbos.Migration.ensure_ecto_migration_available!(false)
      end

    assert error.message =~ "Ecto.Migration"
    assert error.message =~ "ecto_sql"
    assert error.message =~ "mix deps.compile dbos --force"
  end

  defp migrate_up do
    Ecto.Migrator.up(Repo, next_version(), MigrationFixture, log: false)
  end

  defp migrate_down do
    version = Process.get(:dbos_migration_test_last_version)
    Ecto.Migrator.down(Repo, version, MigrationFixture, log: false)
  end

  defp next_version do
    version = System.unique_integer([:positive, :monotonic])
    Process.put(:dbos_migration_test_last_version, version)
    version
  end

  defp seed_base_schema_only(prefix) do
    {base_sql, _extension_sql} = Migrator.schema_parts(prefix)

    base_sql
    |> Migrator.statements()
    |> Enum.each(fn statement -> Repo.query!(statement, []) end)
  end

  defp base_version(prefix), do: version(prefix, "dbos_migrations")
  defp extension_version(prefix), do: version(prefix, "extension_migrations")

  defp version(prefix, table) do
    %{rows: [[version]]} = Repo.query!(~s(SELECT version FROM "#{prefix}".#{table}), [])
    version
  end

  defp table_exists?(prefix, table) do
    %{rows: [[exists]]} =
      Repo.query!("SELECT to_regclass($1) IS NOT NULL", ["#{prefix}.#{table}"])

    exists
  end
end
