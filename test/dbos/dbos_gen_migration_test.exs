defmodule Mix.Tasks.Dbos.Gen.MigrationTest do
  use ExUnit.Case, async: false

  alias Dbos.GenMigrationTestRepo, as: Repo

  @priv_dir Path.expand("../../priv/dbos_gen_migration_test", __DIR__)
  @migrations_dir Path.join(@priv_dir, "migrations")

  setup do
    File.rm_rf!(@priv_dir)

    {_output, 0} = System.cmd("dropdb", ["--if-exists", "dbos_gen_migration_test"])
    {_output, 0} = System.cmd("createdb", ["dbos_gen_migration_test"])

    Application.put_env(:dbos, Repo,
      database: "dbos_gen_migration_test",
      priv: "priv/dbos_gen_migration_test",
      pool_size: 2,
      log: false
    )

    {:ok, pid} = Repo.start_link()

    on_exit(fn -> File.rm_rf!(@priv_dir) end)

    {:ok, repo_pid: pid}
  end

  test "generates a migration file that compiles and installs the schema" do
    Mix.Tasks.Dbos.Gen.Migration.run(["-r", "Dbos.GenMigrationTestRepo"])

    [file] = Path.wildcard(Path.join(@migrations_dir, "*_add_dbos.exs"))

    assert Path.basename(file) =~ ~r/^\d{14}_add_dbos\.exs$/

    [{module, _binary}] = Code.compile_file(file)

    version = System.unique_integer([:positive, :monotonic])
    Ecto.Migrator.up(Repo, version, module, log: false)

    %{rows: [[base_version]]} = Repo.query!(~s(SELECT version FROM "dbos".dbos_migrations), [])
    assert base_version == Dbos.Migrator.expected_version()
  end
end
