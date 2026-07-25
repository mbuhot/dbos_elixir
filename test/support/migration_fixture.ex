if Code.ensure_loaded?(Ecto.Migration) do
  defmodule Dbos.MigrationFixture do
    @moduledoc """
    An `Ecto.Migration` calling `Dbos.Migration.up/1` and `Dbos.Migration.down/1` with the
    prefix stashed in application env by the test, since `Ecto.Migrator` runs migrations in a
    separate process from the one that scheduled them.
    """

    use Ecto.Migration

    def up, do: Dbos.Migration.up(prefix: fixture_prefix())

    def down, do: Dbos.Migration.down(prefix: fixture_prefix())

    defp fixture_prefix, do: Application.fetch_env!(:dbos, :migration_fixture_prefix)
  end
end
