if Code.ensure_loaded?(Ecto) do
  defmodule Mix.Tasks.Dbos.Gen.Migration do
    @shortdoc "Generates a migration that installs the dbos system database schema"

    @moduledoc """
    Generates a migration that installs this engine's system database schema through the host
    application's own Ecto migration sequence, the same way `mix ecto.gen.migration` generates
    one for the host's own tables.

        mix dbos.gen.migration
        mix dbos.gen.migration -r MyApp.Repo

    The repository must be set under `:ecto_repos` in the current app configuration, or given
    via `-r`/`--repo`. The generated file is written to that repo's `priv/repo/migrations`
    directory, prefixed with the current UTC timestamp, and calls `Dbos.Migration.up/1` and
    `Dbos.Migration.down/1` — see that module's docs for what those do and
    `guides/integrating-dbos.md` for where this fits in a deploy.

    ## Command line options

      * `-r`, `--repo` - the repo to generate the migration for
    """

    use Mix.Task

    import Mix.Ecto
    import Mix.EctoSQL
    import Mix.Generator

    @switches [repo: [:string, :keep]]
    @aliases [r: :repo]

    @impl Mix.Task
    def run(args) do
      repos = parse_repo(args)

      Enum.each(repos, fn repo ->
        ensure_repo(repo, args)
        generate(repo, args)
      end)
    end

    defp generate(repo, args) do
      {_opts, _rest} = OptionParser.parse!(args, strict: @switches, aliases: @aliases)
      ensure_dbos_migration_compiled!()

      path = Path.join(source_repo_priv(repo), "migrations")
      unless File.dir?(path), do: create_directory(path)

      case Path.wildcard(Path.join(path, "*_add_dbos.exs")) do
        [] ->
          file = Path.join(path, "#{timestamp()}_add_dbos.exs")
          mod = Module.concat([repo, Migrations, AddDbos])
          create_file(file, migration_template(mod: mod))

        [existing | _rest] ->
          Mix.raise("migration can't be created, dbos is already installed by #{existing}")
      end
    end

    defp ensure_dbos_migration_compiled! do
      unless Code.ensure_loaded?(Dbos.Migration) do
        Mix.raise("""
        Dbos.Migration is not compiled, so the migration this task generates would fail at \
        runtime. This usually means :dbos compiled before :ecto_sql during `mix deps.compile`. \
        Run:

            mix deps.compile dbos --force

        then retry this task.
        """)
      end
    end

    defp timestamp do
      {{y, m, d}, {hh, mm, ss}} = :calendar.universal_time()
      "#{y}#{pad(m)}#{pad(d)}#{pad(hh)}#{pad(mm)}#{pad(ss)}"
    end

    defp pad(i) when i < 10, do: <<?0, ?0 + i>>
    defp pad(i), do: to_string(i)

    embed_template(:migration, """
    defmodule <%= inspect @mod %> do
      use Ecto.Migration

      def up, do: Dbos.Migration.up()

      def down, do: Dbos.Migration.down()
    end
    """)
  end
end
