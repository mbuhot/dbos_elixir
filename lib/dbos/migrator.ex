defmodule Dbos.Migrator do
  @moduledoc """
  Verifies the system database is at the schema versions this engine targets: `dbos_migrations`
  tracks the base schema, `extension_migrations` tracks the additional tables this engine adds on
  top, and the two are versioned independently so one changing never shifts the other. Also
  applies the vendored schema fixture for dev and tests.
  """

  alias Dbos.Config

  @expected_version 42
  @expected_extension_version 5
  @extension_marker "-- Extension tables:"
  @extension_section_marker ~r/^-- extension migration (\d+):/m

  @doc "The base schema's `dbos_migrations.version` this engine targets."
  def expected_version, do: @expected_version

  @doc "This engine's own `extension_migrations.version` it targets."
  def expected_extension_version, do: @expected_extension_version

  @doc """
  Raises unless both `<schema>.dbos_migrations.version` and
  `<schema>.extension_migrations.version` are exactly at the versions this engine targets, naming
  which one is wrong.
  """
  def verify!(%Config{} = config) do
    verify_table_version!(config, "dbos_migrations", @expected_version)
    verify_table_version!(config, "extension_migrations", @expected_extension_version)
    :ok
  end

  @doc """
  Applies whichever parts of `priv/schema/dbos_schema.sql` are absent. For dev and tests.

  The base schema and the extension tables are applied independently, each guarded by its own
  version marker, so a database holding only the base schema gains the extension tables alone.

  A real deployment installs the schema through its own migration sequence instead — see
  `Dbos.Migration` and `mix dbos.gen.migration`.
  """
  def create!(%Config{} = config) do
    {base_sql, extension_sql} = schema_parts(config.schema)

    apply_unless_present(config, "dbos_migrations", base_sql)

    config
    |> pending_extension_sections(extension_sql)
    |> Enum.each(fn {_version, sql} -> apply_statements(config, sql) end)

    :ok
  end

  @doc """
  Reads `priv/schema/dbos_schema.sql`, rewritten for `schema`, split into its base part and its
  extension part (marked by `#{@extension_marker}`). Shared by `create!/1` and `Dbos.Migration`
  so both apply the exact same statements, guarded the exact same way.
  """
  def schema_parts(schema \\ "dbos") do
    sql =
      schema_path()
      |> File.read!()
      |> rewrite_schema(schema)

    case String.split(sql, @extension_marker, parts: 2) do
      [base, extension] -> {base, @extension_marker <> extension}
      [base] -> {base, ""}
    end
  end

  @doc "Splits one `schema_parts/1` part into its individual SQL statements."
  def statements(sql), do: split_statements(sql)

  @doc """
  The extension part of `schema_parts/1` split into its individually versioned sections,
  `{version, sql}` ascending. Each section ends by writing its own number into
  `extension_migrations`, so applying a suffix of them brings a database holding an earlier
  version forward.
  """
  def extension_sections(extension_sql) do
    case Regex.split(@extension_section_marker, extension_sql, include_captures: true) do
      [_unmarked] ->
        []

      [preamble | marked] ->
        marked
        |> Enum.chunk_every(2)
        |> Enum.map(fn [marker, body] -> {section_version(marker), marker <> body} end)
        |> prepend_preamble(preamble)
    end
  end

  @doc "The `extension_sections/1` of `extension_sql` this database has yet to apply."
  def pending_extension_sections(%Config{} = config, extension_sql) do
    applied =
      case current_version(config, "extension_migrations") do
        {:ok, version} -> version
        {:error, :not_found} -> 0
      end

    extension_sql
    |> extension_sections()
    |> Enum.filter(fn {version, _sql} -> version > applied end)
  end

  @doc """
  The marker table's current version, or `{:error, :not_found}` if the table doesn't exist yet.
  Safe to call inside an open transaction: existence is checked with `to_regclass`, which never
  raises, before the table is queried.
  """
  def current_version(config, table) do
    if table_exists?(config, table) do
      sql = ~s(SELECT version FROM "#{config.schema}".#{table})

      case config.db.query(config.conn, sql, []) do
        {:ok, %{rows: [[version]]}} -> {:ok, version}
        {:ok, %{rows: []}} -> {:error, :not_found}
        {:error, _reason} -> {:error, :not_found}
      end
    else
      {:error, :not_found}
    end
  end

  defp table_exists?(config, table) do
    sql = "SELECT to_regclass($1) IS NOT NULL"
    qualified_name = ~s("#{config.schema}"."#{table}")

    case config.db.query(config.conn, sql, [qualified_name]) do
      {:ok, %{rows: [[exists]]}} -> exists
      _other -> false
    end
  end

  defp rewrite_schema(sql, "dbos"), do: sql

  defp rewrite_schema(sql, schema) do
    sql
    |> String.replace(~s("dbos"), ~s("#{schema}"))
    |> String.replace("'dbos'", "'#{schema}'")
  end

  defp prepend_preamble([{version, sql} | rest], preamble),
    do: [{version, preamble <> sql} | rest]

  defp prepend_preamble([], _preamble), do: []

  defp section_version(marker) do
    [_match, digits] = Regex.run(@extension_section_marker, marker)
    String.to_integer(digits)
  end

  defp apply_unless_present(config, marker_table, sql) do
    case current_version(config, marker_table) do
      {:ok, _version} -> :ok
      {:error, :not_found} -> apply_statements(config, sql)
    end
  end

  defp apply_statements(config, sql) do
    sql
    |> statements()
    |> Enum.each(fn statement ->
      {:ok, _result} = config.db.query(config.conn, statement, [])
    end)
  end

  defp verify_table_version!(config, table, expected_version) do
    case current_version(config, table) do
      {:ok, ^expected_version} ->
        :ok

      {:ok, other} ->
        raise "dbos schema #{inspect(config.schema)}'s #{table}.version is #{other}, " <>
                "expected #{expected_version}; apply this engine's schema migrations up to " <>
                "that version"

      {:error, :not_found} ->
        raise "dbos schema #{inspect(config.schema)} has no #{table} table (expected " <>
                "version #{expected_version}); run `mix dbos.gen.migration` and apply the " <>
                "resulting migration, or call Dbos.Migrator.create!/1 for dev and tests"
    end
  end

  defp schema_path do
    Application.app_dir(:dbos, "priv/schema/dbos_schema.sql")
  end

  defp split_statements(sql) do
    split_statements(sql, "", [], false, false)
  end

  defp split_statements(<<>>, current, acc, _in_dollar_block, _in_line_comment) do
    acc
    |> append_statement(current)
    |> Enum.reverse()
  end

  defp split_statements(<<"\n", rest::binary>>, current, acc, in_dollar_block, true) do
    split_statements(rest, current <> "\n", acc, in_dollar_block, false)
  end

  defp split_statements(<<char::utf8, rest::binary>>, current, acc, in_dollar_block, true) do
    split_statements(rest, current <> <<char::utf8>>, acc, in_dollar_block, true)
  end

  defp split_statements(<<"--", rest::binary>>, current, acc, in_dollar_block, false) do
    split_statements(rest, current <> "--", acc, in_dollar_block, true)
  end

  defp split_statements(<<"$$", rest::binary>>, current, acc, in_dollar_block, false) do
    split_statements(rest, current <> "$$", acc, not in_dollar_block, false)
  end

  defp split_statements(<<";", rest::binary>>, current, acc, false, false) do
    split_statements(rest, "", append_statement(acc, current), false, false)
  end

  defp split_statements(<<char::utf8, rest::binary>>, current, acc, in_dollar_block, false) do
    split_statements(rest, current <> <<char::utf8>>, acc, in_dollar_block, false)
  end

  defp append_statement(acc, statement) do
    case String.trim(statement) do
      "" -> acc
      trimmed -> [trimmed | acc]
    end
  end
end
