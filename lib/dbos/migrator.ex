defmodule Dbos.Migrator do
  @moduledoc """
  Verifies the system database is at the schema versions this engine targets: `dbos_migrations`
  tracks the base schema, `extension_migrations` tracks the additional tables this engine adds on
  top, and the two are versioned independently so one changing never shifts the other. Also
  applies the vendored schema fixture for dev and tests.
  """

  alias Dbos.Config

  @expected_version 42
  @expected_extension_version 1

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

  @doc "Applies `priv/schema/dbos_schema.sql` verbatim, statement by statement. For dev and tests."
  def create!(%Config{} = config) do
    schema_path()
    |> File.read!()
    |> split_statements()
    |> Enum.each(fn statement ->
      {:ok, _result} = config.db.query(config.conn, statement, [])
    end)

    :ok
  end

  defp verify_table_version!(config, table, expected_version) do
    case current_version(config, table) do
      {:ok, ^expected_version} ->
        :ok

      {:ok, other} ->
        raise "dbos schema #{inspect(config.schema)}'s #{table}.version is #{other}, " <>
                "expected #{expected_version}; apply the reference migrations up to that version"

      {:error, :not_found} ->
        raise "dbos schema #{inspect(config.schema)} has no #{table} table (expected " <>
                "version #{expected_version}); run Dbos.Migrator.create!/1 or apply the " <>
                "reference migrations before starting the engine"
    end
  end

  defp current_version(config, table) do
    sql = ~s(SELECT version FROM "#{config.schema}".#{table})

    case config.db.query(config.conn, sql, []) do
      {:ok, %{rows: [[version]]}} -> {:ok, version}
      {:ok, %{rows: []}} -> {:error, :not_found}
      {:error, _reason} -> {:error, :not_found}
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
