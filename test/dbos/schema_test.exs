defmodule Dbos.SchemaTest do
  use Dbos.Case, async: false

  test "the applied schema is at migration version 42", %{conn: conn} do
    result = Postgrex.query!(conn, "SELECT version FROM dbos.dbos_migrations", [])
    assert result.rows == [[42]]
  end
end
