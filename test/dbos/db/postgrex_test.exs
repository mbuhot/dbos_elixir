defmodule Dbos.DB.PostgrexTest do
  use Dbos.Case, async: false

  alias Dbos.DB.Postgrex, as: DB

  test "query with params", %{conn: conn} do
    assert {:ok, %{rows: [[1]], num_rows: 1}} = DB.query(conn, "SELECT $1::int", [1])
  end

  test "transaction commits", %{conn: conn} do
    {:ok, _} =
      DB.transaction(conn, [], fn tx_conn ->
        DB.query!(tx_conn, "INSERT INTO dbos.queues (name) VALUES ($1)", ["q1"])
      end)

    assert {:ok, %{rows: [["q1"]]}} = DB.query(conn, "SELECT name FROM dbos.queues", [])
  end

  test "transaction rolls back on raise", %{conn: conn} do
    assert_raise RuntimeError, fn ->
      DB.transaction(conn, [], fn tx_conn ->
        DB.query!(tx_conn, "INSERT INTO dbos.queues (name) VALUES ($1)", ["q2"])
        raise "boom"
      end)
    end

    assert {:ok, %{rows: []}} = DB.query(conn, "SELECT name FROM dbos.queues", [])
  end

  test "in_transaction? reports true inside a transaction and false outside", %{conn: conn} do
    refute DB.in_transaction?(conn)

    DB.transaction(conn, [], fn tx_conn ->
      assert DB.in_transaction?(tx_conn)
    end)
  end

  test "isolation level takes effect inside a transaction", %{conn: conn} do
    {:ok, isolation} =
      DB.transaction(conn, [isolation: :serializable], fn tx_conn ->
        {:ok, %{rows: [[level]]}} = DB.query(tx_conn, "SHOW transaction_isolation", [])
        level
      end)

    assert isolation == "serializable"
  end
end
