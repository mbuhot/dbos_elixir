defmodule Dbos.DB.EctoTest do
  use Dbos.Case, async: false

  alias Dbos.DB.Ecto, as: DB

  test "query with params" do
    assert {:ok, %{rows: [[1]], num_rows: 1}} = DB.query(Dbos.TestRepo, "SELECT $1::int", [1])
  end

  test "transaction commits" do
    {:ok, _} =
      DB.transaction(Dbos.TestRepo, [], fn repo ->
        DB.query!(repo, "INSERT INTO dbos.queues (name) VALUES ($1)", ["q1"])
      end)

    assert {:ok, %{rows: [["q1"]]}} = DB.query(Dbos.TestRepo, "SELECT name FROM dbos.queues", [])
  end

  test "transaction rolls back on raise" do
    assert_raise RuntimeError, fn ->
      DB.transaction(Dbos.TestRepo, [], fn repo ->
        DB.query!(repo, "INSERT INTO dbos.queues (name) VALUES ($1)", ["q2"])
        raise "boom"
      end)
    end

    assert {:ok, %{rows: []}} = DB.query(Dbos.TestRepo, "SELECT name FROM dbos.queues", [])
  end

  test "in_transaction? reports true inside a transaction and false outside" do
    refute DB.in_transaction?(Dbos.TestRepo)

    DB.transaction(Dbos.TestRepo, [], fn repo ->
      assert DB.in_transaction?(repo)
    end)
  end

  test "isolation level takes effect inside a transaction" do
    {:ok, isolation} =
      DB.transaction(Dbos.TestRepo, [isolation: :serializable], fn repo ->
        {:ok, %{rows: [[level]]}} = DB.query(repo, "SHOW transaction_isolation", [])
        level
      end)

    assert isolation == "serializable"
  end

  test "an Ecto.Repo call inside a transactional step enlists on the same connection" do
    DB.transaction(Dbos.TestRepo, [], fn repo ->
      DB.query!(repo, "INSERT INTO dbos.queues (name) VALUES ($1)", ["shared_conn"])
      assert Dbos.TestRepo.in_transaction?()
    end)
  end
end
