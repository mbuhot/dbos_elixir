defmodule Dbos.TransactionTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      Keyword.merge(
        [
          name: name,
          db: {Dbos.DB.Postgrex, Dbos.TestConn},
          executor_id: "exec-#{System.unique_integer([:positive])}",
          workflows: workflows,
          migrations: :skip
        ],
        extra_opts
      )

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp new_users_table do
    table = "tx_users_#{System.unique_integer([:positive])}"

    Postgrex.query!(
      Dbos.TestConn,
      "CREATE TABLE IF NOT EXISTS #{table} (id text primary key)",
      []
    )

    table
  end

  defp user_exists?(table, id) do
    %{rows: rows} =
      Postgrex.query!(Dbos.TestConn, "SELECT 1 FROM #{table} WHERE id = $1", [id])

    rows != []
  end

  test "a successful transaction leaves both the user row and the checkpoint" do
    engine =
      start_engine([
        {"transactional_insert/3", {SampleWorkflows, :transactional_insert, 3}}
      ])

    config = Dbos.config(engine)
    table = new_users_table()

    {:ok, handle} =
      Dbos.start("transactional_insert/3", [table, Postgrex, "user-1"], engine: engine)

    assert {:ok, "user-1"} = Dbos.await(handle)
    assert user_exists?(table, "user-1")

    {:ok, [step]} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert step.function_name == "insert_user/2"
    assert step.output == "user-1"
  end

  test "a transaction that raises leaves neither the user row nor the checkpoint" do
    engine =
      start_engine([
        {"transactional_insert_then_raise/3",
         {SampleWorkflows, :transactional_insert_then_raise, 3}}
      ])

    config = Dbos.config(engine)
    table = new_users_table()

    {:ok, handle} =
      Dbos.start("transactional_insert_then_raise/3", [table, Postgrex, "user-2"], engine: engine)

    assert {:error, %RuntimeError{message: "boom"}} = Dbos.await(handle)
    refute user_exists?(table, "user-2")

    {:ok, []} = SystemDb.get_workflow_steps(config, handle.workflow_id)
  end

  test "transaction-in-transaction is rejected" do
    engine =
      start_engine([
        {"transaction_in_transaction/0", {SampleWorkflows, :transaction_in_transaction, 0}}
      ])

    {:ok, handle} = Dbos.start("transaction_in_transaction/0", [], engine: engine)
    assert {:error, %Dbos.NestedTransactionError{}} = Dbos.await(handle)
  end

  test "a plain step inside a transaction is rejected by the added guard" do
    engine =
      start_engine([{"step_in_transaction/0", {SampleWorkflows, :step_in_transaction, 0}}])

    {:ok, handle} = Dbos.start("step_in_transaction/0", [], engine: engine)
    assert {:error, %Dbos.StepInTransactionError{}} = Dbos.await(handle)
  end

  test "a transaction inside a plain step is allowed and records no extra durability row" do
    engine =
      start_engine([{"transaction_in_step/0", {SampleWorkflows, :transaction_in_step, 0}}])

    config = Dbos.config(engine)
    {:ok, handle} = Dbos.start("transaction_in_step/0", [], engine: engine)
    assert {:ok, :ok} = Dbos.await(handle)

    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert Enum.map(steps, & &1.function_name) == ["outer_step/0"]
  end

  test "isolation level takes effect for a transactional step" do
    engine =
      start_engine([
        {"transaction_isolation_probe/2", {SampleWorkflows, :transaction_isolation_probe, 2}}
      ])

    table = new_users_table()
    {:ok, handle} = Dbos.start("transaction_isolation_probe/2", [table, Postgrex], engine: engine)
    assert {:ok, "serializable"} = Dbos.await(handle)
  end

  test "an Ecto Repo.query inside a transactional step commits on the same connection as the checkpoint" do
    engine =
      start_engine(
        [{"transactional_insert/3", {SampleWorkflows, :transactional_insert, 3}}],
        db: {Dbos.DB.Ecto, Dbos.TestRepo}
      )

    config = Dbos.config(engine)
    table = new_users_table()

    {:ok, handle} =
      Dbos.start("transactional_insert/3", [table, Ecto.Adapters.SQL, "user-ecto"],
        engine: engine
      )

    assert {:ok, "user-ecto"} = Dbos.await(handle)
    assert user_exists?(table, "user-ecto")

    {:ok, [step]} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    assert step.output == "user-ecto"
  end
end
