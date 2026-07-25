defmodule Dbos.EctoKillTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      Keyword.merge(
        [
          name: name,
          db: {Dbos.DB.Ecto, Dbos.TestRepo},
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

  defp wait_until(fun, attempts \\ 100)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end

  test "ecto adapter: killing mid-transaction still lets recovery complete it" do
    engine =
      start_engine([
        {"transactional_insert_blocking/4", {SampleWorkflows, :transactional_insert_blocking, 4}}
      ])

    _config = Dbos.config(engine)
    table = new_users_table()
    ets_table = :"tx_kill_gate_ecto_#{System.unique_integer([:positive])}"
    :ets.new(ets_table, [:named_table, :public, :set])

    {:ok, handle} =
      Dbos.start(
        "transactional_insert_blocking/4",
        [table, ets_table, Ecto.Adapters.SQL, "user-kill-ecto"],
        engine: engine
      )

    wait_until(fn -> :ets.lookup(ets_table, :reached_gate) != [] end)

    {:ok, pid} = Dbos.WorkflowSup.whereis(engine, handle.workflow_id)
    Process.exit(pid, :kill)

    Dbos.Recovery.recover_pending(engine)

    wait_until(fn ->
      case Dbos.WorkflowSup.whereis(engine, handle.workflow_id) do
        {:ok, _new_pid} -> true
        _ -> false
      end
    end)

    {:ok, new_pid} = Dbos.WorkflowSup.whereis(engine, handle.workflow_id)
    send(new_pid, :go)

    assert {:ok, "user-kill-ecto"} = Dbos.await(handle, timeout_ms: 10_000)
    assert user_exists?(table, "user-kill-ecto")
  end
end
