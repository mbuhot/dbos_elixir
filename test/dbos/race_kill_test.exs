defmodule Dbos.RaceKillTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    {:ok, _} =
      Postgrex.start_link(
        name: :"race_conn_#{System.unique_integer([:positive])}",
        database: "dbos_test",
        pool_size: 1
      )

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

  test "race: kill then immediately recover repeatedly" do
    engine =
      start_engine([
        {"transactional_insert_blocking/4", {SampleWorkflows, :transactional_insert_blocking, 4}}
      ])

    config = Dbos.config(engine)
    table = new_users_table()

    for i <- 1..30 do
      ets_table = :"race_gate_#{i}_#{System.unique_integer([:positive])}"
      :ets.new(ets_table, [:named_table, :public, :set])

      {:ok, handle} =
        Dbos.start(
          "transactional_insert_blocking/4",
          [table, ets_table, Postgrex, "user-race-#{i}"], engine: engine)

      wait_loop(fn -> :ets.lookup(ets_table, :reached_gate) != [] end)

      {:ok, pid} = Dbos.WorkflowSup.whereis(engine, handle.workflow_id)
      Process.exit(pid, :kill)

      try do
        Dbos.Recovery.recover_pending(engine)
      rescue
        e ->
          IO.puts(
            "ITER #{i}: recover_pending RAISED: #{Exception.format(:error, e, __STACKTRACE__)}"
          )
      end

      case Dbos.WorkflowSup.whereis(engine, handle.workflow_id) do
        {:ok, new_pid} -> send(new_pid, :go)
        :error -> IO.puts("ITER #{i}: no new process found after recovery!")
      end

      case Dbos.await(handle, timeout_ms: 2_000) do
        {:ok, _} -> :ok
        other -> IO.puts("ITER #{i}: await result = #{inspect(other)}")
      end
    end
  end

  defp wait_loop(fun, attempts \\ 200)
  defp wait_loop(_fun, 0), do: flunk("gate never reached")

  defp wait_loop(fun, attempts) do
    if fun.(),
      do: :ok,
      else:
        (
          Process.sleep(2)
          wait_loop(fun, attempts - 1)
        )
  end
end
