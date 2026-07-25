defmodule Dbos.Integration.Workflows do
  @moduledoc """
  Workflow bodies hosted by the Docker integration suite's `node1`/`node2` processes.

  Every step body records itself twice against the shared Postgres database, outside the
  `dbos` schema: once into `execution_attempts` (append-only, so it captures every actual
  invocation of the step body, including a duplicate caused by a race) and once into
  `execution_log` (unique on `(workflow_id, step_name)`, so it captures the durable,
  deduplicated count of completed executions). Both tables outlive the process that wrote to
  them, which is what lets a test count executions across a hard `SIGKILL`.
  """

  alias Dbos.Runtime

  @doc "Checkpoints `reserve/1` immediately, then blocks in `await_release/1` until a matching row appears in `release_signals`."
  def hard_kill_workflow(_arg) do
    Runtime.run_step("reserve/1", [], fn -> record_execution("reserve/1") end)

    Runtime.run_step("await_release/1", [], fn ->
      wait_for_release(Runtime.current_workflow_id())
      record_execution("await_release/1")
    end)
  end

  @doc "A single checkpointed step, used to exercise two nodes racing to start the same workflow id."
  def concurrent_start_workflow(value) do
    Runtime.run_step("record/1", [], fn -> record_execution("record/1") end)
    value
  end

  @doc "Inserts one durable, deduplicated execution row and one append-only attempt row for the current workflow."
  def record_execution(step_name) do
    conn = execution_conn()
    workflow_id = Runtime.current_workflow_id()
    executor_id = Runtime.current_config().executor_id

    Postgrex.query!(
      conn,
      "INSERT INTO execution_attempts (workflow_id, step_name, executor_id) VALUES ($1, $2, $3)",
      [workflow_id, step_name, executor_id]
    )

    Postgrex.query!(
      conn,
      """
      INSERT INTO execution_log (workflow_id, step_name, executor_id) VALUES ($1, $2, $3)
      ON CONFLICT (workflow_id, step_name) DO NOTHING
      """,
      [workflow_id, step_name, executor_id]
    )

    :ok
  end

  @doc "Blocks (polling) until `release_signals` has a row for `workflow_id`, or raises after `attempts` tries."
  def wait_for_release(workflow_id, attempts \\ 300)

  def wait_for_release(workflow_id, 0) do
    raise "release signal for #{workflow_id} never arrived"
  end

  def wait_for_release(workflow_id, attempts) do
    conn = execution_conn()

    case Postgrex.query!(conn, "SELECT 1 FROM release_signals WHERE workflow_id = $1", [
           workflow_id
         ]) do
      %{num_rows: 0} ->
        Process.sleep(100)
        wait_for_release(workflow_id, attempts - 1)

      _matched ->
        :ok
    end
  end

  defp execution_conn do
    opts = [
      name: __MODULE__.Conn,
      hostname: System.get_env("PGHOST", "localhost"),
      port: String.to_integer(System.get_env("PGPORT", "5432")),
      username: System.get_env("PGUSER", "postgres"),
      password: System.get_env("PGPASSWORD", "postgres"),
      database: System.get_env("PGDATABASE", "dbos_integration")
    ]

    case Postgrex.start_link(opts) do
      {:ok, pid} -> pid
      {:error, {:already_started, pid}} -> pid
    end
  end
end
