defmodule Dbos.Integration.Workflows do
  @moduledoc """
  Workflow bodies hosted by the Docker integration suite's `node1`/`node2` processes.

  These bodies run many at a time (the queue competition test dequeues 50 at once). The Postgrex
  pool they record through is started once at node boot by `test/integration/node_runtime.exs`,
  linked to that script, and shared by name, so its lifetime spans every workflow process that
  uses it.

  Every step body records itself twice against the shared Postgres database, outside the
  `dbos` schema: once into `execution_attempts` (append-only, so it captures every actual
  invocation of the step body, including a duplicate caused by a race) and once into
  `execution_log` (unique on `(workflow_id, step_name)`, so it captures the durable,
  deduplicated count of completed executions). Both tables outlive the process that wrote to
  them, which is what lets a test count executions across a hard `SIGKILL`.
  """

  alias Dbos.Runtime

  @conn __MODULE__.Conn

  @doc "Starts the shared recording pool, linked to the caller — the node runtime script, which outlives every workflow."
  def start_conn! do
    {:ok, pid} =
      Postgrex.start_link(
        name: @conn,
        hostname: System.get_env("PGHOST", "postgres"),
        port: String.to_integer(System.get_env("PGPORT", "5432")),
        username: System.get_env("PGUSER", "postgres"),
        password: System.get_env("PGPASSWORD", "postgres"),
        database: System.get_env("PGDATABASE", "dbos_integration"),
        pool_size: 20,
        queue_target: 5_000,
        queue_interval: 30_000,
        timeout: 30_000
      )

    pid
  end

  @doc "Checkpoints `reserve/1` immediately, then blocks in `await_release/1` until a matching row appears in `release_signals`."
  def hard_kill_workflow(_arg) do
    Runtime.run_step("reserve/1", [], fn -> record_execution("reserve/1") end)

    Runtime.run_step("await_release/1", [], fn ->
      wait_for_release(Runtime.current_workflow_id())
      record_execution("await_release/1")
    end)
  end

  @doc """
  The same shape as `hard_kill_workflow/1`, registered only on the node whose
  `DBOS_HOST_EXCLUSIVE_WORKFLOW` is set, so a peer reclaiming a dead executor's rows has no
  implementation of this name to run.
  """
  def exclusive_workflow(arg), do: hard_kill_workflow(arg)

  @doc "A single checkpointed step, used to exercise two nodes racing to start the same workflow id."
  def concurrent_start_workflow(value) do
    Runtime.run_step("record/1", [], fn -> record_execution("record/1") end)
    value
  end

  @doc "A single checkpointed step, used to exercise two nodes racing to dequeue the same queue's backlog."
  def queue_competition_workflow(value) do
    Runtime.run_step("record/1", [], fn -> record_execution("record/1") end)
    value
  end

  @doc "Inserts one durable, deduplicated execution row and one append-only attempt row for the current workflow."
  def record_execution(step_name) do
    workflow_id = Runtime.current_workflow_id()
    executor_id = Runtime.current_config().executor_id

    Postgrex.query!(
      @conn,
      "INSERT INTO execution_attempts (workflow_id, step_name, executor_id) VALUES ($1, $2, $3)",
      [workflow_id, step_name, executor_id]
    )

    Postgrex.query!(
      @conn,
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
    case Postgrex.query!(@conn, "SELECT 1 FROM release_signals WHERE workflow_id = $1", [
           workflow_id
         ]) do
      %{num_rows: 0} ->
        Process.sleep(100)
        wait_for_release(workflow_id, attempts - 1)

      _matched ->
        :ok
    end
  end
end
