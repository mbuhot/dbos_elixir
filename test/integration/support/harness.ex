defmodule Dbos.Integration.Harness do
  @moduledoc """
  Drives the `docker-compose.yml` stack for the integration suite: bringing services up, waiting
  for each node's engine to finish booting, killing a node without a graceful shutdown, and
  querying the shared system database directly with Postgrex.

  Readiness means the engine is serving, not merely that distributed Erlang answers. A node's VM
  accepts connections seconds before `test/integration/node_runtime.exs` has compiled and started
  `Dbos.Supervisor`, and an RPC landing in that window raises `:undef` or
  `Dbos.NotStartedError`. `node_runtime.exs` registers `:dbos_integration_ready` once
  `Dbos.Supervisor.start_link/1` has returned, and `up!/0`/`start!/1` block on that name.

  Runs from inside the `test-runner` container, which has the Docker CLI and the `docker
  compose` plugin installed and the host's Docker socket mounted, so `docker compose` here
  controls its sibling `node1`/`node2` containers.
  """

  @project "dbos_integration"
  @compose_file Path.expand("../../../docker-compose.yml", __DIR__)

  @doc "Runs `docker compose <args>`, raising on a non-zero exit."
  def compose!(args) do
    case compose(args) do
      {output, 0} ->
        output

      {output, status} ->
        raise "docker compose #{Enum.join(args, " ")} exited #{status}: #{output}"
    end
  end

  @doc "Runs `docker compose <args>` against this project's compose file, returning `{output, exit_status}`."
  def compose(args) do
    System.cmd("docker", ["compose", "-p", @project, "-f", @compose_file] ++ args,
      stderr_to_stdout: true
    )
  end

  @doc "Brings up `postgres`, `migrate`, `node1`, `node2` and waits for both engines to finish booting."
  def up! do
    compose!(["up", "-d", "postgres", "migrate", "node1", "node2"])
    wait_for_node!(:node1)
    wait_for_node!(:node2)
    :ok
  end

  @doc "Tears the stack down, removing volumes."
  def down! do
    compose(["down", "-v", "--remove-orphans"])
    :ok
  end

  @doc "Sends `signal` (default `SIGKILL`) directly to `service`'s container, bypassing `terminate/2`."
  def kill!(service, signal \\ "SIGKILL") do
    compose!(["kill", "-s", signal, Atom.to_string(service)])
    :ok
  end

  @doc "Starts a previously killed service back up and waits for its engine to finish booting again."
  def start!(service) do
    compose!(["start", Atom.to_string(service)])
    wait_for_node!(service)
  end

  @doc "The distributed Erlang node name for a compose service, e.g. `node_name(:node1)` is `:\"node1@node1\"`."
  def node_name(service), do: :"#{service}@#{service}"

  @doc "Calls `module.function(args)` on `service`'s node over `:rpc`, connecting first if needed."
  def rpc(service, module, function, args) do
    node = node_name(service)
    true = Node.connect(node)
    :rpc.call(node, module, function, args)
  end

  @doc "A bare Postgrex connection to the shared database, for the `execution_log`/`execution_attempts`/`release_signals` tables."
  def conn do
    {:ok, conn} = Postgrex.start_link(postgrex_opts())
    conn
  end

  @doc "Unblocks `wait_for_release/2` in a hosted workflow by inserting its id into `release_signals`."
  def release!(conn, workflow_id) do
    Postgrex.query!(
      conn,
      "INSERT INTO release_signals (workflow_id) VALUES ($1) ON CONFLICT DO NOTHING",
      [workflow_id]
    )

    :ok
  end

  @doc "How many rows `execution_log` (deduplicated) has for `workflow_id`/`step_name`."
  def execution_count(conn, workflow_id, step_name) do
    %{rows: [[count]]} =
      Postgrex.query!(
        conn,
        "SELECT count(*) FROM execution_log WHERE workflow_id = $1 AND step_name = $2",
        [workflow_id, step_name]
      )

    count
  end

  @doc "How many rows `execution_attempts` (append-only) has for `workflow_id`/`step_name` — every actual invocation, including a duplicate caused by a race."
  def attempt_count(conn, workflow_id, step_name) do
    %{rows: [[count]]} =
      Postgrex.query!(
        conn,
        "SELECT count(*) FROM execution_attempts WHERE workflow_id = $1 AND step_name = $2",
        [workflow_id, step_name]
      )

    count
  end

  @doc "How many `dbos.operation_outputs` rows exist for `workflow_id`, and whether `function_id` is duplicated."
  def operation_output_function_ids(conn, workflow_id) do
    %{rows: rows} =
      Postgrex.query!(
        conn,
        ~s(SELECT function_id FROM "dbos".operation_outputs WHERE workflow_uuid = $1 ORDER BY function_id),
        [workflow_id]
      )

    Enum.map(rows, fn [function_id] -> function_id end)
  end

  @doc "The `executor_id` and status currently recorded for `workflow_id`, read straight from `dbos.workflow_status`."
  def owner_and_status(conn, workflow_id) do
    %{rows: [[executor_id, status, recovery_attempts]]} =
      Postgrex.query!(
        conn,
        ~s(SELECT executor_id, status, recovery_attempts FROM "dbos".workflow_status WHERE workflow_uuid = $1),
        [workflow_id]
      )

    %{executor_id: executor_id, status: status, recovery_attempts: recovery_attempts}
  end

  @doc "Polls `fun.()` until it returns a truthy value, or raises after `attempts` (default 150, ~15s at the default 100ms poll)."
  def wait_until(fun, attempts \\ 150)

  def wait_until(_fun, 0), do: raise("condition never became true")

  def wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(100)
      wait_until(fun, attempts - 1)
    end
  end

  defp wait_for_node!(service, attempts \\ 240)

  defp wait_for_node!(service, 0) do
    raise "#{service}'s engine (#{node_name(service)}) never finished booting"
  end

  defp wait_for_node!(service, attempts) do
    if engine_ready?(service) do
      :ok
    else
      Process.sleep(500)
      wait_for_node!(service, attempts - 1)
    end
  end

  defp engine_ready?(service) do
    node = node_name(service)

    Node.connect(node) == true and
      is_pid(:rpc.call(node, Process, :whereis, [:dbos_integration_ready]))
  end

  defp postgrex_opts do
    [
      hostname: System.get_env("PGHOST", "localhost"),
      port: String.to_integer(System.get_env("PGPORT", "55432")),
      username: System.get_env("PGUSER", "postgres"),
      password: System.get_env("PGPASSWORD", "postgres"),
      database: System.get_env("PGDATABASE", "dbos_integration")
    ]
  end
end
