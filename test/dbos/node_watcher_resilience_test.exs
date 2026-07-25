defmodule Dbos.NodeWatcherResilienceTest do
  use Dbos.Case, async: false

  import ExUnit.CaptureLog

  alias Dbos.Cluster.NodeWatcher
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.Test.FaultyDB

  setup do
    on_exit(fn ->
      FaultyDB.clear_injection()
      Application.delete_env(:dbos, :system_db_retry)
    end)

    :ok
  end

  defp fast_retries(max_attempts) do
    Application.put_env(:dbos, :system_db_retry,
      max_attempts: max_attempts,
      base_delay_ms: 1,
      max_delay_ms: 5
    )
  end

  defp start_engine(workflows, extra_opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {FaultyDB, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        workflows: workflows,
        migrations: :skip
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp wait_until(fun, attempts \\ 500)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  defp status_of(config, workflow_id) do
    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    status
  end

  test "a :nodedown whose reclaim raises does not permanently strand the dead node's workflows" do
    fast_retries(1)

    engine =
      start_engine([{"add/2", {SampleWorkflows, :add, 2}}], cluster: [enabled: true])

    config = Dbos.config(engine)

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-nodedown-stranded",
      status: :pending,
      name: "add/2",
      inputs: [1, 2]
    })

    FaultyDB.inject_connection_error(["SET executor_id = $1"], times: 100)

    log =
      capture_log(fn ->
        send(NodeWatcher.process_name(engine), {:nodedown, node()})
        Process.sleep(50)
      end)

    assert log =~ "reclaim for departed node"
    assert status_of(config, "wf-nodedown-stranded").status == :pending

    FaultyDB.clear_injection()

    wait_until(fn -> status_of(config, "wf-nodedown-stranded").status == :success end)
  end
end
