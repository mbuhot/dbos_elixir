defmodule Dbos.NodeWatcherResilienceTest do
  use Dbos.Case, async: false

  import ExUnit.CaptureLog

  alias Dbos.Cluster.NodeWatcher
  alias Dbos.Cluster.OrphanSweep
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

  test "a :nodedown triggers a sweep pass, but reclaims nothing while the departed node's lease is still valid" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}], cluster: [enabled: true])
    config = Dbos.config(engine)

    SystemDb.renew_lease(%{config | executor_id: "exec-departed"}, 60_000)

    SystemDb.insert_workflow_status(%{config | executor_id: "exec-departed"}, %{
      workflow_id: "wf-nodedown-live-lease",
      status: :pending,
      name: "add/2",
      inputs: [1, 2]
    })

    send(NodeWatcher.process_name(engine), {:nodedown, :departed@nowhere})
    Process.sleep(50)

    assert status_of(config, "wf-nodedown-live-lease").status == :pending
    assert status_of(config, "wf-nodedown-live-lease").executor_id == "exec-departed"

    SystemDb.expire_lease(%{config | executor_id: "exec-departed"})
    send(NodeWatcher.process_name(engine), {:nodedown, :departed@nowhere})

    wait_until(fn -> status_of(config, "wf-nodedown-live-lease").status == :success end)
  end

  test "a :nodedown whose triggered sweep fails does not crash the node watcher, and a later sweep pass still succeeds" do
    fast_retries(1)

    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}], cluster: [enabled: true])
    config = Dbos.config(engine)

    SystemDb.expire_lease(%{config | executor_id: "exec-gone"})

    SystemDb.insert_workflow_status(%{config | executor_id: "exec-gone"}, %{
      workflow_id: "wf-nodedown-sweep-failure",
      status: :pending,
      name: "add/2",
      inputs: [1, 2]
    })

    FaultyDB.inject_connection_error(["SET executor_id = $1"], times: 100)

    log =
      capture_log(fn ->
        send(NodeWatcher.process_name(engine), {:nodedown, :departed@nowhere})
        wait_until(fn -> FaultyDB.injected_count() > 0 end)
        :sys.get_state(NodeWatcher.process_name(engine))
      end)

    assert log =~ "sweep pass triggered by :nodedown"
    assert status_of(config, "wf-nodedown-sweep-failure").status == :pending
    assert Process.alive?(Process.whereis(NodeWatcher.process_name(engine)))

    FaultyDB.clear_injection()
    OrphanSweep.sweep_now(engine)

    wait_until(fn -> status_of(config, "wf-nodedown-sweep-failure").status == :success end)
  end

  test "a departed executor with a live lease defers the sweep until that lease expires" do
    engine = start_engine([], cluster: [enabled: true])
    config = Dbos.config(engine)
    peer = "exec-peer-#{System.unique_integer([:positive])}"

    ttl_ms = 4_000
    SystemDb.renew_lease(%{config | executor_id: peer}, ttl_ms)

    delay_ms = NodeWatcher.sweep_delay_ms(engine, [peer])

    assert delay_ms > 3_000
    assert delay_ms <= ttl_ms + 500
  end

  test "a departed executor whose lease has already expired sweeps immediately" do
    engine = start_engine([], cluster: [enabled: true])
    config = Dbos.config(engine)
    peer = "exec-peer-#{System.unique_integer([:positive])}"

    SystemDb.renew_lease(%{config | executor_id: peer}, -1_000)

    assert NodeWatcher.sweep_delay_ms(engine, [peer]) == 0
  end

  test "a departed executor that never held a lease sweeps immediately" do
    engine = start_engine([], cluster: [enabled: true])

    assert NodeWatcher.sweep_delay_ms(engine, ["exec-never-leased"]) == 0
  end

  test "the earliest lease among several departed executors decides the delay" do
    engine = start_engine([], cluster: [enabled: true])
    config = Dbos.config(engine)
    soon = "exec-soon-#{System.unique_integer([:positive])}"
    later = "exec-later-#{System.unique_integer([:positive])}"

    SystemDb.renew_lease(%{config | executor_id: soon}, 2_000)
    SystemDb.renew_lease(%{config | executor_id: later}, 30_000)

    delay_ms = NodeWatcher.sweep_delay_ms(engine, [later, soon])

    assert delay_ms > 1_000
    assert delay_ms <= 2_500
  end
end
