defmodule Dbos.WaitsResilienceTest do
  use Dbos.Case, async: false

  import ExUnit.CaptureLog

  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.Test.FaultyDB
  alias Dbos.Waits
  alias Dbos.WorkflowSup

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

  defp wait_until(fun, attempts \\ 300)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  test "an exception waking one parked wait leaves every other parked wait intact and still able to wake" do
    fast_retries(1)

    engine =
      start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}], park_exit_threshold_ms: 100)

    {:ok, doomed} = Dbos.start("sleeper/1", [200], engine: engine)
    {:ok, survivor} = Dbos.start("sleeper/1", [200], engine: engine)

    wait_until(fn -> WorkflowSup.whereis(engine, doomed.workflow_id) == :error end)
    wait_until(fn -> WorkflowSup.whereis(engine, survivor.workflow_id) == :error end)
    assert Waits.count(engine) == 2

    FaultyDB.inject_connection_error(
      ["FROM \"dbos\".workflow_status WHERE workflow_uuid = $1"],
      param: doomed.workflow_id,
      times: 100
    )

    log =
      capture_log(fn ->
        assert {:ok, :woke} = Dbos.await(survivor, timeout_ms: 5_000)
      end)

    assert log =~ "waking parked workflow #{doomed.workflow_id} failed"
    assert Waits.count(engine) == 0

    FaultyDB.clear_injection()
    config = Dbos.config(engine)
    {:ok, status} = SystemDb.get_workflow_status(config, doomed.workflow_id)
    assert status.status == :pending
  end

  test "killing the process that owns the parked-waits table still wakes the waits parked on it" do
    engine =
      start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}], park_exit_threshold_ms: 100)

    {:ok, handle_a} = Dbos.start("sleeper/1", [800], engine: engine)
    {:ok, handle_b} = Dbos.start("sleeper/1", [800], engine: engine)

    wait_until(fn -> WorkflowSup.whereis(engine, handle_a.workflow_id) == :error end)
    wait_until(fn -> WorkflowSup.whereis(engine, handle_b.workflow_id) == :error end)
    assert Waits.count(engine) == 2

    waits_pid = Process.whereis(Waits.process_name(engine))
    Process.exit(waits_pid, :kill)

    wait_until(fn ->
      case Process.whereis(Waits.process_name(engine)) do
        nil -> false
        pid -> pid != waits_pid
      end
    end)

    assert Waits.count(engine) == 2

    assert {:ok, :woke} = Dbos.await(handle_a, timeout_ms: 5_000)
    assert {:ok, :woke} = Dbos.await(handle_b, timeout_ms: 5_000)
  end
end
