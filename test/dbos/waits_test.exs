defmodule Dbos.WaitsTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.Waits
  alias Dbos.WorkflowSup

  defp start_engine(workflows, extra_opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        workflows: workflows,
        migrations: :skip
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp wait_until(fun, attempts \\ 200)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(10)
      wait_until(fun, attempts - 1)
    end
  end

  defp force_gc do
    Enum.each(Process.list(), &:erlang.garbage_collect/1)
  end

  defp new_table do
    name = :"waits_test_#{System.unique_integer([:positive])}"
    :ets.new(name, [:named_table, :public, :set])
    name
  end

  test "1. a workflow sleeping longer than the threshold releases its process" do
    engine =
      start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}], park_exit_threshold_ms: 100)

    {:ok, handle} = Dbos.start("sleeper/1", [3_000], engine: engine)

    wait_until(fn -> WorkflowSup.whereis(engine, handle.workflow_id) == :error end)

    assert Waits.count(engine) == 1
    assert {:ok, :woke} = Dbos.await(handle, timeout_ms: 5_000)
  end

  test "2. a parked workflow wakes at the right time and does not re-execute completed steps" do
    engine =
      start_engine(
        [{"counted_steps_then_sleep/3", {SampleWorkflows, :counted_steps_then_sleep, 3}}],
        park_exit_threshold_ms: 100
      )

    table = new_table()
    started_at = System.monotonic_time(:millisecond)

    {:ok, handle} =
      Dbos.start("counted_steps_then_sleep/3", [table, 4, 400], engine: engine)

    wait_until(fn -> WorkflowSup.whereis(engine, handle.workflow_id) == :error end)

    assert {:ok, :woke} = Dbos.await(handle, timeout_ms: 5_000)
    elapsed_ms = System.monotonic_time(:millisecond) - started_at

    assert elapsed_ms >= 350
    assert elapsed_ms < 5_000
    assert :ets.lookup_element(table, :padding_runs, 2) == 4
  end

  test "3. a workflow parked in recv is woken early by a message arriving, ahead of its deadline" do
    engine =
      start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}],
        park_exit_threshold_ms: 100,
        notifications: :listen,
        notifications_conn_opts: [database: "dbos_test"]
      )

    {:ok, handle} = Dbos.start("receiver/2", ["topic", 10_000], engine: engine)

    wait_until(fn -> WorkflowSup.whereis(engine, handle.workflow_id) == :error end)
    assert Waits.count(engine) == 1

    started_at = System.monotonic_time(:millisecond)
    :ok = Dbos.send_message(handle.workflow_id, "topic", :hello, engine: engine)

    assert {:ok, :hello} = Dbos.await(handle, timeout_ms: 5_000)
    elapsed_ms = System.monotonic_time(:millisecond) - started_at
    assert elapsed_ms < 5_000
  end

  test "4. a workflow waiting less than the threshold stays resident" do
    engine =
      start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}], park_exit_threshold_ms: 5_000)

    {:ok, handle} = Dbos.start("sleeper/1", [300], engine: engine)

    Process.sleep(100)
    assert {:ok, _pid} = WorkflowSup.whereis(engine, handle.workflow_id)
    assert Waits.count(engine) == 0

    assert {:ok, :woke} = Dbos.await(handle, timeout_ms: 2_000)
  end

  test "5. a workflow with many completed steps stays resident despite a long wait" do
    engine =
      start_engine(
        [{"counted_steps_then_sleep/3", {SampleWorkflows, :counted_steps_then_sleep, 3}}],
        park_exit_threshold_ms: 100,
        park_replay_ceiling: 5
      )

    table = new_table()

    {:ok, handle} =
      Dbos.start("counted_steps_then_sleep/3", [table, 10, 5_000], engine: engine)

    Process.sleep(200)
    assert {:ok, _pid} = WorkflowSup.whereis(engine, handle.workflow_id)
    assert Waits.count(engine) == 0

    :ok = Dbos.cancel(handle.workflow_id, engine: engine)
    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle, timeout_ms: 2_000)
  end

  test "6. cancelling a parked workflow ends it CANCELLED promptly" do
    engine =
      start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}], park_exit_threshold_ms: 100)

    config = Dbos.config(engine)
    {:ok, handle} = Dbos.start("sleeper/1", [10_000], engine: engine)

    wait_until(fn -> WorkflowSup.whereis(engine, handle.workflow_id) == :error end)

    started_at = System.monotonic_time(:millisecond)
    :ok = Dbos.cancel(handle.workflow_id, engine: engine)

    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle, timeout_ms: 2_000)
    elapsed_ms = System.monotonic_time(:millisecond) - started_at
    assert elapsed_ms < 2_000

    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :cancelled
  end

  test "7. killing the engine with a parked wait outstanding recovers it on restart" do
    workflows = [{"sleeper/1", {SampleWorkflows, :sleeper, 1}}]
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")
    executor_id = "exec-#{System.unique_integer([:positive])}"

    opts = [
      name: name,
      db: {Dbos.DB.Postgrex, Dbos.TestConn},
      executor_id: executor_id,
      workflows: workflows,
      migrations: :skip,
      park_exit_threshold_ms: 100
    ]

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)

    {:ok, handle} = Dbos.start("sleeper/1", [400], engine: name)

    wait_until(fn -> WorkflowSup.whereis(name, handle.workflow_id) == :error end)
    assert Waits.count(name) == 1

    stop_supervised!(name)

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)

    assert {:ok, :woke} = Dbos.await(handle, timeout_ms: 5_000)

    config = Dbos.config(name)
    {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
    assert status.status == :success
  end

  test "8. scale: bounded process/memory footprint for many concurrently parked waits" do
    engine =
      start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}], park_exit_threshold_ms: 100)

    wait_count = 2_000

    force_gc()
    process_count_before = :erlang.system_info(:process_count)
    memory_before = :erlang.memory(:total)

    handles =
      for _ <- 1..wait_count do
        {:ok, handle} = Dbos.start("sleeper/1", [5_000], engine: engine)
        handle
      end

    wait_until(fn -> Waits.count(engine) == wait_count end, 1_000)

    force_gc()
    process_count_after = :erlang.system_info(:process_count)
    memory_after = :erlang.memory(:total)

    ets_words = :ets.info(Waits.table_name(engine), :memory)
    ets_bytes_per_wait = ets_words * :erlang.system_info(:wordsize) / wait_count

    process_growth = process_count_after - process_count_before
    memory_growth_bytes = memory_after - memory_before
    bytes_per_wait = memory_growth_bytes / wait_count

    IO.puts(
      "scale test: #{wait_count} parked waits, process_growth=#{process_growth}, " <>
        "vm_memory_growth_bytes=#{memory_growth_bytes}, vm_bytes_per_wait=#{bytes_per_wait}, " <>
        "ets_table_bytes_per_wait=#{ets_bytes_per_wait}"
    )

    assert process_growth < wait_count
    assert ets_bytes_per_wait < 2_000

    Enum.each(handles, &Dbos.cancel(&1.workflow_id, engine: engine))
  end
end
