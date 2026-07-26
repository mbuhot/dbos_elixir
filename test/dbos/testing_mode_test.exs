defmodule Dbos.TestingModeTest do
  @moduledoc """
  Tests `Dbos.Supervisor`'s `:inline`/`:manual` testing modes: no background process starts,
  `Dbos.start/3` and `Dbos.enqueue/3` run synchronously, checkpoints are still written and still
  replay correctly, durable waits behave per the documented testing-mode rules, and the whole
  thing works with `Ecto.Adapters.SQL.Sandbox` in `:manual` mode.
  """

  use Dbos.Case, async: false

  import ExUnit.CaptureLog

  alias Dbos.Notifications
  alias Dbos.Queue
  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.Testing
  alias Dbos.WorkflowSup

  @workflows [
    {"add/2", {SampleWorkflows, :add, 2}},
    {"three_steps/1", {SampleWorkflows, :three_steps, 1}},
    {"sleeper/1", {SampleWorkflows, :sleeper, 1}},
    {"receiver/2", {SampleWorkflows, :receiver, 2}},
    {"counted_steps_then_sleep/3", {SampleWorkflows, :counted_steps_then_sleep, 3}},
    {"stream_writer/2", {SampleWorkflows, :stream_writer, 2}},
    {"stream_self_reader/2", {SampleWorkflows, :stream_self_reader, 2}}
  ]

  defp start_engine(extra_opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        migrations: :skip,
        workflows: @workflows
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    name
  end

  defp new_table do
    table = :"testing_mode_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])
    table
  end

  describe ":inline mode" do
    test "starts none of the background processes" do
      engine = start_engine(testing: :inline)

      refute Process.whereis(Notifications.process_name(engine))
      refute Process.whereis(Dbos.Waits.process_name(engine))
      refute Process.whereis(Dbos.Waits.table_owner_name(engine))
      refute Process.whereis(Dbos.Lease.process_name(engine))
      refute Process.whereis(Module.concat(engine, Recovery))
      refute Process.whereis(Dbos.Scheduler.process_name(engine))
      refute Process.whereis(Module.concat(engine, Queue.Sup))
      refute Process.whereis(Dbos.LeaseSweep.process_name(engine))
      refute Process.whereis(WorkflowSup.process_name(engine))
      assert Process.whereis(Module.concat(engine, Registry))
    end

    test "Dbos.start/3 runs synchronously and writes every checkpoint" do
      engine = start_engine(testing: :inline)
      config = Dbos.config(engine)

      {:ok, handle} = Dbos.start("three_steps/1", ["order-1"], engine: engine)

      assert {:ok, %Dbos.WorkflowStatus{status: :success}} =
               SystemDb.get_workflow_status(config, handle.workflow_id)

      {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)

      assert Enum.map(steps, & &1.function_name) == [
               "reserve_stock/1",
               "charge_card/1",
               "ship_order/1"
             ]
    end

    test "Dbos.sleep/1 returns promptly and still checkpoints its wake time" do
      engine = start_engine(testing: :inline)
      config = Dbos.config(engine)

      started_at = System.monotonic_time(:millisecond)
      {:ok, handle} = Dbos.start("sleeper/1", [3_600_000], engine: engine)
      elapsed_ms = System.monotonic_time(:millisecond) - started_at

      assert elapsed_ms < 1_000
      assert {:ok, :woke} = Dbos.await(handle, engine: engine)

      {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
      assert [%{function_name: "DBOS.sleep", output: wake_time_ms}] = steps
      assert wake_time_ms > started_at
    end

    test "replaying a workflow does not re-execute its already-checkpointed steps" do
      engine = start_engine(testing: :inline)
      config = Dbos.config(engine)
      table = new_table()
      workflow_id = "inline-replay-#{System.unique_integer([:positive])}"

      {:ok, handle} =
        Dbos.start("counted_steps_then_sleep/3", [table, 3, 3_600_000],
          engine: engine,
          workflow_id: workflow_id
        )

      assert {:ok, :woke} = Dbos.await(handle, engine: engine)
      assert :ets.lookup_element(table, :padding_runs, 2) == 3

      {:ok, steps_before} = SystemDb.get_workflow_steps(config, workflow_id)

      Dbos.DB.Postgrex.query!(
        config.conn,
        "UPDATE dbos.workflow_status SET status = 'PENDING' WHERE workflow_uuid = $1",
        [workflow_id]
      )

      assert Recovery.recover_pending(engine) == [workflow_id]

      assert :ets.lookup_element(table, :padding_runs, 2) == 3

      {:ok, steps_after} = SystemDb.get_workflow_steps(config, workflow_id)
      assert Enum.map(steps_after, & &1.function_id) == Enum.map(steps_before, & &1.function_id)

      assert {:ok, %Dbos.WorkflowStatus{status: :success}} =
               SystemDb.get_workflow_status(config, workflow_id)
    end

    test "Dbos.enqueue/3 drains and runs synchronously, returning a finished handle" do
      engine = start_engine(testing: :inline, queues: [Queue.new("inline_queue")])

      {:ok, handle} = Dbos.enqueue("add/2", [1, 2], queue_name: "inline_queue", engine: engine)

      assert {:ok, 3} = Dbos.await(handle, engine: engine)
    end

    test "recv_message with nothing pending raises immediately instead of blocking" do
      engine = start_engine(testing: :inline)

      {:ok, handle} = Dbos.start("receiver/2", ["topic", 5_000], engine: engine)

      assert {:error, %Dbos.TestingModeWaitError{} = error} = Dbos.await(handle, engine: engine)
      assert error.operation == "recv_message"
      assert Exception.message(error) =~ "manual"
    end

    test "writing and closing a stream completes, and the items read back in order" do
      engine = start_engine(testing: :inline)
      key = "log"

      {:ok, handle} =
        Dbos.start("stream_writer/2", [key, [:a, :b, :c]], engine: engine)

      assert {:ok, :ok} = Dbos.await(handle, engine: engine)

      assert Dbos.read_stream(handle.workflow_id, key, engine: engine) |> Enum.to_list() == [
               :a,
               :b,
               :c
             ]
    end

    test "reading a stream that is still open with nothing more pending raises instead of blocking" do
      engine = start_engine(testing: :inline)
      key = "log"

      {:ok, handle} =
        Dbos.start("stream_self_reader/2", [key, :only_value], engine: engine)

      assert {:error, %Dbos.TestingModeWaitError{} = error} = Dbos.await(handle, engine: engine)
      assert error.operation == "read_stream"
      assert error.topic_or_key == key
      assert Exception.message(error) =~ "manual"
    end

    test "a workflow_timeout_ms deadline is recorded, and stopping the engine leaves no stray process behind" do
      engine = start_engine(testing: :inline)
      config = Dbos.config(engine)

      {:ok, handle} = Dbos.start("add/2", [1, 2], timeout_ms: 100, engine: engine)

      assert {:ok, 3} = Dbos.await(handle, engine: engine)

      assert {:ok, %Dbos.WorkflowStatus{workflow_deadline_epoch_ms: deadline}} =
               SystemDb.get_workflow_status(config, handle.workflow_id)

      assert is_integer(deadline)

      log =
        capture_log(fn ->
          stop_supervised!(engine)
          Process.sleep(200)
        end)

      refute log =~ "CRASH REPORT"
      refute log =~ "ArgumentError"
    end
  end

  describe ":manual mode" do
    test "Dbos.enqueue/3 only inserts the row; Dbos.Testing.drain_queue/2 runs it" do
      engine = start_engine(testing: :manual, queues: [Queue.new("manual_queue")])
      config = Dbos.config(engine)

      {:ok, handle} = Dbos.enqueue("add/2", [1, 2], queue_name: "manual_queue", engine: engine)

      assert {:ok, %Dbos.WorkflowStatus{status: :enqueued}} =
               SystemDb.get_workflow_status(config, handle.workflow_id)

      assert Testing.drain_queue("manual_queue", engine: engine) == 1
      assert {:ok, 3} = Dbos.await(handle, engine: engine)
    end

    test "drain_queue is a no-op-safe call that returns 0 when there is nothing to run" do
      engine = start_engine(testing: :manual, queues: [Queue.new("empty_queue")])
      assert Testing.drain_queue("empty_queue", engine: engine) == 0
    end

    test "drain_all drains every declared queue, including the internal one" do
      engine =
        start_engine(
          testing: :manual,
          queues: [Queue.new("manual_queue_a"), Queue.new("manual_queue_b")]
        )

      {:ok, handle_a} =
        Dbos.enqueue("add/2", [1, 2], queue_name: "manual_queue_a", engine: engine)

      {:ok, handle_b} =
        Dbos.enqueue("add/2", [2, 2], queue_name: "manual_queue_b", engine: engine)

      assert Testing.drain_all(engine: engine) == 2
      assert {:ok, 3} = Dbos.await(handle_a, engine: engine)
      assert {:ok, 4} = Dbos.await(handle_b, engine: engine)
    end

    test "recover_pending is a no-op-safe call that returns an empty list when there is nothing pending" do
      engine = start_engine(testing: :manual)
      assert Recovery.recover_pending(engine) == []
    end

    test "send_message before draining lets recv_message consume an already-pending message" do
      engine = start_engine(testing: :manual, queues: [Queue.new("approval_queue")])
      workflow_id = "manual-recv-#{System.unique_integer([:positive])}"

      {:ok, handle} =
        Dbos.enqueue("receiver/2", ["topic", 5_000],
          queue_name: "approval_queue",
          workflow_id: workflow_id,
          engine: engine
        )

      :ok = Dbos.send_message(workflow_id, "topic", "approved", engine: engine)

      assert Testing.drain_queue("approval_queue", engine: engine) == 1
      assert {:ok, "approved"} = Dbos.await(handle, engine: engine)
    end
  end

  describe "sandbox acceptance" do
    setup do
      Application.put_env(:dbos, Dbos.SandboxRepo,
        database: Application.fetch_env!(:dbos, :test_database),
        hostname: System.get_env("PGHOST", "localhost"),
        username: System.get_env("PGUSER", "postgres"),
        password: System.get_env("PGPASSWORD", "postgres"),
        pool: Ecto.Adapters.SQL.Sandbox,
        pool_size: 2
      )

      start_supervised!(Dbos.SandboxRepo)
      Ecto.Adapters.SQL.Sandbox.mode(Dbos.SandboxRepo, :manual)
      :ok = Ecto.Adapters.SQL.Sandbox.checkout(Dbos.SandboxRepo)
      Ecto.Adapters.SQL.Sandbox.mode(Dbos.SandboxRepo, {:shared, self()})

      :ok
    end

    test "the engine works end to end under a real sandbox ownership checkout in :manual mode" do
      name = Module.concat(__MODULE__, :"SandboxEngine#{System.unique_integer([:positive])}")

      start_supervised!(
        {Dbos.Supervisor,
         name: name,
         db: {Dbos.DB.Ecto, Dbos.SandboxRepo},
         executor_id: "sandbox-exec",
         migrations: :skip,
         testing: :manual,
         workflows: @workflows,
         queues: [Queue.new("sandbox_queue")]},
        id: name
      )

      {:ok, handle} = Dbos.enqueue("add/2", [20, 22], queue_name: "sandbox_queue", engine: name)

      assert Testing.drain_queue("sandbox_queue", engine: name) == 1
      assert {:ok, 42} = Dbos.await(handle, engine: name)
    end
  end
end
