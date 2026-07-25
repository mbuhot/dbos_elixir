defmodule Dbos.MessagingAcceptanceTest do
  use Dbos.Case, async: false

  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowHandle
  alias Dbos.WorkflowSup

  defmodule Order do
    defstruct [:id, :currency]
  end

  defp start_engine(workflows, extra_opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        workflows: workflows,
        migrations: :skip
      ] ++ notifications_opts(extra_opts) ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp notifications_opts(extra_opts) do
    if Keyword.get(extra_opts, :notifications) == :listen do
      [notifications_conn_opts: [database: "dbos_test"]]
    else
      []
    end
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

  for notifications <- [:listen, :poll] do
    describe "notifications: #{notifications}" do
      @notifications notifications

      test "1. recv blocks until another process sends, payload round-trips atoms and a nested struct" do
        engine =
          start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}],
            notifications: @notifications
          )

        {:ok, handle} = Dbos.start("receiver/2", ["topic", 5_000], engine: engine)
        config = Dbos.config(engine)

        wait_until(fn ->
          {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
          length(steps) == 1
        end)

        payload = %Order{id: "ord-1", currency: :aud}
        assert :ok = Dbos.send_message(handle.workflow_id, "topic", payload, engine: engine)

        assert {:ok, ^payload} = Dbos.await(handle, timeout_ms: 5_000)
      end

      test "2. the race: a send landing before recv starts is still delivered" do
        engine =
          start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}],
            notifications: @notifications
          )

        config = Dbos.config(engine)
        workflow_id = "wf-race-#{System.unique_integer([:positive])}"

        SystemDb.insert_workflow_status(config, %{
          workflow_id: workflow_id,
          status: :pending,
          name: "receiver/2",
          inputs: ["topic", 5_000]
        })

        assert :ok = Dbos.send_message(workflow_id, "topic", :raced, engine: engine)

        {:ok, _pid} =
          WorkflowSup.start_workflow(engine, workflow_id, {SampleWorkflows, :receiver, 2}, [
            "topic",
            5_000
          ])

        handle = %WorkflowHandle{engine: engine, workflow_id: workflow_id}
        assert {:ok, :raced} = Dbos.await(handle, timeout_ms: 5_000)
      end

      test "3. a recv timeout returns the documented no-message result, sleep id allocated regardless" do
        engine =
          start_engine([{"receiver/2", {SampleWorkflows, :receiver, 2}}],
            notifications: @notifications
          )

        {:ok, handle} = Dbos.start("receiver/2", ["topic", 100], engine: engine)

        assert {:error, %Dbos.RecvTimeoutError{}} = Dbos.await(handle, timeout_ms: 5_000)

        config = Dbos.config(engine)
        {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
        assert Enum.map(steps, & &1.function_id) == [0, 1]
        assert Enum.map(steps, & &1.function_name) == ["DBOS.recv", "DBOS.sleep"]
      end

      test "8. durable sleep waits only the remaining time after a crash mid-sleep" do
        engine =
          start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}],
            notifications: @notifications
          )

        config = Dbos.config(engine)
        started_at = System.monotonic_time(:millisecond)

        {:ok, handle} = Dbos.start("sleeper/1", [1_000], engine: engine)

        wait_until(fn ->
          {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
          length(steps) == 1
        end)

        Process.sleep(300)

        {:ok, pid} = WorkflowSup.whereis(engine, handle.workflow_id)
        Process.exit(pid, :kill)

        Process.sleep(500)

        Dbos.Recovery.recover_pending(engine)

        assert {:ok, :woke} = Dbos.await(handle, timeout_ms: 5_000)

        elapsed_ms = System.monotonic_time(:millisecond) - started_at
        assert elapsed_ms < 1_800
      end

      test "9. the exact step-id layout for recv, getEvent, setEvent, a stream write, and a plain step" do
        engine =
          start_engine(
            [
              {"add/2", {SampleWorkflows, :add, 2}},
              {"step_id_layout_workflow/1", {SampleWorkflows, :step_id_layout_workflow, 1}}
            ],
            notifications: @notifications
          )

        config = Dbos.config(engine)

        {:ok, setter_handle} = Dbos.start("add/2", [1, 2], engine: engine)
        assert {:ok, 3} = Dbos.await(setter_handle)
        SystemDb.set_event_value(config, setter_handle.workflow_id, 0, "key", :preset)

        workflow_id = "wf-layout-#{System.unique_integer([:positive])}"

        SystemDb.insert_workflow_status(config, %{
          workflow_id: workflow_id,
          status: :pending,
          name: "step_id_layout_workflow/1",
          inputs: [setter_handle.workflow_id]
        })

        assert :ok = Dbos.send_message(workflow_id, "topic", :hi, engine: engine)

        {:ok, _pid} =
          WorkflowSup.start_workflow(
            engine,
            workflow_id,
            {SampleWorkflows, :step_id_layout_workflow, 1},
            [setter_handle.workflow_id]
          )

        handle = %WorkflowHandle{engine: engine, workflow_id: workflow_id}
        assert {:ok, {:hi, :preset, :ok}} = Dbos.await(handle, timeout_ms: 5_000)

        {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)

        assert Enum.map(steps, &{&1.function_id, &1.function_name}) == [
                 {0, "DBOS.recv"},
                 {2, "DBOS.getEvent"},
                 {4, "DBOS.setEvent"},
                 {5, "DBOS.writeStream"},
                 {6, "plain_step/0"}
               ]
      end
    end
  end
end
