defmodule Dbos.OrphansTest do
  use Dbos.Case, async: false

  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

  setup do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts = [
      name: name,
      db: {Dbos.DB.Postgrex, Dbos.TestConn},
      executor_id: "exec-live-#{System.unique_integer([:positive])}",
      application_version: "v1",
      workflows: [{"add/2", {SampleWorkflows, :add, 2}, "2"}],
      lease_sweep: [enabled: false],
      migrations: :skip
    ]

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Recovery.await_boot_recovery(name)

    {:ok, engine: name, config: Dbos.config(name)}
  end

  defp insert_pending(config, workflow_id, attrs) do
    config
    |> struct!(Map.take(attrs, [:executor_id, :application_version]))
    |> SystemDb.insert_workflow_status(%{
      workflow_id: workflow_id,
      status: :pending,
      name: attrs.name,
      inputs: [1, 2],
      ex_workflow_version: Map.get(attrs, :workflow_version)
    })
  end

  defp lease(config, executor_id, opts) do
    SystemDb.renew_lease(%{config | executor_id: executor_id}, Keyword.fetch!(opts, :ttl_ms),
      capabilities: Keyword.get(opts, :capabilities)
    )
  end

  defp group(orphans, name), do: Enum.find(orphans, &(&1.name == name))

  defp task_output(engine) do
    Mix.Tasks.Dbos.Orphans.run(["--engine", inspect(engine)])
    assert_received {:mix_shell, :info, [output]}
    output
  end

  test "a workflow whose name nothing deployed implements is reported as orphaned", %{
    engine: engine,
    config: config
  } do
    insert_pending(config, "wf-unknown-name", %{
      executor_id: "exec-gone",
      application_version: "v1",
      name: "retired_workflow/1"
    })

    orphan = group(Recovery.orphans(engine), "retired_workflow/1")

    assert orphan.reason == :name_not_registered
    assert orphan.count == 1
    assert orphan.application_version == "v1"
    assert orphan.example_workflow_id == "wf-unknown-name"
  end

  test "a workflow at a version nothing deployed runs is reported as orphaned", %{
    engine: engine,
    config: config
  } do
    insert_pending(config, "wf-old-version", %{
      executor_id: "exec-gone",
      application_version: "v0",
      name: "add/2",
      workflow_version: "1"
    })

    orphan = group(Recovery.orphans(engine), "add/2")

    assert orphan.reason == :version_mismatch
    assert orphan.count == 1
    assert orphan.workflow_version == "1"
    assert orphan.application_version == "v0"
    assert orphan.example_workflow_id == "wf-old-version"
  end

  test "a workflow a live executor can claim is not reported", %{
    engine: engine,
    config: config
  } do
    insert_pending(config, "wf-claimable", %{
      executor_id: "exec-gone",
      application_version: "v1",
      name: "add/2",
      workflow_version: "2"
    })

    assert group(Recovery.orphans(engine), "add/2") == nil
  end

  test "a workflow held by an executor whose lease is live is not reported", %{
    engine: engine,
    config: config
  } do
    lease(config, "exec-busy", ttl_ms: 60_000, capabilities: [{"something_else/0", nil}])

    insert_pending(config, "wf-running", %{
      executor_id: "exec-busy",
      application_version: "v0",
      name: "add/2",
      workflow_version: "1"
    })

    assert group(Recovery.orphans(engine), "add/2") == nil
  end

  test "a workflow is not reported while an executor that has not published its capabilities is live",
       %{engine: engine, config: config} do
    lease(config, "exec-upgrading", ttl_ms: 60_000, capabilities: nil)

    insert_pending(config, "wf-during-upgrade", %{
      executor_id: "exec-gone",
      application_version: "v0",
      name: "add/2",
      workflow_version: "1"
    })

    assert group(Recovery.orphans(engine), "add/2") == nil
  end

  test "an expired peer's capabilities no longer keep a workflow off the report", %{
    engine: engine,
    config: config
  } do
    lease(config, "exec-expired", ttl_ms: -1, capabilities: [{"add/2", "1"}])

    insert_pending(config, "wf-expired-peer", %{
      executor_id: "exec-gone",
      application_version: "v0",
      name: "add/2",
      workflow_version: "1"
    })

    assert group(Recovery.orphans(engine), "add/2").reason == :version_mismatch
  end

  test "several workflows sharing a name and version are reported as one group with a count", %{
    engine: engine,
    config: config
  } do
    for index <- 1..3 do
      insert_pending(config, "wf-group-#{index}", %{
        executor_id: "exec-gone",
        application_version: "v0",
        name: "add/2",
        workflow_version: "1"
      })
    end

    orphan = group(Recovery.orphans(engine), "add/2")

    assert orphan.count == 3
    assert orphan.oldest_created_at_epoch_ms <= System.os_time(:millisecond)
  end

  test "an orphaned group emits one telemetry event carrying its count and reason", %{
    engine: engine,
    config: config
  } do
    handler_id = "orphans-test-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach(
      handler_id,
      [:dbos, :recovery, :orphaned],
      fn _event, measurements, metadata, _cfg ->
        send(test_pid, {:orphaned, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    insert_pending(config, "wf-telemetry", %{
      executor_id: "exec-gone",
      application_version: "v0",
      name: "add/2",
      workflow_version: "1"
    })

    Recovery.orphans(engine)

    assert_receive {:orphaned, %{count: 1}, metadata}
    assert metadata.name == "add/2"
    assert metadata.row_version == "1"
    assert metadata.reason == :version_mismatch
  end

  test "every pending workflow is orphaned when the whole fleet is down", %{
    engine: engine,
    config: config
  } do
    insert_pending(config, "wf-fleet-down", %{
      executor_id: "exec-gone",
      application_version: "v1",
      name: "add/2",
      workflow_version: "2"
    })

    SystemDb.expire_lease(config)

    assert group(Recovery.orphans(engine), "add/2").reason == :no_live_executors
  end

  describe "mix dbos.orphans" do
    setup do
      shell = Mix.shell()
      Mix.shell(Mix.Shell.Process)
      on_exit(fn -> Mix.shell(shell) end)
      :ok
    end

    test "says so when nothing is orphaned", %{engine: engine} do
      assert task_output(engine) =~ "No orphaned PENDING workflows"
    end

    test "tabulates each orphaned group with its count, version and reason", %{
      engine: engine,
      config: config
    } do
      insert_pending(config, "wf-task", %{
        executor_id: "exec-gone",
        application_version: "v0",
        name: "add/2",
        workflow_version: "1"
      })

      output = task_output(engine)

      assert output =~ "count"
      assert output =~ "add/2"
      assert output =~ "v0"
      assert output =~ "version_mismatch"
      assert output =~ "wf-task"
    end
  end

  test "a queued workflow waiting for its queue is not reported", %{engine: engine} do
    Postgrex.query!(
      Dbos.TestConn,
      """
      INSERT INTO dbos.workflow_status
        (workflow_uuid, status, name, executor_id, application_version, queue_name)
      VALUES ($1, 'PENDING', 'add/2', 'exec-gone', 'v0', 'default')
      """,
      ["wf-queued"]
    )

    assert group(Recovery.orphans(engine), "add/2") == nil
  end
end
