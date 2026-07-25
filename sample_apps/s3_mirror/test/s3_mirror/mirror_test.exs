defmodule S3Mirror.MirrorTest do
  use ExUnit.Case, async: false

  alias Dbos.SystemDb
  alias S3Mirror.ObjectStore.Local
  alias S3Mirror.Workflows

  @tables ~w(
    workflow_status operation_outputs notifications workflow_events
    workflow_events_history streams event_dispatch_kv application_versions
    workflow_schedules queues
  )

  setup do
    truncate_dbos_tables()

    source = %{root: tmp_dir("source")}
    dest = %{root: tmp_dir("dest")}

    for i <- 1..5 do
      File.write!(Path.join(source.root, "object-#{i}.txt"), "content-#{i}")
    end

    start_supervised!(
      {Dbos.Supervisor,
       db: {Dbos.DB.Postgrex, S3Mirror.TestConn},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Workflows],
       queues: [Dbos.Queue.new(Workflows.queue_name(), worker_concurrency: 5)],
       migrations: :skip}
    )

    Dbos.Recovery.await_boot_recovery(Dbos)

    {:ok, source: source, dest: dest}
  end

  test "interrupting the mirror mid-run and resuming copies every object exactly once", %{
    source: source,
    dest: dest
  } do
    {:ok, _handle} =
      Workflows.mirror_bucket(Local, source, Local, dest, "", workflow_id: "mirror-run-1")

    wait_until(fn -> length(File.ls!(dest.root)) >= 2 end)

    kill_every_workflow_process()

    {:ok, status} = SystemDb.get_workflow_status(Dbos.config(), "mirror-run-1")
    refute status.status in [:success, :error]

    Dbos.Recovery.recover_pending(Dbos)

    handle = %Dbos.WorkflowHandle{engine: Dbos, workflow_id: "mirror-run-1"}
    assert {:ok, %{copied: 5, skipped: 0}} = Dbos.await(handle, timeout_ms: 10_000)

    {:ok, keys} = Local.list_keys(source, "")
    assert length(keys) == 5

    for key <- keys do
      assert {:ok, content} = Local.read(source, key)
      assert {:ok, ^content} = Local.read(dest, key)

      workflow_id = Workflows.workflow_id(Local, dest, key)
      {:ok, steps} = SystemDb.get_workflow_steps(Dbos.config(), workflow_id)
      write_steps = Enum.filter(steps, &(&1.function_name == "copy_into_dest/4"))
      assert length(write_steps) == 1
    end
  end

  test "objects already present at the destination are reported as skipped, not re-copied", %{
    source: source,
    dest: dest
  } do
    {:ok, keys} = Local.list_keys(source, "")

    for key <- keys do
      {:ok, content} = Local.read(source, key)
      :ok = Local.write(dest, key, content)
    end

    {:ok, handle} =
      Workflows.mirror_bucket(Local, source, Local, dest, "", workflow_id: "mirror-run-2")

    assert {:ok, %{copied: 0, skipped: 5}} = Dbos.await(handle)
  end

  defp kill_every_workflow_process do
    Dbos
    |> Dbos.WorkflowSup.process_name()
    |> DynamicSupervisor.which_children()
    |> Enum.each(fn {_id, pid, _type, _modules} -> Process.exit(pid, :kill) end)

    Process.sleep(50)
  end

  defp tmp_dir(name) do
    path =
      Path.join(System.tmp_dir!(), "s3_mirror_test_#{name}_#{System.unique_integer([:positive])}")

    File.mkdir_p!(path)
    path
  end

  defp truncate_dbos_tables do
    tables = Enum.map_join(@tables, ", ", &"dbos.#{&1}")
    Postgrex.query!(S3Mirror.TestConn, "TRUNCATE TABLE #{tables} CASCADE", [])
  end

  defp wait_until(fun, attempts \\ 100)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end
end
