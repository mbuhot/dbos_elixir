defmodule Dbos.WorkflowVersionTest do
  use Dbos.Case, async: false

  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  defmodule VersionedWorkflows do
    @moduledoc false
    use Dbos

    defworkflow priced(amount), name: "versioned.priced", version: "2" do
      amount * 2
    end

    defworkflow undeclared(amount), name: "versioned.undeclared" do
      amount + 1
    end

    defworkflow parent(amount), name: "versioned.parent", version: "7" do
      priced(amount)
    end
  end

  defp start_engine(opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       Keyword.merge(
         [
           name: name,
           db: {Dbos.DB.Postgrex, Dbos.TestConn},
           executor_id: "exec-#{System.unique_integer([:positive])}",
           workflows: [VersionedWorkflows],
           lease_sweep: [enabled: false],
           migrations: :skip
         ],
         opts
       )},
      id: name
    )

    Recovery.await_boot_recovery(name)
    name
  end

  defp stamped_version(workflow_id) do
    %{rows: [[version]]} =
      Postgrex.query!(
        Dbos.TestConn,
        ~s(SELECT ex_workflow_version FROM "dbos".workflow_status WHERE workflow_uuid = $1),
        [workflow_id]
      )

    version
  end

  defp reclaim_engine(conn, executor_id, workflows) do
    name = Module.concat(__MODULE__, :"Reclaim#{System.unique_integer([:positive])}")

    config = %Dbos.Config{
      name: name,
      db: Dbos.DB.Postgrex,
      conn: conn,
      executor_id: executor_id,
      application_version: "v2"
    }

    Dbos.put_config(config)
    start_supervised!({Dbos.Registry, name: name, workflows: workflows}, id: name)

    start_supervised!({Registry, keys: :unique, name: WorkflowSup.process_registry_name(name)},
      id: {:process_registry, name}
    )

    start_supervised!({WorkflowSup, name: name}, id: {:sup, name})

    {name, config}
  end

  defp insert_pending(config, attrs) do
    SystemDb.insert_workflow_status(
      %{config | executor_id: "exec-dead", application_version: Map.fetch!(attrs, :app_version)},
      %{
        workflow_id: Map.fetch!(attrs, :workflow_id),
        status: :pending,
        name: Map.fetch!(attrs, :name),
        inputs: [1, 2],
        ex_workflow_version: Map.get(attrs, :version)
      }
    )
  end

  test "a declared version is recorded on the workflow it starts" do
    engine = start_engine([])
    Dbos.put_engine(engine)

    {:ok, handle} = VersionedWorkflows.priced(21)
    assert {:ok, 42} = Dbos.await(handle)

    assert stamped_version(handle.workflow_id) == "2"
  end

  test "a workflow declaring no version records none" do
    engine = start_engine([])
    Dbos.put_engine(engine)

    {:ok, handle} = VersionedWorkflows.undeclared(1)
    assert {:ok, 2} = Dbos.await(handle)

    assert stamped_version(handle.workflow_id) == nil
  end

  test "an enqueued workflow carries its declared version" do
    engine = start_engine(queues: [%Dbos.Queue{name: "versions"}])
    Dbos.put_engine(engine)

    {:ok, handle} = VersionedWorkflows.priced(5, queue_name: "versions")
    assert {:ok, 10} = Dbos.await(handle)

    assert stamped_version(handle.workflow_id) == "2"
  end

  test "a workflow started under one application version is reclaimed by an executor at another",
       %{conn: conn} do
    {engine, config} =
      reclaim_engine(conn, "exec-new", [{"add/2", {SampleWorkflows, :add, 2}, "2"}])

    insert_pending(config, %{
      workflow_id: "wf-across-deploys",
      name: "add/2",
      app_version: "v1",
      version: "2"
    })

    Recovery.reclaim(engine, ["exec-dead"])

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-across-deploys")
    assert status.executor_id == "exec-new"
  end

  test "a workflow whose declared version this executor does not run is left alone", %{conn: conn} do
    {engine, config} =
      reclaim_engine(conn, "exec-new", [{"add/2", {SampleWorkflows, :add, 2}, "3"}])

    insert_pending(config, %{
      workflow_id: "wf-old-body",
      name: "add/2",
      app_version: "v1",
      version: "2"
    })

    Recovery.reclaim(engine, ["exec-dead"])

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-old-body")
    assert status.executor_id == "exec-dead"
  end

  test "a workflow declaring no version is reclaimed by any executor registering its name", %{
    conn: conn
  } do
    {engine, config} = reclaim_engine(conn, "exec-new", [{"add/2", {SampleWorkflows, :add, 2}}])

    insert_pending(config, %{workflow_id: "wf-undeclared", name: "add/2", app_version: "v1"})

    Recovery.reclaim(engine, ["exec-dead"])

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-undeclared")
    assert status.executor_id == "exec-new"
  end

  test "a child workflow carries its own declared version, not its parent's" do
    engine = start_engine([])
    Dbos.put_engine(engine)

    {:ok, handle} = VersionedWorkflows.parent(4)
    assert {:ok, 8} = Dbos.await(handle)

    assert stamped_version(handle.workflow_id) == "7"
    assert stamped_version(handle.workflow_id <> "-0") == "2"
  end

  test "a fork carries the original's declared version" do
    engine = start_engine([])
    Dbos.put_engine(engine)

    {:ok, handle} = VersionedWorkflows.priced(3)
    assert {:ok, 6} = Dbos.await(handle)

    {:ok, forked} = Dbos.fork(handle.workflow_id, 0)
    assert {:ok, 6} = Dbos.await(forked)

    assert stamped_version(forked.workflow_id) == "2"
  end

  test "a version that is not a string is a compile error" do
    module = Module.concat(__MODULE__, :"BadVersion#{System.unique_integer([:positive])}")

    source = """
    defmodule #{inspect(module)} do
      use Dbos

      defworkflow bad(a), name: "bad_version", version: 2 do
        a
      end
    end
    """

    error =
      assert_raise CompileError, fn ->
        Code.compile_string(source, "test/fixture.ex")
      end

    assert error.description =~ "bad/1"
    assert error.description =~ "must be a string literal"
  end
end
