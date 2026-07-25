defmodule Dbos.ClusterTest do
  use Dbos.Case, async: false

  alias Dbos.Cluster
  alias Dbos.Cluster.OrphanSweep
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  defp unique_engine_name do
    Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")
  end

  defp put_config(name, conn, extra \\ %{}) do
    config =
      Map.merge(
        %Dbos.Config{
          name: name,
          db: Dbos.DB.Postgrex,
          conn: conn,
          executor_id: "exec-1"
        },
        extra
      )

    Dbos.put_config(config)
    config
  end

  describe "roster/1 with distributed Erlang disabled" do
    test "yields a single-entry roster of just this engine and does not crash", %{conn: conn} do
      name = unique_engine_name()
      config = put_config(name, conn)

      start_supervised!({Cluster, name: name})

      assert Cluster.roster(name) == MapSet.new([{node(), config.executor_id}])
    end

    test "executor_ids_for_node/2 resolves this node to its own executor id", %{conn: conn} do
      name = unique_engine_name()
      config = put_config(name, conn)

      start_supervised!({Cluster, name: name})

      assert Cluster.executor_ids_for_node(name, node()) == [config.executor_id]
    end

    test "executor_ids_for_node/2 returns an empty list for a node never seen", %{conn: conn} do
      name = unique_engine_name()
      put_config(name, conn)

      start_supervised!({Cluster, name: name})

      assert Cluster.executor_ids_for_node(name, :"never-seen@nowhere") == []
    end
  end

  describe "Dbos.Cluster.OrphanSweep, lease-based, with clustering disabled" do
    setup %{conn: conn} do
      name = unique_engine_name()
      config = put_config(name, conn)

      start_supervised!(
        {Dbos.Registry, name: name, workflows: [{"add/2", {SampleWorkflows, :add, 2}}]}
      )

      start_supervised!({Registry, keys: :unique, name: WorkflowSup.process_registry_name(name)},
        id: :process_registry
      )

      start_supervised!({WorkflowSup, name: name})

      {:ok, config: config, name: name}
    end

    test "works with no Dbos.Cluster process running at all: reclaims a PENDING row whose executor has no lease row",
         %{config: config, name: name} do
      assert Process.whereis(Cluster.process_name(name)) == nil

      SystemDb.insert_workflow_status(%{config | executor_id: "exec-no-lease"}, %{
        workflow_id: "wf-orphan-no-lease",
        status: :pending,
        name: "add/2",
        inputs: [1, 2]
      })

      OrphanSweep.sweep_now(name)

      wait_until(fn ->
        {:ok, status} = SystemDb.get_workflow_status(config, "wf-orphan-no-lease")
        status.status == :success
      end)
    end

    test "reclaims a PENDING row whose executor's lease has expired", %{
      config: config,
      name: name
    } do
      SystemDb.renew_lease(%{config | executor_id: "exec-expired"}, 60_000)
      SystemDb.expire_lease(%{config | executor_id: "exec-expired"})

      SystemDb.insert_workflow_status(%{config | executor_id: "exec-expired"}, %{
        workflow_id: "wf-orphan-expired",
        status: :pending,
        name: "add/2",
        inputs: [1, 2]
      })

      OrphanSweep.sweep_now(name)

      wait_until(fn ->
        {:ok, status} = SystemDb.get_workflow_status(config, "wf-orphan-expired")
        status.status == :success
      end)
    end

    test "a live lease makes a row immune even when it is hours stale by updated_at — the regression test for the old staleness signal",
         %{config: config, name: name} do
      hours_stale = System.os_time(:millisecond) - :timer.hours(6)

      SystemDb.renew_lease(%{config | executor_id: "exec-alive"}, 60_000)

      SystemDb.insert_workflow_status(%{config | executor_id: "exec-alive"}, %{
        workflow_id: "wf-alive-but-stale",
        status: :pending,
        name: "add/2",
        inputs: [3, 4],
        updated_at: hours_stale
      })

      OrphanSweep.sweep_now(name)
      Process.sleep(50)

      {:ok, status} = SystemDb.get_workflow_status(config, "wf-alive-but-stale")
      assert status.status == :pending
      assert status.executor_id == "exec-alive"
    end
  end

  defp wait_until(fun, attempts \\ 50)
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
