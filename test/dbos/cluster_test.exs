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

  describe "Dbos.Cluster.OrphanSweep" do
    setup %{conn: conn} do
      name = unique_engine_name()

      config =
        put_config(name, conn, %{orphan_sweep_threshold_ms: 1_000})

      start_supervised!({Cluster, name: name})

      start_supervised!(
        {Dbos.Registry, name: name, workflows: [{"add/2", {SampleWorkflows, :add, 2}}]}
      )

      start_supervised!({Registry, keys: :unique, name: WorkflowSup.process_registry_name(name)},
        id: :process_registry
      )

      start_supervised!({WorkflowSup, name: name})

      {:ok, config: config, name: name}
    end

    test "reclaims a PENDING row whose executor is absent from the roster and older than the threshold, leaving a fresh one alone",
         %{config: config, name: name} do
      stale_at = System.os_time(:millisecond) - 10_000

      SystemDb.insert_workflow_status(%{config | executor_id: "exec-stale-dead"}, %{
        workflow_id: "wf-orphan-stale",
        status: :pending,
        name: "add/2",
        inputs: [1, 2],
        updated_at: stale_at
      })

      SystemDb.insert_workflow_status(%{config | executor_id: "exec-fresh-dead"}, %{
        workflow_id: "wf-orphan-fresh",
        status: :pending,
        name: "add/2",
        inputs: [3, 4]
      })

      OrphanSweep.sweep_now(name)

      wait_until(fn ->
        {:ok, status} = SystemDb.get_workflow_status(config, "wf-orphan-stale")
        status.status == :success
      end)

      {:ok, fresh_status} = SystemDb.get_workflow_status(config, "wf-orphan-fresh")
      assert fresh_status.status == :pending
      assert fresh_status.executor_id == "exec-fresh-dead"
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
