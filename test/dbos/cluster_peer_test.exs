defmodule Dbos.ClusterPeerTest do
  @moduledoc """
  Exercises `Dbos.Cluster`/`Dbos.Cluster.NodeWatcher` against a genuinely separate, connected
  BEAM node started via `:peer.start_link/1`, per the task's acceptance tests for the
  many-to-one roster and real `:nodedown` reclaim. Tagged `:integration` (excluded by default,
  `test/test_helper.exs`) since it starts distributed Erlang for the whole test run: run with
  `mix test --include integration test/dbos/cluster_peer_test.exs` in isolation.
  """

  use Dbos.Case, async: false

  @moduletag :integration
  @moduletag timeout: 30_000

  alias Dbos.Cluster
  alias Dbos.Cluster.NodeWatcher
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  setup_all do
    unless Node.alive?() do
      {:ok, _pid} = Node.start(:"dbos_cluster_peer_primary@127.0.0.1", :longnames)
    end

    :ok
  end

  defp start_peer! do
    pa_args =
      :code.get_path()
      |> Enum.reject(&(List.to_string(&1) == "."))
      |> Enum.flat_map(&[~c"-pa", &1])

    {:ok, peer_pid, peer_node} =
      :peer.start_link(%{
        name: :"dbos_cluster_peer_#{System.unique_integer([:positive])}",
        host: ~c"127.0.0.1",
        args: pa_args
      })

    {peer_pid, peer_node}
  end

  defp unique_engine_name(suffix) do
    Module.concat(__MODULE__, :"Engine#{suffix}#{System.unique_integer([:positive])}")
  end

  test "a peer node's departure resolves to its executor id via the roster, and triggers reclaim",
       %{conn: conn} do
    group = Module.concat(__MODULE__, :"Group#{System.unique_integer([:positive])}")
    local_engine = unique_engine_name("Local")
    peer_engine = unique_engine_name("Peer")
    peer_executor_id = "exec-on-peer-#{System.unique_integer([:positive])}"

    local_config = %Dbos.Config{
      name: local_engine,
      db: Dbos.DB.Postgrex,
      conn: conn,
      executor_id: "exec-local",
      cluster_group: group,
      reclaim_batch_size: 50
    }

    Dbos.put_config(local_config)

    {peer_pid, peer_node} = start_peer!()
    on_exit(fn -> if Process.alive?(peer_pid), do: :peer.stop(peer_pid) end)

    peer_config = %{local_config | name: peer_engine, executor_id: peer_executor_id, conn: nil}
    :ok = :erpc.call(peer_node, Dbos, :put_config, [peer_config])

    {:ok, _peer_cluster_pid} =
      :erpc.call(peer_node, GenServer, :start, [
        Cluster,
        peer_engine,
        [name: Cluster.process_name(peer_engine)]
      ])

    start_supervised!({Cluster, name: local_engine})
    start_supervised!({NodeWatcher, name: local_engine})

    start_supervised!(
      {Dbos.Registry, name: local_engine, workflows: [{"add/2", {SampleWorkflows, :add, 2}}]}
    )

    start_supervised!(
      {Registry, keys: :unique, name: WorkflowSup.process_registry_name(local_engine)},
      id: :process_registry
    )

    start_supervised!({WorkflowSup, name: local_engine})

    wait_until(fn ->
      peer_executor_id in Cluster.executor_ids_for_node(local_engine, peer_node)
    end)

    SystemDb.insert_workflow_status(%{local_config | executor_id: peer_executor_id}, %{
      workflow_id: "wf-peer-died",
      status: :pending,
      name: "add/2",
      inputs: [10, 20]
    })

    :peer.stop(peer_pid)

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(local_config, "wf-peer-died")
      status.status == :success
    end)

    {:ok, status} = SystemDb.get_workflow_status(local_config, "wf-peer-died")
    assert status.output == 30
    assert status.executor_id == "exec-local"
  end

  defp wait_until(fun, attempts \\ 100)
  defp wait_until(_fun, 0), do: flunk("condition never became true")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(50)
      wait_until(fun, attempts - 1)
    end
  end
end
