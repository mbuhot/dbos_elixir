defmodule Dbos.LeaseSweepTest do
  use Dbos.Case, async: false

  alias Dbos.LeaseSweep
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  defp unique_engine_name do
    Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")
  end

  defp put_config(name, conn) do
    config = %Dbos.Config{
      name: name,
      db: Dbos.DB.Postgrex,
      conn: conn,
      executor_id: "exec-1"
    }

    Dbos.put_config(config)
    config
  end

  describe "reclaiming workflows from a dead executor" do
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

    test "a pending workflow whose executor never took a lease is picked up and finishes", %{
      config: config,
      name: name
    } do
      SystemDb.insert_workflow_status(%{config | executor_id: "exec-no-lease"}, %{
        workflow_id: "wf-orphan-no-lease",
        status: :pending,
        name: "add/2",
        inputs: [1, 2]
      })

      LeaseSweep.sweep_now(name)

      wait_until(fn ->
        {:ok, status} = SystemDb.get_workflow_status(config, "wf-orphan-no-lease")
        status.status == :success
      end)
    end

    test "a pending workflow whose executor's lease has expired is picked up and finishes", %{
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

      LeaseSweep.sweep_now(name)

      wait_until(fn ->
        {:ok, status} = SystemDb.get_workflow_status(config, "wf-orphan-expired")
        status.status == :success
      end)
    end

    test "a workflow untouched for hours stays with its executor while that executor's lease is live",
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

      LeaseSweep.sweep_now(name)
      Process.sleep(50)

      {:ok, status} = SystemDb.get_workflow_status(config, "wf-alive-but-stale")
      assert status.status == :pending
      assert status.executor_id == "exec-alive"
    end
  end

  defp start_engine(extra_opts) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        migrations: :skip,
        workflows: []
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  describe "engine configuration" do
    test "an engine sweeps for expired leases by default" do
      name = start_engine([])

      assert Process.whereis(LeaseSweep.process_name(name)) != nil
    end

    test "an engine started with the sweep disabled does not sweep" do
      name = start_engine(lease_sweep: [enabled: false])

      assert Process.whereis(LeaseSweep.process_name(name)) == nil
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
