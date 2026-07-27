defmodule Dbos.LeaseTest do
  use Dbos.Case, async: false

  alias Dbos.Recovery
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

  defp start_engine(extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        workflows: [{"add/2", {SampleWorkflows, :add, 2}}],
        migrations: :skip
      ]
      |> Keyword.merge(extra_opts)

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Recovery.await_boot_recovery(name)
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

  test "a lease is written at engine boot" do
    name = start_engine()
    config = Dbos.config(name)

    lease = SystemDb.get_executor_lease(config, config.executor_id)
    assert lease != nil
    assert lease.lease_expires_epoch_ms > System.os_time(:millisecond)
  end

  test "the lease publishes the workflows this executor can run and at which version" do
    name =
      start_engine(
        application_version: "v1",
        workflows: [{"add/2", {SampleWorkflows, :add, 2}, "3"}]
      )

    config = Dbos.config(name)

    lease = SystemDb.get_executor_lease(config, config.executor_id)

    assert %{name: "add/2", version: "3"} in lease.capabilities
  end

  test "a workflow declaring no version publishes a null one" do
    name = start_engine(application_version: "v1")
    config = Dbos.config(name)

    lease = SystemDb.get_executor_lease(config, config.executor_id)

    assert %{name: "add/2", version: nil} in lease.capabilities
  end

  test "the lease publishes the built-in unwind, so the fleet knows this executor can run one" do
    name = start_engine()
    config = Dbos.config(name)

    lease = SystemDb.get_executor_lease(config, config.executor_id)

    assert %{name: "DBOS.compensate", version: nil} in lease.capabilities
  end

  test "an executor that has never published its capabilities reports none rather than an empty set" do
    name = start_engine()
    config = Dbos.config(name)

    SystemDb.renew_lease(%{config | executor_id: "exec-legacy"}, 60_000)

    assert SystemDb.get_executor_lease(config, "exec-legacy").capabilities == nil
  end

  test "the lease is renewed on the configured interval" do
    name = start_engine(lease: [ttl_ms: 200, renew_interval_ms: 20])
    config = Dbos.config(name)

    first = SystemDb.get_executor_lease(config, config.executor_id)

    wait_until(fn ->
      current = SystemDb.get_executor_lease(config, config.executor_id)
      current.renewed_at_epoch_ms > first.renewed_at_epoch_ms
    end)
  end

  test "graceful shutdown expires the lease immediately" do
    name = start_engine()
    config = Dbos.config(name)

    assert SystemDb.get_executor_lease(config, config.executor_id).lease_expires_epoch_ms >
             System.os_time(:millisecond)

    :ok = stop_supervised(name)

    lease = SystemDb.get_executor_lease(config, config.executor_id)
    assert lease.lease_expires_epoch_ms <= System.os_time(:millisecond)
  end

  test "Recovery.reclaim/2 still reclaims a live-leased executor's rows, since it is an operator override" do
    name = start_engine()
    config = Dbos.config(name)

    SystemDb.renew_lease(%{config | executor_id: "exec-still-alive"}, 60_000)

    SystemDb.insert_workflow_status(%{config | executor_id: "exec-still-alive"}, %{
      workflow_id: "wf-operator-override",
      status: :pending,
      name: "add/2",
      inputs: [1, 2]
    })

    Recovery.reclaim(name, ["exec-still-alive"])

    wait_until(fn ->
      {:ok, status} = SystemDb.get_workflow_status(config, "wf-operator-override")
      status.status == :success
    end)

    {:ok, status} = SystemDb.get_workflow_status(config, "wf-operator-override")
    assert status.executor_id == config.executor_id
  end
end
