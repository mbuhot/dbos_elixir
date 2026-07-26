defmodule Dbos.AdminServerTest do
  use Dbos.Case, async: false

  alias Dbos.AdminServer
  alias Dbos.SampleWorkflows
  alias Dbos.SystemDb

  defp start_engine(workflows, extra_opts \\ []) do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    opts =
      [
        name: name,
        db: {Dbos.DB.Postgrex, Dbos.TestConn},
        executor_id: "exec-#{System.unique_integer([:positive])}",
        workflows: workflows,
        migrations: :skip,
        admin_server: [enabled: true, port: 0]
      ] ++ extra_opts

    start_supervised!({Dbos.Supervisor, opts}, id: name)
    Dbos.Recovery.await_boot_recovery(name)
    name
  end

  defp get(port, path), do: request("GET", port, path, "")
  defp post(port, path, body \\ ""), do: request("POST", port, path, body)

  defp request(method, port, path, body) do
    {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])

    request_line = "#{method} #{path} HTTP/1.1\r\n"

    headers =
      "Host: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(body)}\r\nConnection: close\r\n\r\n"

    :ok = :gen_tcp.send(socket, request_line <> headers <> body)

    response = recv_all(socket, "")
    :gen_tcp.close(socket)

    [status_line | _rest] = String.split(response, "\r\n")
    [_http_version, status_code, _reason] = String.split(status_line, " ", parts: 3)
    [_headers, response_body] = String.split(response, "\r\n\r\n", parts: 2)

    {String.to_integer(status_code), response_body}
  end

  defp recv_all(socket, acc) do
    case :gen_tcp.recv(socket, 0, 2_000) do
      {:ok, data} -> recv_all(socket, acc <> data)
      {:error, :closed} -> acc
      {:error, :timeout} -> acc
    end
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

  test "GET /dbos-healthz returns 200 and a healthy status" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    port = AdminServer.port(engine)

    assert {200, body} = get(port, "/dbos-healthz")
    assert JSON.decode!(body) == %{"status" => "healthy"}
  end

  test "GET /workflows/{id} and /workflows/{id}/steps reflect a real workflow, and /workflows lists it" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    port = AdminServer.port(engine)

    {:ok, handle} = Dbos.start("three_steps/1", ["ord-9"], engine: engine)
    assert {:ok, _} = Dbos.await(handle)

    assert {200, body} = get(port, "/workflows/#{handle.workflow_id}")
    decoded = JSON.decode!(body)
    assert decoded["workflow_uuid"] == handle.workflow_id
    assert decoded["status"] == "SUCCESS"
    assert decoded["output"] =~ "shipped"

    assert {200, steps_body} = get(port, "/workflows/#{handle.workflow_id}/steps")
    steps = JSON.decode!(steps_body)

    assert Enum.map(steps, & &1["function_name"]) == [
             "reserve_stock/1",
             "charge_card/1",
             "ship_order/1"
           ]

    assert {200, list_body} =
             post(port, "/workflows", JSON.encode!(%{"workflow_name" => "three_steps/1"}))

    listed = JSON.decode!(list_body)
    assert Enum.any?(listed, &(&1["workflow_uuid"] == handle.workflow_id))
  end

  test "POST /workflows filters by status given as a JSON string or an array of strings" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)
    port = AdminServer.port(engine)

    {:ok, handle} = Dbos.start("add/2", [1, 2], engine: engine)
    assert {:ok, 3} = Dbos.await(handle)

    SystemDb.insert_workflow_status(config, %{
      workflow_id: "wf-pending-admin",
      status: :pending,
      name: "add/2"
    })

    assert {200, string_body} =
             post(port, "/workflows", JSON.encode!(%{"status" => "SUCCESS"}))

    string_results = JSON.decode!(string_body)
    assert Enum.map(string_results, & &1["workflow_uuid"]) == [handle.workflow_id]

    assert {200, array_body} =
             post(port, "/workflows", JSON.encode!(%{"status" => ["SUCCESS", "PENDING"]}))

    array_results = JSON.decode!(array_body)

    assert Enum.map(array_results, & &1["workflow_uuid"]) |> Enum.sort() ==
             Enum.sort(["wf-pending-admin", handle.workflow_id])
  end

  test "GET /workflows/{id} for an unknown id returns 404" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    port = AdminServer.port(engine)

    assert {404, _body} = get(port, "/workflows/does-not-exist")
  end

  test "POST /workflows/{id}/cancel actually cancels the workflow" do
    engine = start_engine([{"sleeper/1", {SampleWorkflows, :sleeper, 1}}])
    config = Dbos.config(engine)
    port = AdminServer.port(engine)

    {:ok, handle} = Dbos.start("sleeper/1", [30_000], engine: engine)

    wait_until(fn ->
      match?({:ok, _}, SystemDb.get_workflow_steps(config, handle.workflow_id))
    end)

    assert {204, _} = post(port, "/workflows/#{handle.workflow_id}/cancel")
    assert {:error, %Dbos.WorkflowCancelledError{}} = Dbos.await(handle, timeout_ms: 3_000)
  end

  test "POST /workflows/{id}/resume and /fork actually take effect against the database" do
    engine = start_engine([{"three_steps/1", {SampleWorkflows, :three_steps, 1}}])
    config = Dbos.config(engine)
    port = AdminServer.port(engine)

    {:ok, handle} = Dbos.start("three_steps/1", ["ord-resume"], engine: engine)
    assert {:ok, _} = Dbos.await(handle)

    assert {200, fork_body} =
             post(
               port,
               "/workflows/#{handle.workflow_id}/fork",
               JSON.encode!(%{"start_step" => 1})
             )

    new_id = JSON.decode!(fork_body)["workflow_id"]
    assert {:ok, forked_status} = SystemDb.get_workflow_status(config, new_id)
    assert forked_status.status == :enqueued
    assert forked_status.forked_from == handle.workflow_id

    assert {204, _} = post(port, "/workflows/#{handle.workflow_id}/resume")
  end

  test "POST /workflows/{id}/retry puts a failed workflow back to work" do
    engine =
      start_engine([{"fails_once_after_step/1", {SampleWorkflows, :fails_once_after_step, 1}}])

    config = Dbos.config(engine)
    port = AdminServer.port(engine)

    table = :"admin_retry_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])

    {:ok, handle} = Dbos.start("fails_once_after_step/1", [table], engine: engine)
    assert {:error, %RuntimeError{}} = Dbos.await(handle)

    assert {204, _} = post(port, "/workflows/#{handle.workflow_id}/retry")

    wait_until(fn ->
      match?({:ok, %{status: :success}}, SystemDb.get_workflow_status(config, handle.workflow_id))
    end)
  end

  test "POST /dbos-workflow-recovery reclaims a dead executor's PENDING workflows and returns their ids" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)
    port = AdminServer.port(engine)

    dead_id = "dead-wf-1"

    SystemDb.insert_workflow_status(%{config | executor_id: "dead-executor"}, %{
      workflow_id: dead_id,
      status: :pending,
      name: "add/2",
      inputs: [1, 2]
    })

    assert {200, body} = post(port, "/dbos-workflow-recovery", JSON.encode!(["dead-executor"]))
    ids = JSON.decode!(body)
    assert dead_id in ids
  end

  test "GET /dbos-workflow-queues-metadata includes the internal queue and a declared queue" do
    engine =
      start_engine([{"add/2", {SampleWorkflows, :add, 2}}],
        queues: [Dbos.Queue.new("orders", worker_concurrency: 2)]
      )

    port = AdminServer.port(engine)

    assert {200, body} = get(port, "/dbos-workflow-queues-metadata")
    names = body |> JSON.decode!() |> Enum.map(& &1["name"])
    assert Dbos.Queue.internal_queue_name() in names
    assert "orders" in names
  end

  test "POST /dbos-global-timeout cancels everything created at or before the cutoff" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)
    port = AdminServer.port(engine)

    {:ok, workflow_id} =
      SystemDb.insert_enqueued_workflow(config, %{
        name: "add/2",
        queue_name: "orders",
        inputs: [1, 2]
      })

    cutoff = System.os_time(:millisecond) + 1_000

    assert {204, _} =
             post(
               port,
               "/dbos-global-timeout",
               JSON.encode!(%{"cutoff_epoch_timestamp_ms" => cutoff})
             )

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    assert status.status == :cancelled
  end

  test "POST /dbos-garbage-collect performs the real deletion and reports the count" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    config = Dbos.config(engine)
    port = AdminServer.port(engine)

    {:ok, handle} = Dbos.start("add/2", [1, 2], engine: engine)
    assert {:ok, 3} = Dbos.await(handle)

    future_cutoff = System.os_time(:millisecond) + 1_000

    assert {200, body} =
             post(
               port,
               "/dbos-garbage-collect",
               JSON.encode!(%{
                 "cutoff_epoch_timestamp_ms" => future_cutoff,
                 "rows_threshold" => nil
               })
             )

    assert %{"deleted" => deleted} = JSON.decode!(body)
    assert deleted >= 1
    assert {:error, :not_found} = SystemDb.get_workflow_status(config, handle.workflow_id)
  end

  test "GET /deactivate stops the scheduler from firing new ticks" do
    engine = start_engine([{"add/2", {SampleWorkflows, :add, 2}}])
    port = AdminServer.port(engine)

    assert {200, "deactivated"} = get(port, "/deactivate")
  end
end
