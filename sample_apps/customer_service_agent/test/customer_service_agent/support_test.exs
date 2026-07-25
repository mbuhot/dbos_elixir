defmodule CustomerServiceAgent.SupportTest do
  use ExUnit.Case, async: false

  alias CustomerServiceAgent.OrderStore
  alias CustomerServiceAgent.StubLLM
  alias CustomerServiceAgent.Support
  alias Dbos.SystemDb

  setup do
    reset_execution_counters()
    Application.put_env(:customer_service_agent, :llm, StubLLM)
    start_supervised!(OrderStore)

    engine = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: engine,
       db: {Dbos.DB.Postgrex, CustomerServiceAgent.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [Support],
       migrations: :skip,
       notifications_conn_opts: [
         hostname: System.get_env("PGHOST", "localhost"),
         database: System.get_env("PGDATABASE", "customer_service_agent_test")
       ]},
      id: engine
    )

    Dbos.Recovery.await_boot_recovery(engine)
    {:ok, engine: engine}
  end

  test "immediately refunds a low-value order and replies to the customer", %{engine: engine} do
    {:ok, handle} =
      Dbos.start("customer_request", ["cust-1", "Please refund order 101"], engine: engine)

    assert {:ok, %{customer_id: "cust-1", reply: reply}} = Dbos.await(handle, timeout_ms: 5_000)
    assert reply =~ "refunded"
    assert OrderStore.get(101).status == :refunded
  end

  test "a crash before the order lookup finishes does not repeat the LLM call that already completed",
       %{engine: engine} do
    message = "Please refund order 101"
    initial_messages = [%{role: "user", content: message}]
    workflow_id = "conv-#{System.unique_integer([:positive])}"

    {:ok, handle} =
      Dbos.start("customer_request", ["cust-1", message], workflow_id: workflow_id, engine: engine)

    config = Dbos.config(engine)

    wait_until(fn ->
      {:ok, steps} = SystemDb.get_workflow_steps(config, workflow_id)
      length(steps) >= 1
    end)

    {:ok, pid} = Dbos.WorkflowSup.whereis(engine, workflow_id)
    Process.exit(pid, :kill)
    wait_until(fn -> not Process.alive?(pid) end)

    {:ok, status} = SystemDb.get_workflow_status(config, workflow_id)
    assert status.status == :pending

    Dbos.Recovery.recover_pending(engine)

    assert {:ok, %{reply: reply}} = Dbos.await(handle, timeout_ms: 5_000)
    assert reply =~ "refunded"

    assert StubLLM.call_count(initial_messages) == 1
  end

  test "escalates a high-value refund to human approval, parks the wait, and completes exactly once on approval",
       %{engine: engine} do
    order_id = 202
    approval_id = Support.approval_workflow_id(order_id)

    {:ok, handle} =
      Dbos.start("customer_request", ["cust-2", "Please refund order #{order_id}"], engine: engine)

    config = Dbos.config(engine)

    wait_until(fn ->
      match?({:ok, %{status: :pending}}, SystemDb.get_workflow_status(config, approval_id))
    end)

    wait_until(fn -> Dbos.WorkflowSup.whereis(engine, approval_id) == :error end)

    Dbos.send_message(approval_id, "approval_decision", "approve", engine: engine)

    assert {:ok, %{reply: reply}} = Dbos.await(handle, timeout_ms: 10_000)
    assert reply =~ "refunded"

    assert OrderStore.get(order_id).status == :refunded
    assert Support.execution_count({:get_purchase, order_id}) == 1
    assert Support.execution_count({:update_purchase_status, order_id, :refunded}) == 1
  end

  defp reset_execution_counters do
    for {{module, _} = key, _value} <- :persistent_term.get(),
        module in [CustomerServiceAgent.Support, CustomerServiceAgent.StubLLM] do
      :persistent_term.erase(key)
    end
  end

  defp wait_until(fun, attempts \\ 200)

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
