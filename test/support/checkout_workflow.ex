defmodule Dbos.CheckoutWorkflow do
  @moduledoc "Fixture module exercising `use Dbos`'s macros end to end, used by `Dbos.MacrosTest`."

  use Dbos

  defworkflow process_order(order_id, amount), name: "process_order" do
    charge = charge_card(order_id, amount)
    receipt = record_receipt(order_id, charge)
    %{charge: charge, receipt: receipt}
  end

  defworkflow parent_flow(order_id), name: "parent_flow" do
    child_flow(order_id)
  end

  defworkflow parent_flow_with_opts(order_id, child_workflow_id), name: "parent_flow_with_opts" do
    child_flow(order_id, workflow_id: child_workflow_id)
  end

  defworkflow parent_flow_queueing_child(order_id), name: "parent_flow_queueing_child" do
    result = child_flow(order_id, queue_name: "children")
    charge_card(result, 1)
  end

  defworkflow child_flow(order_id), name: "child_flow" do
    order_id
  end

  defworkflow slow_flow(order_id), name: "slow_flow" do
    Dbos.sleep(5_000)
    order_id
  end

  defworkflow greet(name \\ "world"), name: "greet" do
    "hello, #{name}"
  end

  defworkflow inspects_other_workflow(order_id, target_workflow_id),
    name: "inspects_other_workflow" do
    {:ok, _enqueue_handle} = Dbos.enqueue("process_order", [order_id, 100], queue_name: "orders")
    {:ok, _fork_handle} = Dbos.fork(target_workflow_id, 0)
    Dbos.status(target_workflow_id)
  end

  defworkflow cancels_and_resumes_other_workflow(target_workflow_id),
    name: "cancels_and_resumes_other_workflow" do
    :ok = Dbos.cancel(target_workflow_id)
    :ok = Dbos.resume(target_workflow_id)
  end

  defworkflow retries_other_workflow(target_workflow_id), name: "retries_other_workflow" do
    :ok = Dbos.retry(target_workflow_id)
  end

  defworkflow uses_a_patch(order_id), name: "uses_a_patch" do
    charge = charge_card(order_id, 100)

    if Dbos.patch("fraud-check") do
      record_receipt(order_id, charge)
    end

    charge
  end

  defworkflow even_branches(order_id, outcome), name: "even_branches" do
    case outcome do
      :approve -> charge_card(order_id, 100)
      _ -> charge_card(order_id, 0)
    end
  end

  defworkflow uneven_branches(order_id, outcome), name: "uneven_branches" do
    case outcome do
      :approve ->
        charge_card(order_id, 100)
        record_receipt(order_id, %{charge_id: "x"})

      _ ->
        charge_card(order_id, 0)
    end
  end

  defstep charge_card(order_id, amount) do
    %{order_id: order_id, amount: amount, charge_id: "ch_#{order_id}"}
  end

  defstep reserve_stock(order_id), name: "custom_reserve_name" do
    order_id
  end

  deftransaction record_receipt(order_id, charge) do
    {order_id, charge.charge_id}
  end
end
