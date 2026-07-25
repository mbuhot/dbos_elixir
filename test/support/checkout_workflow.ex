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

  defworkflow child_flow(order_id), name: "child_flow" do
    order_id
  end

  defworkflow greet(name \\ "world"), name: "greet" do
    "hello, #{name}"
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
