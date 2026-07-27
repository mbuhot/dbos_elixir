defmodule WidgetStore.CheckoutTest do
  use ExUnit.Case, async: false

  alias WidgetStore.Order
  alias WidgetStore.Product
  alias WidgetStore.Repo

  setup do
    start_supervised!(
      {Dbos.Supervisor,
       db: {Dbos.DB.Ecto, Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [WidgetStore.Checkout],
       migrations: :verify}
    )

    Dbos.Recovery.await_boot_recovery(Dbos)

    :ok
  end

  defp seed_product(product_id, inventory) do
    %Product{product_id: product_id, name: "Widget", inventory: inventory}
    |> Repo.insert!()
  end

  defp unique_id(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"

  defp await_unwind(order_id) do
    Dbos.await(
      %Dbos.WorkflowHandle{engine: Dbos, workflow_id: Dbos.Compensation.workflow_id(order_id)},
      timeout_ms: 10_000
    )
  end

  test "surviving a crash between charging and payment confirmation completes the order exactly once" do
    product_id = unique_id("widget")
    order_id = unique_id("order")
    seed_product(product_id, 5)

    {:ok, _handle} =
      WidgetStore.Checkout.checkout(order_id, product_id, 2, workflow_id: order_id)

    wait_until(fn -> Repo.get(Order, order_id) != nil end)

    assert %Product{inventory: 3} = Repo.get(Product, product_id)
    assert %Order{status: "pending"} = Repo.get(Order, order_id)

    {:ok, pid} = Dbos.WorkflowSup.whereis(Dbos, order_id)
    Process.exit(pid, :kill)
    wait_until(fn -> not Process.alive?(pid) end)

    {:ok, status} = Dbos.SystemDb.get_workflow_status(Dbos.config(), order_id)
    assert status.status == :pending

    Dbos.Recovery.recover_pending(Dbos)
    Dbos.send_message(order_id, "payment", :paid)

    {:ok, result} = Dbos.await(%Dbos.WorkflowHandle{engine: Dbos, workflow_id: order_id})
    assert result == %{order_id: order_id, status: :dispatched}

    assert %Product{inventory: 3} = Repo.get(Product, product_id)
    assert %Order{status: "dispatched"} = Repo.get(Order, order_id)
  end

  test "a declined payment unwinds the checkout: the charge is refunded and the stock restored" do
    product_id = unique_id("widget")
    order_id = unique_id("order")
    seed_product(product_id, 5)

    {:ok, handle} =
      WidgetStore.Checkout.checkout(order_id, product_id, 2, workflow_id: order_id)

    wait_until(fn -> Repo.get(Order, order_id) != nil end)
    assert %Product{inventory: 3} = Repo.get(Product, product_id)

    Dbos.send_message(order_id, "payment", :declined)

    assert {:error, %Dbos.AbortError{reason: {:payment_not_confirmed, :declined}}} =
             Dbos.await(handle)

    assert {:ok, 2} = await_unwind(order_id)

    assert %Product{inventory: 5} = Repo.get(Product, product_id)
    assert %Order{status: "cancelled"} = Repo.get(Order, order_id)
  end

  test "each undo is a checkpointed step of the unwind, in reverse order" do
    product_id = unique_id("widget")
    order_id = unique_id("order")
    seed_product(product_id, 5)

    {:ok, handle} =
      WidgetStore.Checkout.checkout(order_id, product_id, 2, workflow_id: order_id)

    wait_until(fn -> Repo.get(Order, order_id) != nil end)
    Dbos.send_message(order_id, "payment", :declined)
    assert {:error, _reason} = Dbos.await(handle)
    assert {:ok, 2} = await_unwind(order_id)

    unwind_id = Dbos.Compensation.workflow_id(order_id)
    {:ok, steps} = Dbos.SystemDb.get_workflow_steps(Dbos.config(), unwind_id)

    assert Enum.map(steps, & &1.function_name) == [
             "refund_charge/2",
             "release_and_cancel_order/4"
           ]
  end

  test "an order that cannot be filled fails with nothing to unwind" do
    product_id = unique_id("widget")
    order_id = unique_id("order")
    seed_product(product_id, 1)

    {:ok, handle} =
      WidgetStore.Checkout.checkout(order_id, product_id, 5, workflow_id: order_id)

    assert {:error, %WidgetStore.OutOfStockError{available: 1, requested: 5}} =
             Dbos.await(handle)

    assert Dbos.SystemDb.get_workflow_status(
             Dbos.config(),
             Dbos.Compensation.workflow_id(order_id)
           ) == {:error, :not_found}

    assert %Product{inventory: 1} = Repo.get(Product, product_id)
    assert Repo.get(Order, order_id) == nil
  end

  defp wait_until(fun, attempts \\ 100)

  defp wait_until(_fun, 0), do: flunk("condition not met in time")

  defp wait_until(fun, attempts) do
    if fun.() do
      :ok
    else
      Process.sleep(20)
      wait_until(fun, attempts - 1)
    end
  end
end
