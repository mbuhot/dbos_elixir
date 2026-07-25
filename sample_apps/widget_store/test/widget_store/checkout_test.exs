defmodule WidgetStore.CheckoutTest do
  use ExUnit.Case, async: false

  alias WidgetStore.Product
  alias WidgetStore.Repo

  setup do
    engine = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: engine,
       db: {Dbos.DB.Ecto, Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [WidgetStore.Checkout],
       migrations: :create_if_absent},
      id: engine
    )

    Dbos.Recovery.await_boot_recovery(engine)

    {:ok, engine: engine}
  end

  defp seed_product(product_id, inventory) do
    %Product{product_id: product_id, name: "Widget", inventory: inventory}
    |> Repo.insert!()
  end

  defp unique_id(prefix), do: "#{prefix}-#{System.unique_integer([:positive])}"

  test "surviving a crash between charging and payment confirmation completes the order exactly once",
       %{engine: engine} do
    product_id = unique_id("widget")
    order_id = unique_id("order")
    seed_product(product_id, 5)

    {:ok, _handle} =
      Dbos.start("checkout", [order_id, product_id, 2], workflow_id: order_id, engine: engine)

    wait_until(fn -> Repo.get(WidgetStore.Order, order_id) != nil end)

    assert %Product{inventory: 3} = Repo.get(Product, product_id)
    assert %WidgetStore.Order{status: "pending"} = Repo.get(WidgetStore.Order, order_id)

    {:ok, pid} = Dbos.WorkflowSup.whereis(engine, order_id)
    Process.exit(pid, :kill)
    wait_until(fn -> not Process.alive?(pid) end)

    {:ok, status} = Dbos.SystemDb.get_workflow_status(Dbos.config(engine), order_id)
    assert status.status == :pending

    Dbos.Recovery.recover_pending(engine)
    Dbos.send_message(order_id, "payment", :paid, engine: engine)

    {:ok, result} = Dbos.await(%Dbos.WorkflowHandle{engine: engine, workflow_id: order_id})
    assert result == %{order_id: order_id, status: :dispatched}

    assert %Product{inventory: 3} = Repo.get(Product, product_id)
    assert %WidgetStore.Order{status: "dispatched"} = Repo.get(WidgetStore.Order, order_id)
  end

  test "a declined payment refunds the customer and restores inventory", %{engine: engine} do
    product_id = unique_id("widget")
    order_id = unique_id("order")
    seed_product(product_id, 5)

    {:ok, handle} =
      Dbos.start("checkout", [order_id, product_id, 2], workflow_id: order_id, engine: engine)

    wait_until(fn -> Repo.get(WidgetStore.Order, order_id) != nil end)

    Dbos.send_message(order_id, "payment", :declined, engine: engine)

    {:ok, result} = Dbos.await(handle)
    assert result == %{order_id: order_id, status: :refunded}

    assert %Product{inventory: 5} = Repo.get(Product, product_id)
    assert %WidgetStore.Order{status: "cancelled"} = Repo.get(WidgetStore.Order, order_id)
  end

  test "checkout rejects an order with insufficient inventory, without touching stock", %{
    engine: engine
  } do
    product_id = unique_id("widget")
    order_id = unique_id("order")
    seed_product(product_id, 1)

    {:ok, handle} =
      Dbos.start("checkout", [order_id, product_id, 5], workflow_id: order_id, engine: engine)

    {:ok, result} = Dbos.await(handle)
    assert result == %{order_id: order_id, status: :rejected, reason: :out_of_stock}

    assert %Product{inventory: 1} = Repo.get(Product, product_id)
    assert Repo.get(WidgetStore.Order, order_id) == nil
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
