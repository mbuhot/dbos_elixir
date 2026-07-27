defmodule WidgetStore.Checkout do
  @moduledoc """
  The fault-tolerant checkout workflow: reserve inventory, ask the payment gateway to charge
  the customer, then wait for a payment confirmation that arrives out of band (a webhook, or
  `mix widget_store.demo confirm` standing in for one).

  The happy path is the only path this body describes. Each step that changes something declares
  how to change it back, and a checkout that cannot go on calls `Dbos.abort/1` — the engine then
  runs those undos in reverse, in a durable workflow of its own, so an interrupted unwind resumes
  rather than restarting.

  A crash at any point — before the inventory write, after it but before the charge is initiated,
  or while parked waiting on the payment confirmation — costs only time. Restarting the
  application replays every checkpointed step from its recorded output and resumes exactly where
  it left off: never a double decrement, never a double charge, never a lost refund.
  """

  use Dbos, repo: WidgetStore.Repo

  alias WidgetStore.Order
  alias WidgetStore.OutOfStockError
  alias WidgetStore.PaymentGateway
  alias WidgetStore.Product
  alias WidgetStore.Repo

  @payment_timeout_ms :timer.seconds(30)

  defworkflow checkout(order_id, product_id, quantity), name: "checkout" do
    amount = reserve_and_create_order(order_id, product_id, quantity)
    charge_customer(order_id, amount)

    case await_payment() do
      :paid -> :ok
      other -> Dbos.abort({:payment_not_confirmed, other})
    end

    dispatch_order(order_id)
    %{order_id: order_id, status: :dispatched}
  end

  @doc """
  Decrements `product_id`'s inventory and inserts the `PENDING` order — one atomic write plus its
  checkpoint. Raises `WidgetStore.OutOfStockError` when there is not enough stock, which fails the
  checkout before anything has been reserved.
  """
  deftransaction reserve_and_create_order(order_id, product_id, quantity),
    compensate: &release_and_cancel_order(order_id, product_id, quantity, &1) do
    case Repo.get(Product, product_id) do
      nil ->
        raise OutOfStockError, product_id: product_id, requested: quantity, available: 0

      %Product{inventory: inventory} when inventory < quantity ->
        raise OutOfStockError,
          product_id: product_id,
          requested: quantity,
          available: inventory

      product ->
        product
        |> Ecto.Changeset.change(inventory: product.inventory - quantity)
        |> Repo.update!()

        %Order{order_id: order_id, product_id: product_id, quantity: quantity, status: "pending"}
        |> Repo.insert!()

        quantity
    end
  end

  @doc "Puts `quantity` back on the shelf and marks `order_id` `CANCELLED` — one atomic write."
  deftransaction release_and_cancel_order(order_id, product_id, quantity, _reserved) do
    product_id
    |> fetch_product!()
    |> restore_inventory(quantity)

    order_id
    |> fetch_order!()
    |> Ecto.Changeset.change(status: "cancelled")
    |> Repo.update!()
  end

  @doc "Asks the payment gateway to charge the customer, retrying on a transient failure."
  defstep charge_customer(order_id, amount),
    max_retries: 3,
    base_interval_ms: 200,
    compensate: &refund_charge(order_id, &1) do
    PaymentGateway.initiate(order_id, amount)
  end

  @doc "Refunds a charge the checkout has decided not to keep. Idempotent on `order_id`."
  defstep refund_charge(order_id, _charge) do
    PaymentGateway.refund(order_id)
  end

  @doc "Marks `order_id` `DISPATCHED` once payment is confirmed."
  deftransaction dispatch_order(order_id) do
    order_id
    |> fetch_order!()
    |> Ecto.Changeset.change(status: "dispatched")
    |> Repo.update!()
  end

  defp await_payment do
    Dbos.recv_message("payment", @payment_timeout_ms)
  rescue
    Dbos.RecvTimeoutError -> :timeout
  end

  defp restore_inventory(product, quantity) do
    product
    |> Ecto.Changeset.change(inventory: product.inventory + quantity)
    |> Repo.update!()
  end

  defp fetch_order!(order_id), do: Repo.get!(Order, order_id)
  defp fetch_product!(product_id), do: Repo.get!(Product, product_id)
end
