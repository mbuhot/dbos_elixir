defmodule WidgetStore.Checkout do
  @moduledoc """
  The fault-tolerant checkout workflow: reserve inventory, ask the payment gateway to charge
  the customer, then wait for a payment confirmation that arrives out of band (a webhook, or
  `mix widget_store.demo confirm` standing in for one).

  A crash at any point — before the inventory write, after it but before the charge is
  initiated, or while parked waiting on the payment confirmation — costs only time. Restarting
  the application replays every checkpointed step from its recorded output and resumes exactly
  where it left off: never a double decrement, never a double charge, never a lost refund.
  """

  use Dbos, repo: WidgetStore.Repo

  alias WidgetStore.Order
  alias WidgetStore.PaymentGateway
  alias WidgetStore.Product
  alias WidgetStore.Repo

  @payment_timeout_ms :timer.seconds(30)

  defworkflow checkout(order_id, product_id, quantity), name: "checkout" do
    case reserve_and_create_order(order_id, product_id, quantity) do
      {:error, reason} ->
        %{order_id: order_id, status: :rejected, reason: reason}

      {:ok, amount} ->
        :ok = charge_customer(order_id, amount)

        payment_outcome =
          try do
            Dbos.recv_message("payment", @payment_timeout_ms)
          rescue
            Dbos.RecvTimeoutError -> :timeout
          end

        case payment_outcome do
          :paid ->
            dispatch_order(order_id)
            %{order_id: order_id, status: :dispatched}

          _other ->
            refund_and_restore(order_id, product_id, quantity)
            %{order_id: order_id, status: :refunded}
        end
    end
  end

  @doc "Decrements `product_id`'s inventory and inserts the `PENDING` order, or rejects the order — one atomic write plus its checkpoint."
  deftransaction reserve_and_create_order(order_id, product_id, quantity) do
    case Repo.get(Product, product_id) do
      nil ->
        {:error, :product_not_found}

      %Product{inventory: inventory} when inventory < quantity ->
        {:error, :out_of_stock}

      product ->
        product
        |> Ecto.Changeset.change(inventory: product.inventory - quantity)
        |> Repo.update!()

        %Order{order_id: order_id, product_id: product_id, quantity: quantity, status: "pending"}
        |> Repo.insert!()

        {:ok, quantity}
    end
  end

  @doc "Asks the payment gateway to charge the customer, retrying on a transient failure."
  defstep charge_customer(order_id, amount), max_retries: 3, base_interval_ms: 200 do
    PaymentGateway.initiate(order_id, amount)
  end

  @doc "Marks `order_id` `DISPATCHED` once payment is confirmed."
  deftransaction dispatch_order(order_id) do
    order_id
    |> fetch_order!()
    |> Ecto.Changeset.change(status: "dispatched")
    |> Repo.update!()
  end

  @doc "Restores `product_id`'s inventory and marks `order_id` `CANCELLED` after a declined or timed-out payment."
  deftransaction refund_and_restore(order_id, product_id, quantity) do
    product_id
    |> fetch_product!()
    |> restore_inventory(quantity)

    order_id
    |> fetch_order!()
    |> Ecto.Changeset.change(status: "cancelled")
    |> Repo.update!()
  end

  defp restore_inventory(product, quantity) do
    product
    |> Ecto.Changeset.change(inventory: product.inventory + quantity)
    |> Repo.update!()
  end

  defp fetch_order!(order_id), do: Repo.get!(Order, order_id)
  defp fetch_product!(product_id), do: Repo.get!(Product, product_id)
end
