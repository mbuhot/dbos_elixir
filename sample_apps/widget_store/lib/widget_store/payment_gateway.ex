defmodule WidgetStore.PaymentGateway do
  @moduledoc """
  A stand-in for a real payment processor. `initiate/2` starts an async charge and always
  succeeds in this demo — the customer (or the `widget_store.demo` task standing in for a
  payment webhook) reports the outcome later via `Dbos.send_message/4`, decoupling "we asked
  to be paid" from "we got paid."

  `refund/1` is what the checkout's unwind calls to take a charge back. A real gateway needs the
  order id as its idempotency key, since an unwind may retry it.
  """

  require Logger

  @doc "Fire-and-forget request to charge `order_id` for `amount`; the confirmation arrives later, out of band."
  def initiate(order_id, amount) do
    Logger.info("PaymentGateway: initiating charge of #{amount} for #{order_id}")
    :ok
  end

  @doc "Refunds whatever was charged for `order_id`, keyed on the order so a retry is harmless."
  def refund(order_id) do
    Logger.info("PaymentGateway: refunding #{order_id}")
    :refunded
  end
end
