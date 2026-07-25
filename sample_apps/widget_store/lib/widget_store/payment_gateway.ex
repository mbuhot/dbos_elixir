defmodule WidgetStore.PaymentGateway do
  @moduledoc """
  A stand-in for a real payment processor. `initiate/2` starts an async charge and always
  succeeds in this demo — the customer (or the `widget_store.demo` task standing in for a
  payment webhook) reports the outcome later via `Dbos.send_message/4`, decoupling "we asked
  to be paid" from "we got paid."
  """

  require Logger

  @doc "Fire-and-forget request to charge `order_id` for `amount`; the confirmation arrives later, out of band."
  def initiate(order_id, amount) do
    Logger.info("PaymentGateway: initiating charge of #{amount} for #{order_id}")
    :ok
  end
end
