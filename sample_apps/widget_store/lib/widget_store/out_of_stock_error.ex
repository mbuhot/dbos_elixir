defmodule WidgetStore.OutOfStockError do
  @moduledoc """
  Raised by the checkout's reservation step when a product cannot cover the order.

  It fails the step, so the step checkpoints a failure and records no compensation — there is
  nothing to put back. The checkout reaches `ERROR` with an empty unwind, and the engine enqueues
  no compensator at all.
  """

  defexception [:product_id, :requested, :available]

  @impl true
  def message(%__MODULE__{product_id: product_id, requested: requested, available: available}) do
    "product #{product_id} has #{available} in stock, cannot reserve #{requested}"
  end
end
