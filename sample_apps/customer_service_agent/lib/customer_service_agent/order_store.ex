defmodule CustomerServiceAgent.OrderStore do
  @moduledoc "An in-memory table of purchase orders, read and written only from inside `Dbos` steps."

  @doc false
  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  @doc "Starts the store, seeded with `opts[:orders]` (default a couple of demo orders)."
  def start_link(opts \\ []) do
    orders = Keyword.get(opts, :orders, default_orders())
    Agent.start_link(fn -> orders end, name: __MODULE__)
  end

  @doc "Looks up an order by id, or `nil` if it does not exist."
  def get(order_id), do: Agent.get(__MODULE__, &Map.get(&1, order_id))

  @doc "Sets `order_id`'s status, returning the updated order."
  def update_status(order_id, status) do
    Agent.get_and_update(__MODULE__, fn orders ->
      updated = Map.update!(orders, order_id, &%{&1 | status: status})
      {Map.fetch!(updated, order_id), updated}
    end)
  end

  defp default_orders do
    %{
      101 => %{order_id: 101, item: "Wireless Mouse", amount: 29.99, status: :paid},
      202 => %{order_id: 202, item: "4K Monitor", amount: 1299.00, status: :paid}
    }
  end
end
