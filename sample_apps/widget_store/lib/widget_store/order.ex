defmodule WidgetStore.Order do
  @moduledoc "A checkout attempt and where it currently stands: pending, paid, dispatched, cancelled."

  use Ecto.Schema

  @primary_key {:order_id, :string, autogenerate: false}
  schema "orders" do
    field(:product_id, :string)
    field(:quantity, :integer)
    field(:status, :string)

    timestamps(type: :utc_datetime_usec)
  end
end
