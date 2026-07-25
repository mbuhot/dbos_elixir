defmodule WidgetStore.Product do
  @moduledoc "A single storefront product and its remaining inventory."

  use Ecto.Schema

  @primary_key {:product_id, :string, autogenerate: false}
  schema "products" do
    field :name, :string
    field :inventory, :integer

    timestamps(type: :utc_datetime_usec)
  end
end
