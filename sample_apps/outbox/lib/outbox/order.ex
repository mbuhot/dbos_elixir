defmodule Outbox.Order do
  @moduledoc "A placed order — the business record whose commit must never outrun its outbox event, or vice versa."

  use Ecto.Schema

  schema "orders" do
    field :customer, :string
    field :item, :string
    field :quantity, :integer

    timestamps(type: :utc_datetime_usec)
  end
end
