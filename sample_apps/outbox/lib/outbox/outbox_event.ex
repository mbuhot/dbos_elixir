defmodule Outbox.OutboxEvent do
  @moduledoc """
  A pending or sent side effect, written in the same transaction as the business record it
  describes. `status` moves `"pending"` -> `"sent"` only after `Outbox.ExternalSystem.publish/2`
  actually succeeds — a row can be published more than once (a crash between publish and the
  status write retries it), but it is never lost.
  """

  use Ecto.Schema

  schema "outbox_events" do
    field :order_id, :integer
    field :event_type, :string
    field :payload, :map
    field :status, :string

    timestamps(type: :utc_datetime_usec)
  end
end
