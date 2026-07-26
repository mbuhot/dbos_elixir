defmodule LiveApprovals.Approvals.RequestEvent do
  @moduledoc """
  One durable timeline entry for a request. Unique on `(request_id, stage)`, so announcing the
  same stage twice leaves exactly one row.
  """

  use Ecto.Schema

  alias LiveApprovals.Approvals.ExpenseRequest

  schema "request_events" do
    field :request_id, :string
    field :stage, Ecto.Enum, values: ExpenseRequest.stages()
    field :detail, :string

    timestamps(type: :utc_datetime_usec, updated_at: false)
  end
end
