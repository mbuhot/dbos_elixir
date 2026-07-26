defmodule LiveApprovals.Approvals.ExpenseRequest do
  @moduledoc "One expense claim: its money, its current stage, and the human decision it carries."

  use Ecto.Schema

  import Ecto.Changeset

  alias LiveApprovals.Approvals.RequestEvent

  @stages [:submitted, :validating, :policy_check, :awaiting_decision, :approved, :rejected]
  @decisions [:approved, :rejected]

  @primary_key {:id, :string, autogenerate: false}
  @derive {Phoenix.Param, key: :id}

  schema "expense_requests" do
    field :title, :string
    field :amount_cents, :integer
    field :submitter, :string
    field :stage, Ecto.Enum, values: @stages, default: :submitted
    field :decision, Ecto.Enum, values: @decisions
    field :decided_by, :string

    has_many :events, RequestEvent, foreign_key: :request_id

    timestamps(type: :utc_datetime_usec)
  end

  @doc "Every stage a request can occupy, in the order the review flow reaches them."
  def stages, do: @stages

  @doc "Every decision a human approver can record."
  def decisions, do: @decisions

  @doc "Validates a newly submitted claim."
  def submission_changeset(attrs) do
    %__MODULE__{}
    |> cast(attrs, [:id, :title, :amount_cents, :submitter])
    |> validate_required([:id, :title, :amount_cents, :submitter])
    |> validate_length(:title, min: 3, max: 120)
    |> validate_number(:amount_cents, greater_than: 0)
  end

  @doc "Moves a request to `stage`."
  def stage_changeset(request, stage) do
    change(request, stage: stage)
  end

  @doc "Records the human decision `decision` made by `decided_by`."
  def decision_changeset(request, decision, decided_by) do
    request
    |> change(decision: decision, decided_by: decided_by)
    |> validate_required([:decided_by])
  end
end
