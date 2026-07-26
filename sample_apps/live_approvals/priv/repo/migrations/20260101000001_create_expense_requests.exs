defmodule LiveApprovals.Repo.Migrations.CreateExpenseRequests do
  use Ecto.Migration

  def change do
    create table(:expense_requests, primary_key: false) do
      add :id, :string, primary_key: true
      add :title, :string, null: false
      add :amount_cents, :integer, null: false
      add :submitter, :string, null: false
      add :stage, :string, null: false
      add :decision, :string
      add :decided_by, :string

      timestamps(type: :utc_datetime_usec)
    end

    create table(:request_events) do
      add :request_id, references(:expense_requests, type: :string, on_delete: :delete_all),
        null: false

      add :stage, :string, null: false
      add :detail, :string

      timestamps(type: :utc_datetime_usec, updated_at: false)
    end

    create unique_index(:request_events, [:request_id, :stage])
    create index(:expense_requests, [:stage])
  end
end
