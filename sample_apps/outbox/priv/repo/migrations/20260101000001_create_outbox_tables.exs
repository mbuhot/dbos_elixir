defmodule Outbox.Repo.Migrations.CreateOutboxTables do
  use Ecto.Migration

  def change do
    create table(:orders) do
      add :customer, :string, null: false
      add :item, :string, null: false
      add :quantity, :integer, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create table(:outbox_events) do
      add :order_id, :integer, null: false
      add :event_type, :string, null: false
      add :payload, :map, null: false
      add :status, :string, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create index(:outbox_events, [:status])
  end
end
