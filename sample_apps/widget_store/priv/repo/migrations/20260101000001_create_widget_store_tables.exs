defmodule WidgetStore.Repo.Migrations.CreateWidgetStoreTables do
  use Ecto.Migration

  def change do
    create table(:products, primary_key: false) do
      add :product_id, :string, primary_key: true
      add :name, :string, null: false
      add :inventory, :integer, null: false

      timestamps(type: :utc_datetime_usec)
    end

    create table(:orders, primary_key: false) do
      add :order_id, :string, primary_key: true
      add :product_id, :string, null: false
      add :quantity, :integer, null: false
      add :status, :string, null: false

      timestamps(type: :utc_datetime_usec)
    end
  end
end
