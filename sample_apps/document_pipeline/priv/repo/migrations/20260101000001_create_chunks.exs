defmodule DocumentPipeline.Repo.Migrations.CreateChunks do
  use Ecto.Migration

  def change do
    create table(:chunks) do
      add :document_id, :string, null: false
      add :chunk_index, :integer, null: false
      add :text, :text, null: false
      add :embedding, {:array, :float}, null: false
      timestamps()
    end

    create unique_index(:chunks, [:document_id, :chunk_index])
  end
end
