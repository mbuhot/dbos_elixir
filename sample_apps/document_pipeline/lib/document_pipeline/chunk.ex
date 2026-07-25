defmodule DocumentPipeline.Chunk do
  @moduledoc """
  One embedded chunk of a document. `embedding` is a plain Postgres float array — this sample
  has no need for a vector column (`pgvector`); a nearest-neighbour search over these rows would
  want one, but ingestion and storage do not.
  """

  use Ecto.Schema
  import Ecto.Changeset

  schema "chunks" do
    field :document_id, :string
    field :chunk_index, :integer
    field :text, :string
    field :embedding, {:array, :float}
    timestamps()
  end

  @doc "Builds a changeset for inserting or upserting one chunk row."
  def changeset(chunk, attrs) do
    chunk
    |> cast(attrs, [:document_id, :chunk_index, :text, :embedding])
    |> validate_required([:document_id, :chunk_index, :text, :embedding])
  end
end
