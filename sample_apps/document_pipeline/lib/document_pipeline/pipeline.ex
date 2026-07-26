defmodule DocumentPipeline.Pipeline do
  @moduledoc """
  Ingests one document: fetch its text, split it into chunks, embed each chunk, store the
  result. Embedding and storing a chunk are each their own checkpoint (`embed_chunk/3`,
  `store_chunk/4`), so recovering a document that crashed partway through re-runs only the
  chunks that had not yet finished — the expensive, rate-limited embed call never repeats for a
  chunk already recorded.
  """

  use Dbos, repo: DocumentPipeline.Repo

  alias DocumentPipeline.Chunk
  alias DocumentPipeline.Chunker
  alias DocumentPipeline.Repo

  @queue_name "documents"

  @doc "The queue name this app's documents are enqueued onto."
  def queue_name, do: @queue_name

  @doc """
  Enqueues one `ingest_document` workflow per `{document_id, source}` pair onto the documents
  queue, returning immediately — concurrency across the batch comes from the queue's
  `worker_concurrency`, not from this call blocking on any one document.
  """
  def ingest_batch(documents) do
    Enum.map(documents, fn {document_id, source} ->
      {:ok, handle} =
        ingest_document(document_id, source,
          queue_name: @queue_name,
          workflow_id: document_id
        )

      handle
    end)
  end

  defworkflow ingest_document(document_id, source), name: "ingest_document" do
    text = fetch_document(source)
    chunks = chunk(text)

    chunks
    |> Enum.with_index()
    |> Enum.map(fn {chunk_text, chunk_index} ->
      embedding = embed_chunk(document_id, chunk_index, chunk_text)
      store_chunk(document_id, chunk_index, chunk_text, embedding)
    end)
  end

  @doc "Fetches the document's raw text: `{:text, string}` for inline content, `{:url, url}` to fetch it over HTTP."
  defstep fetch_document(source) do
    case source do
      {:text, text} -> text
      {:url, url} -> Req.get!(url).body
    end
  end

  @doc "Embeds one chunk of text. The expensive, rate-limited operation this whole sample exists to protect from re-running."
  defstep embed_chunk(_document_id, _chunk_index, chunk_text) do
    Process.sleep(embed_delay_ms())
    {:ok, embedding} = embedder().embed(chunk_text)
    embedding
  end

  @doc "Upserts one chunk's text and embedding, keyed by `(document_id, chunk_index)`."
  deftransaction store_chunk(document_id, chunk_index, chunk_text, embedding) do
    %Chunk{}
    |> Chunk.changeset(%{
      document_id: document_id,
      chunk_index: chunk_index,
      text: chunk_text,
      embedding: embedding
    })
    |> Repo.insert!(
      on_conflict: {:replace, [:text, :embedding, :updated_at]},
      conflict_target: [:document_id, :chunk_index]
    )
  end

  defp embedder,
    do: Application.get_env(:document_pipeline, :embedder, DocumentPipeline.Embedder.Stub)

  defp embed_delay_ms, do: Application.get_env(:document_pipeline, :embed_delay_ms, 0)

  defp chunk(text), do: Chunker.split(text)
end
