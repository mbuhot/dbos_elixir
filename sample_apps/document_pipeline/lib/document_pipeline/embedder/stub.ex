defmodule DocumentPipeline.Embedder.Stub do
  @moduledoc """
  Deterministic offline embedder: hashes the chunk text into a fixed-size float vector, so the
  sample runs without any API key or network access. Emits a `[:document_pipeline, :embed]`
  telemetry event on every real call, so a test can count exactly how many chunks were actually
  (re-)embedded — the number that matters for proving recovery does not redo already-checkpointed
  work.
  """

  @behaviour DocumentPipeline.Embedder

  @dimensions 8

  @impl true
  def embed(text) do
    :telemetry.execute([:document_pipeline, :embed], %{count: 1}, %{text: text})

    embedding =
      text
      |> :erlang.md5()
      |> :binary.bin_to_list()
      |> Enum.take(@dimensions)
      |> Enum.map(&(&1 / 255))

    {:ok, embedding}
  end
end
