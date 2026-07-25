defmodule DocumentPipeline.Chunker do
  @moduledoc "Splits document text into fixed-size chunks for embedding."

  @default_chunk_size 500

  @doc "Splits `text` into a list of chunks of at most `chunk_size` characters each."
  def split(text, chunk_size \\ @default_chunk_size) do
    text
    |> String.graphemes()
    |> Enum.chunk_every(chunk_size)
    |> Enum.map(&Enum.join/1)
  end
end
