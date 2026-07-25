defmodule DocumentPipeline.Embedder do
  @moduledoc "Behaviour for turning one chunk of text into an embedding vector, kept swappable so the sample runs offline against a deterministic stub or against a real API."

  @callback embed(text :: String.t()) :: {:ok, [float()]} | {:error, term()}
end
