defmodule DocumentPipeline.Embedder.OpenAI do
  @moduledoc "Embeds chunk text via OpenAI's embeddings API. Requires the `OPENAI_API_KEY` environment variable."

  @behaviour DocumentPipeline.Embedder

  @endpoint "https://api.openai.com/v1/embeddings"
  @model "text-embedding-3-small"

  @impl true
  def embed(text) do
    api_key =
      System.get_env("OPENAI_API_KEY") ||
        raise "OPENAI_API_KEY is not set; export it or use DocumentPipeline.Embedder.Stub instead"

    response =
      Req.post!(@endpoint,
        auth: {:bearer, api_key},
        json: %{model: @model, input: text}
      )

    case response.body do
      %{"data" => [%{"embedding" => embedding} | _]} -> {:ok, embedding}
      other -> {:error, other}
    end
  end
end
