defmodule HackerNewsAgent.Claude do
  @moduledoc "Thin client over Anthropic's Messages API for single-turn text completions."

  @endpoint "https://api.anthropic.com/v1/messages"
  @model "claude-sonnet-5"
  @max_tokens 1024
  @anthropic_version "2023-06-01"

  @doc "Completes `system` and `user_prompt` in one turn, returning the response text."
  def complete(system, user_prompt) do
    api_key = fetch_api_key!()

    body = %{
      model: @model,
      max_tokens: @max_tokens,
      system: system,
      messages: [%{role: "user", content: user_prompt}]
    }

    @endpoint
    |> Req.post!(
      json: body,
      headers: [{"x-api-key", api_key}, {"anthropic-version", @anthropic_version}]
    )
    |> extract_text!()
  end

  defp extract_text!(%Req.Response{status: 200, body: %{"content" => content}}) do
    content
    |> Enum.filter(&(&1["type"] == "text"))
    |> Enum.map_join("", & &1["text"])
  end

  defp extract_text!(%Req.Response{status: status, body: body}) do
    raise "Anthropic API request failed (status #{status}): #{inspect(body)}"
  end

  defp fetch_api_key! do
    System.get_env("ANTHROPIC_API_KEY") ||
      raise "ANTHROPIC_API_KEY environment variable is not set. Export it before running this app."
  end
end
