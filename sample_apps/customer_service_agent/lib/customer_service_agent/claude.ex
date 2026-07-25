defmodule CustomerServiceAgent.Claude do
  @moduledoc "Thin client over Anthropic's Messages API with tool use."

  @endpoint "https://api.anthropic.com/v1/messages"
  @model "claude-sonnet-5"
  @max_tokens 1024
  @anthropic_version "2023-06-01"
  @system """
  You are a customer service agent. Use the available tools to look up purchase orders and
  process refunds. Refunds over $1000 require human approval and may take a while to complete;
  tell the customer that plainly when it applies.
  """

  @doc "Sends `messages` and the bound `tools` for one conversational turn."
  def messages(messages, tools) do
    api_key = fetch_api_key!()

    body = %{
      model: @model,
      max_tokens: @max_tokens,
      system: @system,
      messages: messages,
      tools: tools
    }

    @endpoint
    |> Req.post!(
      json: body,
      headers: [{"x-api-key", api_key}, {"anthropic-version", @anthropic_version}]
    )
    |> parse_response!()
  end

  defp parse_response!(%Req.Response{status: 200, body: %{"content" => content}}) do
    case Enum.filter(content, &(&1["type"] == "tool_use")) do
      [] ->
        text = content |> Enum.filter(&(&1["type"] == "text")) |> Enum.map_join("", & &1["text"])
        {:text, text}

      tool_uses ->
        {:tool_calls,
         Enum.map(tool_uses, fn tu -> %{id: tu["id"], name: tu["name"], input: tu["input"]} end)}
    end
  end

  defp parse_response!(%Req.Response{status: status, body: body}) do
    raise "Anthropic API request failed (status #{status}): #{inspect(body)}"
  end

  defp fetch_api_key! do
    System.get_env("ANTHROPIC_API_KEY") ||
      raise "ANTHROPIC_API_KEY environment variable is not set. Export it before running this app."
  end
end
