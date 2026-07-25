defmodule CustomerServiceAgent.StubLLM do
  @moduledoc """
  A deterministic, network-free `CustomerServiceAgent.LLM`: asks for a refund of whatever order
  id appears in the customer's message, then hands back the tool's result as the final reply.
  Bumps a `:persistent_term` counter per real invocation, keyed by the exact message history it
  was called with, so a test can prove a checkpointed call did not run twice after a crash.
  """

  @behaviour CustomerServiceAgent.LLM

  @impl true
  def chat(messages, _tools) do
    bump(messages)
    respond(List.last(messages))
  end

  defp respond(%{role: "user", content: content}) when is_binary(content) do
    order_id = extract_order_id(content)
    {:tool_calls, [%{id: "call-1", name: "request_refund", input: %{"order_id" => order_id}}]}
  end

  defp respond(%{role: "user", content: [%{type: "tool_result", content: result_text}]}) do
    {:text, "Here's an update on your refund: #{result_text}"}
  end

  defp extract_order_id(content) do
    [id_str] = Regex.run(~r/\d+/, content, capture: :first)
    String.to_integer(id_str)
  end

  @doc "How many times `chat/2` has actually run for the exact message history `messages`."
  def call_count(messages), do: :persistent_term.get({__MODULE__, messages}, 0)

  defp bump(messages) do
    key = {__MODULE__, messages}
    :persistent_term.put(key, :persistent_term.get(key, 0) + 1)
  end
end
