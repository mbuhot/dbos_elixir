defmodule CustomerServiceAgent.ClaudeLLM do
  @moduledoc "LLM-backed `CustomerServiceAgent.LLM`, over `CustomerServiceAgent.Claude`."

  @behaviour CustomerServiceAgent.LLM

  @impl true
  def chat(messages, tools), do: CustomerServiceAgent.Claude.messages(messages, tools)
end
