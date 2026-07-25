defmodule CustomerServiceAgent.LLM do
  @moduledoc "A conversational turn with tools bound: either the model's final reply, or the tools it wants executed next."

  @type tool_call :: %{id: String.t(), name: String.t(), input: map}

  @callback chat(messages :: [map], tools :: [map]) :: {:text, String.t()} | {:tool_calls, [tool_call]}
end
