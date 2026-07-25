defmodule HackerNewsAgent.Agent do
  @moduledoc "The research judgment calls an LLM makes: evaluating findings, deciding whether to keep researching, generating follow-up queries, and writing the final report."

  @callback evaluate(topic :: String.t(), stories :: [map], threads :: [map]) :: %{
              relevance: number,
              insights: [String.t()]
            }
  @callback should_continue?(
              topic :: String.t(),
              findings :: [map],
              iteration :: pos_integer,
              max_iterations :: pos_integer
            ) :: boolean
  @callback generate_follow_up(topic :: String.t(), findings :: [map]) :: String.t()
  @callback synthesize(topic :: String.t(), findings :: [map]) :: String.t()
end
