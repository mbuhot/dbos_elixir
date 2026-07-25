defmodule HackerNewsAgent.HnClient do
  @moduledoc "Read-only Hacker News access this agent's steps depend on: story discovery and thread reads."

  @callback search_stories(query :: String.t()) :: [map]
  @callback read_thread(story_id :: integer) :: map
end
