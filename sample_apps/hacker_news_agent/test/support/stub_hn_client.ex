defmodule HackerNewsAgent.StubHnClient do
  @moduledoc "Deterministic, network-free `HackerNewsAgent.HnClient` for tests. Bumps a `:persistent_term` counter per call so a test can prove a checkpointed call did not re-execute after a simulated crash."

  @behaviour HackerNewsAgent.HnClient

  @impl true
  def search_stories(query) do
    bump({:search_stories, query})

    [
      %{id: 1, title: "#{query}: a deep dive", url: "https://example.com/1", points: 120, num_comments: 40},
      %{id: 2, title: "#{query} in production", url: "https://example.com/2", points: 80, num_comments: 15}
    ]
  end

  @impl true
  def read_thread(story_id) do
    bump({:read_thread, story_id})
    Process.sleep(100)
    %{story_id: story_id, title: "story #{story_id}", comments: ["insightful comment #{story_id}"]}
  end

  @doc "How many times `call` (e.g. `{:search_stories, query}`) has actually executed."
  def call_count(call), do: :persistent_term.get({__MODULE__, call}, 0)

  defp bump(call) do
    key = {__MODULE__, call}
    :persistent_term.put(key, :persistent_term.get(key, 0) + 1)
  end
end
