defmodule HackerNewsAgent.HnClient.Algolia do
  @moduledoc "Discovers stories via Algolia's HN search index, then reads full threads via the Firebase Item API."

  @behaviour HackerNewsAgent.HnClient

  @algolia_search "https://hn.algolia.com/api/v1/search"
  @firebase_item "https://hacker-news.firebaseio.com/v0/item"
  @comments_per_story 5

  @impl true
  def search_stories(query) do
    %Req.Response{status: 200, body: body} =
      Req.get!(@algolia_search, params: [query: query, tags: "story", hitsPerPage: 20])

    Enum.map(body["hits"], fn hit ->
      %{
        id: String.to_integer(hit["objectID"]),
        title: hit["title"],
        url: hit["url"],
        points: hit["points"],
        num_comments: hit["num_comments"]
      }
    end)
  end

  @impl true
  def read_thread(story_id) do
    story = fetch_item!(story_id)
    comment_ids = story["kids"] || []

    comments =
      comment_ids
      |> Enum.take(@comments_per_story)
      |> Enum.map(&fetch_item!/1)
      |> Enum.map(&comment_text/1)
      |> Enum.reject(&is_nil/1)

    %{story_id: story_id, title: story["title"], comments: comments}
  end

  defp fetch_item!(id) do
    %Req.Response{status: 200, body: body} = Req.get!("#{@firebase_item}/#{id}.json")
    body
  end

  defp comment_text(%{"text" => text}), do: text
  defp comment_text(_other), do: nil
end
