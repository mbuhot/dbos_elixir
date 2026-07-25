defmodule DeploySlackbot.Slack.Logging do
  @moduledoc """
  `DeploySlackbot.Slack` over an `Agent`: prints every message and records it in order, so a
  test (or a terminal, for the sample) can see exactly what would have been posted. The
  `client` is the Agent's registered name.
  """

  @behaviour DeploySlackbot.Slack

  use Agent

  @doc "Starts the recorder under `opts[:name]`."
  def start_link(opts) do
    Agent.start_link(fn -> [] end, name: Keyword.fetch!(opts, :name))
  end

  @doc "Every `{channel, text}` posted to `client`, oldest first."
  def posts(client), do: Agent.get(client, & &1)

  @impl true
  def post_message(client, channel, text) do
    Agent.update(client, fn posts -> posts ++ [{channel, text}] end)
    IO.puts("[slack:#{channel}] #{text}")
    {:ok, %{channel: channel, text: text}}
  end
end
