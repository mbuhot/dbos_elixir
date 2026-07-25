defmodule DeploySlackbot.Slack do
  @moduledoc """
  Posting a deployment notification. `DeploySlackbot.Slack.Logging` implements this with no
  Slack workspace required; `DeploySlackbot.Slack.WebApi` posts for real via the Slack Web API.
  """

  @type client :: term
  @type channel :: String.t()
  @type text :: String.t()

  @doc "Posts `text` to `channel`."
  @callback post_message(client, channel, text) :: {:ok, term} | {:error, term}
end
