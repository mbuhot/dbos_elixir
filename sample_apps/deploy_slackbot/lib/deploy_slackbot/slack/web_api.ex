defmodule DeploySlackbot.Slack.WebApi do
  @moduledoc """
  `DeploySlackbot.Slack` over the real Slack Web API (`chat.postMessage`) via `Req`. The
  `client` is a bot token. Used only when `SLACK_BOT_TOKEN` is present in the environment; see
  the README.
  """

  @behaviour DeploySlackbot.Slack

  @doc "The bot token from `SLACK_BOT_TOKEN`. Raises if it is not set."
  def token_from_env! do
    System.get_env("SLACK_BOT_TOKEN") ||
      raise "SLACK_BOT_TOKEN is not set; export it, or use DeploySlackbot.Slack.Logging instead"
  end

  @impl true
  def post_message(token, channel, text) do
    case Req.post("https://slack.com/api/chat.postMessage",
           auth: {:bearer, token},
           json: %{channel: channel, text: text}
         ) do
      {:ok, %{status: 200, body: %{"ok" => true} = body}} -> {:ok, body}
      {:ok, %{status: 200, body: %{"ok" => false, "error" => error}}} -> {:error, error}
      {:ok, %{status: status, body: body}} -> {:error, {:http_error, status, body}}
      {:error, reason} -> {:error, reason}
    end
  end
end
