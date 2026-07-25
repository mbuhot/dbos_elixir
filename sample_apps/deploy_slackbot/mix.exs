defmodule DeploySlackbot.MixProject do
  use Mix.Project

  def project do
    [
      app: :deploy_slackbot,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {DeploySlackbot.Application, []},
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:dbos, path: "../.."},
      {:postgrex, "~> 0.21"},
      {:req, "~> 0.5"}
    ]
  end
end
