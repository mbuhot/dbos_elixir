defmodule QueuePatterns.MixProject do
  use Mix.Project

  def project do
    [
      app: :queue_patterns,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    base = [extra_applications: [:logger]]

    if Mix.env() == :test do
      base
    else
      Keyword.put(base, :mod, {QueuePatterns.Application, []})
    end
  end

  defp deps do
    [
      {:dbos, path: "../.."},
      {:postgrex, "~> 0.21"}
    ]
  end
end
