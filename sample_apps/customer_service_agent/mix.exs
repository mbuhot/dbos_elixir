defmodule CustomerServiceAgent.MixProject do
  use Mix.Project

  def project do
    [
      app: :customer_service_agent,
      version: "0.1.0",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    base = [extra_applications: [:logger]]

    if Mix.env() == :test do
      base
    else
      Keyword.put(base, :mod, {CustomerServiceAgent.Application, []})
    end
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:dbos, path: "../.."},
      {:postgrex, "~> 0.21"},
      {:req, "~> 0.5"},
      {:jason, "~> 1.4"}
    ]
  end
end
