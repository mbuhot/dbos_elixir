defmodule Dbos.MixProject do
  use Mix.Project

  def project do
    [
      app: :dbos,
      version: "0.1.0",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: ["test/dbos"],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:postgrex, "~> 0.21"},
      {:telemetry, "~> 1.3"},
      {:ecto_sql, "~> 3.13", optional: true},
      {:jason, "~> 1.4", only: [:dev, :test]}
    ]
  end

  defp aliases do
    [
      "test.integration": ["test test/integration --include integration"]
    ]
  end
end
