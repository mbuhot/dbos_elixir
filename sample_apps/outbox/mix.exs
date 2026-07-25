defmodule Outbox.MixProject do
  use Mix.Project

  def project do
    [
      app: :outbox,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]] ++ mod(Mix.env())
  end

  defp mod(:test), do: []
  defp mod(_env), do: [mod: {Outbox.Application, []}]

  defp deps do
    [
      {:dbos, path: "../.."},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, "~> 0.21"},
      {:jason, "~> 1.4"}
    ]
  end
end
