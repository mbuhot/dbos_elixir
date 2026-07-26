defmodule AgentInbox.MixProject do
  use Mix.Project

  def project do
    [
      app: :agent_inbox,
      version: "0.1.0",
      elixir: "~> 1.19",
      compilers: [:dbos] ++ Mix.compilers(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:dbos, path: "../.."},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, "~> 0.21"}
    ]
  end
end
