defmodule QueueWorker.MixProject do
  use Mix.Project

  def project do
    [
      app: :queue_worker,
      version: "0.1.0",
      elixir: "~> 1.19",
      compilers: [:dbos] ++ Mix.compilers(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    base = [extra_applications: [:logger]]

    if Mix.env() == :test do
      base
    else
      Keyword.put(base, :mod, {QueueWorker.Application, []})
    end
  end

  defp deps do
    [
      {:dbos, path: "../.."},
      {:ecto_sql, "~> 3.13"},
      {:postgrex, "~> 0.21"}
    ]
  end
end
