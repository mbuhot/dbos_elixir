defmodule S3Mirror.MixProject do
  use Mix.Project

  def project do
    [
      app: :s3_mirror,
      version: "0.1.0",
      elixir: "~> 1.19",
      compilers: [:dbos] ++ Mix.compilers(),
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      mod: {S3Mirror.Application, []},
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
