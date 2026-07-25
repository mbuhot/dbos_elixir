defmodule Dbos.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/mbuhot/dbos"

  def project do
    [
      app: :dbos,
      version: @version,
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_paths: ["test/dbos"],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      aliases: aliases(),
      name: "Dbos",
      source_url: @source_url,
      description:
        "Durable execution for Elixir: workflows and steps that checkpoint to Postgres.",
      docs: docs()
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
      {:jason, "~> 1.4", only: [:dev, :test]},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp aliases do
    [
      "test.integration": ["test test/integration --include integration"]
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: "v#{@version}",
      extras: extras(),
      groups_for_extras: groups_for_extras(),
      groups_for_modules: groups_for_modules(),
      extra_section: "GUIDES",
      formatters: ["html"]
    ]
  end

  defp extras do
    [
      "README.md",
      "guides/quickstart.md",
      "guides/why-dbos.md",
      "guides/architecture.md",
      "guides/programming-guide.md",
      "guides/integrating-dbos.md",
      "guides/tutorials/workflows.md",
      "guides/tutorials/steps.md",
      "guides/tutorials/transactions.md",
      "guides/tutorials/queues.md",
      "guides/tutorials/workflow-communication.md",
      "guides/tutorials/workflow-management.md",
      "guides/tutorials/scheduled-workflows.md",
      "guides/tutorials/upgrading-workflows.md",
      "guides/tutorials/testing.md",
      "docs/determinism.md",
      "docs/system-database.md",
      "docs/clustering.md",
      "docs/telemetry.md",
      "docs/interop-migration.md",
      "guides/production-checklist.md",
      "guides/faq.md",
      "DECISIONS.md"
    ]
  end

  defp groups_for_extras do
    [
      "Get Started": ~r{guides/(quickstart|why-dbos|architecture)\.md},
      "Develop with Elixir": ~r{guides/(programming-guide|integrating-dbos)\.md},
      Tutorials: ~r{guides/tutorials/.*},
      "Concepts and Explanations": ~r{docs/.*},
      "Deploy to Production": ~r{guides/production-checklist\.md},
      "Troubleshooting & FAQ": ~r{guides/faq\.md}
    ]
  end

  defp groups_for_modules do
    [
      "Writing Workflows": [Dbos, Dbos.RetryPolicy, Dbos.WorkflowHandle],
      Setup: [Dbos.Supervisor, Dbos.Config, Dbos.Migrator, Dbos.Registry],
      Queues: [Dbos.Queue, Dbos.Debouncer],
      Scheduling: [Dbos.Cron, Dbos.Scheduler],
      Operations: [Dbos.Recovery, Dbos.Cluster, Dbos.AdminServer, Dbos.Waits],
      "Data Access": [Dbos.DB, Dbos.DB.Ecto, Dbos.DB.Postgrex, Dbos.Serialization],
      Errors: ~r{Dbos\..*Error$}
    ]
  end
end
