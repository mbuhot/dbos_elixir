defmodule Dbos.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/mbuhot/dbos_elixir"

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
      source_ref: source_ref(),
      extras: extras(),
      groups_for_extras: groups_for_extras(),
      groups_for_modules: groups_for_modules(),
      extra_section: "GUIDES",
      formatters: ["html"],
      assets: %{"assets" => "."},
      before_closing_body_tag: &before_closing_body_tag/1
    ]
  end

  defp source_ref do
    System.get_env("GITHUB_SHA") || "main"
  end

  defp before_closing_body_tag(:html) do
    """
    <script src="https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.min.js"></script>
    <script>
      (() => {
        const darkQuery = window.matchMedia("(prefers-color-scheme: dark)");
        let sequence = 0;
        let scheduled = false;

        const isDark = () => {
          const stored = document.body.getAttribute("data-theme") || localStorage.getItem("ex_doc:settings:night_mode");
          if (stored === "dark") return true;
          if (stored === "light") return false;
          return darkQuery.matches;
        };

        const draw = (container, definition) => {
          sequence += 1;
          mermaid
            .render("mermaid-graph-" + sequence, definition)
            .then(({ svg, bindFunctions }) => {
              container.innerHTML = svg;
              if (bindFunctions) bindFunctions(container);
            })
            .catch(() => {
              container.textContent = definition;
            });
        };

        const renderPending = () => {
          const pending = document.querySelectorAll("pre > code.mermaid");
          if (pending.length === 0) return;

          mermaid.initialize({ startOnLoad: false, theme: isDark() ? "dark" : "default" });

          pending.forEach((code) => {
            const definition = code.textContent;
            const container = document.createElement("div");
            container.className = "mermaid-diagram";
            container.dataset.mermaidSource = definition;
            code.parentElement.replaceWith(container);
            draw(container, definition);
          });
        };

        const schedule = () => {
          if (scheduled) return;
          scheduled = true;
          window.requestAnimationFrame(() => {
            scheduled = false;
            renderPending();
          });
        };

        let lastTheme = null;

        const redrawForTheme = () => {
          const theme = isDark() ? "dark" : "default";
          if (theme === lastTheme) return;
          lastTheme = theme;

          mermaid.initialize({ startOnLoad: false, theme: theme });
          document.querySelectorAll(".mermaid-diagram[data-mermaid-source]").forEach((container) => {
            draw(container, container.dataset.mermaidSource);
          });
        };

        const start = () => {
          renderPending();

          new MutationObserver(schedule).observe(document.body, {
            childList: true,
            subtree: true
          });

          new MutationObserver(redrawForTheme).observe(document.body, {
            attributes: true,
            attributeFilter: ["data-theme", "class"]
          });

          darkQuery.addEventListener("change", redrawForTheme);
        };

        if (document.readyState === "loading") {
          document.addEventListener("DOMContentLoaded", start);
        } else {
          start();
        }
      })();
    </script>
    """
  end

  defp before_closing_body_tag(_), do: ""

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
      "guides/faq.md"
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
      Setup: [Dbos.Supervisor, Dbos.Config, Dbos.Migration, Dbos.Migrator, Dbos.Registry],
      Queues: [Dbos.Queue, Dbos.Debouncer],
      Scheduling: [Dbos.Cron, Dbos.Scheduler],
      Operations: [Dbos.Recovery, Dbos.Cluster, Dbos.AdminServer, Dbos.Waits],
      "Data Access": [Dbos.DB, Dbos.DB.Ecto, Dbos.DB.Postgrex, Dbos.Serialization],
      Errors: ~r{Dbos\..*Error$}
    ]
  end
end
