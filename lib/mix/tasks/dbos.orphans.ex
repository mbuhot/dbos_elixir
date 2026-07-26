defmodule Mix.Tasks.Dbos.Orphans do
  @shortdoc "Prints the PENDING workflows no live executor in the fleet can claim"

  @moduledoc """
  Prints every group of `PENDING` workflows that no live executor can pick up, with why, how
  many, how long the oldest has been waiting, and one workflow id to look at.

      mix dbos.orphans
      mix dbos.orphans --engine MyApp.Dbos

  Run this after a deploy. A group here is work that will sit untouched until something that can
  run it is deployed: either a build registering that workflow name, or one whose
  `application_version` matches the rows'.

  The answer is fleet-wide, read from the capabilities every executor publishes with its lease,
  so it holds regardless of which node the task runs on.

  ## Command line options

    * `--engine` - the engine module to query (default: `Dbos`)
  """

  use Mix.Task

  alias Dbos.Recovery

  @switches [engine: :string]

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("app.start")

    {opts, _rest} = OptionParser.parse!(args, strict: @switches)

    engine = Module.concat([Keyword.get(opts, :engine, "Dbos")])

    engine
    |> orphans()
    |> render()
    |> Mix.shell().info()
  end

  defp orphans(engine) do
    Recovery.orphans(engine)
  rescue
    Dbos.NotStartedError ->
      Mix.raise(
        "the engine #{inspect(engine)} is not running. Start your application's engine (or " <>
          "pass --engine) so this task can reach the system database."
      )
  end

  defp render([]), do: "No orphaned PENDING workflows: every one of them has a live claimant."

  defp render(orphans) do
    header = ["count", "name", "application_version", "reason", "oldest", "example"]
    rows = Enum.map(orphans, &row/1)
    widths = widths([header | rows])

    [header, separator(widths) | rows]
    |> Enum.map_join("\n", &line(&1, widths))
  end

  defp row(orphan) do
    [
      to_string(orphan.count),
      orphan.name,
      to_string(orphan.application_version),
      to_string(orphan.reason),
      age(orphan.oldest_created_at_epoch_ms),
      orphan.example_workflow_id
    ]
  end

  defp age(created_at_epoch_ms) do
    seconds = div(System.os_time(:millisecond) - created_at_epoch_ms, 1000)

    cond do
      seconds < 60 -> "#{seconds}s"
      seconds < 3600 -> "#{div(seconds, 60)}m"
      seconds < 86_400 -> "#{div(seconds, 3600)}h"
      true -> "#{div(seconds, 86_400)}d"
    end
  end

  defp widths(rows) do
    rows
    |> Enum.zip_with(& &1)
    |> Enum.map(fn column -> column |> Enum.map(&String.length/1) |> Enum.max() end)
  end

  defp separator(widths), do: Enum.map(widths, &String.duplicate("-", &1))

  defp line(cells, widths) do
    cells
    |> Enum.zip(widths)
    |> Enum.map_join("  ", fn {cell, width} -> String.pad_trailing(cell, width) end)
    |> String.trim_trailing()
  end
end
