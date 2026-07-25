defmodule Mix.Tasks.Dbos.Explain do
  @shortdoc "Prints a defworkflow's statically-derivable step-id sequence"

  @moduledoc """
  Prints the statically derivable step-id sequence for a `defworkflow`, and flags branches
  (`case`/`cond`/`if`) that allocate ids unevenly across their branches — the classic replay bug
  described in `docs/determinism.md`.

      mix dbos.explain Checkout.process_order/2

  Where the sequence cannot be determined statically (a call this analysis cannot resolve to a
  local step or child workflow, a comprehension, a captured function, ...), it says so rather
  than guessing.
  """

  use Mix.Task

  alias Dbos.Explain

  @impl Mix.Task
  def run(args) do
    Mix.Task.run("compile")

    case args do
      [target] -> explain(target)
      _other -> Mix.raise("usage: mix dbos.explain Module.function/arity")
    end
  end

  defp explain(target) do
    with {:ok, {module, fun, arity}} <- Explain.parse_target(target),
         {:ok, name, ast} <- Explain.find_workflow(module, fun, arity) do
      Mix.shell().info(Explain.render(module, name, ast))
    else
      {:error, reason} -> Mix.raise(reason)
    end
  end
end
