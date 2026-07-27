defmodule Dbos.Compensation do
  @moduledoc """
  The unwind: a workflow that reverses another workflow's recorded effects.

  A step declaring `compensate:` writes a recipe for reversing itself onto its own checkpoint row
  (`Dbos.Macros.defstep/3`). This workflow reads one workflow's history back in reverse and runs
  each recorded recipe, so the effects are undone in the opposite order they happened.

  It is registered by `Dbos.Supervisor` on every engine under a reserved name, rather than declared
  with `defworkflow`, so a host application gains it on upgrade without touching its `:workflows`
  list. Its input is the id of the workflow to unwind — which is not always a workflow that
  failed, since a workflow that succeeded is unwound when something above it fails.

  ## Determinism

  Replay reproduces the walk because the history it walks cannot change: a workflow's compensator
  exists only once that workflow has reached a terminal status, and a terminal workflow writes no
  further checkpoints. So the same rows come back in the same order on every replay, and each undo
  keeps the step id it took the first time.

  ## When an undo fails

  Nothing is caught. The first undo to exhaust its retries lands this workflow in `ERROR`, and
  `[:dbos, :compensation, :stuck]` reports how much of the history is still outstanding.

  Resume it with `Dbos.fork/3` at the `step_id` that event carries, once the downstream system is
  repaired: every undo before it keeps its checkpoint, and the walk continues from the one that
  failed. `Dbos.retry/2` is the wrong tool here — a failed step checkpoints its failure, so
  re-running the same workflow replays that failure rather than retrying the undo.
  """

  alias Dbos.Runtime
  alias Dbos.SystemDb
  alias Dbos.Telemetry

  @workflow_name "DBOS.compensate"

  @doc "The reserved workflow name this engine registers the unwind under."
  def workflow_name, do: @workflow_name

  @doc "The `{name, mfa, version}` registry entry `Dbos.Supervisor` adds to every engine."
  def workflow_entry, do: {@workflow_name, {__MODULE__, :unwind, 1}, nil}

  @doc """
  The compensator's own workflow id for `target_workflow_id`. Derived from the target, so starting
  one is idempotent: a replayed walk above it, and the target reaching its own compensator through
  its own failure, converge on one workflow rather than unwinding the same history twice.
  """
  def workflow_id(target_workflow_id), do: target_workflow_id <> "-compensate"

  @doc """
  Unwinds `target_workflow_id`: runs every compensation recorded in its history, newest first.
  Returns how many undos ran. This is the compensator's workflow body — call `Dbos.unwind/2`
  rather than this function.
  """
  def unwind(target_workflow_id) do
    config = Runtime.current_config()

    config
    |> SystemDb.list_compensations(target_workflow_id)
    |> run_undos(config, target_workflow_id)
  end

  defp run_undos(compensations, config, target_workflow_id) do
    total = length(compensations)

    compensations
    |> Enum.with_index()
    |> Enum.each(fn {compensation, reversed} ->
      run_undo(compensation, config, target_workflow_id, reversed, total)
    end)

    total
  end

  defp run_undo(compensation, config, target_workflow_id, reversed, total) do
    %{undo: {module, function, args}} = compensation

    try do
      apply(module, function, args)
    catch
      kind, reason ->
        report_stuck(compensation, config, target_workflow_id, reversed, total, kind, reason)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp report_stuck(compensation, config, target_workflow_id, reversed, total, kind, reason) do
    Telemetry.compensation_stuck(
      %{
        engine: config.name,
        workflow_id: Runtime.current_workflow_id(),
        step_id: Runtime.current_step_id(),
        target_workflow_id: target_workflow_id,
        function_name: compensation.function_name,
        kind: kind,
        reason: reason
      },
      %{reversed: reversed, outstanding: total - reversed}
    )
  end
end
