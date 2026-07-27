defmodule Dbos.Compensation do
  @moduledoc """
  The unwind: a workflow that reverses another workflow's recorded effects.

  A step declaring `compensate:` writes a recipe for reversing itself onto its own checkpoint row
  (`Dbos.Macros.defstep/3`). This workflow reads one workflow's history back in reverse and runs
  each recorded recipe, so the effects are undone in the opposite order they happened.

  A step that spawned another workflow is reversed by unwinding that workflow, not by reaching into
  its history: each workflow's effects are its own to reverse. So the walk hands a descendant to
  its own compensator and waits for it, and depth is ordinary workflow nesting.

  It is registered by `Dbos.Supervisor` on every engine under a reserved name, rather than declared
  with `defworkflow`, so a host application gains it on upgrade without touching its `:workflows`
  list. Its input is the id of the workflow to unwind — which is not always a workflow that
  failed, since a workflow that succeeded is unwound when something above it fails.

  ## Determinism

  Replay reproduces the walk because the history it walks cannot change: a workflow's compensator
  exists only once that workflow has reached a terminal status, and a terminal workflow writes no
  further checkpoints. So the same rows come back in the same order on every replay, and each undo
  keeps the step id it took the first time.

  A descendant's state is read as a step of its own, so the branch taken for it is checkpointed
  too. A replay reproduces that branch — and so the ids it consumed — from the recorded state
  rather than from whatever the descendant looks like now.

  ## When an undo fails

  Nothing is caught. The first undo to exhaust its retries lands this workflow in `ERROR`, and
  `[:dbos, :compensation, :stuck]` reports how much of the history is still outstanding.

  Resume it with `Dbos.fork/3` at the `step_id` that event carries, once the downstream system is
  repaired: every undo before it keeps its checkpoint, and the walk continues from the one that
  failed. `Dbos.retry/2` is the wrong tool here — a failed step checkpoints its failure, so
  re-running the same workflow replays that failure rather than retrying the undo.
  """

  alias Dbos.Runtime
  alias Dbos.StepNames
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
    |> SystemDb.list_unwind_steps(target_workflow_id)
    |> run_steps(config, target_workflow_id)
  end

  defp run_steps(steps, config, target_workflow_id) do
    total = length(steps)

    steps
    |> Enum.with_index()
    |> Enum.each(fn {step, reversed} ->
      run_step(step, config, target_workflow_id, reversed, total)
    end)

    total
  end

  defp run_step(step, config, target_workflow_id, reversed, total) do
    reverse(step, config)
  catch
    kind, reason ->
      report_stuck(step, config, target_workflow_id, reversed, total, kind, reason)
      :erlang.raise(kind, reason, __STACKTRACE__)
  end

  defp reverse(%{kind: :undo, undo: {module, function, args}}, _config) do
    apply(module, function, args)
  end

  defp reverse(%{kind: :descendant, workflow_id: workflow_id}, config) do
    unwind_descendant(config, workflow_id, descendant_state(config, workflow_id))
  end

  # Read as its own step, so the branch below is decided once and reproduced on every replay.
  defp descendant_state(config, workflow_id) do
    Dbos.step(StepNames.compensation_status(), fn ->
      case SystemDb.get_workflow_status(config, workflow_id) do
        {:ok, %{status: status}} -> {status, SystemDb.unwindable?(config, workflow_id)}
        {:error, :not_found} -> {:not_found, false}
      end
    end)
  end

  defp unwind_descendant(config, workflow_id, {status, _unwindable})
       when status in [:enqueued, :delayed, :pending] do
    Dbos.cancel(workflow_id)
    await(config, workflow_id)
    unwind_cancelled_descendant(config, workflow_id)
  end

  defp unwind_descendant(_config, workflow_id, {:success, true}) do
    {:ok, handle} = Dbos.unwind(workflow_id)
    await_handle(handle)
  end

  defp unwind_descendant(_config, _workflow_id, _state), do: :skipped

  # Cancelling a descendant that was running enqueues its unwind only if it had anything to
  # reverse, so whether one exists is read as a step before waiting on it. By now the descendant is
  # terminal, which is what makes that answer stable across replays.
  defp unwind_cancelled_descendant(config, workflow_id) do
    unwind_id = workflow_id(workflow_id)

    case descendant_state(config, unwind_id) do
      {:not_found, _unwindable} -> :nothing_to_unwind
      _state -> await(config, unwind_id)
    end
  end

  defp await(config, workflow_id) do
    await_handle(%Dbos.WorkflowHandle{engine: config.name, workflow_id: workflow_id})
  end

  defp await_handle(handle) do
    case Dbos.await(handle) do
      {:ok, value} -> value
      {:error, exception} -> exception
    end
  end

  defp report_stuck(step, config, target_workflow_id, reversed, total, kind, reason) do
    Telemetry.compensation_stuck(
      %{
        engine: config.name,
        workflow_id: Runtime.current_workflow_id(),
        step_id: Runtime.current_step_id(),
        target_workflow_id: target_workflow_id,
        function_name: step.function_name,
        kind: kind,
        reason: reason
      },
      %{reversed: reversed, outstanding: total - reversed}
    )
  end
end
