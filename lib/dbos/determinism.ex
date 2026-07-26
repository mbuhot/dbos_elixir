defmodule Dbos.Determinism do
  @moduledoc """
  Compile-time determinism checking over a `defworkflow`/`defstep`/`deftransaction` body's AST,
  per `docs/determinism.md`. Raises a `CompileError` naming the offending call, its file and line,
  and the fix, for anything in the shared rule table.

  This layer sees only the literal `do` block handed to the macro. A violation reached through a
  helper function is the job of `Mix.Tasks.Compile.Dbos`.
  """

  alias Dbos.Determinism.Rules

  @doc """
  Walks `body` (the workflow's raw do-block AST) and raises `CompileError` on the banned
  constructs found, collecting every violation into one message. `context`: `:env` (the
  `Macro.Env` at the `defworkflow` call site), `:workflow_name`, `:repo` (module banned from
  direct calls, or `nil`).
  """
  def check!(body, context) do
    violations = Macro.prewalk(body, [], &collect_violation(&1, &2, context)) |> elem(1)

    case Enum.reverse(violations) do
      [] ->
        :ok

      messages ->
        raise CompileError, file: context.env.file, description: Enum.join(messages, "\n\n")
    end
  end

  @doc """
  Walks `body` (a `defstep`/`deftransaction`'s raw do-block AST) and raises `CompileError` on the
  first call that hands execution to another process — `Task.*`, `spawn`/`spawn_link`/
  `spawn_monitor` — since that process starts with none of the workflow context the process
  dictionary carries, and any durable call made from inside it silently skips its checkpoint.
  `context`: `:env` (the `Macro.Env` at the call site), `:step_name`.
  """
  def check_step!(body, context) do
    violations = Macro.prewalk(body, [], &collect_step_violation(&1, &2, context)) |> elem(1)

    case Enum.reverse(violations) do
      [] ->
        :ok

      messages ->
        raise CompileError, file: context.env.file, description: Enum.join(messages, "\n\n")
    end
  end

  defp collect_violation(node, acc, context) do
    case banned(node, context) do
      nil -> {node, acc}
      rule -> {node, [format_violation(context, node, rule) | acc]}
    end
  end

  defp collect_step_violation(node, acc, context) do
    case Rules.match_ast(node, :step) do
      nil -> {node, acc}
      rule -> {node, [format_step_violation(context, node, rule) | acc]}
    end
  end

  defp format_violation(context, node, rule) do
    "#{context.env.file}:#{line_of(node)}: workflow #{inspect(context.workflow_name)} calls " <>
      "#{rule.target}, which is nondeterministic and breaks replay on recovery.\n    #{rule.fix}"
  end

  defp format_step_violation(context, node, rule) do
    "#{context.env.file}:#{line_of(node)}: step #{inspect(context.step_name)} calls " <>
      "#{rule.target}, which runs in a new process with no workflow context — any durable call " <>
      "made from inside it takes the passthrough path and silently skips its checkpoint.\n" <>
      "    #{rule.fix}"
  end

  defp line_of({_, meta, _}) when is_list(meta), do: Keyword.get(meta, :line, 0)
  defp line_of(_), do: 0

  defp banned(node, context) do
    case Rules.match_ast(node, :workflow) do
      nil -> repo_call(node, context)
      rule -> rule
    end
  end

  defp repo_call({{:., _, [mod_ast, fun]}, _, args}, %{repo: repo} = context)
       when not is_nil(repo) do
    if resolve_module(mod_ast, context.env) == repo do
      %{
        id: :repo_call,
        target: "#{inspect(repo)}.#{fun}/#{length(args)}",
        fix: "wrap the call in a deftransaction, so it commits atomically with its checkpoint."
      }
    end
  end

  defp repo_call(_node, _context), do: nil

  defp resolve_module({:__aliases__, _, _} = alias_ast, env) do
    case Macro.expand(alias_ast, env) do
      mod when is_atom(mod) -> mod
      _other -> nil
    end
  end

  defp resolve_module(mod, _env) when is_atom(mod), do: mod
  defp resolve_module(_other, _env), do: nil
end
