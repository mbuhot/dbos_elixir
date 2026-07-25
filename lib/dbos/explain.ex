# Static step-id sequence analysis for mix dbos.explain, over a defworkflow body's stored AST.
# Recognizes durable operations this module can prove consume a fixed number of ids — the built-in
# Dbos.* primitives, and same-module defstep/deftransaction/defworkflow calls — and flags a
# case/cond/if whose branches consume a different number of ids, the classic replay bug. Anything
# else (a call this module cannot resolve, a comprehension, a capture, ...) is reported as
# indeterminate.
defmodule Dbos.Explain do
  @moduledoc false

  @doc "Parses `\"Mod.fun/arity\"` into `{module, function, arity}`."
  def parse_target(str) do
    case Regex.run(~r/^(.+)\.([a-zA-Z_][a-zA-Z0-9_?!]*)\/(\d+)$/, str) do
      [_, mod_str, fun_str, arity_str] ->
        module = mod_str |> String.split(".") |> Module.concat()
        {:ok, {module, String.to_atom(fun_str), String.to_integer(arity_str)}}

      _ ->
        {:error, "expected MODULE.function/arity, got #{inspect(str)}"}
    end
  end

  @doc "Finds `module`'s registered `defworkflow fun/arity`, returning `{:ok, name, ast}`."
  def find_workflow(module, fun, arity) do
    with {:module, ^module} <- Code.ensure_loaded(module),
         true <- function_exported?(module, :__dbos_workflows__, 0) do
      body_fun = Dbos.Macros.body_function_name(fun)

      module.__dbos_workflows__()
      |> Enum.find(fn {_name, {mod, bfun, barity}, _ast} ->
        mod == module and bfun == body_fun and barity == arity
      end)
      |> case do
        nil -> {:error, "#{inspect(module)} has no defworkflow #{fun}/#{arity}"}
        {name, _mfa, ast} -> {:ok, name, ast}
      end
    else
      {:error, reason} -> {:error, "could not load #{inspect(module)}: #{inspect(reason)}"}
      false -> {:error, "#{inspect(module)} does not `use Dbos` (no registered workflows)"}
    end
  end

  @doc "Renders the statically-derivable step-id sequence for `ast` (a workflow body) as text."
  def render(module, name, ast) do
    steps = module.__dbos_steps__()
    workflows = local_workflow_heads(module)
    {lines, _next_id, _determinate?} = analyze_seq(as_list(ast), 0, steps, workflows, module)

    ["workflow #{inspect(name)} (#{inspect(module)}):" | Enum.map(lines, &("  " <> &1))]
    |> Enum.join("\n")
  end

  defp local_workflow_heads(module) do
    Enum.map(module.__dbos_workflows__(), fn {name, {mod, body_fun, arity}, _ast} ->
      fun_name =
        body_fun
        |> Atom.to_string()
        |> String.replace_suffix("__dbos_workflow_body__", "")
        |> String.to_atom()

      {{mod, fun_name, arity}, name}
    end)
  end

  defp as_list({:__block__, _, exprs}), do: exprs
  defp as_list(expr), do: [expr]

  defp analyze_seq([], id, _steps, _wfs, _mod), do: {[], id, true}

  defp analyze_seq([expr | rest], id, steps, wfs, mod) do
    case classify(expr, steps, wfs, mod) do
      {:step, count, description} ->
        line = "#{id_label(id, count)}: #{description}"
        {rest_lines, next_id, determinate?} = analyze_seq(rest, id + count, steps, wfs, mod)
        {[line | rest_lines], next_id, determinate?}

      {:branch, kind, branches} ->
        render_branch(kind, branches, id, rest, steps, wfs, mod)

      {:conditional, description} ->
        {["id #{id}: #{description}"], id, false}

      :unknown ->
        {["id #{id}: CANNOT BE STATICALLY DETERMINED — #{unknown_reason(expr)}"], id, false}

      :ignore ->
        analyze_seq(rest, id, steps, wfs, mod)
    end
  end

  defp render_branch(kind, branches, id, rest, steps, wfs, mod) do
    results =
      Enum.map(branches, fn {label, body} ->
        {label, analyze_seq(as_list(body), id, steps, wfs, mod)}
      end)

    counts =
      Enum.map(results, fn
        {_label, {_lines, next_id, true}} -> next_id - id
        {_label, {_lines, _next_id, false}} -> :indeterminate
      end)

    uneven? = counts |> Enum.uniq() |> length() > 1

    branch_lines =
      Enum.flat_map(results, fn {label, {lines, next_id, determinate?}} ->
        consumed = if determinate?, do: "#{next_id - id} id(s)", else: "indeterminate"
        ["branch #{label}: consumes #{consumed}" | Enum.map(lines, &("  " <> &1))]
      end)

    header_line =
      if uneven? do
        "#{kind} at id #{id}: UNEVEN ID ALLOCATION ACROSS BRANCHES — replaying a different " <>
          "branch than the one originally taken will misalign every step id after this point. " <>
          "Use a patch instead of a bare conditional here."
      else
        "#{kind} at id #{id}:"
      end

    all_determinate? = Enum.all?(results, fn {_label, {_lines, _next_id, det?}} -> det? end)

    if uneven? or not all_determinate? do
      {[header_line | branch_lines], id, false}
    else
      {_label, {_lines, next_id, true}} = hd(results)
      {rest_lines, final_id, rest_determinate?} = analyze_seq(rest, next_id, steps, wfs, mod)
      {[header_line | branch_lines] ++ rest_lines, final_id, rest_determinate?}
    end
  end

  defp id_label(id, 1), do: "id #{id}"
  defp id_label(id, count), do: "ids #{id}-#{id + count - 1}"

  defp classify({:=, _, [_lhs, rhs]}, steps, wfs, mod), do: classify(rhs, steps, wfs, mod)

  defp classify({:case, _, [_subject, [do: clauses]]}, _steps, _wfs, _mod) do
    {:branch, "case",
     Enum.map(clauses, fn {:->, _, [pattern, body]} ->
       {Macro.to_string(pattern), body}
     end)}
  end

  defp classify({:cond, _, [[do: clauses]]}, _steps, _wfs, _mod) do
    {:branch, "cond",
     Enum.map(clauses, fn {:->, _, [[cond_expr], body]} -> {Macro.to_string(cond_expr), body} end)}
  end

  defp classify(
         {:if, _,
          [
            {{:., _, [{:__aliases__, _, [:Dbos]}, :patch]}, _, [_name_ast]} = patch_call,
            _branches
          ]},
         steps,
         wfs,
         mod
       ) do
    classify(patch_call, steps, wfs, mod)
    |> case do
      {:conditional, description} ->
        {:conditional,
         description <>
           " — this if's branch is patch-gated, so its own ids are not counted as an " <>
           "uneven-branch violation"}
    end
  end

  defp classify({:if, _, [_cond, branches]}, _steps, _wfs, _mod) when is_list(branches) do
    then_body = Keyword.get(branches, :do)
    else_body = Keyword.get(branches, :else, {:__block__, [], []})
    {:branch, "if", [{"true", then_body}, {"false", else_body}]}
  end

  defp classify(
         {{:., _, [{:__aliases__, _, [:Dbos]}, fun]}, _, [name_ast]},
         _steps,
         _wfs,
         _mod
       )
       when fun in [:patch, :deprecate_patch] do
    {:conditional,
     "Dbos.#{fun}(#{Macro.to_string(name_ast)}) consumes 0 or 1 id, CONDITIONAL on the runtime " <>
       "patch decision — not statically countable, and everything after it is not analyzed"}
  end

  defp classify({{:., _, [{:__aliases__, _, [:Dbos]}, fun]}, _, args}, _steps, _wfs, _mod) do
    dbos_op(fun, length(args))
  end

  defp classify(
         {{:., _, [{:__aliases__, _, [:Dbos, :Runtime]}, fun]}, _, _args},
         _steps,
         _wfs,
         _mod
       )
       when fun in [:run_step, :run_step_at] do
    {:step, 1, "step (Dbos.Runtime.#{fun})"}
  end

  defp classify({:%{}, _, _}, _steps, _wfs, _mod), do: :ignore
  defp classify({:{}, _, _}, _steps, _wfs, _mod), do: :ignore
  defp classify({:<<>>, _, _}, _steps, _wfs, _mod), do: :ignore
  defp classify({:__block__, _, _}, _steps, _wfs, _mod), do: :ignore

  defp classify({fun, _, args}, steps, wfs, mod)
       when is_atom(fun) and (is_list(args) or is_nil(args)) do
    arity = length(args || [])

    cond do
      match =
          Enum.find(steps, fn {_name, {step_fun, step_arity}} ->
            step_fun == fun and step_arity == arity
          end) ->
        {name, _} = match
        {:step, 1, "step #{fun}/#{arity} (#{inspect(name)})"}

      match =
          Enum.find(wfs, fn {{wf_mod, wf_fun, wf_arity}, _name} ->
            wf_mod == mod and wf_fun == fun and wf_arity == arity
          end) ->
        {_key, name} = match
        {:step, 2, "child workflow #{fun}/#{arity} (#{inspect(name)}) (start + DBOS.getResult)"}

      true ->
        :unknown
    end
  end

  defp classify(expr, _steps, _wfs, _mod) do
    if pure_literal?(expr), do: :ignore, else: :unknown
  end

  defp dbos_op(:sleep, _arity), do: {:step, 1, "Dbos.sleep"}
  defp dbos_op(:recv_message, _arity), do: {:step, 2, "Dbos.recv_message (recv + internal sleep)"}
  defp dbos_op(:get_event, _arity), do: {:step, 2, "Dbos.get_event (getEvent + internal sleep)"}
  defp dbos_op(:set_event, _arity), do: {:step, 1, "Dbos.set_event"}
  defp dbos_op(:write_stream, _arity), do: {:step, 1, "Dbos.write_stream"}
  defp dbos_op(:close_stream, _arity), do: {:step, 1, "Dbos.close_stream"}
  defp dbos_op(:send_message, _arity), do: {:step, 1, "Dbos.send_message"}
  defp dbos_op(:transaction, _arity), do: {:step, 1, "Dbos.transaction"}
  defp dbos_op(:start, _arity), do: {:step, 1, "Dbos.start (child workflow)"}
  defp dbos_op(:await, _arity), do: {:step, 1, "Dbos.await (DBOS.getResult)"}
  defp dbos_op(:enqueue, _arity), do: {:step, 1, "Dbos.enqueue"}
  defp dbos_op(:fork, _arity), do: {:step, 1, "Dbos.fork (DBOS.forkWorkflow)"}
  defp dbos_op(:status, _arity), do: {:step, 1, "Dbos.status (DBOS.getStatus)"}
  defp dbos_op(:cancel, _arity), do: {:step, 1, "Dbos.cancel (DBOS.cancelWorkflow)"}
  defp dbos_op(:resume, _arity), do: {:step, 1, "Dbos.resume (DBOS.resumeWorkflow)"}
  defp dbos_op(_fun, _arity), do: :unknown

  defp pure_literal?(expr) do
    case expr do
      _ when is_number(expr) or is_atom(expr) or is_binary(expr) -> true
      {name, _, ctx} when is_atom(name) and (is_atom(ctx) or is_nil(ctx)) -> true
      {:%{}, _, _} -> true
      {:{}, _, _} -> true
      list when is_list(list) -> Enum.all?(list, &pure_literal?/1)
      _ -> false
    end
  end

  defp unknown_reason({:=, _, [_lhs, rhs]}), do: unknown_reason(rhs)

  defp unknown_reason({{:., _, [{:__aliases__, _, mod_parts}, fun]}, _, args}) do
    "call to `#{Enum.join(mod_parts, ".")}.#{fun}/#{length(args)}`, not a step this module can see"
  end

  defp unknown_reason({fun, _, args}) when is_atom(fun) do
    "call to `#{fun}/#{length(args || [])}`, not a locally registered step or child workflow"
  end

  defp unknown_reason(_expr), do: "an expression this analysis does not statically interpret"
end
