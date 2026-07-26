defmodule Dbos.Compiler.Analysis do
  @moduledoc """
  Forward reachability over the call graph collected by `Mix.Tasks.Compile.Dbos`.

  One breadth-first walk per entry point — a `defworkflow` body, or a `defstep`/`deftransaction`
  body under the weaker process-handoff rule set. Descent stops at every other entry point, at
  `Dbos` engine internals, at anything annotated `@dbos_deterministic` or listed in the project's
  `trusted:` list, and at every module outside the application being compiled. What remains is
  the user's own helpers, and each banned construct found in there yields one diagnostic carrying
  the chain that reaches it. A `receive` arrives as an edge to `Kernel.SpecialForms.receive/1` and
  is treated like any other banned target.

  The result is neither sound nor complete: `apply/3` with a computed module, protocol dispatch
  and behaviour callbacks through a variable module are invisible, and a branch never taken at
  runtime is still reported.
  """

  alias Dbos.Determinism.Rules
  alias Mix.Task.Compiler.Diagnostic

  @dbos_root_module Dbos
  @apply_mfas [
    {Kernel, :apply, 2},
    {Kernel, :apply, 3},
    {:erlang, :apply, 2},
    {:erlang, :apply, 3}
  ]

  @doc """
  Diagnostics for `calls` and `entries` (the raw `Dbos.Compiler.State` rows). `app_modules` is the
  module list of the application being compiled; entry points outside it are not checked, since
  their call graph was collected in another compile run. `opts[:trusted]` is the project-level
  list of trusted modules and MFAs.
  """
  def diagnostics(calls, entries, app_modules, opts \\ []) do
    graph = Enum.group_by(calls, & &1.from)
    app_modules = MapSet.new(app_modules)
    by_kind = Enum.group_by(entries, & &1.kind)

    points =
      Enum.filter(
        Map.get(by_kind, :workflow, []) ++
          Map.get(by_kind, :step, []) ++ Map.get(by_kind, :transaction, []),
        &MapSet.member?(app_modules, elem(&1.mfa, 0))
      )

    annotated = Map.get(by_kind, :trusted, [])
    project_trusted = normalize_trusted(Keyword.get(opts, :trusted, []))

    context = %{
      graph: graph,
      app_modules: app_modules,
      entry_mfas: MapSet.new(points, & &1.mfa),
      repos: MapSet.new(Map.get(by_kind, :repo, []), & &1.module),
      annotated: MapSet.new(annotated, & &1.mfa),
      project_trusted: project_trusted
    }

    walks = Enum.map(points, &walk(&1, context))
    reached = walks |> Enum.flat_map(& &1.reached) |> MapSet.new()

    (Enum.flat_map(walks, & &1.diagnostics) ++
       unused_suppressions(annotated, project_trusted, reached, app_modules) ++
       unscannable(Map.get(by_kind, :unscannable, []), reached, app_modules))
    |> Enum.uniq_by(&{&1.file, &1.position, &1.message})
    |> Enum.sort_by(&{&1.file, &1.position})
  end

  defp walk(point, context) do
    scope = if point.kind == :workflow, do: :workflow, else: :step
    state = %{visited: MapSet.new([point.mfa]), preds: %{}, found: [], reached: MapSet.new()}
    state = bfs([point.mfa], state, point, scope, context)

    %{
      reached: MapSet.to_list(state.reached),
      diagnostics: Enum.map(state.found, &diagnostic(&1, point, scope, state.preds))
    }
  end

  defp bfs([], state, _point, _scope, _context), do: state

  defp bfs([node | rest], state, point, scope, context) do
    edges = Map.get(context.graph, node, [])
    {state, discovered} = Enum.reduce(edges, {state, []}, &visit(&1, &2, node, scope, context))
    bfs(rest ++ discovered, state, point, scope, context)
  end

  defp visit(edge, {state, discovered}, node, scope, context) do
    to = edge.to
    state = %{state | reached: MapSet.put(state.reached, to)}

    state =
      case rule_for(to, scope, context) do
        nil -> state
        rule -> %{state | found: [{node, edge, rule} | state.found]}
      end

    cond do
      MapSet.member?(state.visited, to) ->
        {state, discovered}

      descend?(to, context) ->
        state = %{
          state
          | visited: MapSet.put(state.visited, to),
            preds: Map.put(state.preds, to, {node, edge})
        }

        {state, discovered ++ [to]}

      true ->
        {state, discovered}
    end
  end

  defp rule_for(mfa, scope, context) do
    if trusted?(mfa, context), do: nil, else: banned(mfa, scope, context)
  end

  defp banned({module, fun, arity} = mfa, scope, context) do
    cond do
      rule = Rules.match_mfa(module, fun, arity, scope) ->
        rule

      scope == :workflow and MapSet.member?(context.repos, module) ->
        repo_rule(module, fun, arity)

      mfa in @apply_mfas ->
        apply_rule(module, fun, arity)

      true ->
        nil
    end
  end

  defp repo_rule(module, fun, arity) do
    %{
      id: :repo_call,
      severity: :warning,
      target: "#{inspect(module)}.#{fun}/#{arity}",
      fix: "wrap the call in a deftransaction, so it commits atomically with its checkpoint."
    }
  end

  defp apply_rule(module, fun, arity) do
    %{
      id: :dynamic_dispatch,
      severity: :hint,
      target: "#{inspect(module)}.#{fun}/#{arity}",
      fix:
        "the determinism checker cannot see through a dynamic dispatch. Whatever this reaches " <>
          "is unchecked."
    }
  end

  defp descend?({module, _fun, _arity} = mfa, context) do
    MapSet.member?(context.app_modules, module) and not dbos_module?(module) and
      not MapSet.member?(context.entry_mfas, mfa) and not trusted?(mfa, context)
  end

  defp trusted?({module, _fun, _arity} = mfa, context) do
    MapSet.member?(context.annotated, mfa) or MapSet.member?(context.project_trusted, mfa) or
      MapSet.member?(context.project_trusted, module)
  end

  defp dbos_module?(@dbos_root_module), do: true

  defp dbos_module?(module) when is_atom(module) do
    String.starts_with?(Atom.to_string(module), "Elixir.Dbos.")
  end

  defp diagnostic({node, edge, rule}, point, scope, preds) do
    chain = chain(node, point.mfa, preds, [])
    file = edge.file || point.file
    position = edge.line || point.line

    %Diagnostic{
      compiler_name: "dbos",
      file: file,
      position: position,
      severity: Map.get(rule, :severity, :warning),
      message: message(point, scope, chain, edge, rule),
      details: %{
        entry: point.mfa,
        entry_name: point.name,
        rule: rule.id,
        chain: Enum.map(chain ++ [edge], &{&1.to, &1.file, &1.line})
      }
    }
  end

  defp chain(node, entry_mfa, _preds, acc) when node == entry_mfa, do: acc

  defp chain(node, entry_mfa, preds, acc) do
    case Map.fetch(preds, node) do
      {:ok, {previous, edge}} -> chain(previous, entry_mfa, preds, [edge | acc])
      :error -> acc
    end
  end

  defp message(point, scope, chain, edge, rule) do
    heading =
      "#{subject(rule.id)} reachable from a #{kind_label(point.kind)} body\n\n" <>
        "  #{format_mfa(point.mfa)}  (#{kind_label(point.kind)} #{inspect(point.name)})"

    steps =
      Enum.map_join(chain ++ [edge], "", fn step ->
        "\n    → #{format_mfa(step.to)}  #{location(step, point)}"
      end)

    heading <> steps <> "\n\n  #{scope_note(scope)} #{capitalize(rule.fix)}"
  end

  defp subject(:receive), do: "blocking receive"
  defp subject(_id), do: "nondeterministic call"

  defp capitalize(<<first::utf8, rest::binary>>), do: String.upcase(<<first::utf8>>) <> rest

  defp scope_note(:workflow), do: "This is nondeterministic and breaks replay on recovery."

  defp scope_note(:step),
    do:
      "This runs in a new process with no workflow context, so any durable call made from " <>
        "inside it silently skips its checkpoint."

  defp location(%{file: nil}, point), do: "#{point.file}:#{point.line}"
  defp location(%{file: file, line: nil}, _point), do: file
  defp location(%{file: file, line: line}, _point), do: "#{file}:#{line}"

  defp kind_label(:workflow), do: "workflow"
  defp kind_label(:step), do: "step"
  defp kind_label(:transaction), do: "transaction"

  defp format_mfa({Kernel.SpecialForms, fun, arity}), do: "#{fun}/#{arity}"

  defp format_mfa({module, fun, arity}) do
    "#{inspect(module)}.#{strip_body_suffix(fun)}/#{arity}"
  end

  defp strip_body_suffix(fun) do
    fun |> Atom.to_string() |> String.replace_suffix("__dbos_workflow_body__", "")
  end

  defp unused_suppressions(annotated, project_trusted, reached, app_modules) do
    from_annotations =
      annotated
      |> Enum.reject(&MapSet.member?(reached, &1.mfa))
      |> Enum.filter(&MapSet.member?(app_modules, elem(&1.mfa, 0)))
      |> Enum.map(fn row ->
        %Diagnostic{
          compiler_name: "dbos",
          file: row.file,
          position: row.line,
          severity: :hint,
          message:
            "@dbos_deterministic on #{format_mfa(row.mfa)} suppresses nothing: no workflow, " <>
              "step or transaction body reaches it.",
          details: %{rule: :unused_suppression, mfa: row.mfa, reason: row.reason}
        }
      end)

    unused_trusted =
      project_trusted
      |> Enum.reject(&reached_trusted?(&1, reached))
      |> Enum.map(fn entry ->
        %Diagnostic{
          compiler_name: "dbos",
          file: "mix.exs",
          position: 0,
          severity: :hint,
          message:
            "dbos: [trusted: [...]] lists #{format_trusted(entry)}, which no workflow, step or " <>
              "transaction body reaches.",
          details: %{rule: :unused_trusted, entry: entry}
        }
      end)

    from_annotations ++ unused_trusted
  end

  defp unscannable(rows, reached, app_modules) do
    reached_modules = MapSet.new(reached, &elem(&1, 0))

    rows
    |> Enum.filter(&MapSet.member?(app_modules, &1.module))
    |> Enum.filter(&MapSet.member?(reached_modules, &1.module))
    |> Enum.uniq_by(& &1.module)
    |> Enum.map(fn row ->
      %Diagnostic{
        compiler_name: "dbos",
        file: row.file,
        position: 0,
        severity: :hint,
        message:
          "#{inspect(row.module)} was compiled without debug info, so it was not scanned for " <>
            "a blocking receive. A workflow reaches it.",
        details: %{rule: :unscannable, module: row.module}
      }
    end)
  end

  defp reached_trusted?(module, reached) when is_atom(module) do
    Enum.any?(reached, fn {reached_module, _fun, _arity} -> reached_module == module end)
  end

  defp reached_trusted?(mfa, reached), do: MapSet.member?(reached, mfa)

  defp format_trusted(module) when is_atom(module), do: inspect(module)
  defp format_trusted(mfa), do: format_mfa(mfa)

  defp normalize_trusted(list) do
    MapSet.new(list, fn
      {module, fun, arity} when is_atom(module) and is_atom(fun) and is_integer(arity) ->
        {module, fun, arity}

      module when is_atom(module) ->
        module
    end)
  end
end
