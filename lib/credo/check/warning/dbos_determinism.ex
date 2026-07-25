if Code.ensure_loaded?(Credo.Check) do
  defmodule Credo.Check.Warning.DbosDeterminism do
    @moduledoc """
    Finds nondeterministic constructs that a `defworkflow`/`defstep` body reaches through a helper
    function defined in the same module.

    The `defworkflow` and `defstep` macros walk the literal `do` block they are handed, so a banned
    construct hidden one call deep is invisible to them:

        defworkflow process(id), name: "process" do
          fan_out(id)
        end

        defp fan_out(id), do: Task.async_stream(id, &work/1)

    This check builds a call graph of the `def`/`defp` functions in each module and walks it from
    every workflow, step and transaction body, reporting the banned constructs it reaches. The
    banned-construct table is `Dbos.Determinism`'s, so the two checkers always agree.

    Scope: same-module reachability only. A helper that lives in another module, a call made
    through `apply/3`, and a call through a function value the checker cannot resolve to a local
    definition are all invisible to this check. The literal workflow and step bodies are skipped,
    since the compile-time checker already rejects those.
    """

    use Credo.Check,
      base_priority: :high,
      category: :warning,
      explanations: [
        check: """
        A workflow body must be deterministic on replay, and so must every same-module helper it
        calls. The compile-time checker in the `defworkflow` macro only sees the literal `do`
        block, so this check follows local calls transitively and reports the banned constructs
        they reach.

        Same-module reachability only: helpers in other modules, and calls made through `apply/3`
        or an unresolvable function value, are out of scope.
        """
      ]

    @entry_kinds [:defworkflow, :defstep, :deftransaction]
    @definition_kinds [:def, :defp]

    @doc false
    @impl true
    def run(%SourceFile{} = source_file, params) do
      issue_meta = IssueMeta.for(source_file, params)

      source_file
      |> Credo.Code.ast()
      |> module_bodies()
      |> Enum.flat_map(&module_issues(&1, issue_meta))
    end

    defp module_bodies({:ok, ast}), do: collect_module_bodies(ast, [])
    defp module_bodies(_error), do: []

    defp collect_module_bodies({:defmodule, _, [_alias, body_opts]}, acc)
         when is_list(body_opts) do
      case Keyword.fetch(body_opts, :do) do
        {:ok, body} -> Enum.reduce(statements(body), [body | acc], &collect_module_bodies/2)
        :error -> acc
      end
    end

    defp collect_module_bodies(_node, acc), do: acc

    defp module_issues(module_body, issue_meta) do
      statements = statements(module_body)
      definitions = definitions(statements)
      entries = entries(statements)
      entry_signatures = MapSet.new(entries, & &1.signature)

      Enum.flat_map(entries, &entry_issues(&1, definitions, entry_signatures, issue_meta))
    end

    defp statements({:__block__, _, statements}), do: statements
    defp statements(statement), do: [statement]

    defp definitions(statements) do
      statements
      |> Enum.flat_map(&declaration(&1, @definition_kinds))
      |> Enum.reduce(%{}, fn {_kind, signature, body}, acc ->
        Map.update(acc, signature, [body], &[body | &1])
      end)
    end

    defp entries(statements) do
      statements
      |> Enum.flat_map(&declaration(&1, @entry_kinds))
      |> Enum.map(fn {kind, signature, body} ->
        %{kind: kind, signature: signature, body: body}
      end)
    end

    defp declaration({kind, _, args}, kinds) when is_atom(kind) and is_list(args) do
      with true <- kind in kinds,
           [call | [_ | _] = rest] <- args,
           body_opts when is_list(body_opts) <- List.last(rest),
           {:ok, body} <- Keyword.fetch(body_opts, :do),
           signature when not is_nil(signature) <- signature(call) do
        [{kind, signature, body}]
      else
        _other -> []
      end
    end

    defp declaration(_statement, _kinds), do: []

    defp signature({:when, _, [call, _guard]}), do: signature(call)

    defp signature({name, _, args}) when is_atom(name) and is_list(args),
      do: {name, length(args)}

    defp signature({name, _, nil}) when is_atom(name), do: {name, 0}
    defp signature(_other), do: nil

    defp entry_issues(entry, definitions, entry_signatures, issue_meta) do
      entry.body
      |> local_calls(definitions)
      |> reachable(definitions, entry_signatures, MapSet.new(), [])
      |> Enum.flat_map(fn {signature, body} ->
        body_issues(body, signature, entry, issue_meta)
      end)
    end

    defp reachable([], _definitions, _entry_signatures, _seen, acc), do: Enum.reverse(acc)

    defp reachable([signature | rest], definitions, entry_signatures, seen, acc) do
      if MapSet.member?(seen, signature) or MapSet.member?(entry_signatures, signature) do
        reachable(rest, definitions, entry_signatures, MapSet.put(seen, signature), acc)
      else
        bodies = Map.fetch!(definitions, signature)
        callees = Enum.flat_map(bodies, &local_calls(&1, definitions))
        found = Enum.map(bodies, &{signature, &1})

        reachable(
          rest ++ callees,
          definitions,
          entry_signatures,
          MapSet.put(seen, signature),
          Enum.reverse(found) ++ acc
        )
      end
    end

    defp local_calls(body, definitions) do
      {_ast, calls} = Macro.prewalk(body, [], &collect_local_call(&1, &2, definitions))
      Enum.reverse(calls)
    end

    defp collect_local_call({:&, _, [{:/, _, [{name, _, _}, arity]}]} = node, acc, definitions)
         when is_atom(name) and is_integer(arity) do
      {node, maybe_local({name, arity}, acc, definitions)}
    end

    defp collect_local_call({name, _, args} = node, acc, definitions)
         when is_atom(name) and is_list(args) do
      {node, maybe_local({name, length(args)}, acc, definitions)}
    end

    defp collect_local_call(node, acc, _definitions), do: {node, acc}

    defp maybe_local(signature, acc, definitions) do
      if Map.has_key?(definitions, signature), do: [signature | acc], else: acc
    end

    defp body_issues(body, signature, entry, issue_meta) do
      {_ast, issues} =
        Macro.prewalk(body, [], fn node, acc ->
          case banned_construct(entry.kind, node) do
            nil -> {node, acc}
            {description, fix} -> {node, [{node, description, fix} | acc]}
          end
        end)

      Enum.map(Enum.reverse(issues), fn {node, description, fix} ->
        format_issue(issue_meta,
          message: message(entry, signature, description, fix),
          trigger: description,
          line_no: line_of(node)
        )
      end)
    end

    defp banned_construct(:defworkflow, node),
      do: Dbos.Determinism.workflow_banned_construct(node)

    defp banned_construct(_step_or_transaction, node),
      do: Dbos.Determinism.step_banned_construct(node)

    defp message(entry, {helper_name, helper_arity}, description, fix) do
      {entry_name, entry_arity} = entry.signature

      "#{kind_label(entry.kind)} #{entry_name}/#{entry_arity} reaches #{description} through " <>
        "#{helper_name}/#{helper_arity}, which is nondeterministic and breaks replay on " <>
        "recovery. #{fix}"
    end

    defp kind_label(:defworkflow), do: "workflow"
    defp kind_label(:defstep), do: "step"
    defp kind_label(:deftransaction), do: "transaction"

    defp line_of({_, meta, _}) when is_list(meta), do: Keyword.get(meta, :line, 1)
    defp line_of(_node), do: 1
  end
end
