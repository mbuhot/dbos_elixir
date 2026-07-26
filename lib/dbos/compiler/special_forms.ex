defmodule Dbos.Compiler.SpecialForms do
  @moduledoc """
  Finds banned special forms in a compiled module's abstract code.

  A special form is not a call, so it raises no tracer event and leaves no edge in the call
  graph. `receive` is the one the determinism rule table bans, in both its plain and its
  `after` shape. The `:on_module` trace event carries the module's bytecode, whose debug-info
  chunk yields the Erlang Abstract Format with real source lines.

  Each occurrence becomes an edge from the enclosing function to `Kernel.SpecialForms.receive/1`,
  so the reachability walk reports it with the same witness chain as any traced call.

  A module compiled without debug info yields `:no_debug_info`.
  """

  @receive_mfa {Kernel.SpecialForms, :receive, 1}

  @doc "The synthetic call-graph node standing for a `receive`."
  def receive_mfa, do: @receive_mfa

  @doc """
  Every banned special form in `bytecode`, as `{{name, arity}, mfa, line}` tuples naming the
  enclosing function. `:no_debug_info` when the module carries no abstract code.
  """
  def scan(bytecode) do
    case :beam_lib.chunks(bytecode, [:abstract_code]) do
      {:ok, {_module, [abstract_code: {:raw_abstract_v1, forms}]}} ->
        {:ok, Enum.flat_map(forms, &function_occurrences/1)}

      _other ->
        :no_debug_info
    end
  end

  defp function_occurrences({:function, _anno, name, arity, clauses}) do
    clauses
    |> walk([])
    |> Enum.reverse()
    |> Enum.uniq()
    |> Enum.map(fn {mfa, line} -> {{name, arity}, mfa, line} end)
  end

  defp function_occurrences(_form), do: []

  defp walk({:receive, anno, clauses}, acc) do
    walk(clauses, [{@receive_mfa, line(anno)} | acc])
  end

  defp walk({:receive, anno, clauses, timeout, after_body}, acc) do
    walk([clauses, timeout, after_body], [{@receive_mfa, line(anno)} | acc])
  end

  defp walk(node, acc) when is_tuple(node), do: walk(Tuple.to_list(node), acc)
  defp walk(nodes, acc) when is_list(nodes), do: Enum.reduce(nodes, acc, &walk/2)
  defp walk(_node, acc), do: acc

  defp line(anno), do: :erl_anno.line(anno)
end
