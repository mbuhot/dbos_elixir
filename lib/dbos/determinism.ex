defmodule Dbos.Determinism do
  @moduledoc """
  Compile-time determinism checker over a `defworkflow` body's AST, per `docs/determinism.md`.
  Raises a `CompileError` naming the offending call, its file and line, and the fix, for anything
  in the banned-construct table. Separately emits a suppressible `IO.warn/2` for a call to a
  public function in another module that is not a registered `defstep`/`deftransaction`, since
  that is where undeclared side effects hide.
  """

  @allowed_pure_modules [
    Kernel,
    Enum,
    Map,
    MapSet,
    String,
    List,
    Tuple,
    Keyword,
    Integer,
    Float,
    Atom,
    Base,
    Bitwise,
    Regex,
    URI,
    Range,
    Stream,
    Access,
    Function,
    Record
  ]

  @doc """
  Walks `body` (the workflow's raw do-block AST) and raises `CompileError` on the first banned
  construct found (collecting every violation into one message). `context`: `:env` (the
  `Macro.Env` at the `defworkflow` call site), `:workflow_name`, `:repo` (module banned from
  direct calls, or `nil`), `:warn_cross_module_calls` (boolean).
  """
  def check!(body, context) do
    violations = Macro.prewalk(body, [], &collect_violation(&1, &2, context)) |> elem(1)

    case Enum.reverse(violations) do
      [] ->
        :ok

      messages ->
        raise CompileError, file: context.env.file, description: Enum.join(messages, "\n\n")
    end

    maybe_warn_cross_module_calls(body, context)
    :ok
  end

  @doc """
  The banned-construct entry `{description, fix}` for `node` in a workflow body, or `nil` when the
  node is allowed. Repo-call detection needs a `Macro.Env`, so it is not covered here.
  """
  def workflow_banned_construct(node), do: banned(node, %{repo: nil})

  @doc "The banned-construct entry `{description, fix}` for `node` in a step body, or `nil`."
  def step_banned_construct(node), do: step_banned(node)

  defp collect_violation(node, acc, context) do
    case banned(node, context) do
      nil -> {node, acc}
      {description, fix} -> {node, [format_violation(context, node, description, fix) | acc]}
    end
  end

  defp format_violation(context, node, description, fix) do
    line = line_of(node)

    "#{context.env.file}:#{line}: workflow #{inspect(context.workflow_name)} calls #{description}, " <>
      "which is nondeterministic and breaks replay on recovery.\n    #{fix}"
  end

  defp line_of({_, meta, _}) when is_list(meta), do: Keyword.get(meta, :line, 0)
  defp line_of(_), do: 0

  defp banned({{:., _, [:rand, fun]}, _, args}, _context) do
    {":rand.#{fun}/#{length(args)}",
     "generate the random value inside a step so it is checkpointed and replayed as a fixed value."}
  end

  defp banned({{:., _, [{:__aliases__, _, [:DateTime]}, :utc_now]}, _, args}, _context) do
    {"DateTime.utc_now/#{length(args)}", "read the current time inside a step instead."}
  end

  defp banned({{:., _, [{:__aliases__, _, [:NaiveDateTime]}, :utc_now]}, _, args}, _context) do
    {"NaiveDateTime.utc_now/#{length(args)}", "read the current time inside a step instead."}
  end

  defp banned({{:., _, [{:__aliases__, _, [:Date]}, :utc_today]}, _, args}, _context) do
    {"Date.utc_today/#{length(args)}", "read the current date inside a step instead."}
  end

  defp banned({{:., _, [{:__aliases__, _, [:System]}, fun]}, _, args}, _context)
       when fun in [:system_time, :os_time, :monotonic_time, :unique_integer] do
    {"System.#{fun}/#{length(args)}",
     "use a step for a fresh value, or the workflow/step id the engine already gives you."}
  end

  defp banned({{:., _, [{:__aliases__, _, [:Process]}, :sleep]}, _, args}, _context) do
    {"Process.sleep/#{length(args)}", "use Dbos.sleep/1, the engine's durable sleep operation."}
  end

  defp banned({:receive, _, _clauses}, _context) do
    {"receive/1",
     "use the engine's durable send/recv operations (Dbos.send_message/recv_message)."}
  end

  defp banned({fun, _, args}, _context)
       when fun in [:spawn, :spawn_link, :spawn_monitor] and is_list(args) do
    {"#{fun}/#{length(args)}", "use a step, or a child workflow, instead of a bare process."}
  end

  defp banned({{:., _, [{:__aliases__, _, [:Kernel]}, fun]}, _, args}, _context)
       when fun in [:spawn, :spawn_link, :spawn_monitor] do
    {"Kernel.#{fun}/#{length(args)}",
     "use a step, or a child workflow, instead of a bare process."}
  end

  defp banned({{:., _, [{:__aliases__, _, [:Task]}, fun]}, _, args}, _context)
       when fun in [:async, :await, :async_stream, :start] do
    {"Task.#{fun}/#{length(args)}",
     "a Task does not inherit the workflow context, so steps called inside it silently skip " <>
       "checkpointing. Use a step, or a child workflow, for concurrency."}
  end

  defp banned({:send, _, args}, _context) when is_list(args) and length(args) == 2 do
    {"send/2", "use Dbos.send_message/4, the engine's durable send."}
  end

  defp banned({{:., _, [{:__aliases__, _, [:Kernel]}, :send]}, _, args}, _context)
       when is_list(args) and length(args) == 2 do
    {"Kernel.send/2", "use Dbos.send_message/4, the engine's durable send."}
  end

  defp banned({:make_ref, _, args}, _context) when args in [[], nil] do
    {"make_ref/0", "not needed — use the workflow/step ids the engine already gives you."}
  end

  defp banned({{:., _, [:erlang, :make_ref]}, _, _args}, _context) do
    {":erlang.make_ref/0", "not needed — use the workflow/step ids the engine already gives you."}
  end

  defp banned({{:., _, [mod_ast, fun]}, _, args} = node, %{repo: repo} = context)
       when not is_nil(repo) do
    if resolve_module(mod_ast, context.env) == repo do
      {"#{inspect(repo)}.#{fun}/#{length(args)}",
       "wrap the call in a deftransaction, so it commits atomically with its checkpoint."}
    else
      banned_fallthrough(node, context)
    end
  end

  defp banned(node, context), do: banned_fallthrough(node, context)

  defp banned_fallthrough(_node, _context), do: nil

  defp resolve_module({:__aliases__, _, _} = alias_ast, env) do
    case Macro.expand(alias_ast, env) do
      mod when is_atom(mod) -> mod
      _other -> nil
    end
  end

  defp resolve_module(mod, _env) when is_atom(mod), do: mod
  defp resolve_module(_other, _env), do: nil

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

  defp collect_step_violation(node, acc, context) do
    case step_banned(node) do
      nil -> {node, acc}
      {description, fix} -> {node, [format_step_violation(context, node, description, fix) | acc]}
    end
  end

  defp format_step_violation(context, node, description, fix) do
    line = line_of(node)

    "#{context.env.file}:#{line}: step #{inspect(context.step_name)} calls #{description}, " <>
      "which runs in a new process with no workflow context — any durable call made from " <>
      "inside it takes the passthrough path and silently skips its checkpoint.\n    #{fix}"
  end

  defp step_banned({fun, _, args})
       when fun in [:spawn, :spawn_link, :spawn_monitor] and is_list(args) do
    {"#{fun}/#{length(args)}",
     "run the work inline in this step, or move it into its own step, instead of a bare process."}
  end

  defp step_banned({{:., _, [{:__aliases__, _, [:Kernel]}, fun]}, _, args})
       when fun in [:spawn, :spawn_link, :spawn_monitor] do
    {"Kernel.#{fun}/#{length(args)}",
     "run the work inline in this step, or move it into its own step, instead of a bare process."}
  end

  defp step_banned({{:., _, [{:__aliases__, _, [:Task]}, fun]}, _, args})
       when fun in [:async, :await, :async_stream, :start, :start_link] do
    {"Task.#{fun}/#{length(args)}",
     "run the work inline in this step, or call a durable step/child workflow for real " <>
       "concurrency — a Task does not inherit the workflow context."}
  end

  defp step_banned(_node), do: nil

  defp maybe_warn_cross_module_calls(_body, %{warn_cross_module_calls: false}), do: :ok

  defp maybe_warn_cross_module_calls(body, context) do
    Macro.prewalk(body, fn node -> warn_if_undeclared(node, context) end)
    :ok
  end

  defp warn_if_undeclared({{:., meta, [mod_ast, fun]}, _, args} = node, context) do
    with resolved_mod when not is_nil(resolved_mod) <- resolve_module(mod_ast, context.env),
         true <- resolved_mod != context.env.module,
         true <- not pure_module?(resolved_mod),
         true <- not dbos_module?(resolved_mod),
         false <- registered_step?(resolved_mod, fun, length(args)) do
      line = Keyword.get(meta, :line, 0)

      IO.warn(
        "workflow #{inspect(context.workflow_name)} calls #{inspect(resolved_mod)}.#{fun}/" <>
          "#{length(args)}, a public function that is not a registered step. Undeclared side " <>
          "effects here will not be checkpointed and will re-run in full on every replay. " <>
          "Wrap it in a defstep/deftransaction if it has side effects, or pass " <>
          "warn_cross_module_calls: false to `use Dbos` to suppress this check.",
        file: context.env.file,
        line: line
      )
    end

    node
  end

  defp warn_if_undeclared(node, _context), do: node

  @dbos_primitive_modules [Dbos, Dbos.Runtime, Dbos.StepNames]

  defp pure_module?(mod), do: mod in @allowed_pure_modules

  defp dbos_module?(mod), do: mod in @dbos_primitive_modules

  defp registered_step?(mod, fun, arity) do
    Code.ensure_loaded?(mod) and function_exported?(mod, :__dbos_steps__, 0) and
      Enum.any?(mod.__dbos_steps__(), fn {_name, {step_fun, step_arity}} ->
        step_fun == fun and step_arity == arity
      end)
  end
end
