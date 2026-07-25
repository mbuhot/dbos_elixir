defmodule Dbos.Macros do
  @moduledoc """
  `use Dbos` brings in `defstep/2`, `deftransaction/2`, and `defworkflow/2`.
  `defstep`/`deftransaction` wrap a plain function body in `Dbos.Runtime.run_step/3` /
  `Dbos.transaction/3`; `defworkflow` additionally runs `Dbos.Determinism.check!/2` over the body
  at compile time and generates a durable dispatcher.

  `defworkflow` calls are captured and processed once, in `@before_compile`, alongside every
  other `defworkflow` in the module: anything that must see every `defworkflow` in the module —
  the determinism checker's repo/warn config, and duplicate-name-or-arity detection — runs after
  every form has pushed onto a real `@attr`, once compile-time side effects across all of the
  module's top-level forms are guaranteed visible.

  A bare call to a `defworkflow`-defined function is durable, but its return type depends on
  where it's called from: inside another workflow it's a child workflow, and the call blocks for
  and returns the child's result; called from ordinary code (a controller, a test, an `iex`
  session) it starts a root workflow and returns `{:ok, %Dbos.WorkflowHandle{}}` immediately,
  without blocking the caller for however long the workflow takes to finish — await it explicitly
  with `Dbos.await/2` when the result is needed.

  `use Dbos` options: `:repo` — the module direct calls to which are banned inside a workflow
  body (see `docs/determinism.md`); `:warn_cross_module_calls` (default `true`) — set `false` to
  suppress the undeclared-cross-module-call warning for the whole module.
  """

  defmacro __using__(opts) do
    repo = Keyword.get(opts, :repo)
    warn_cross_module_calls = Keyword.get(opts, :warn_cross_module_calls, true)

    quote do
      import Dbos.Macros,
        only: [
          defstep: 2,
          defstep: 3,
          deftransaction: 2,
          deftransaction: 3,
          defworkflow: 2,
          defworkflow: 3
        ]

      Module.register_attribute(__MODULE__, :dbos_steps, accumulate: true)
      Module.register_attribute(__MODULE__, :dbos_workflow_defs, accumulate: true)
      @dbos_repo unquote(repo)
      @dbos_warn_cross_module_calls unquote(warn_cross_module_calls)

      @before_compile Dbos.Macros
    end
  end

  defmacro __before_compile__(env) do
    steps = env.module |> Module.get_attribute(:dbos_steps, []) |> List.wrap() |> Enum.reverse()

    workflow_defs =
      env.module |> Module.get_attribute(:dbos_workflow_defs, []) |> List.wrap() |> Enum.reverse()

    repo = Module.get_attribute(env.module, :dbos_repo)

    warn_cross_module_calls =
      Module.get_attribute(env.module, :dbos_warn_cross_module_calls, true)

    reject_duplicate_workflows!(workflow_defs, env)
    reject_ambiguous_opts_arity!(workflow_defs, env)

    built = Enum.map(workflow_defs, &build_workflow(&1, env, repo, warn_cross_module_calls))
    workflow_asts = Enum.map(built, &elem(&1, 0))
    workflows_meta = Enum.map(built, &elem(&1, 1))
    schedules_meta = built |> Enum.map(&elem(&1, 2)) |> Enum.reject(&is_nil/1)

    quote do
      unquote_splicing(workflow_asts)

      @doc "This module's registered workflows: `[{name, {module, function, arity}}]`."
      def __dbos_workflows__, do: unquote(Macro.escape(workflows_meta))

      @doc "This module's registered cron schedules: `[%{schedule_name:, workflow_name:, cron:, ...}]`."
      def __dbos_schedules__, do: unquote(Macro.escape(schedules_meta))

      @doc false
      def __dbos_steps__, do: unquote(Macro.escape(steps))
    end
  end

  @doc """
  Wraps `call`'s body in `Dbos.Runtime.run_step/3`. The step name defaults to `"name/arity"`;
  override with `name:`. Any other option (`:max_retries`, `:base_interval_ms`,
  `:backoff_factor`, `:max_interval_ms`) is forwarded to `Dbos.RetryPolicy`.
  """
  defmacro defstep(call, do_block), do: build_step(call, do_block)
  defmacro defstep(call, opts, do_block), do: build_step(call, Keyword.merge(opts, do_block))

  @doc """
  Wraps `call`'s body in `Dbos.transaction/3`. The step name defaults to `"name/arity"`; override
  with `name:`. `opts[:isolation]` is forwarded to `Dbos.transaction/3`.
  """
  defmacro deftransaction(call, do_block), do: build_transaction(call, do_block)

  defmacro deftransaction(call, opts, do_block),
    do: build_transaction(call, Keyword.merge(opts, do_block))

  @doc """
  Defines a durable workflow. `name:` is required (recovery dispatches on it). Generates three
  functions: the workflow body (run by the engine, under a generated internal name), a public
  dispatcher under `call`'s own name/arity, and a second public dispatcher at one arity higher,
  taking every declared argument explicitly (no default-argument skipping) plus a trailing
  options keyword list — `:workflow_id`, `:priority`, `:deduplication_id`,
  `:application_version` outside a workflow; `:workflow_id`, `:application_version` for the child
  call this becomes inside one. A pinned `:workflow_id` is the usual way to make a start
  idempotent. Both dispatchers' return type depends on where they're called from — this
  asymmetry is deliberate: inside a workflow they start a child workflow, await it, and return
  the unwrapped result (the same value the call would have produced as a plain function); outside
  a workflow they start a root workflow and return `{:ok, %Dbos.WorkflowHandle{}}` immediately,
  without blocking the caller for the workflow's lifetime — await it explicitly with
  `Dbos.await/2` when the result is needed. With the engine not started, `Dbos.start/3` raises
  `Dbos.NotStartedError` on either path.

  The options dispatcher's arity (declared argument count + 1) is always strictly greater than
  every arity a default argument can produce, so it never collides with the bare dispatcher; a
  second `defworkflow` in the same module whose own declared arity happens to equal that
  arity is rejected at compile time instead of silently misdispatching.

  Runs `Dbos.Determinism.check!/2` over the body at compile time. Does not support a `when` guard
  on the head (a workflow's name must map to exactly one deterministic body — see
  `docs/determinism.md`); does support default arguments.

  `schedule:` declares this workflow as cron-scheduled (`Dbos.Scheduler`, `workflow_schedules`).
  Either a bare cron string (`"0 * * * * *"`,
  six fields: second minute hour day-of-month month day-of-week — see `Dbos.Cron`), or a keyword
  list: `cron:` (required), `name:` (the schedule's own name, default this workflow's name),
  `automatic_backfill:` (default `false`), `timezone:`, `queue_name:`, `context:` (a compile-time
  literal passed as this workflow's second argument on every fire; the first is always the fired
  occurrence's scheduled epoch-ms, so a missed window can be backfilled deterministically). The
  workflow function itself must therefore take exactly `(scheduled_time_ms, context)`.
  """
  defmacro defworkflow(call, do_block), do: capture_workflow(call, do_block, __CALLER__)

  defmacro defworkflow(call, opts, do_block) do
    capture_workflow(call, Keyword.merge(opts, do_block), __CALLER__)
  end

  @doc """
  Runtime support for a bare workflow call. Inside a workflow context: starts `name` with `args`
  (and `opts`) as a child workflow, blocks for its result, and returns the unwrapped value or
  re-raises the recorded exception. Outside a workflow context: starts `name` with `args` (and
  `opts`) as a root workflow and returns `{:ok, %Dbos.WorkflowHandle{}}` immediately, without
  awaiting it. With the engine not started, `Dbos.start/3` raises `Dbos.NotStartedError` on
  either path.
  """
  def dispatch_workflow(name, args, opts \\ []) do
    if Dbos.Runtime.in_workflow?() do
      {:ok, handle} = Dbos.start(name, args, opts)

      case Dbos.await(handle) do
        {:ok, value} -> value
        {:error, exception} -> raise exception
      end
    else
      Dbos.start(name, args, opts)
    end
  end

  defp build_step(call, opts) do
    {block, extra_opts} = Keyword.pop!(opts, :do)
    {fun_name, arity} = head_name_arity(call)
    step_name = Keyword.get(extra_opts, :name, "#{fun_name}/#{arity}")
    run_opts = Keyword.delete(extra_opts, :name)

    quote do
      @dbos_steps {unquote(step_name), {unquote(fun_name), unquote(arity)}}
      def unquote(call), do: unquote(wrap_run_step(step_name, run_opts, block))
    end
  end

  defp build_transaction(call, opts) do
    {block, extra_opts} = Keyword.pop!(opts, :do)
    {fun_name, arity} = head_name_arity(call)
    step_name = Keyword.get(extra_opts, :name, "#{fun_name}/#{arity}")
    run_opts = Keyword.delete(extra_opts, :name)

    quote do
      @dbos_steps {unquote(step_name), {unquote(fun_name), unquote(arity)}}
      def unquote(call), do: unquote(wrap_transaction(step_name, run_opts, block))
    end
  end

  defp wrap_run_step(step_name, run_opts, block) do
    quote do
      Dbos.Runtime.run_step(unquote(step_name), unquote(run_opts), fn ->
        Dbos.Telemetry.span_step(unquote(step_telemetry_metadata(step_name)), fn ->
          unquote(block)
        end)
      end)
    end
  end

  defp wrap_transaction(step_name, run_opts, block) do
    quote do
      Dbos.transaction(unquote(step_name), unquote(run_opts), fn _conn ->
        Dbos.Telemetry.span_step(unquote(step_telemetry_metadata(step_name)), fn ->
          unquote(block)
        end)
      end)
    end
  end

  defp step_telemetry_metadata(step_name) do
    quote do
      %{
        function_name: unquote(step_name),
        workflow_id: if(Dbos.Runtime.in_workflow?(), do: Dbos.Runtime.current_workflow_id())
      }
    end
  end

  defp capture_workflow(call, opts, env) do
    quote do
      @dbos_workflow_defs {unquote(Macro.escape(call)), unquote(Macro.escape(opts)),
                           unquote(env.line), Module.get_attribute(__MODULE__, :doc)}

      Module.delete_attribute(__MODULE__, :doc)
    end
  end

  defp build_workflow({call, opts, line, doc}, env, repo, warn_cross_module_calls) do
    {block, extra_opts} = Keyword.pop!(opts, :do)

    reject_guard!(call, env, line)
    {fun_name, args, arity} = workflow_head_info(call)
    name = fetch_required_name!(extra_opts, fun_name, arity, env, line)

    Dbos.Determinism.check!(block, %{
      env: env,
      workflow_name: name,
      repo: repo,
      warn_cross_module_calls: warn_cross_module_calls
    })

    body_fun = body_function_name(fun_name)
    dispatcher_head = build_dispatcher_head(fun_name, args)
    forward_args = dispatcher_forward_args(dispatcher_head)
    body_head = {body_fun, [], strip_defaults(args)}

    {opts_dispatcher_head, opts_forward_args, opts_var} =
      build_opts_dispatcher_head(fun_name, args)

    ast =
      quote do
        @doc false
        def unquote(body_head), do: unquote(block)

        unquote(doc_attribute_ast(doc))

        def unquote(dispatcher_head) do
          Dbos.Macros.dispatch_workflow(unquote(name), unquote(forward_args))
        end

        def unquote(opts_dispatcher_head) do
          Dbos.Macros.dispatch_workflow(
            unquote(name),
            unquote(opts_forward_args),
            unquote(opts_var)
          )
        end
      end

    schedule_meta = build_schedule_meta(Keyword.get(extra_opts, :schedule), name)

    {ast, {name, {env.module, body_fun, arity}, block}, schedule_meta}
  end

  defp doc_attribute_ast(nil), do: nil
  defp doc_attribute_ast({_line, content}), do: quote(do: @doc(unquote(content)))

  defp build_schedule_meta(nil, _workflow_name), do: nil

  defp build_schedule_meta(cron, workflow_name) when is_binary(cron) do
    %{
      schedule_name: workflow_name,
      workflow_name: workflow_name,
      cron: cron,
      automatic_backfill: false,
      cron_timezone: nil,
      queue_name: nil,
      context: nil
    }
  end

  defp build_schedule_meta(opts, workflow_name) when is_list(opts) do
    %{
      schedule_name: Keyword.get(opts, :name, workflow_name),
      workflow_name: workflow_name,
      cron: Keyword.fetch!(opts, :cron),
      automatic_backfill: Keyword.get(opts, :automatic_backfill, false),
      cron_timezone: Keyword.get(opts, :timezone),
      queue_name: Keyword.get(opts, :queue_name),
      context: Keyword.get(opts, :context)
    }
  end

  defp reject_duplicate_workflows!(workflow_defs, env) do
    workflow_defs
    |> Enum.map(fn {call, _opts, line, _doc} ->
      {fun_name, _args, arity} = workflow_head_info(call)
      {{fun_name, arity}, line}
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.each(fn
      {{_fun_name, _arity}, [_one]} ->
        :ok

      {{fun_name, arity}, lines} ->
        raise CompileError,
          file: env.file,
          line: Enum.max(lines),
          description:
            "defworkflow #{fun_name}/#{arity} is declared more than once in " <>
              "#{inspect(env.module)} (lines #{Enum.join(Enum.sort(lines), ", ")}). Multiple " <>
              "defworkflow clauses for the same name/arity are not supported — each workflow " <>
              "name must map to exactly one body. Give each a distinct name/arity, or fold the " <>
              "branching into a single #{fun_name}/#{arity} body."
    end)
  end

  defp head_name_arity({:when, _, [inner, _guard]}), do: head_name_arity(inner)
  defp head_name_arity({name, _, args}) when is_atom(name), do: {name, length(args || [])}

  defp workflow_head_info({name, _, args}) when is_atom(name) do
    args = args || []
    {name, args, length(args)}
  end

  defp reject_guard!(call, env, line) do
    case call do
      {:when, _, [{name, _, args}, _guard]} ->
        raise CompileError,
          file: env.file,
          line: line,
          description:
            "defworkflow #{name}/#{length(args || [])} cannot have a `when` guard: a workflow " <>
              "name must dispatch to exactly one deterministic body. Move the branching inside " <>
              "the workflow body instead."

      _other ->
        :ok
    end
  end

  defp fetch_required_name!(extra_opts, fun_name, arity, env, line) do
    case Keyword.fetch(extra_opts, :name) do
      {:ok, name} ->
        name

      :error ->
        raise CompileError,
          file: env.file,
          line: line,
          description:
            "defworkflow #{fun_name}/#{arity} requires a name: option — recovery dispatches on " <>
              "the workflow's name, not its module/function, so it cannot default. Add " <>
              "`name: \"#{fun_name}\"` (or another stable string) to the defworkflow declaration."
    end
  end

  @doc "The generated internal name a `defworkflow`'s body runs under: `fun_name` with `__dbos_workflow_body__` appended."
  def body_function_name(fun_name), do: :"#{fun_name}__dbos_workflow_body__"

  defp build_dispatcher_head(fun_name, args) do
    dispatcher_args =
      args
      |> Enum.with_index()
      |> Enum.map(fn
        {{:\\, meta, [_pattern, default]}, index} ->
          {:\\, meta, [Macro.var(:"arg#{index}", nil), default]}

        {_pattern, index} ->
          Macro.var(:"arg#{index}", nil)
      end)

    {fun_name, [], dispatcher_args}
  end

  defp build_opts_dispatcher_head(fun_name, args) do
    arg_vars =
      args
      |> Enum.with_index()
      |> Enum.map(fn {_pattern, index} -> Macro.var(:"arg#{index}", nil) end)

    opts_var = Macro.var(:dbos_opts, nil)
    {{fun_name, [], arg_vars ++ [opts_var]}, arg_vars, opts_var}
  end

  defp reject_ambiguous_opts_arity!(workflow_defs, env) do
    declared =
      Enum.map(workflow_defs, fn {call, _opts, _line, _doc} ->
        {fun_name, _args, arity} = workflow_head_info(call)
        {fun_name, arity}
      end)

    declared_set = MapSet.new(declared)

    Enum.each(workflow_defs, fn {call, _opts, line, _doc} ->
      {fun_name, _args, arity} = workflow_head_info(call)
      opts_arity = arity + 1

      if MapSet.member?(declared_set, {fun_name, opts_arity}) do
        raise CompileError,
          file: env.file,
          line: line,
          description:
            "defworkflow #{fun_name}/#{arity}'s generated options dispatcher would be " <>
              "#{fun_name}/#{opts_arity}, which collides with another declared defworkflow " <>
              "#{fun_name}/#{opts_arity} in #{inspect(env.module)}. Give one of them a " <>
              "different function name."
      end
    end)
  end

  defp dispatcher_forward_args({_fun_name, _meta, dispatcher_args}) do
    Enum.map(dispatcher_args, fn
      {:\\, _, [var, _default]} -> var
      var -> var
    end)
  end

  defp strip_defaults(args) do
    Enum.map(args, fn
      {:\\, _, [pattern, _default]} -> pattern
      pattern -> pattern
    end)
  end
end
