defmodule Dbos.Determinism.Rules do
  @moduledoc false

  @random_fix "generate the random value inside a step so it is checkpointed and replayed as a fixed value."
  @time_fix "read the current time inside a step instead."
  @date_fix "read the current date inside a step instead."
  @system_fix "use a step for a fresh value, or the workflow/step id the engine already gives you."
  @sleep_fix "use Dbos.sleep/1, the engine's durable sleep operation."
  @receive_fix "use the engine's durable send/recv operations (Dbos.send_message/recv_message)."
  @send_fix "use Dbos.send_message/4, the engine's durable send."
  @make_ref_fix "not needed — use the workflow/step ids the engine already gives you."
  @spawn_workflow_fix "use a step, or a child workflow, instead of a bare process."
  @task_workflow_fix "a Task does not inherit the workflow context, so steps called inside it silently skip checkpointing. Use a step, or a child workflow, for concurrency."
  @spawn_step_fix "run the work inline in this step, or move it into its own step, instead of a bare process."
  @task_step_fix "run the work inline in this step, or call a durable step/child workflow for real concurrency — a Task does not inherit the workflow context."

  @spawn_funs [:spawn, :spawn_link, :spawn_monitor]
  @workflow_task_funs [:async, :await, :async_stream, :start]
  @step_task_funs [:async, :await, :async_stream, :start, :start_link]
  @system_funs [:system_time, :os_time, :monotonic_time, :unique_integer]

  @doc """
  The rule matching a resolved `{module, function, arity}` under `scope` (`:workflow` or `:step`),
  or `nil`. Mirrors `match_ast/2` so the AST walk and the compilation tracer always agree.
  """
  def match_mfa(module, function, arity, scope)

  def match_mfa(Kernel, fun, arity, scope) when fun in @spawn_funs,
    do: rule(:spawn, "Kernel.#{fun}/#{arity}", spawn_fix(scope))

  def match_mfa(:erlang, fun, arity, scope) when fun in @spawn_funs,
    do: rule(:spawn, ":erlang.#{fun}/#{arity}", spawn_fix(scope))

  def match_mfa(Task, fun, arity, :workflow) when fun in @workflow_task_funs,
    do: rule(:task, "Task.#{fun}/#{arity}", @task_workflow_fix)

  def match_mfa(Task, fun, arity, :step) when fun in @step_task_funs,
    do: rule(:task, "Task.#{fun}/#{arity}", @task_step_fix)

  def match_mfa(:rand, fun, arity, :workflow),
    do: rule(:rand, ":rand.#{fun}/#{arity}", @random_fix)

  def match_mfa(DateTime, :utc_now, arity, :workflow),
    do: rule(:utc_now, "DateTime.utc_now/#{arity}", @time_fix)

  def match_mfa(NaiveDateTime, :utc_now, arity, :workflow),
    do: rule(:utc_now, "NaiveDateTime.utc_now/#{arity}", @time_fix)

  def match_mfa(Date, :utc_today, arity, :workflow),
    do: rule(:utc_today, "Date.utc_today/#{arity}", @date_fix)

  def match_mfa(System, fun, arity, :workflow) when fun in @system_funs,
    do: rule(:system_time, "System.#{fun}/#{arity}", @system_fix)

  def match_mfa(Process, :sleep, arity, :workflow),
    do: rule(:sleep, "Process.sleep/#{arity}", @sleep_fix)

  def match_mfa(Kernel, :send, 2, :workflow), do: rule(:send, "Kernel.send/2", @send_fix)
  def match_mfa(:erlang, :send, 2, :workflow), do: rule(:send, ":erlang.send/2", @send_fix)

  def match_mfa(Kernel, :make_ref, 0, :workflow),
    do: rule(:make_ref, "Kernel.make_ref/0", @make_ref_fix)

  def match_mfa(:erlang, :make_ref, 0, :workflow),
    do: rule(:make_ref, ":erlang.make_ref/0", @make_ref_fix)

  def match_mfa(_module, _fun, _arity, _scope), do: nil

  @doc """
  The rule matching an unexpanded AST `node` under `scope` (`:workflow` or `:step`), or `nil`.
  A direct call to the module passed as `use Dbos, repo:` is resolved separately, by
  `Dbos.Determinism`, since it needs the caller's `Macro.Env`.
  """
  def match_ast(node, scope)

  def match_ast({{:., _, [:rand, fun]}, _, args}, :workflow),
    do: rule(:rand, ":rand.#{fun}/#{length(args)}", @random_fix)

  def match_ast({{:., _, [{:__aliases__, _, [:DateTime]}, :utc_now]}, _, args}, :workflow),
    do: rule(:utc_now, "DateTime.utc_now/#{length(args)}", @time_fix)

  def match_ast({{:., _, [{:__aliases__, _, [:NaiveDateTime]}, :utc_now]}, _, args}, :workflow),
    do: rule(:utc_now, "NaiveDateTime.utc_now/#{length(args)}", @time_fix)

  def match_ast({{:., _, [{:__aliases__, _, [:Date]}, :utc_today]}, _, args}, :workflow),
    do: rule(:utc_today, "Date.utc_today/#{length(args)}", @date_fix)

  def match_ast({{:., _, [{:__aliases__, _, [:System]}, fun]}, _, args}, :workflow)
      when fun in @system_funs,
      do: rule(:system_time, "System.#{fun}/#{length(args)}", @system_fix)

  def match_ast({{:., _, [{:__aliases__, _, [:Process]}, :sleep]}, _, args}, :workflow),
    do: rule(:sleep, "Process.sleep/#{length(args)}", @sleep_fix)

  def match_ast({:receive, _, _clauses}, :workflow), do: rule(:receive, "receive/1", @receive_fix)

  def match_ast({:send, _, args}, :workflow) when is_list(args) and length(args) == 2,
    do: rule(:send, "send/2", @send_fix)

  def match_ast({{:., _, [{:__aliases__, _, [:Kernel]}, :send]}, _, args}, :workflow)
      when is_list(args) and length(args) == 2,
      do: rule(:send, "Kernel.send/2", @send_fix)

  def match_ast({:make_ref, _, args}, :workflow) when args in [[], nil],
    do: rule(:make_ref, "make_ref/0", @make_ref_fix)

  def match_ast({{:., _, [:erlang, :make_ref]}, _, _args}, :workflow),
    do: rule(:make_ref, ":erlang.make_ref/0", @make_ref_fix)

  def match_ast({fun, _, args}, scope) when fun in @spawn_funs and is_list(args),
    do: rule(:spawn, "#{fun}/#{length(args)}", spawn_fix(scope))

  def match_ast({{:., _, [{:__aliases__, _, [:Kernel]}, fun]}, _, args}, scope)
      when fun in @spawn_funs,
      do: rule(:spawn, "Kernel.#{fun}/#{length(args)}", spawn_fix(scope))

  def match_ast({{:., _, [{:__aliases__, _, [:Task]}, fun]}, _, args}, :workflow)
      when fun in @workflow_task_funs,
      do: rule(:task, "Task.#{fun}/#{length(args)}", @task_workflow_fix)

  def match_ast({{:., _, [{:__aliases__, _, [:Task]}, fun]}, _, args}, :step)
      when fun in @step_task_funs,
      do: rule(:task, "Task.#{fun}/#{length(args)}", @task_step_fix)

  def match_ast(_node, _scope), do: nil

  defp spawn_fix(:workflow), do: @spawn_workflow_fix
  defp spawn_fix(:step), do: @spawn_step_fix

  defp rule(id, target, fix), do: %{id: id, target: target, fix: fix}
end
