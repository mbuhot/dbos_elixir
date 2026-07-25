defmodule Dbos.Runtime do
  @moduledoc """
  The workflow context and durable-step protocol. Workflow state (config, workflow id, the
  step-id counter, and replay status) lives in the process dictionary under one key, matching
  Ecto's own approach to threading a transaction, since a step body runs on the same process as
  the workflow that invoked it.
  """

  alias Dbos.Config
  alias Dbos.RetryPolicy
  alias Dbos.Serialization
  alias Dbos.SystemDb

  @context_key :dbos_workflow_context

  defmodule Context do
    @moduledoc "One workflow's in-process state: its config, id, step-id counter, and replay flag."

    defstruct [:config, :workflow_id, :replay, step_id: -1]

    @type t :: %__MODULE__{
            config: Config.t(),
            workflow_id: String.t(),
            replay: boolean,
            step_id: integer
          }
  end

  @doc "Whether the calling process is currently inside a workflow context."
  def in_workflow?, do: Process.get(@context_key) != nil

  @doc "The workflow id of the active context. Raises `Dbos.NotInWorkflowError` outside one."
  def current_workflow_id, do: fetch_context!().workflow_id

  @doc "The `Dbos.Config` of the active context. Raises `Dbos.NotInWorkflowError` outside one."
  def current_config, do: fetch_context!().config

  @doc "The last step id allocated in the active context. Raises `Dbos.NotInWorkflowError` outside one."
  def current_step_id, do: fetch_context!().step_id

  @doc """
  Allocates and returns the next step id, `0, 1, 2, ...` in call order. Raises
  `Dbos.NotInWorkflowError` outside an active context.
  """
  def next_function_id do
    context = fetch_context!()
    next_id = context.step_id + 1
    Process.put(@context_key, %{context | step_id: next_id})
    next_id
  end

  @doc """
  Establishes a workflow context around `fun`, restoring whatever context (if any) was active
  before this call once `fun` returns, raises, throws, or exits. `opts` is `:config`,
  `:workflow_id`, and optionally `:replay` (default `false`).
  """
  def with_context(opts, fun) do
    previous_context = Process.get(@context_key)

    context = %Context{
      config: Keyword.fetch!(opts, :config),
      workflow_id: Keyword.fetch!(opts, :workflow_id),
      replay: Keyword.get(opts, :replay, false)
    }

    Process.put(@context_key, context)

    try do
      fun.()
    after
      restore_context(previous_context)
    end
  end

  @doc """
  Runs `fun` as a durable step named `name`. Inside a workflow context: allocates the next
  step id, replays a checkpointed outcome if one is recorded, otherwise executes `fun` (with
  retries per `opts`) and checkpoints the outcome. Outside a workflow context: invokes `fun`
  directly and records nothing.

  `opts`: `:max_retries` (default `0`), `:base_interval_ms` (default `100`), `:backoff_factor`
  (default `2.0`), `:max_interval_ms` (default `5000`) — see `notes/steps-retry.md`.
  """
  def run_step(name, opts \\ [], fun)

  def run_step(name, opts, fun) do
    if in_workflow?() do
      function_id = next_function_id()
      run_step_at(function_id, name, opts, fun)
    else
      fun.()
    end
  end

  @doc """
  Like `run_step/3`, but against a `function_id` the caller has already allocated (via
  `next_function_id/0`) rather than allocating its own. Used by operations that must reserve
  more than one id up front regardless of which branch they end up taking — `recv` and
  `getEvent`'s internal timeout-sleep step, per `notes/step-ids.md`. Must be called from within a
  workflow context.
  """
  def run_step_at(function_id, name, opts \\ [], fun) do
    context = fetch_context!()

    case SystemDb.check_operation_execution(
           context.config,
           context.workflow_id,
           function_id,
           name
         ) do
      {:replay, output} ->
        output

      {:replay_failure, failure} ->
        Serialization.reraise_failure(failure)

      :none ->
        execute_and_checkpoint_step(context, function_id, name, opts, fun)
    end
  end

  defp execute_and_checkpoint_step(context, function_id, name, opts, fun) do
    started_at = System.os_time(:millisecond)
    outcome = run_with_retries(context.workflow_id, name, opts, fun)
    completed_at = System.os_time(:millisecond)

    case outcome do
      {:ok, value} ->
        record_step_success(context, function_id, name, value, started_at, completed_at)
        value

      {:failed, kind, value, stacktrace} ->
        record_step_failure(
          context,
          function_id,
          name,
          kind,
          value,
          stacktrace,
          started_at,
          completed_at
        )

        :erlang.raise(kind, value, stacktrace)
    end
  end

  defp record_step_success(context, function_id, name, value, started_at, completed_at) do
    SystemDb.record_operation_result(context.config, %{
      workflow_id: context.workflow_id,
      function_id: function_id,
      function_name: name,
      output: Serialization.encode(value),
      started_at: started_at,
      completed_at: completed_at
    })
  end

  defp record_step_failure(
         context,
         function_id,
         name,
         kind,
         value,
         stacktrace,
         started_at,
         completed_at
       ) do
    SystemDb.record_operation_result(context.config, %{
      workflow_id: context.workflow_id,
      function_id: function_id,
      function_name: name,
      error: Serialization.encode_failure(kind, value, stacktrace),
      started_at: started_at,
      completed_at: completed_at
    })
  end

  defp run_with_retries(workflow_id, name, opts, fun) do
    attempt(workflow_id, name, fun, RetryPolicy.new(opts), 1)
  end

  defp attempt(workflow_id, name, fun, policy, run) do
    case run_once(fun) do
      {:ok, value} ->
        {:ok, value}

      {:failed, kind, value, stacktrace} ->
        settle_attempt(workflow_id, name, fun, policy, run, kind, value, stacktrace)
    end
  end

  defp settle_attempt(workflow_id, name, fun, policy, run, kind, value, stacktrace) do
    if RetryPolicy.retry?(policy, run) do
      Process.sleep(RetryPolicy.delay_ms(policy, run))
      attempt(workflow_id, name, fun, policy, run + 1)
    else
      exhaust_retries(workflow_id, name, policy.max_retries, kind, value, stacktrace)
    end
  end

  defp exhaust_retries(_workflow_id, _name, max_retries, kind, value, stacktrace)
       when max_retries <= 0 do
    {:failed, kind, value, stacktrace}
  end

  defp exhaust_retries(workflow_id, name, max_retries, _kind, value, _stacktrace) do
    try do
      raise Dbos.MaxStepRetriesExceededError,
        workflow_id: workflow_id,
        function_name: name,
        max_retries: max_retries,
        cause: value
    rescue
      exception -> {:failed, :error, exception, __STACKTRACE__}
    end
  end

  defp run_once(fun) do
    try do
      {:ok, fun.()}
    rescue
      exception -> {:failed, :error, exception, __STACKTRACE__}
    catch
      kind, value -> {:failed, kind, value, __STACKTRACE__}
    end
  end

  defp fetch_context! do
    case Process.get(@context_key) do
      nil -> raise Dbos.NotInWorkflowError
      context -> context
    end
  end

  defp restore_context(nil), do: Process.delete(@context_key)
  defp restore_context(context), do: Process.put(@context_key, context)
end
