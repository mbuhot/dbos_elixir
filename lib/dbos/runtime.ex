defmodule Dbos.Runtime do
  @moduledoc """
  The durable-step protocol, and the workflow context ambient to a running workflow.

  `run_step/3` is what `defstep` expands to, and is callable directly for a one-off step that
  does not warrant its own named function. `current_workflow_id/0`, `in_workflow?/0` and friends
  report on the workflow the calling process is currently executing.
  """

  alias Dbos.Config
  alias Dbos.RetryPolicy
  alias Dbos.Serialization
  alias Dbos.SystemDb

  @context_key :dbos_workflow_context

  # One workflow's in-process state: its config, id, step-id counter, replay flag, deadline (if
  # any), and whether the calling frame is nested inside a step or a transaction body.
  defmodule Context do
    @moduledoc false

    defstruct [
      :config,
      :workflow_id,
      :replay,
      :deadline_epoch_ms,
      step_id: -1,
      in_step: false,
      in_transaction: false
    ]

    @type t :: %__MODULE__{
            config: Config.t(),
            workflow_id: String.t(),
            replay: boolean,
            deadline_epoch_ms: integer | nil,
            step_id: integer,
            in_step: String.t() | false,
            in_transaction: String.t() | false
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

  @doc "The active context's resolved deadline (epoch ms), or `nil` if none. Raises `Dbos.NotInWorkflowError` outside one."
  def current_deadline_epoch_ms, do: fetch_context!().deadline_epoch_ms

  @doc """
  Which frame the caller is running in: `:step`, `:transaction`, or `:workflow_body`. Raises
  `Dbos.NotInWorkflowError` outside an active context.
  """
  def current_frame do
    context = fetch_context!()

    cond do
      context.in_transaction -> :transaction
      context.in_step -> :step
      true -> :workflow_body
    end
  end

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
  Returns the id that the next `next_function_id/0` call would allocate, without consuming it.
  Used by operations that must peek at a step's checkpoint before deciding whether to consume its
  id — currently `DBOS.getResult`. Raises `Dbos.NotInWorkflowError` outside an active context.
  """
  def peek_next_function_id, do: fetch_context!().step_id + 1

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
  (default `2.0`), `:max_interval_ms` (default `5000`), and `:compensation` — a zero-arity
  function returning this step's compensation record, called only on the branch that checkpoints
  a success.
  """
  def run_step(name, opts \\ [], fun)

  def run_step(name, opts, fun) do
    cond do
      not in_workflow?() ->
        fun.()

      enclosing_body() && not engine_operation?(name) ->
        fun.()

      true ->
        run_step_at(next_function_id(), name, opts, fun)
    end
  end

  # The engine's own primitives all record a reserved "DBOS." name (Dbos.StepNames); a user's
  # step never does. That is the line between an inner call folded into its caller and a durable
  # operation that would need a checkpoint of its own.
  defp engine_operation?(name), do: String.starts_with?(name, "DBOS.")

  @doc """
  The step or transaction body this process is inside as `{kind, name}`, or `nil` outside both.

  A `deftransaction` is a step that commits its checkpoint with its write, so both are single
  checkpoints and both refuse the same operations.
  """
  def enclosing_body do
    case Process.get(@context_key) do
      %Context{in_transaction: name} when is_binary(name) -> {:transaction, name}
      %Context{in_step: name} when is_binary(name) -> {:step, name}
      _other -> nil
    end
  end

  @doc """
  Like `run_step/3`, but against a `function_id` the caller has already allocated (via
  `next_function_id/0`) rather than allocating its own. Used by operations that must reserve
  more than one id up front regardless of which branch they end up taking — `recv` and
  `getEvent`'s internal timeout-sleep step. Must be called from within a workflow context.
  """
  def run_step_at(function_id, name, opts \\ [], fun) do
    context = fetch_context!()

    case enclosing_body() do
      nil ->
        :ok

      {kind, enclosing} ->
        raise Dbos.OperationInStepError,
          workflow_id: context.workflow_id,
          kind: kind,
          name: enclosing,
          operation: "call #{name}"
    end

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
    {compensation, retry_opts} = Keyword.pop(opts, :compensation)
    started_at = System.os_time(:millisecond)

    outcome =
      with_flag(:in_step, name, fn ->
        run_with_retries(context.workflow_id, name, retry_opts, fun)
      end)

    completed_at = System.os_time(:millisecond)

    case outcome do
      {:ok, value} ->
        record_step_success(
          context,
          function_id,
          name,
          value,
          started_at,
          completed_at,
          compensation
        )

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

  defp record_step_success(
         context,
         function_id,
         name,
         value,
         started_at,
         completed_at,
         compensation
       ) do
    SystemDb.record_operation_result(context.config, %{
      workflow_id: context.workflow_id,
      function_id: function_id,
      function_name: name,
      output: Serialization.encode(value),
      started_at: started_at,
      completed_at: completed_at,
      compensation: encoded_compensation(compensation, value)
    })
  end

  # Built here rather than at the call site so the record is written only when the step actually
  # checkpoints a success: a replayed step, a folded one, and a failed one all skip it, and the
  # bound arguments are never evaluated on those paths.
  defp encoded_compensation(nil, _value), do: nil

  defp encoded_compensation(compensation, value) when is_function(compensation, 0) do
    Serialization.encode(substitute_checkpoint(compensation.(), value))
  end

  defp substitute_checkpoint(%{undo: {module, function, args}} = record, value) do
    substituted =
      Enum.map(args, fn
        :__checkpoint__ -> value
        arg -> arg
      end)

    %{record | undo: {module, function, substituted}}
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

  @doc """
  Runs `fun.(conn)` as a durable transactional step named `name`: the user's writes made through
  `conn` and the `operation_outputs` checkpoint commit together in one `config.db.transaction/3`
  call on the system database's own connection/pool.

  Raises `Dbos.NestedTransactionError` from inside another transaction's body. Called from
  inside a plain step's body, runs a real transaction but records no separate durability row — it
  rides on the enclosing step's own checkpoint. `opts`: `:isolation` (`:read_committed` |
  `:repeatable_read` | `:serializable`, default the adapter's own default).
  """
  def run_transaction(name, opts \\ [], fun) do
    context = fetch_context!()

    cond do
      context.in_transaction ->
        raise Dbos.NestedTransactionError, workflow_id: context.workflow_id

      context.in_step ->
        run_transaction_within_step(context, opts, fun)

      true ->
        function_id = next_function_id()
        run_transaction_at(context, function_id, name, opts, fun)
    end
  end

  defp run_transaction_at(context, function_id, name, opts, fun) do
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
        execute_and_checkpoint_transaction(context, function_id, name, opts, fun)
    end
  end

  defp execute_and_checkpoint_transaction(context, function_id, name, opts, fun) do
    isolation = Keyword.get(opts, :isolation)
    compensation = Keyword.get(opts, :compensation)
    started_at = System.os_time(:millisecond)

    {:ok, value} =
      context.config.db.transaction(context.config.conn, [isolation: isolation], fn conn ->
        value = with_flag(:in_transaction, name, fn -> fun.(conn) end)
        completed_at = System.os_time(:millisecond)
        tx_config = %{context.config | conn: conn}

        SystemDb.record_operation_result(tx_config, %{
          workflow_id: context.workflow_id,
          function_id: function_id,
          function_name: name,
          output: Serialization.encode(value),
          started_at: started_at,
          completed_at: completed_at,
          compensation: encoded_compensation(compensation, value)
        })

        value
      end)

    value
  end

  defp run_transaction_within_step(context, opts, fun) do
    isolation = Keyword.get(opts, :isolation)

    {:ok, value} =
      context.config.db.transaction(context.config.conn, [isolation: isolation], fn conn ->
        with_flag(:in_transaction, true, fn -> fun.(conn) end)
      end)

    value
  end

  defp with_flag(key, value, fun) do
    context = fetch_context!()
    previous = Map.fetch!(context, key)
    Process.put(@context_key, Map.put(context, key, value))

    try do
      fun.()
    after
      restored = fetch_context!()
      Process.put(@context_key, Map.put(restored, key, previous))
    end
  end

  @doc """
  Resolves and arms this workflow's durable deadline: if a `workflow_timeout_ms` is set but no
  `workflow_deadline_epoch_ms` yet, computes and persists `now + timeout`; otherwise reuses
  whatever deadline is already recorded (so recovery/resume/fork do not restart the clock). If a
  deadline results, stores it on the active context (so a child workflow started from here
  inherits it).

  Outside `:inline`/`:manual` testing modes, also starts an unsupervised timer task that calls
  `Dbos.cancel/2` once the deadline passes — a no-op if the workflow has already reached a
  terminal status by then. Under those testing modes this task is never started: it would escape
  the calling process and could fire later against a connection the test's sandbox has already
  checked back in, or an engine that has already stopped. An `:inline`/`:manual` workflow runs to
  completion inside this one call, so a wall-clock deadline enforced by a background task has no
  meaningful effect there; `workflow_timeout_ms`/`workflow_deadline_epoch_ms` are still resolved
  and persisted the same way, so a test can assert on them.
  """
  def arm_deadline(config, workflow_id) do
    case SystemDb.resolve_workflow_deadline(config, workflow_id) do
      nil ->
        :ok

      deadline_ms ->
        context = fetch_context!()
        Process.put(@context_key, %{context | deadline_epoch_ms: deadline_ms})

        unless config.testing in [:inline, :manual] do
          start_deadline_task(config, workflow_id, deadline_ms)
        end

        :ok
    end
  end

  defp start_deadline_task(config, workflow_id, deadline_ms) do
    remaining_ms = max(deadline_ms - System.os_time(:millisecond), 0)
    engine = config.name

    Task.start(fn ->
      Process.sleep(remaining_ms)

      try do
        Dbos.cancel(workflow_id, engine: engine)
      rescue
        _ -> :ok
      catch
        _, _ -> :ok
      end
    end)
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
