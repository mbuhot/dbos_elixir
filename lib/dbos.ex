defmodule Dbos do
  @moduledoc """
  The entry point for starting, awaiting, and managing durable workflows.

  `use Dbos` brings in the `defworkflow`, `defstep` and `deftransaction` macros (see
  `Dbos.Macros`). The functions here operate on workflows once they exist: starting and enqueuing
  them, awaiting their results, cancelling, resuming, retrying and forking them, and the messaging, event
  and stream primitives a workflow uses to communicate.

  Every function takes an engine name; `config/1` resolves that name to the running engine's
  `Dbos.Config`. A call that names none resolves one from the calling process — see
  `put_engine/1` — and falls back to `Dbos`.
  """

  alias Dbos.Client
  alias Dbos.Config
  alias Dbos.Debouncer
  alias Dbos.Messaging
  alias Dbos.Notifications
  alias Dbos.Options
  alias Dbos.Queue
  alias Dbos.Registry
  alias Dbos.Runtime
  alias Dbos.Serialization
  alias Dbos.StepNames
  alias Dbos.SystemDb
  alias Dbos.Uuid
  alias Dbos.WorkflowHandle
  alias Dbos.WorkflowStatus
  alias Dbos.WorkflowSup

  @engine_key :dbos_engine

  @doc """
  Brings in `defstep/2`, `deftransaction/2`, and `defworkflow/2` — see `Dbos.Macros`. `opts`:
  `:repo`.
  """
  defmacro __using__(opts) do
    quote do
      use Dbos.Macros, unquote(opts)
    end
  end

  @doc """
  Starts a workflow. `name_or_capture` is either a registered workflow name, or a capture
  (`&Mod.fun/n`) resolved through the registry via `Function.info/1`. `args` is the list of
  positional arguments to apply. Called from inside a workflow, this starts a *child* workflow:
  consumes a step id, defaults the child's id to `"<parent_id>-<parent_step_id>"`, and replaying
  the parent does not start the child a second time.

  Reach for this to dispatch a workflow by name; a `defworkflow`'s generated options dispatcher
  (`Dbos.Macros`) is the call form for dispatching by function.

  `opts`: `:workflow_id`, `:engine` (default `Dbos`), `:deduplication_id`, `:priority`,
  `:application_version`, `:timeout_ms`. Any other key raises
  `Dbos.InvalidWorkflowOptionError`.
  """
  def start(name_or_capture, args, opts \\ []) do
    Options.validate_start!(name_or_capture, opts)

    if Runtime.in_workflow?() do
      config = Runtime.current_config()
      {name, mfa} = resolve_workflow(config.name, name_or_capture)
      start_child_workflow(config, config.name, name, mfa, args, opts)
    else
      engine = engine(opts)
      config = config(engine)
      {name, mfa} = resolve_workflow(engine, name_or_capture)
      start_root_workflow(config, engine, name, mfa, args, opts)
    end
  end

  @doc """
  Enqueues a workflow onto `opts[:queue_name]` rather than starting it directly: the row is
  inserted `ENQUEUED` (or `DELAYED` if `opts[:delay_ms]` is a positive integer, promoted to
  `ENQUEUED` once it elapses) and the queue's runner
  claims and dispatches it later. `opts`: `:queue_name` (required), `:engine` (default `Dbos`),
  `:workflow_id`, `:priority` (default `0`, lower runs first), `:deduplication_id`,
  `:partition_key`, `:delay_ms`, `:application_version`, `:timeout_ms`. `:deduplication_id` and
  `:partition_key` are mutually exclusive, and any other key raises
  `Dbos.InvalidWorkflowOptionError`. Raises `Dbos.QueueDeduplicatedError` if `:deduplication_id`
  is already held by another workflow on the same queue.

  Reach for this to enqueue a workflow by name, and inside a workflow to enqueue a child and carry
  on without waiting for it; a `defworkflow`'s generated options dispatcher (`Dbos.Macros`) is the
  call form for dispatching by function.

  Called from inside a workflow, this consumes a step id and checkpoints a `"DBOS.enqueue"` step,
  so replaying the parent does not enqueue a second copy. Outside a workflow, no id is allocated
  and nothing is checkpointed.

  Under an `:inline` testing-mode engine (`Dbos.Supervisor`'s `:testing` option), the enqueued
  workflow is drained and run synchronously, in the caller, before this returns — the handle
  refers to an already-finished workflow. Under `:manual`, the row is only inserted; nothing
  runs until `Dbos.Testing.drain_queue/2` or `Dbos.Testing.drain_all/1` is called.
  """
  def enqueue(name_or_capture, args, opts \\ []) do
    Options.validate_enqueue!(name_or_capture, opts)

    {engine, config} = engine_and_config(opts)
    {name, _mfa} = resolve_workflow(engine, name_or_capture)
    queue_name = Keyword.fetch!(opts, :queue_name)

    workflow_id =
      Runtime.run_step(StepNames.enqueue(), [], fn ->
        params = %{
          workflow_id: Keyword.get_lazy(opts, :workflow_id, &Uuid.v4/0),
          name: name,
          queue_name: queue_name,
          inputs: args,
          priority: Keyword.get(opts, :priority, 0),
          deduplication_id: Keyword.get(opts, :deduplication_id),
          queue_partition_key: Keyword.get(opts, :partition_key),
          delay_ms: Keyword.get(opts, :delay_ms),
          application_version:
            Keyword.get(opts, :application_version, config.application_version),
          workflow_timeout_ms: Keyword.get(opts, :timeout_ms)
        }

        {:ok, workflow_id} = SystemDb.insert_enqueued_workflow(config, params)
        workflow_id
      end)

    if config.testing == :inline, do: Dbos.Testing.drain_queue(queue_name, engine: engine)

    {:ok, %WorkflowHandle{engine: engine, workflow_id: workflow_id}}
  end

  @doc """
  Collapses rapid, repeated enqueues of the same logical unit of work into one delayed workflow.
  Each call either starts a fresh `DELAYED` workflow keyed by `opts[:debounce_key]`, or bounces
  one still waiting out its delay: replacing its inputs with `args` and pushing its wake time
  forward by `opts[:period_ms]`.

  `name_or_capture` and `args` mean what they do in `enqueue/3`. `opts`: `:queue_name`
  (required), `:debounce_key` (required), `:period_ms` (required), `:deadline_ms` (optional — an
  absolute ceiling on the total delay, fixed at the first call and never extended by a later
  bounce), `:engine` (default `Dbos`).

  Returns `{:ok, handle}` referring to the same workflow id across every bounce while the key is
  held. Raises `Dbos.QueueDeduplicatedError` if the key is currently held by a plain (non-debounced)
  `:deduplication_id` enqueue, or by a debounced workflow under a different name.

  Called from inside a workflow, this consumes a step id and checkpoints a `"DBOS.debounce"` step,
  so replaying the parent does not bounce a second time.
  """
  def debounce(name_or_capture, args, opts \\ []) do
    {engine, config} = engine_and_config(opts)
    {name, _mfa} = resolve_workflow(engine, name_or_capture)

    workflow_id =
      Runtime.run_step(StepNames.debounce(), [], fn ->
        {:ok, workflow_id} = Debouncer.debounce(config, name, args, opts)
        workflow_id
      end)

    {:ok, %WorkflowHandle{engine: engine, workflow_id: workflow_id}}
  end

  @doc """
  Waits until `handle`'s workflow reaches a terminal status, returning `{:ok, output}`,
  `{:error, exception}`, or `{:error, :timeout}`. Wakes on `Dbos.Notifications.notify_status/2`,
  fired in-process the moment this engine durably records the outcome, falling back to polling
  every `opts[:poll_interval_ms]` (default `100`) — the transport for a workflow finished by a
  different engine instance. `opts[:timeout_ms]`, if given, bounds how long this call waits.

  Called from inside a workflow, this consumes a step id and checkpoints a `"DBOS.getResult"` step
  only after the wait completes: a non-consuming peek at the next id is taken first, and replaying
  a completed await replays the checkpointed outcome without waiting again. A `{:error, :timeout}`
  outcome is never checkpointed. Outside a workflow, no id is allocated and nothing is checkpointed.
  """
  def await(%WorkflowHandle{} = handle, opts \\ []) do
    if Runtime.in_workflow?() do
      await_within_workflow(handle, opts)
    else
      poll_handle_outcome(handle, opts)
    end
  end

  defp poll_handle_outcome(handle, opts) do
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, 100)
    timeout_ms = Keyword.get(opts, :timeout_ms)
    config = config(handle.engine)
    deadline = timeout_ms && System.monotonic_time(:millisecond) + timeout_ms

    poll_for_outcome(config, handle.engine, handle.workflow_id, poll_interval_ms, deadline)
  end

  defp await_within_workflow(handle, opts) do
    config = Runtime.current_config()
    workflow_id = Runtime.current_workflow_id()
    refuse_in_step!("await workflow #{handle.workflow_id}")
    peeked_id = Runtime.peek_next_function_id()

    case SystemDb.check_operation_execution(
           config,
           workflow_id,
           peeked_id,
           StepNames.get_result()
         ) do
      {:replay, output} ->
        Runtime.next_function_id()
        {:ok, output}

      {:replay_failure, %{value: exception}} ->
        Runtime.next_function_id()
        {:error, exception}

      :none ->
        started_at = System.os_time(:millisecond)
        outcome = poll_handle_outcome(handle, opts)
        checkpoint_get_result(config, workflow_id, handle.workflow_id, started_at, outcome)
    end
  end

  defp checkpoint_get_result(
         _config,
         _workflow_id,
         _child_workflow_id,
         _started_at,
         {:error, :timeout} = outcome
       ) do
    outcome
  end

  defp checkpoint_get_result(config, workflow_id, child_workflow_id, started_at, outcome) do
    function_id = Runtime.next_function_id()
    completed_at = System.os_time(:millisecond)

    attrs =
      case outcome do
        {:ok, output} -> %{output: Serialization.encode(output)}
        {:error, exception} -> %{error: Serialization.encode_failure(:error, exception, [])}
      end

    SystemDb.record_operation_result(
      config,
      Map.merge(
        %{
          workflow_id: workflow_id,
          function_id: function_id,
          function_name: StepNames.get_result(),
          child_workflow_id: child_workflow_id,
          started_at: started_at,
          completed_at: completed_at
        },
        attrs
      )
    )

    outcome
  end

  @doc """
  Sends `message` to `destination_id` on `topic` (`nil` normalizes to the null-topic sentinel).
  A durable, checkpointed step inside a workflow; a direct write outside one. Named
  `send_message` (not `send`) to avoid reading like a call to `Kernel.send/2` at the call site.
  `opts[:engine]` (default `Dbos`) is only consulted outside a workflow.
  """
  def send_message(destination_id, topic, message, opts \\ []) do
    config = workflow_or_engine_config(opts)
    Messaging.send_message(config, destination_id, topic, message)
  end

  @doc """
  Receives a message on `topic`, blocking up to `timeout_ms`. Must be called from inside a
  workflow. Named `recv_message` to read unambiguously alongside `send_message/4`.
  """
  def recv_message(topic, timeout_ms, _opts \\ []) do
    Messaging.recv_message(Runtime.current_config(), topic, timeout_ms)
  end

  @doc "Sets `key` to `value` in this workflow's event store. Must be called from inside a workflow."
  def set_event(key, value, _opts \\ []) do
    Messaging.set_event(Runtime.current_config(), key, value)
  end

  @doc """
  Reads `key` from `target_workflow_id`'s event store, blocking up to `timeout_ms` (`nil` for no
  timeout), returning `nil` if it's never set. Works both inside and outside a workflow;
  `opts[:engine]` (default `Dbos`) is only consulted outside one.
  """
  def get_event(target_workflow_id, key, timeout_ms \\ nil, opts \\ []) do
    config = workflow_or_engine_config(opts)
    Messaging.get_event(config, target_workflow_id, key, timeout_ms)
  end

  @doc "Appends `value` to this workflow's stream `key`. Must be called from inside a workflow."
  def write_stream(key, value, _opts \\ []) do
    Messaging.write_stream(Runtime.current_config(), key, value)
  end

  @doc "Writes the close sentinel for this workflow's stream `key`. Must be called from inside a workflow."
  def close_stream(key, _opts \\ []) do
    Messaging.close_stream(Runtime.current_config(), key)
  end

  @doc """
  An Elixir `Stream` over `workflow_id`'s stream `key`, from the beginning, terminating once the
  close sentinel is read. `opts[:engine]` defaults to `Dbos`.
  """
  def read_stream(workflow_id, key, opts \\ []) do
    Messaging.read_stream(config(engine(opts)), workflow_id, key)
  end

  @doc """
  Durably sleeps for `ms`: checkpoints the absolute wake time under one step, then waits only
  the remaining interval, so a recovered workflow does not wait the full duration again. Must be
  called from inside a workflow.
  """
  def sleep(ms) do
    Messaging.sleep(Runtime.current_config(), ms)
  end

  @doc """
  Checks whether `patch_name`'s new code path should run. Must be called from inside a workflow
  context, and never from inside a step. Peeks (non-consuming) at the next step id and checks
  `operation_outputs` for a checkpoint there:

  - No row → a brand-new workflow, or an old workflow that has not yet reached this point.
    Writes `"DBOS.patch-<patch_name>"` at that id and returns `true`, consuming the id.
  - A row already recorded with that same `function_name` → a replay of an execution that
    already took the patched path. Returns `true` again, consuming the id the same way.
  - A row recorded with a different `function_name` → an execution that already ran past this
    point under the old code. Returns `false` without consuming an id, so its original step-id
    sequence stays intact and the new code this patch guards is skipped on replay.
  """
  def patch(patch_name) do
    config = patch_config!(patch_name)
    workflow_id = Runtime.current_workflow_id()
    peeked_id = Runtime.peek_next_function_id()
    function_name = StepNames.patch(patch_name)

    case SystemDb.patch(config, workflow_id, peeked_id, function_name) do
      true ->
        Runtime.next_function_id()
        true

      false ->
        false
    end
  end

  @doc """
  Retires `patch_name` at the call site `patch/1` occupied, once every execution that predates
  the patch has drained and the code it guarded runs unconditionally. Must be called from inside
  a workflow context, and never from inside a step. Peeks (non-consuming) at the next step id and
  checks `operation_outputs` for a checkpoint there:

  - A row already recorded with `"DBOS.patch-<patch_name>"` → a replay of an execution that took
    the patched path while the marker was still being written. Consumes the id, so the steps
    after it line up with the ids that execution recorded.
  - No row → a brand-new workflow, or an old workflow that has not yet reached this point.
    Writes nothing and consumes no id, so the marker is absent from this execution's sequence
    and the following step takes the id the marker used to hold.
  - A row recorded with a different `function_name` → an execution that ran past this point
    without the marker. Consumes no id, so its original step-id sequence stays intact.

  Returns `:ok`.
  """
  def deprecate_patch(patch_name) do
    config = patch_config!(patch_name)
    workflow_id = Runtime.current_workflow_id()
    peeked_id = Runtime.peek_next_function_id()
    function_name = StepNames.patch(patch_name)

    if SystemDb.deprecate_patch(config, workflow_id, peeked_id, function_name) do
      Runtime.next_function_id()
    end

    :ok
  end

  defp patch_config!(patch_name) do
    config = Runtime.current_config()

    case Runtime.current_frame() do
      :workflow_body ->
        config

      enclosing ->
        raise Dbos.PatchInStepError,
          workflow_id: Runtime.current_workflow_id(),
          patch_name: patch_name,
          enclosing: enclosing
    end
  end

  @doc """
  Runs `fun` as a one-off durable step named `name`, without a `defstep`: the escape hatch for a
  step that does not deserve its own named function. Same checkpoint/replay semantics as
  `defstep` — see `Dbos.Runtime.run_step/3`. `opts` (default `[]`): `:max_retries`,
  `:base_interval_ms`, `:backoff_factor`, `:max_interval_ms`, forwarded to `Dbos.RetryPolicy`.
  """
  def step(name, fun) when is_function(fun, 0), do: step(name, [], fun)

  def step(name, opts, fun) when is_function(fun, 0) do
    Runtime.run_step(name, opts, fun)
  end

  @doc """
  Runs `fun.(conn)` as a durable transactional step named `name`: the user's writes made through
  `conn` and the step's `operation_outputs` checkpoint commit together in one database
  transaction. Must be called from inside a workflow. Raises
  `Dbos.NestedTransactionError` from inside another transaction's body, and
  `Dbos.OperationInStepError` if a durable operation other than a step runs inside this one.
  A step called from here is folded in, taking no id and writing no checkpoint of its own.
  `opts[:isolation]`: `:read_committed` | `:repeatable_read` | `:serializable`.
  """
  def transaction(name, opts \\ [], fun) do
    Runtime.run_transaction(name, opts, fun)
  end

  @doc """
  Fails the current workflow deliberately, raising `Dbos.AbortError` with `reason`.

  The workflow reaches `ERROR` and its unwind is enqueued in the same transaction, so every step
  that declared `compensate:` is reversed. Use it where the business transaction cannot go on but
  nothing has actually gone wrong — a declined payment, a rejected approval — so the unwind reads
  as a decision rather than a bug. Callable from a workflow body or a step; the exception
  propagates either way.
  """
  def abort(reason), do: raise(Dbos.AbortError, reason: reason)

  @doc """
  Starts the workflow that unwinds `workflow_id`: every step of its history that declared
  `compensate:` is run in reverse. Returns `{:ok, %Dbos.WorkflowHandle{}}` without waiting for it;
  `Dbos.await/2` blocks for the count of undos it ran.

  The compensator's id is derived from `workflow_id`, so calling this twice converges on one
  workflow rather than unwinding the same history twice. Called from inside a workflow, it starts a
  child — which is how a workflow unwinds a descendant of its own.

  Unwind a workflow that has reached a terminal status. Its history is what the walk reads, and a
  workflow still running may yet add to it.
  """
  def unwind(workflow_id, opts \\ []) do
    start(
      Dbos.Compensation.workflow_name(),
      [workflow_id],
      Keyword.put(opts, :workflow_id, Dbos.Compensation.workflow_id(workflow_id))
    )
  end

  @doc """
  Marks `workflow_id` `CANCELLED`, waking a live process so a blocked wait ends promptly.

  A workflow that is still running and has compensable effects goes to `CANCELLING` instead: it
  stops at its next checkpoint, reverses what it did, and reaches `CANCELLED` once the unwind is
  enqueued. `Dbos.await/2` keeps waiting through that, since `CANCELLING` is not terminal. A
  workflow with nothing to compensate is cancelled outright.

  `opts[:cancel_children]` (default `false`) cancels its descendant tree in the same
  transaction. Cancelling a terminal workflow is a no-op. Inside a workflow this consumes a
  step id and checkpoints; outside one it consumes none.
  """
  def cancel(workflow_id, opts \\ []) do
    {engine, config} = engine_and_config(opts)
    cancel_children? = Keyword.get(opts, :cancel_children, false)

    Runtime.run_step(StepNames.cancel_workflow(), [], fn ->
      config
      |> cancel_with_descendants(workflow_id, cancel_children?)
      |> Enum.each(&wake_live_process(engine, &1))
    end)

    :ok
  end

  defp cancel_with_descendants(config, workflow_id, false) do
    SystemDb.cancel_workflows(config, [workflow_id])
  end

  defp cancel_with_descendants(config, workflow_id, true) do
    {:ok, cancelled} =
      config.db.transaction(config.conn, [], fn conn ->
        tx_config = %{config | conn: conn}
        descendant_ids = SystemDb.descendant_workflow_ids(tx_config, workflow_id)
        SystemDb.cancel_workflows(tx_config, [workflow_id | descendant_ids])
      end)

    cancelled
  end

  defp wake_live_process(engine, workflow_id) do
    case WorkflowSup.whereis(engine, workflow_id) do
      {:ok, pid} -> send(pid, {:dbos_notify, :cancelled, workflow_id})
      :error -> :ok
    end
  end

  @doc """
  Resumes `workflow_id` from its last checkpoint: clears its queue assignment and deadline and
  re-enqueues it onto `opts[:queue_name]` (default `Dbos.Queue.internal_queue_name/0`). Resuming a
  workflow already `SUCCESS`/`ERROR` is a silent no-op (the row is left unchanged); `retry/2`
  restarts an `ERROR` workflow. `opts[:engine]` defaults to `Dbos`.

  Called from inside a workflow, this consumes a step id and checkpoints a
  `"DBOS.resumeWorkflow"` step, so replaying the caller does not attempt to resume a second time.
  Outside a workflow, no id is allocated and nothing is checkpointed.
  """
  def resume(workflow_id, opts \\ []) do
    {_engine, config} = engine_and_config(opts)
    queue_name = Keyword.get(opts, :queue_name, Queue.internal_queue_name())

    Runtime.run_step(StepNames.resume_workflow(), [], fn ->
      SystemDb.resume_workflows(config, [workflow_id], queue_name: queue_name)
    end)

    :ok
  end

  @doc """
  Restarts a failed `workflow_id` from its last checkpoint: clears the recorded error, resets
  `recovery_attempts` to `0`, clears the queue assignment and deadline, and re-enqueues it onto
  `opts[:queue_name]` (default `Dbos.Queue.internal_queue_name/0`). `opts[:engine]` defaults to
  `Dbos`.

  Acts only on the statuses in `Dbos.Status.retryable/0` — `ERROR`, `CANCELLED`,
  `MAX_RECOVERY_ATTEMPTS_EXCEEDED`. A `SUCCESS` workflow, whose output its callers have already
  read, and a workflow that is still live (`PENDING`/`ENQUEUED`/`DELAYED`) are both left
  untouched, which also makes two concurrent calls on the same id start it exactly once.

  Every step already checkpointed is skipped on replay. A step whose own failure was checkpointed
  re-raises that failure on replay; `fork/3` re-runs from that step under a new id.

  Called from inside a workflow, this consumes a step id and checkpoints a `"DBOS.retryWorkflow"`
  step, so replaying the caller does not retry a second time. Outside a workflow, no id is
  allocated and nothing is checkpointed.
  """
  def retry(workflow_id, opts \\ []) do
    {_engine, config} = engine_and_config(opts)
    queue_name = Keyword.get(opts, :queue_name, Queue.internal_queue_name())

    Runtime.run_step(StepNames.retry_workflow(), [], fn ->
      SystemDb.retry_workflows(config, [workflow_id], queue_name: queue_name)
    end)

    :ok
  end

  @doc """
  Forks `workflow_id` from step `start_step`: copies every checkpoint before `start_step` into a
  new workflow (`opts[:new_workflow_id]`, default a fresh random id), marks the original
  `was_forked_from`, and enqueues the fork so it re-runs starting at `start_step`. `opts`:
  `:new_workflow_id`, `:queue_name` (default the internal queue),
  `:application_version`, `:engine` (default `Dbos`).

  Called from inside a workflow, this consumes a step id and checkpoints a `"DBOS.forkWorkflow"`
  step, so replaying the caller does not fork a second time. Outside a workflow, no id is
  allocated and nothing is checkpointed.
  """
  def fork(workflow_id, start_step, opts \\ []) do
    {engine, config} = engine_and_config(opts)

    new_workflow_id =
      Runtime.run_step(StepNames.fork_workflow(), [], fn ->
        new_workflow_id = Keyword.get_lazy(opts, :new_workflow_id, &Uuid.v4/0)

        SystemDb.fork_workflow(
          config,
          workflow_id,
          start_step,
          Keyword.put(opts, :new_workflow_id, new_workflow_id)
        )
      end)

    {:ok, %WorkflowHandle{engine: engine, workflow_id: new_workflow_id}}
  end

  defp workflow_or_engine_config(opts) do
    if Runtime.in_workflow?(), do: Runtime.current_config(), else: config(engine(opts))
  end

  # A step is one checkpoint, so a workflow dispatched from inside one would consume a step id
  # that replay never consumes again. Calling a step from a step is fine — it becomes part of
  # the caller's execution rather than a checkpoint of its own.
  defp refuse_in_step!(operation) do
    case Runtime.enclosing_body() do
      nil ->
        :ok

      {kind, name} ->
        raise Dbos.OperationInStepError,
          workflow_id: Runtime.current_workflow_id(),
          kind: kind,
          name: name,
          operation: operation
    end
  end

  defp engine_and_config(opts) do
    if Runtime.in_workflow?() do
      config = Runtime.current_config()
      {config.name, config}
    else
      engine = engine(opts)
      {engine, config(engine)}
    end
  end

  @doc "The resolved config for this process's engine. Raises `Dbos.NotStartedError` if it has not started."
  def config, do: config(current_engine())

  @doc """
  Points this process at `engine`, so every later call that does not name one uses it.

  A call resolves its engine in three steps: an explicit `opts[:engine]`, then this
  process-local setting, then `Dbos`. Processes this one spawns inherit it through
  `$callers`, the same path `Ecto.Adapters.SQL.Sandbox` uses to find a connection owner, so
  a `Task` started mid-call reaches the same engine.

  A deployment with one engine never needs this. It is for a host that runs several — and
  for tests, where it lets each one own an engine and still call application code that names
  none, so those tests can be `async: true`.

  Returns the engine previously set for this process, or `nil`.
  """
  @spec put_engine(atom()) :: atom() | nil
  def put_engine(engine) when is_atom(engine), do: Process.put(@engine_key, engine)

  @doc "The engine this process resolves to when a call does not name one."
  @spec current_engine() :: atom()
  def current_engine do
    Process.get(@engine_key) || inherited_engine() || Dbos
  end

  defp inherited_engine do
    Enum.find_value(Process.get(:"$callers", []), fn pid ->
      case Process.info(pid, :dictionary) do
        {:dictionary, dictionary} -> Keyword.get(dictionary, @engine_key)
        nil -> nil
      end
    end)
  end

  @doc "The resolved config for the engine named `name`. Raises `Dbos.NotStartedError` if it has not started."
  def config(name) do
    case :persistent_term.get(config_key(name), :error) do
      :error -> raise Dbos.NotStartedError
      %Config{} = config -> config
    end
  end

  @doc "Stores `config` under its own `name`, making it reachable via `config/1`."
  def put_config(%Config{} = config) do
    :persistent_term.put(config_key(config.name), config)
    :ok
  end

  @doc """
  Fetches one workflow's status by id. `opts[:engine]` defaults to `Dbos`.

  Called from inside a workflow, this consumes a step id and checkpoints a `"DBOS.getStatus"`
  step, so replaying the caller returns the recorded status rather than reading the (possibly
  since-changed) live row. Outside a workflow, no id is allocated and nothing is checkpointed.
  """
  def status(workflow_id, opts \\ []) do
    Runtime.run_step(StepNames.get_status(), [], fn ->
      opts |> workflow_or_engine_config() |> Client.status(workflow_id)
    end)
  end

  @doc "Returns a workflow's outcome: `{:ok, term}`, `{:error, term}`, or `:pending`. `opts[:engine]` defaults to `Dbos`."
  def result(workflow_id, opts \\ []) do
    opts |> engine() |> config() |> Client.result(workflow_id)
  end

  defp engine(opts), do: Keyword.get(opts, :engine) || current_engine()
  defp config_key(name), do: {__MODULE__, :config, name}

  defp resolve_workflow(engine, name) when is_binary(name) do
    case Registry.lookup(engine, name) do
      {:ok, mfa} -> {name, mfa}
      :error -> raise "workflow #{inspect(name)} is not registered on engine #{inspect(engine)}"
    end
  end

  defp resolve_workflow(engine, capture) when is_function(capture) do
    info = Function.info(capture)
    mfa = {info[:module], info[:name], info[:arity]}

    case Registry.name_for_mfa(engine, mfa) do
      {:ok, name} ->
        {name, mfa}

      :error ->
        resolve_dispatcher_capture(engine, mfa)
    end
  end

  defp resolve_dispatcher_capture(engine, {module, fun_name, arity}) do
    body_mfa = {module, Dbos.Macros.body_function_name(fun_name), arity}

    case Registry.name_for_mfa(engine, body_mfa) do
      {:ok, name} ->
        {name, body_mfa}

      :error ->
        raise "workflow #{inspect({module, fun_name, arity})} is not registered on engine " <>
                "#{inspect(engine)}"
    end
  end

  defp start_root_workflow(config, engine, name, mfa, args, opts) do
    workflow_id = Keyword.get_lazy(opts, :workflow_id, &Uuid.v4/0)
    owner_xid = Uuid.v4()

    result =
      SystemDb.insert_workflow_status(config, %{
        workflow_id: workflow_id,
        status: :pending,
        name: name,
        inputs: args,
        deduplication_id: Keyword.get(opts, :deduplication_id),
        priority: Keyword.get(opts, :priority, 0),
        application_version: Keyword.get(opts, :application_version, config.application_version),
        workflow_timeout_ms: Keyword.get(opts, :timeout_ms),
        owner_xid: owner_xid
      })

    maybe_start_workflow(engine, workflow_id, mfa, args, owner_xid, result)

    {:ok, %WorkflowHandle{engine: engine, workflow_id: workflow_id}}
  end

  defp start_child_workflow(config, engine, name, mfa, args, opts) do
    parent_id = Runtime.current_workflow_id()
    refuse_in_step!("start workflow #{inspect(name)}")
    step_id = Runtime.next_function_id()

    case SystemDb.check_child_workflow(config, parent_id, step_id, name) do
      {:existing, child_id} ->
        {:ok, %WorkflowHandle{engine: engine, workflow_id: child_id}}

      :none ->
        child_id = Keyword.get(opts, :workflow_id, "#{parent_id}-#{step_id}")
        owner_xid = Uuid.v4()

        {:ok, result} =
          config.db.transaction(config.conn, [], fn conn ->
            tx_config = %{config | conn: conn}

            insert_result =
              SystemDb.insert_workflow_status(tx_config, %{
                workflow_id: child_id,
                status: :pending,
                name: name,
                inputs: args,
                parent_workflow_id: parent_id,
                application_version:
                  Keyword.get(opts, :application_version, config.application_version),
                workflow_deadline_epoch_ms: Runtime.current_deadline_epoch_ms(),
                owner_xid: owner_xid
              })

            now = System.os_time(:millisecond)

            SystemDb.record_operation_result(tx_config, %{
              workflow_id: parent_id,
              function_id: step_id,
              function_name: name,
              child_workflow_id: child_id,
              started_at: now,
              completed_at: now
            })

            insert_result
          end)

        maybe_start_workflow(engine, child_id, mfa, args, owner_xid, result)

        {:ok, %WorkflowHandle{engine: engine, workflow_id: child_id}}
    end
  end

  defp maybe_start_workflow(engine, workflow_id, mfa, args, owner_xid, result) do
    unless skip_start?(engine, workflow_id, owner_xid, result) do
      {:ok, _pid} = WorkflowSup.start_workflow(engine, workflow_id, mfa, args)
    end

    :ok
  end

  defp skip_start?(engine, workflow_id, owner_xid, result) do
    result.status in [:success, :error] or
      result.owner_xid != owner_xid or
      match?({:ok, _pid}, WorkflowSup.whereis(engine, workflow_id))
  end

  defp poll_for_outcome(config, engine, workflow_id, poll_interval_ms, deadline) do
    case SystemDb.get_workflow_status(config, workflow_id) do
      {:ok, %WorkflowStatus{status: :success, output: output}} ->
        {:ok, output}

      {:ok, %WorkflowStatus{status: :error, error: %{value: exception}}} ->
        {:error, exception}

      {:ok, %WorkflowStatus{status: :cancelled}} ->
        {:error, %Dbos.WorkflowCancelledError{workflow_id: workflow_id}}

      {:ok, %WorkflowStatus{status: :max_recovery_attempts_exceeded, recovery_attempts: attempts}} ->
        {:error,
         %Dbos.MaxRecoveryAttemptsExceededError{workflow_id: workflow_id, attempts: attempts}}

      {:ok, %WorkflowStatus{status: status}}
      when status in [:pending, :enqueued, :delayed, :cancelling] ->
        if config.testing in [:inline, :manual] do
          run_for_testing(config, engine, workflow_id, status, poll_interval_ms, deadline)
        else
          wait_or_timeout(config, engine, workflow_id, poll_interval_ms, deadline)
        end

      {:error, :not_found} ->
        wait_or_timeout(config, engine, workflow_id, poll_interval_ms, deadline)
    end
  end

  defp run_for_testing(config, engine, workflow_id, :delayed, poll_interval_ms, deadline) do
    SystemDb.transition_delayed_workflows(config)

    case SystemDb.get_workflow_status(config, workflow_id) do
      {:ok, %WorkflowStatus{status: :delayed}} ->
        raise Dbos.TestingModeAwaitError, workflow_id: workflow_id, status: :delayed

      _other ->
        poll_for_outcome(config, engine, workflow_id, poll_interval_ms, deadline)
    end
  end

  defp run_for_testing(config, engine, workflow_id, :enqueued, poll_interval_ms, deadline) do
    if Dbos.Testing.run_enqueued(config, workflow_id) do
      poll_for_outcome(config, engine, workflow_id, poll_interval_ms, deadline)
    else
      raise Dbos.TestingModeAwaitError, workflow_id: workflow_id, status: :enqueued
    end
  end

  defp run_for_testing(_config, _engine, workflow_id, status, _poll_interval_ms, _deadline)
       when status in [:pending, :cancelling] do
    raise Dbos.TestingModeAwaitError, workflow_id: workflow_id, status: status
  end

  defp wait_or_timeout(config, engine, workflow_id, poll_interval_ms, deadline) do
    if deadline && System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      :ok = Notifications.subscribe_status(engine, workflow_id)

      receive do
        {:dbos_notify, :status, ^workflow_id} -> :ok
      after
        wait_bound(poll_interval_ms, deadline) -> :ok
      end

      Notifications.unsubscribe_status(engine, workflow_id)
      poll_for_outcome(config, engine, workflow_id, poll_interval_ms, deadline)
    end
  end

  defp wait_bound(poll_interval_ms, nil), do: poll_interval_ms

  defp wait_bound(poll_interval_ms, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)
    max(min(remaining, poll_interval_ms), 0)
  end
end
