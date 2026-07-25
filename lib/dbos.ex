defmodule Dbos do
  @moduledoc """
  The public entry point for starting and awaiting durable workflows. A running engine's
  resolved `Dbos.Config` lives in `:persistent_term`, keyed by the engine's `name`, so this
  module and `Dbos.Runtime` never have to be told which engine they're talking to beyond that
  name.
  """

  alias Dbos.Client
  alias Dbos.Config
  alias Dbos.Messaging
  alias Dbos.Notifications
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

  @doc """
  Brings in `defstep/2`, `deftransaction/2`, and `defworkflow/2` — see `Dbos.Macros`. `opts`:
  `:repo`, `:warn_cross_module_calls` (default `true`).
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

  `opts`: `:workflow_id`, `:engine` (default `Dbos`), `:deduplication_id`, `:priority`,
  `:application_version`.
  """
  def start(name_or_capture, args, opts \\ []) do
    if Runtime.in_workflow?() do
      config = Runtime.current_config()
      {name, mfa} = resolve_workflow(config.name, name_or_capture)
      start_child_workflow(config, config.name, name, mfa, args, opts)
    else
      engine = Keyword.get(opts, :engine, Dbos)
      config = config(engine)
      {name, mfa} = resolve_workflow(engine, name_or_capture)
      start_root_workflow(config, engine, name, mfa, args, opts)
    end
  end

  @doc """
  Enqueues a workflow onto `opts[:queue_name]` rather than starting it directly: the row is
  inserted `ENQUEUED` (or `DELAYED` if `opts[:delay_ms]` is a positive integer, promoted to
  `ENQUEUED` by `Dbos.Queue.Runner`'s per-tick sweep once it elapses) and a `Dbos.Queue.Runner`
  claims and dispatches it later. `opts`: `:queue_name` (required), `:engine` (default `Dbos`),
  `:workflow_id`, `:priority` (default `0`, lower runs first), `:deduplication_id`,
  `:partition_key`, `:delay_ms`, `:application_version`. `:deduplication_id` and `:partition_key`
  are mutually exclusive. Raises `Dbos.QueueDeduplicatedError` if `:deduplication_id` is already
  held by another workflow on the same queue.
  """
  def enqueue(name_or_capture, args, opts \\ []) do
    if Keyword.has_key?(opts, :deduplication_id) and Keyword.has_key?(opts, :partition_key) do
      raise ArgumentError, "deduplication_id and partition_key cannot be used together"
    end

    engine = Keyword.get(opts, :engine, Dbos)
    config = config(engine)
    {name, _mfa} = resolve_workflow(engine, name_or_capture)
    queue_name = Keyword.fetch!(opts, :queue_name)

    params = %{
      workflow_id: Keyword.get_lazy(opts, :workflow_id, &Uuid.v4/0),
      name: name,
      queue_name: queue_name,
      inputs: args,
      priority: Keyword.get(opts, :priority, 0),
      deduplication_id: Keyword.get(opts, :deduplication_id),
      queue_partition_key: Keyword.get(opts, :partition_key),
      delay_ms: Keyword.get(opts, :delay_ms),
      application_version: Keyword.get(opts, :application_version, config.application_version),
      workflow_timeout_ms: Keyword.get(opts, :timeout_ms)
    }

    {:ok, workflow_id} = SystemDb.insert_enqueued_workflow(config, params)
    {:ok, %WorkflowHandle{engine: engine, workflow_id: workflow_id}}
  end

  @doc """
  Waits until `handle`'s workflow reaches a terminal status, returning `{:ok, output}`,
  `{:error, exception}`, or `{:error, :timeout}`. Wakes on `Dbos.Notifications.notify_status/2`
  (fired in-process by `Dbos.WorkflowProcess` right after it durably records an outcome) when
  possible, falling back to polling every `opts[:poll_interval_ms]` (default `100`) — the transport
  for a workflow finished by a different engine instance. `opts[:timeout_ms]`, if given, bounds how
  long this call waits.

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
  Runs `fun.(conn)` as a durable transactional step named `name`: the user's writes made through
  `conn` and the step's `operation_outputs` checkpoint commit together in one database
  transaction. Must be called from inside a workflow. Raises
  `Dbos.NestedTransactionError` from inside another transaction's body, and
  `Dbos.StepInTransactionError` if any durable step is called from inside this transaction's
  body. `opts[:isolation]`: `:read_committed` | `:repeatable_read` | `:serializable`.
  """
  def transaction(name, opts \\ [], fun) do
    Runtime.run_transaction(name, opts, fun)
  end

  @doc """
  Cancels `workflow_id`: durably marks it `CANCELLED` (a no-op if it is already
  `SUCCESS`/`ERROR`/`CANCELLED`). If the workflow has a live process
  on this engine, wakes it immediately so a blocked `recv`/`get_event`/`sleep` is interrupted
  promptly rather than waiting out its timeout; a workflow actively running plain steps instead
  stops cooperatively at its next step boundary, where `check_operation_execution` observes the
  cancellation. `opts[:engine]` defaults to `Dbos`.
  """
  def cancel(workflow_id, opts \\ []) do
    engine = engine(opts)
    config = config(engine)
    SystemDb.cancel_workflows(config, [workflow_id])

    case WorkflowSup.whereis(engine, workflow_id) do
      {:ok, pid} -> send(pid, {:dbos_notify, :cancelled, workflow_id})
      :error -> :ok
    end

    :ok
  end

  @doc """
  Resumes `workflow_id` from its last checkpoint: clears its queue assignment and deadline and
  re-enqueues it onto `opts[:queue_name]` (default `Dbos.Queue.internal_queue_name/0`). Resuming a
  workflow already `SUCCESS`/`ERROR` is a silent no-op (the row is left unchanged). `opts[:engine]`
  defaults to `Dbos`.
  """
  def resume(workflow_id, opts \\ []) do
    config = opts |> engine() |> config()
    queue_name = Keyword.get(opts, :queue_name, Queue.internal_queue_name())
    SystemDb.resume_workflows(config, [workflow_id], queue_name: queue_name)
    :ok
  end

  @doc """
  Forks `workflow_id` from step `start_step`: copies every checkpoint before `start_step` into a
  new workflow (`opts[:new_workflow_id]`, default a fresh random id), marks the original
  `was_forked_from`, and enqueues the fork so it re-runs starting at `start_step`. `opts`:
  `:new_workflow_id`, `:queue_name` (default the internal queue),
  `:application_version`, `:engine` (default `Dbos`).
  """
  def fork(workflow_id, start_step, opts \\ []) do
    engine = engine(opts)
    config = config(engine)
    new_workflow_id = Keyword.get_lazy(opts, :new_workflow_id, &Uuid.v4/0)

    SystemDb.fork_workflow(
      config,
      workflow_id,
      start_step,
      Keyword.put(opts, :new_workflow_id, new_workflow_id)
    )

    {:ok, %WorkflowHandle{engine: engine, workflow_id: new_workflow_id}}
  end

  defp workflow_or_engine_config(opts) do
    if Runtime.in_workflow?(), do: Runtime.current_config(), else: config(engine(opts))
  end

  @doc "The resolved config for the default engine (`Dbos`). Raises `Dbos.NotStartedError` if it has not started."
  def config, do: config(Dbos)

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

  @doc "Fetches one workflow's status by id. `opts[:engine]` defaults to `Dbos`."
  def status(workflow_id, opts \\ []) do
    opts |> engine() |> config() |> Client.status(workflow_id)
  end

  @doc "Returns a workflow's outcome: `{:ok, term}`, `{:error, term}`, or `:pending`. `opts[:engine]` defaults to `Dbos`."
  def result(workflow_id, opts \\ []) do
    opts |> engine() |> config() |> Client.result(workflow_id)
  end

  defp engine(opts), do: Keyword.get(opts, :engine, Dbos)
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
      {:ok, name} -> {name, mfa}
      :error -> raise "workflow #{inspect(mfa)} is not registered on engine #{inspect(engine)}"
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

      {:ok, %WorkflowStatus{status: status}} when status in [:pending, :enqueued, :delayed] ->
        wait_or_timeout(config, engine, workflow_id, poll_interval_ms, deadline)

      {:error, :not_found} ->
        wait_or_timeout(config, engine, workflow_id, poll_interval_ms, deadline)
    end
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
