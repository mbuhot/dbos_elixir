defmodule Dbos do
  @moduledoc """
  The public entry point for starting and awaiting durable workflows. A running engine's
  resolved `Dbos.Config` lives in `:persistent_term`, keyed by the engine's `name`, so this
  module and `Dbos.Runtime` never have to be told which engine they're talking to beyond that
  name.
  """

  alias Dbos.Client
  alias Dbos.Config
  alias Dbos.Registry
  alias Dbos.Runtime
  alias Dbos.SystemDb
  alias Dbos.Uuid
  alias Dbos.WorkflowHandle
  alias Dbos.WorkflowStatus
  alias Dbos.WorkflowSup

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
      application_version: Keyword.get(opts, :application_version, config.application_version)
    }

    {:ok, workflow_id} = SystemDb.insert_enqueued_workflow(config, params)
    {:ok, %WorkflowHandle{engine: engine, workflow_id: workflow_id}}
  end

  @doc """
  Polls `workflow_status` until `handle`'s workflow reaches a terminal status, returning
  `{:ok, output}`, `{:error, exception}`, or `{:error, :timeout}`. `opts[:poll_interval_ms]`
  defaults to `100`; `opts[:timeout_ms]`, if given, bounds how long this call waits.
  """
  def await(%WorkflowHandle{} = handle, opts \\ []) do
    poll_interval_ms = Keyword.get(opts, :poll_interval_ms, 100)
    timeout_ms = Keyword.get(opts, :timeout_ms)
    config = config(handle.engine)
    deadline = timeout_ms && System.monotonic_time(:millisecond) + timeout_ms

    poll_for_outcome(config, handle.workflow_id, poll_interval_ms, deadline)
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

    SystemDb.insert_workflow_status(config, %{
      workflow_id: workflow_id,
      status: :pending,
      name: name,
      inputs: args,
      deduplication_id: Keyword.get(opts, :deduplication_id),
      priority: Keyword.get(opts, :priority, 0),
      application_version: Keyword.get(opts, :application_version, config.application_version)
    })

    {:ok, _pid} = WorkflowSup.start_workflow(engine, workflow_id, mfa, args)

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

        SystemDb.insert_workflow_status(config, %{
          workflow_id: child_id,
          status: :pending,
          name: name,
          inputs: args,
          parent_workflow_id: parent_id,
          application_version: Keyword.get(opts, :application_version, config.application_version)
        })

        {:ok, _pid} = WorkflowSup.start_workflow(engine, child_id, mfa, args)

        now = System.os_time(:millisecond)

        SystemDb.record_operation_result(config, %{
          workflow_id: parent_id,
          function_id: step_id,
          function_name: name,
          child_workflow_id: child_id,
          started_at: now,
          completed_at: now
        })

        {:ok, %WorkflowHandle{engine: engine, workflow_id: child_id}}
    end
  end

  defp poll_for_outcome(config, workflow_id, poll_interval_ms, deadline) do
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
        wait_or_timeout(config, workflow_id, poll_interval_ms, deadline)

      {:error, :not_found} ->
        wait_or_timeout(config, workflow_id, poll_interval_ms, deadline)
    end
  end

  defp wait_or_timeout(config, workflow_id, poll_interval_ms, deadline) do
    if deadline && System.monotonic_time(:millisecond) >= deadline do
      {:error, :timeout}
    else
      Process.sleep(poll_interval_ms)
      poll_for_outcome(config, workflow_id, poll_interval_ms, deadline)
    end
  end
end
