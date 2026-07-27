# One running workflow instance: establishes the Dbos.Runtime context, invokes the registered
# function with its decoded inputs, and records the outcome. A Dbos.WorkflowCancelledError
# escaping the body means the row is already CANCELLED, so it is swallowed, leaving the status as
# recorded. A Dbos.WorkflowCancellingError means it has effects to reverse first, so this commits
# CANCELLED, which enqueues the unwind alongside it.
defmodule Dbos.WorkflowProcess do
  @moduledoc false

  use Task, restart: :temporary

  alias Dbos.Notifications
  alias Dbos.Runtime
  alias Dbos.Serialization
  alias Dbos.SystemDb
  alias Dbos.Telemetry
  alias Dbos.WorkflowHandle
  alias Dbos.WorkflowSup

  @doc "Starts the process for `process_args` (see `Dbos.WorkflowSup.start_workflow/5`)."
  def start_link(process_args), do: Task.start_link(__MODULE__, :run, [process_args])

  @doc "Runs the workflow body and records its outcome. Not meant to be called directly."
  def run(%{engine: engine, workflow_id: workflow_id} = process_args) do
    register_in_process_registry(engine, workflow_id, process_args)
    ack_registration(process_args)
    finish(process_args, execute(process_args))
  end

  @doc """
  Runs the workflow body and records its outcome on the calling process itself, with no
  supervised child and no process registration — the engine's `:inline`/`:manual` testing modes'
  synchronous execution path. Not meant to be called directly.
  """
  def run_inline(process_args), do: finish(process_args, execute(process_args))

  defp execute(%{
         config: config,
         engine: engine,
         workflow_id: workflow_id,
         mfa: {module, function, arity},
         args: args,
         replay: replay
       }) do
    metadata = %{
      workflow_id: workflow_id,
      name: workflow_name(engine, module, function, arity),
      engine: engine,
      replay: replay
    }

    value =
      Runtime.with_context([config: config, workflow_id: workflow_id, replay: replay], fn ->
        Runtime.arm_deadline(config, workflow_id)
        Telemetry.span_workflow(metadata, fn -> apply(module, function, args) end)
      end)

    {:success, value}
  rescue
    exception -> classify_failure(:error, exception, __STACKTRACE__)
  catch
    kind, value -> classify_failure(kind, value, __STACKTRACE__)
  end

  defp finish(%{config: config, engine: engine, workflow_id: workflow_id}, outcome),
    do: record_outcome(config, engine, workflow_id, outcome)

  defp classify_failure(:error, %Dbos.WorkflowCancelledError{}, _stacktrace),
    do: :already_cancelled

  defp classify_failure(:error, %Dbos.WorkflowCancellingError{}, _stacktrace), do: :cancelling

  defp classify_failure(:error, %Dbos.Waits.Parked{}, _stacktrace), do: :parked

  defp classify_failure(:error, %Dbos.ConcurrentCheckpointConflictError{}, _stacktrace),
    do: :concurrent_conflict

  defp classify_failure(kind, value, stacktrace), do: {:failure, kind, value, stacktrace}

  defp record_outcome(_config, _engine, _workflow_id, :already_cancelled), do: :ok

  defp record_outcome(_config, _engine, _workflow_id, :parked), do: :ok

  # The forward path stopped because this workflow is cancelling. Committing CANCELLED is what
  # enqueues its unwind, in that same transaction — see Dbos.SystemDb.update_workflow_outcome/3.
  defp record_outcome(config, engine, workflow_id, :cancelling) do
    write_outcome(config, engine, workflow_id, %{status: :cancelled})
  end

  defp record_outcome(_config, engine, workflow_id, :concurrent_conflict) do
    Dbos.await(%WorkflowHandle{engine: engine, workflow_id: workflow_id})
    :ok
  end

  defp record_outcome(config, engine, workflow_id, {:success, value}) do
    write_outcome(config, engine, workflow_id, %{
      status: :success,
      output: Serialization.encode(value)
    })
  end

  defp record_outcome(config, engine, workflow_id, {:failure, kind, value, stacktrace}) do
    write_outcome(config, engine, workflow_id, %{
      status: :error,
      error: Serialization.encode_failure(kind, value, stacktrace)
    })
  end

  defp write_outcome(config, engine, workflow_id, attrs) do
    SystemDb.update_workflow_outcome(config, workflow_id, attrs)
    Notifications.notify_status(engine, workflow_id)
  rescue
    Dbos.WorkflowCancelledError ->
      Notifications.notify_status(engine, workflow_id)

    # This workflow ran to an outcome after being cancelled. The outcome is not reportable — the
    # effects behind it are what the unwind reverses — so the cancellation is completed instead.
    Dbos.WorkflowCancellingError ->
      record_outcome(config, engine, workflow_id, :cancelling)
  end

  defp register_in_process_registry(engine, workflow_id, %{
         queue_name: queue_name,
         partition_key: partition_key
       }) do
    Elixir.Registry.register(
      WorkflowSup.process_registry_name(engine),
      workflow_id,
      {queue_name, partition_key}
    )
  end

  defp ack_registration(%{caller: caller}), do: send(caller, {:dbos_workflow_registered, self()})
  defp ack_registration(_process_args), do: :ok

  defp workflow_name(engine, module, function, arity) do
    case Dbos.Registry.name_for_mfa(engine, {module, function, arity}) do
      {:ok, name} -> name
      :error -> nil
    end
  rescue
    ArgumentError -> nil
  end
end
