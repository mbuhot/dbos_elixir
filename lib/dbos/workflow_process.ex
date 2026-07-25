defmodule Dbos.WorkflowProcess do
  @moduledoc """
  One running workflow instance: establishes the `Dbos.Runtime` context, invokes the registered
  function with its decoded inputs, and records the outcome. A `Dbos.WorkflowCancelledError`
  escaping the body is not an application failure — the row is already `CANCELLED` — so it is
  swallowed rather than overwriting the status.
  """

  use Task, restart: :temporary

  alias Dbos.Runtime
  alias Dbos.Serialization
  alias Dbos.SystemDb
  alias Dbos.WorkflowSup

  @doc "Starts the process for `process_args` (see `Dbos.WorkflowSup.start_workflow/5`)."
  def start_link(process_args), do: Task.start_link(__MODULE__, :run, [process_args])

  @doc "Runs the workflow body and records its outcome. Not meant to be called directly."
  def run(
        %{
          config: config,
          engine: engine,
          workflow_id: workflow_id,
          mfa: {module, function, _arity},
          args: args,
          replay: replay
        } = process_args
      ) do
    register_in_process_registry(engine, workflow_id, process_args)

    outcome =
      try do
        value =
          Runtime.with_context([config: config, workflow_id: workflow_id, replay: replay], fn ->
            apply(module, function, args)
          end)

        {:success, value}
      rescue
        exception -> classify_failure(:error, exception, __STACKTRACE__)
      catch
        kind, value -> classify_failure(kind, value, __STACKTRACE__)
      end

    record_outcome(config, workflow_id, outcome)
  end

  defp classify_failure(:error, %Dbos.WorkflowCancelledError{}, _stacktrace),
    do: :already_cancelled

  defp classify_failure(kind, value, stacktrace), do: {:failure, kind, value, stacktrace}

  defp record_outcome(_config, _workflow_id, :already_cancelled), do: :ok

  defp record_outcome(config, workflow_id, {:success, value}) do
    write_outcome(config, workflow_id, %{
      status: :success,
      output: Serialization.encode(value)
    })
  end

  defp record_outcome(config, workflow_id, {:failure, kind, value, stacktrace}) do
    write_outcome(config, workflow_id, %{
      status: :error,
      error: Serialization.encode_failure(kind, value, stacktrace)
    })
  end

  defp write_outcome(config, workflow_id, attrs) do
    SystemDb.update_workflow_outcome(config, workflow_id, attrs)
  rescue
    Dbos.WorkflowCancelledError -> :ok
  end

  defp register_in_process_registry(engine, workflow_id, %{
         queue_name: queue_name,
         partition_key: partition_key
       }) do
    Registry.register(
      WorkflowSup.process_registry_name(engine),
      workflow_id,
      {queue_name, partition_key}
    )
  end
end
