defmodule Dbos.Client do
  @moduledoc """
  Reading and enqueuing workflows from outside a workflow — a controller, a mix task, an admin
  page. Every function takes a `Dbos.Config` and talks only to the system database, so none of it
  requires a workflow context.
  """

  alias Dbos.SystemDb
  alias Dbos.Uuid

  @doc "Enqueues a workflow, generating a UUIDv4 workflow id unless `opts[:workflow_id]` is given."
  def enqueue(config, name, queue_name, inputs, opts \\ []) do
    params =
      opts
      |> Map.new()
      |> Map.put(:name, name)
      |> Map.put(:queue_name, queue_name)
      |> Map.put(:inputs, inputs)
      |> Map.put_new_lazy(:workflow_id, &Uuid.v4/0)

    SystemDb.insert_enqueued_workflow(config, params)
  end

  @doc "Fetches one workflow's status by id."
  def status(config, workflow_id), do: SystemDb.get_workflow_status(config, workflow_id)

  @doc "Lists workflows matching the given filters (`:status`, `:name`, `:queue_name`, ...)."
  def list(config, opts \\ []), do: SystemDb.list_workflows(config, opts)

  @doc "Returns a workflow's checkpointed steps, ordered by `function_id`."
  def steps(config, workflow_id), do: SystemDb.get_workflow_steps(config, workflow_id)

  @doc "Returns a workflow's outcome: `{:ok, term}`, `{:error, exception}`, or `:pending`."
  def result(config, workflow_id), do: SystemDb.get_workflow_result(config, workflow_id)
end
