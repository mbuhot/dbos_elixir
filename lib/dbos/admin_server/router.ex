# Routes one decoded HTTP request to an engine operation and renders its JSON (or plain-text)
# response.
defmodule Dbos.AdminServer.Router do
  @moduledoc false

  alias Dbos.AdminServer.Render
  alias Dbos.Status
  alias Dbos.SystemDb

  @doc "Routes `method` (`:get` | `:post`) and `path` against `engine`, returning `{status, body, content_type}`."
  def route(engine, method, path, body) do
    path
    |> String.split("?")
    |> List.first()
    |> String.split("/", trim: true)
    |> then(&dispatch(engine, method, &1, body))
  end

  defp dispatch(_engine, :get, ["dbos-healthz"], _body), do: json(200, %{"status" => "healthy"})

  defp dispatch(engine, :post, ["dbos-workflow-recovery"], body) do
    executor_ids = decode(body)
    json(200, Dbos.Recovery.reclaim(engine, executor_ids))
  end

  defp dispatch(engine, :get, ["deactivate"], _body) do
    Dbos.Scheduler.deactivate(engine)
    {200, "deactivated", "text/plain"}
  end

  defp dispatch(engine, :get, ["dbos-workflow-queues-metadata"], _body) do
    config = Dbos.config(engine)
    {:ok, queues} = SystemDb.list_queues(config)
    json(200, Enum.map([internal_queue() | queues], &Render.queue/1))
  end

  defp dispatch(engine, :get, ["dbos-orphans"], _body) do
    json(200, Enum.map(Dbos.Recovery.orphans(engine), &Render.orphan/1))
  end

  defp dispatch(engine, :post, ["dbos-garbage-collect"], body) do
    config = Dbos.config(engine)
    params = decode(body)

    opts =
      []
      |> maybe_put(:cutoff_epoch_timestamp_ms, params["cutoff_epoch_timestamp_ms"])
      |> maybe_put(:rows_threshold, params["rows_threshold"])

    deleted = SystemDb.garbage_collect_workflows(config, opts)
    json(200, %{"deleted" => deleted})
  end

  defp dispatch(engine, :post, ["dbos-global-timeout"], body) do
    config = Dbos.config(engine)
    %{"cutoff_epoch_timestamp_ms" => cutoff} = decode(body)

    config
    |> SystemDb.cancel_all_before(cutoff)
    |> then(&Dbos.wake_cancelled(engine, &1))

    {204, "", "application/json"}
  end

  defp dispatch(engine, :post, ["queues"], body), do: list_workflows(engine, body, true)
  defp dispatch(engine, :post, ["workflows"], body), do: list_workflows(engine, body, false)

  defp dispatch(engine, :get, ["workflows", id], _body) do
    config = Dbos.config(engine)

    case SystemDb.get_workflow_status(config, id) do
      {:ok, status} -> json(200, Render.workflow(status))
      {:error, :not_found} -> json(404, %{"error" => "workflow #{id} not found"})
    end
  end

  defp dispatch(engine, :get, ["workflows", id, "steps"], _body) do
    config = Dbos.config(engine)
    {:ok, steps} = SystemDb.get_workflow_steps(config, id)
    json(200, Enum.map(steps, &Render.step/1))
  end

  defp dispatch(engine, :post, ["workflows", id, "cancel"], _body) do
    :ok = Dbos.cancel(id, engine: engine)
    {204, "", "application/json"}
  end

  defp dispatch(engine, :post, ["workflows", id, "resume"], _body) do
    :ok = Dbos.resume(id, engine: engine)
    {204, "", "application/json"}
  end

  defp dispatch(engine, :post, ["workflows", id, "retry"], _body) do
    :ok = Dbos.retry(id, engine: engine)
    {204, "", "application/json"}
  end

  defp dispatch(engine, :post, ["workflows", id, "fork"], body) do
    params = decode(body)
    start_step = Map.get(params, "start_step") || 0

    opts =
      []
      |> maybe_put(:new_workflow_id, params["new_workflow_id"])
      |> maybe_put(:application_version, params["application_version"])
      |> Keyword.put(:engine, engine)

    {:ok, handle} = Dbos.fork(id, start_step, opts)
    json(200, %{"workflow_id" => handle.workflow_id})
  end

  defp dispatch(_engine, _method, _segments, _body), do: json(404, %{"error" => "not found"})

  defp list_workflows(engine, body, queues_only) do
    config = Dbos.config(engine)
    params = decode(body)

    opts =
      []
      |> maybe_put(:name, params["workflow_name"])
      |> maybe_put(:queue_name, params["queue_name"])
      |> maybe_put(:executor_id, params["executor_id"])
      |> maybe_put(:application_version, params["application_version"])
      |> maybe_put(:limit, params["limit"])
      |> maybe_put(:offset, params["offset"])
      |> maybe_put(:status, parse_status(params["status"]))
      |> maybe_put(:workflow_ids, params["workflow_ids"])
      |> maybe_put(:workflow_id_prefix, params["workflow_id_prefix"])
      |> maybe_put(:authenticated_user, params["authenticated_user"])
      |> maybe_put(:forked_from, params["forked_from"])
      |> maybe_put(:parent_workflow_id, params["parent_workflow_id"])
      |> maybe_put(:deduplication_id, params["deduplication_id"])
      |> maybe_put(:completed_after, params["completed_after"])
      |> maybe_put(:completed_before, params["completed_before"])
      |> maybe_put(:dequeued_after, params["dequeued_after"])
      |> maybe_put(:dequeued_before, params["dequeued_before"])
      |> maybe_put(:has_parent, params["has_parent"])
      |> maybe_put(:attributes, params["attributes"])
      |> maybe_put(:schedule_name, params["schedule_name"])
      |> maybe_put(:is_debounced, params["is_debounced"])
      |> maybe_put(:load_input, params["load_input"])
      |> maybe_put(:load_output, params["load_output"])
      |> Keyword.put(:sort, if(params["sort_desc"] == false, do: :asc, else: :desc))
      |> Keyword.put(:queues_only, queues_only)

    {:ok, workflows} = SystemDb.list_workflows(config, opts)
    json(200, Enum.map(workflows, &Render.workflow/1))
  end

  defp parse_status(nil), do: nil
  defp parse_status(list) when is_list(list), do: Enum.map(list, &Status.from_string/1)
  defp parse_status(one), do: Status.from_string(one)

  defp internal_queue, do: %Dbos.Queue{name: Dbos.Queue.internal_queue_name()}

  defp maybe_put(opts, _key, nil), do: opts
  defp maybe_put(opts, key, value), do: Keyword.put(opts, key, value)

  defp decode(""), do: %{}
  defp decode(body), do: JSON.decode!(body)

  defp json(status, term), do: {status, JSON.encode!(term), "application/json"}
end
