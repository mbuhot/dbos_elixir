# JSON-safe rendering of engine structs for Dbos.AdminServer. The system stores ETF-encoded
# Elixir terms (inputs/output/error); over HTTP/JSON, which cannot represent an arbitrary Elixir
# term losslessly, these render through inspect/1 as a readable, unambiguous string. Every other
# column already is (or trivially converts to) a JSON-safe type.
defmodule Dbos.AdminServer.Render do
  @moduledoc false

  alias Dbos.Status

  @doc "Renders a `Dbos.WorkflowStatus` as a JSON-safe map."
  def workflow(%Dbos.WorkflowStatus{} = w) do
    %{
      "workflow_uuid" => w.workflow_uuid,
      "status" => Status.to_string(w.status),
      "name" => w.name,
      "authenticated_user" => w.authenticated_user,
      "assumed_role" => w.assumed_role,
      "authenticated_roles" => w.authenticated_roles,
      "executor_id" => w.executor_id,
      "application_version" => w.application_version,
      "application_id" => w.application_id,
      "attempts" => w.recovery_attempts,
      "queue_name" => w.queue_name,
      "workflow_timeout_ms" => w.workflow_timeout_ms,
      "deduplication_id" => w.deduplication_id,
      "priority" => w.priority,
      "queue_partition_key" => w.queue_partition_key,
      "created_at" => stringify(w.created_at),
      "updated_at" => stringify(w.updated_at),
      "workflow_deadline_epoch_ms" => stringify(w.workflow_deadline_epoch_ms),
      "started_at_epoch_ms" => stringify(w.started_at_epoch_ms),
      "input" => inspect_or_nil(w.inputs),
      "output" => inspect_or_nil(w.output),
      "error" => inspect_or_nil(w.error)
    }
  end

  @doc "Renders a `Dbos.StepInfo` as a JSON-safe map."
  def step(%Dbos.StepInfo{} = s) do
    %{
      "function_id" => s.function_id,
      "function_name" => s.function_name,
      "child_workflow_id" => s.child_workflow_id,
      "started_at_epoch_ms" => s.started_at_epoch_ms,
      "completed_at_epoch_ms" => s.completed_at_epoch_ms,
      "output" => inspect_or_nil(s.output),
      "error" => inspect_or_nil(s.error)
    }
  end

  @doc "Renders one `Dbos.Recovery.orphan/0` group as a JSON-safe map."
  def orphan(orphan) do
    %{
      "name" => orphan.name,
      "application_version" => orphan.application_version,
      "workflow_version" => orphan.workflow_version,
      "reason" => Atom.to_string(orphan.reason),
      "count" => orphan.count,
      "oldest_created_at_epoch_ms" => stringify(orphan.oldest_created_at_epoch_ms),
      "example_workflow_id" => orphan.example_workflow_id
    }
  end

  @doc "Renders a `Dbos.Queue` as a JSON-safe map."
  def queue(%Dbos.Queue{} = q) do
    %{
      "name" => q.name,
      "worker_concurrency" => q.worker_concurrency,
      "global_concurrency" => q.global_concurrency,
      "rate_limit" => rate_limit(q.rate_limit),
      "priority_enabled" => q.priority_enabled,
      "partition_queue" => q.partition_queue,
      "polling_interval_ms" => q.base_polling_interval_ms
    }
  end

  defp rate_limit(nil), do: nil

  defp rate_limit(%{limit: limit, period_ms: period_ms}),
    do: %{"limit" => limit, "period_ms" => period_ms}

  defp stringify(nil), do: nil
  defp stringify(int) when is_integer(int), do: Integer.to_string(int)

  defp inspect_or_nil(nil), do: nil
  defp inspect_or_nil(term), do: inspect(term)
end
