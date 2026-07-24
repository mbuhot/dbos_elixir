defmodule Dbos.SystemDb do
  @moduledoc """
  Reads and writes against the `dbos` system database tables. Every function takes a
  `Dbos.Config` first, so a call site never has to know which adapter or schema it's running
  against.
  """

  alias Dbos.Config
  alias Dbos.Serialization
  alias Dbos.Status
  alias Dbos.StepInfo
  alias Dbos.Uuid
  alias Dbos.WorkflowStatus

  @enqueue_columns ~w(
    workflow_uuid status inputs name class_name config_name authenticated_user
    assumed_role queue_name deduplication_id priority queue_partition_key
    application_version created_at updated_at recovery_attempts workflow_timeout_ms
    workflow_deadline_epoch_ms parent_workflow_id owner_xid serialization
  )

  @doc "Inserts a workflow directly onto a queue, `status = 'ENQUEUED'`. Idempotent on `workflow_uuid`."
  def insert_enqueued_workflow(%Config{} = config, params) do
    workflow_id = Map.get(params, :workflow_id) || Uuid.v4()
    now = System.os_time(:millisecond)

    values = [
      workflow_id,
      Status.to_string(:enqueued),
      Serialization.encode(Map.fetch!(params, :inputs)),
      Map.fetch!(params, :name),
      Map.get(params, :class_name),
      Map.get(params, :config_name),
      "",
      "",
      Map.fetch!(params, :queue_name),
      Map.get(params, :deduplication_id),
      Map.get(params, :priority, 0),
      Map.get(params, :queue_partition_key),
      Map.get(params, :application_version, config.application_version),
      now,
      now,
      0,
      Map.get(params, :workflow_timeout_ms),
      Map.get(params, :workflow_deadline_epoch_ms),
      nil,
      Uuid.v4(),
      Serialization.format_name()
    ]

    placeholders = 1..length(@enqueue_columns) |> Enum.map_join(", ", &"$#{&1}")

    sql = """
    INSERT INTO #{table(config, "workflow_status")} (#{Enum.join(@enqueue_columns, ", ")})
    VALUES (#{placeholders})
    ON CONFLICT (workflow_uuid) DO UPDATE SET updated_at = EXCLUDED.updated_at
    """

    {:ok, _result} = config.db.query(config.conn, sql, values)
    {:ok, workflow_id}
  end

  @doc "Fetches one workflow's status row by id."
  def get_workflow_status(%Config{} = config, workflow_id) do
    sql =
      "SELECT #{select_list(WorkflowStatus)} FROM #{table(config, "workflow_status")} WHERE workflow_uuid = $1"

    case config.db.query(config.conn, sql, [workflow_id]) do
      {:ok, %{rows: [row]}} -> {:ok, WorkflowStatus.from_row(row)}
      {:ok, %{rows: []}} -> {:error, :not_found}
    end
  end

  @doc "Lists workflows matching the given filters."
  def list_workflows(%Config{} = config, opts \\ []) do
    {where_sql, where_params} = list_workflows_where(opts)
    {limit_offset_sql, all_params} = list_workflows_limit_offset(opts, where_params)
    order_sql = if Keyword.get(opts, :sort, :desc) == :asc, do: "ASC", else: "DESC"

    sql = """
    SELECT #{select_list(WorkflowStatus)} FROM #{table(config, "workflow_status")}
    #{where_sql}
    ORDER BY created_at #{order_sql}
    #{limit_offset_sql}
    """

    {:ok, result} = config.db.query(config.conn, sql, all_params)
    {:ok, Enum.map(result.rows, &WorkflowStatus.from_row/1)}
  end

  @doc "Returns a workflow's checkpointed steps, ordered by `function_id`."
  def get_workflow_steps(%Config{} = config, workflow_id) do
    sql = """
    SELECT #{select_list(StepInfo)} FROM #{table(config, "operation_outputs")}
    WHERE workflow_uuid = $1
    ORDER BY function_id ASC
    """

    {:ok, result} = config.db.query(config.conn, sql, [workflow_id])
    {:ok, Enum.map(result.rows, &StepInfo.from_row/1)}
  end

  @doc "Returns a workflow's outcome: `{:ok, term}`, `{:error, exception}`, or `:pending`."
  def get_workflow_result(%Config{} = config, workflow_id) do
    case get_workflow_status(config, workflow_id) do
      {:error, :not_found} ->
        {:error, :not_found}

      {:ok, %WorkflowStatus{status: :success, output: output}} ->
        {:ok, output}

      {:ok, %WorkflowStatus{status: :error, error: error}} ->
        {:error, error}

      {:ok, %WorkflowStatus{}} ->
        :pending
    end
  end

  defp select_list(struct_module) do
    struct_module.columns() |> Enum.join(", ")
  end

  defp table(%Config{schema: schema}, name), do: ~s("#{schema}".#{name})

  defp list_workflows_where(opts) do
    filters = [
      {:status, "status", &Status.to_string/1},
      {:name, "name", & &1},
      {:queue_name, "queue_name", & &1},
      {:executor_id, "executor_id", & &1},
      {:application_version, "application_version", & &1},
      {:created_after, "created_at >=", & &1},
      {:created_before, "created_at <=", & &1}
    ]

    Enum.reduce(filters, {[], []}, fn {key, condition, cast}, {clauses, params} ->
      case Keyword.fetch(opts, key) do
        {:ok, value} ->
          param_index = length(params) + 1
          column_clause = build_clause(condition, param_index)
          {clauses ++ [column_clause], params ++ [cast.(value)]}

        :error ->
          {clauses, params}
      end
    end)
    |> then(fn {clauses, params} ->
      where_sql = if clauses == [], do: "", else: "WHERE " <> Enum.join(clauses, " AND ")
      {where_sql, params}
    end)
  end

  defp build_clause(condition, param_index) do
    if String.contains?(condition, " ") do
      "#{condition} $#{param_index}"
    else
      "#{condition} = $#{param_index}"
    end
  end

  defp list_workflows_limit_offset(opts, params) do
    [{:limit, "LIMIT"}, {:offset, "OFFSET"}]
    |> Enum.reduce({"", params}, fn {key, keyword}, {sql, params} ->
      case Keyword.fetch(opts, key) do
        {:ok, value} -> {sql <> " #{keyword} $#{length(params) + 1}", params ++ [value]}
        :error -> {sql, params}
      end
    end)
  end
end
