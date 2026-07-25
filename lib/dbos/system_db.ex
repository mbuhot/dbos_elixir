defmodule Dbos.SystemDb do
  @moduledoc """
  Reads and writes against the `dbos` system database tables. Every function takes a
  `Dbos.Config` first, so a call site never has to know which adapter or schema it's running
  against.
  """

  require Logger

  alias Dbos.Config
  alias Dbos.InsertWorkflowResult
  alias Dbos.Serialization
  alias Dbos.Status
  alias Dbos.StepInfo
  alias Dbos.Uuid
  alias Dbos.WorkflowStatus

  @insert_workflow_status_columns ~w(
    workflow_uuid status name queue_name authenticated_user assumed_role
    authenticated_roles executor_id application_version application_id created_at
    recovery_attempts updated_at workflow_timeout_ms workflow_deadline_epoch_ms inputs
    deduplication_id priority queue_partition_key owner_xid parent_workflow_id
    class_name config_name serialization delay_until_epoch_ms attributes
    schedule_name debounce_deadline_epoch_ms is_debounced
  )

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

  @doc """
  Upserts a `workflow_status` row: `INSERT ... ON CONFLICT (workflow_uuid) DO UPDATE`, per
  `notes/engine-core.md` §1. `opts[:increment_attempts]` gates whether a re-entrant call bumps
  `recovery_attempts` (only when the incoming status is not `:enqueued`/`:delayed`);
  `opts[:max_retries]` gates the `MAX_RECOVERY_ATTEMPTS_EXCEEDED` transition. `attrs[:owner_xid]`
  defaults to a fresh UUID, as upstream generates on every call; it is written only on first
  insert and is returned unchanged on every later call for the same workflow id.

  Returns a `Dbos.InsertWorkflowResult`, or raises `Dbos.MaxRecoveryAttemptsExceededError` if this
  call tips the row into `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.
  """
  def insert_workflow_status(%Config{} = config, attrs, opts \\ []) do
    increment_attempts = Keyword.get(opts, :increment_attempts, false)
    max_retries = Keyword.get(opts, :max_retries, 0)
    status = Map.fetch!(attrs, :status)
    workflow_id = Map.fetch!(attrs, :workflow_id)
    owner_xid = Map.get_lazy(attrs, :owner_xid, &Uuid.v4/0)

    {:ok, outcome} =
      config.db.transaction(config.conn, [], fn conn ->
        tx_config = %{config | conn: conn}

        insert_result =
          do_insert_workflow_status(tx_config, attrs, status, owner_xid, increment_attempts)

        apply_dead_letter_transition(
          tx_config,
          workflow_id,
          insert_result,
          max_retries,
          owner_xid
        )
      end)

    case outcome do
      {:ok, insert_result} ->
        insert_result

      {:dead_letter, attempts} ->
        raise Dbos.MaxRecoveryAttemptsExceededError, workflow_id: workflow_id, attempts: attempts
    end
  end

  @doc """
  The ordered `CheckOperationExecution` checks (`notes/engine-core.md` §2): workflow existence,
  cancellation, whether the step already ran, and (if so) that its recorded `function_name`
  matches. Returns `:none` (not yet run), `{:replay, output}`, or `{:replay_failure, failure}`
  with both decoded via `Dbos.Serialization.decode/1`.
  """
  def check_operation_execution(%Config{} = config, workflow_id, function_id, function_name) do
    {:ok, result} =
      config.db.transaction(config.conn, [], fn conn ->
        tx_config = %{config | conn: conn}
        status = fetch_workflow_status_for_check!(tx_config, workflow_id)
        ensure_not_cancelled!(status, workflow_id)
        check_recorded_step(tx_config, workflow_id, function_id, function_name)
      end)

    result
  end

  @doc """
  Checkpoints a step outcome (`notes/engine-core.md` §3): `output`/`error` are already-encoded
  strings (or `nil`). `ON CONFLICT (workflow_uuid, function_id) DO NOTHING`, then, only on the
  branch that actually inserted a row, re-stamps `workflow_status.executor_id` to
  `config.executor_id`, swallowing its own failure with a logged warning. A retried write that
  matches the stored row byte-for-byte (including timestamps) is an idempotent no-op; one with a
  different `function_name` raises `Dbos.UnexpectedStepError`.
  """
  def record_operation_result(%Config{} = config, attrs, _opts \\ []) do
    case try_insert_operation_output(config, attrs) do
      {:inserted, true} ->
        refresh_executor_id(config, Map.fetch!(attrs, :workflow_id))
        :ok

      {:inserted, false} ->
        reconcile_existing_operation_output(config, attrs)
    end
  end

  @doc """
  Writes a workflow's terminal outcome (`notes/engine-core.md` §4): `attrs[:status]` is `:success`
  or `:error`, `attrs[:output]`/`attrs[:error]` are already-encoded strings or `nil`. Clears
  `deduplication_id` only; `queue_name` and `started_at_epoch_ms` are left as-is, matching
  upstream. Refuses to overwrite a row already `:success`/`:error`/`:cancelled`; if the row was
  cancelled underneath this call, raises `Dbos.WorkflowCancelledError` instead of reporting the
  outcome this call intended to write.
  """
  def update_workflow_outcome(%Config{} = config, workflow_id, attrs) do
    status = Map.fetch!(attrs, :status)
    output = Map.get(attrs, :output)
    error = Map.get(attrs, :error)
    now = System.os_time(:millisecond)

    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET status = $1, output = $2, error = $3, updated_at = $4, completed_at = $4, deduplication_id = NULL
        WHERE workflow_uuid = $5 AND status NOT IN ($6, $7, $8)
    """

    values = [
      Status.to_string(status),
      output,
      error,
      now,
      workflow_id,
      Status.to_string(:cancelled),
      Status.to_string(:success),
      Status.to_string(:error)
    ]

    {:ok, %{num_rows: num_rows}} = config.db.query(config.conn, sql, values)

    if num_rows == 0 do
      handle_update_outcome_conflict(config, workflow_id)
    else
      :ok
    end
  end

  @doc """
  Checks whether a child workflow has already been recorded at `(parent_workflow_id,
  parent_step_id)`, per `notes/step-ids.md` §6 (`CheckChildWorkflow`). Returns `:none` if no
  child has been started at that step yet, `{:existing, child_workflow_id}` if one has and its
  recorded `function_name` matches `child_name`, or raises `Dbos.UnexpectedStepError` if a
  different child workflow was recorded at that step.
  """
  def check_child_workflow(%Config{} = config, parent_workflow_id, parent_step_id, child_name) do
    sql = """
    SELECT function_name, child_workflow_id
    FROM #{table(config, "operation_outputs")}
    WHERE workflow_uuid = $1 AND function_id = $2
    """

    case config.db.query(config.conn, sql, [parent_workflow_id, parent_step_id]) do
      {:ok, %{rows: []}} ->
        :none

      {:ok, %{rows: [[^child_name, child_workflow_id]]}} ->
        {:existing, child_workflow_id}

      {:ok, %{rows: [[recorded_name, _child_workflow_id]]}} ->
        raise Dbos.UnexpectedStepError,
          workflow_id: parent_workflow_id,
          function_id: parent_step_id,
          expected: child_name,
          recorded: recorded_name
    end
  end

  @doc """
  Puts a queued `PENDING` workflow back to `ENQUEUED`, clearing `started_at_epoch_ms`, per
  `notes/recovery.md` §1 (`ClearQueueAssignment`). Returns `:cleared`, or `:not_cleared` if the
  row was not `PENDING` with a `queue_name` (e.g. another executor already claimed it).
  """
  def clear_queue_assignment(%Config{} = config, workflow_id) do
    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET status = $1, started_at_epoch_ms = NULL
        WHERE workflow_uuid = $2 AND queue_name IS NOT NULL AND status = $3
    """

    {:ok, %{num_rows: num_rows}} =
      config.db.query(config.conn, sql, [
        Status.to_string(:enqueued),
        workflow_id,
        Status.to_string(:pending)
      ])

    if num_rows > 0, do: :cleared, else: :not_cleared
  end

  defp handle_update_outcome_conflict(config, workflow_id) do
    sql = "SELECT status FROM #{table(config, "workflow_status")} WHERE workflow_uuid = $1"

    case config.db.query(config.conn, sql, [workflow_id]) do
      {:ok, %{rows: []}} ->
        :ok

      {:ok, %{rows: [["CANCELLED"]]}} ->
        raise Dbos.WorkflowCancelledError, workflow_id: workflow_id

      {:ok, %{rows: [[_status]]}} ->
        :ok
    end
  end

  defp do_insert_workflow_status(config, attrs, status, owner_xid, increment_attempts) do
    now = System.os_time(:millisecond)
    initial_attempts = if status in [:enqueued, :delayed], do: 0, else: 1
    recovery_increment = if increment_attempts, do: 1, else: 0

    values = [
      Map.fetch!(attrs, :workflow_id),
      Status.to_string(status),
      Map.get(attrs, :name, ""),
      Map.get(attrs, :queue_name),
      Map.get(attrs, :authenticated_user, ""),
      Map.get(attrs, :assumed_role, ""),
      JSON.encode!(Map.get(attrs, :authenticated_roles, [])),
      config.executor_id,
      Map.get(attrs, :application_version, config.application_version),
      Map.get(attrs, :application_id, ""),
      Map.get(attrs, :created_at, now),
      initial_attempts,
      Map.get(attrs, :updated_at, now),
      Map.get(attrs, :workflow_timeout_ms),
      Map.get(attrs, :workflow_deadline_epoch_ms),
      encode_or_nil(Map.get(attrs, :inputs)),
      Map.get(attrs, :deduplication_id),
      Map.get(attrs, :priority, 0),
      Map.get(attrs, :queue_partition_key),
      owner_xid,
      Map.get(attrs, :parent_workflow_id),
      Map.get(attrs, :class_name),
      Map.get(attrs, :config_name),
      Serialization.format_name(),
      Map.get(attrs, :delay_until_epoch_ms),
      encode_json_or_nil(Map.get(attrs, :attributes)),
      Map.get(attrs, :schedule_name),
      Map.get(attrs, :debounce_deadline_epoch_ms),
      Map.get(attrs, :is_debounced, false),
      Status.to_string(:enqueued),
      Status.to_string(:delayed),
      recovery_increment
    ]

    placeholders = 1..length(@insert_workflow_status_columns) |> Enum.map_join(", ", &"$#{&1}")

    sql = """
    INSERT INTO #{table(config, "workflow_status")} (#{Enum.join(@insert_workflow_status_columns, ", ")})
    VALUES (#{placeholders})
    ON CONFLICT (workflow_uuid)
        DO UPDATE SET
            recovery_attempts = CASE
                WHEN EXCLUDED.status NOT IN ($30, $31) THEN workflow_status.recovery_attempts + $32
                ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
                WHEN EXCLUDED.status IN ($30, $31) THEN workflow_status.executor_id
                ELSE EXCLUDED.executor_id
            END
        RETURNING recovery_attempts, status, name, queue_name, queue_partition_key, workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid
    """

    {:ok, %{rows: [row]}} = config.db.query(config.conn, sql, values)
    InsertWorkflowResult.from_row(row)
  end

  defp apply_dead_letter_transition(
         config,
         workflow_id,
         %InsertWorkflowResult{} = result,
         max_retries,
         caller_owner_xid
       ) do
    if dead_letter_transition?(result, max_retries, caller_owner_xid) do
      run_dead_letter_transition(config, workflow_id)
      {:dead_letter, result.attempts}
    else
      {:ok, result}
    end
  end

  defp dead_letter_transition?(result, max_retries, caller_owner_xid) do
    result.status not in [:success, :error] and
      max_retries > 0 and
      result.attempts > max_retries + 1 and
      not owner_xid_matches?(caller_owner_xid, result.owner_xid)
  end

  defp owner_xid_matches?(nil, nil), do: true
  defp owner_xid_matches?(a, b), do: a == b

  defp run_dead_letter_transition(config, workflow_id) do
    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
        WHERE workflow_uuid = $2 AND status = $3
    """

    {:ok, _result} =
      config.db.query(config.conn, sql, [
        Status.to_string(:max_recovery_attempts_exceeded),
        workflow_id,
        Status.to_string(:pending)
      ])

    :ok
  end

  defp fetch_workflow_status_for_check!(config, workflow_id) do
    sql = "SELECT status FROM #{table(config, "workflow_status")} WHERE workflow_uuid = $1"

    case config.db.query(config.conn, sql, [workflow_id]) do
      {:ok, %{rows: [[status]]}} -> Status.from_string(status)
      {:ok, %{rows: []}} -> raise Dbos.NonExistentWorkflowError, workflow_id: workflow_id
    end
  end

  defp ensure_not_cancelled!(:cancelled, workflow_id) do
    raise Dbos.WorkflowCancelledError, workflow_id: workflow_id
  end

  defp ensure_not_cancelled!(_status, _workflow_id), do: :ok

  defp check_recorded_step(config, workflow_id, function_id, function_name) do
    sql = """
    SELECT output, error, function_name, serialization
    FROM #{table(config, "operation_outputs")}
    WHERE workflow_uuid = $1 AND function_id = $2
    """

    case config.db.query(config.conn, sql, [workflow_id, function_id]) do
      {:ok, %{rows: []}} ->
        :none

      {:ok, %{rows: [[output, error, recorded_name, _serialization]]}} ->
        ensure_step_name_matches!(workflow_id, function_id, function_name, recorded_name)
        decode_recorded_step(output, error)
    end
  end

  defp ensure_step_name_matches!(_workflow_id, _function_id, name, name), do: :ok

  defp ensure_step_name_matches!(workflow_id, function_id, expected, recorded) do
    raise Dbos.UnexpectedStepError,
      workflow_id: workflow_id,
      function_id: function_id,
      expected: expected,
      recorded: recorded
  end

  defp decode_recorded_step(output, nil), do: {:replay, Serialization.decode(output)}
  defp decode_recorded_step(_output, error), do: {:replay_failure, Serialization.decode(error)}

  defp try_insert_operation_output(config, attrs) do
    workflow_id = Map.fetch!(attrs, :workflow_id)
    function_id = Map.fetch!(attrs, :function_id)
    function_name = Map.fetch!(attrs, :function_name)
    output = Map.get(attrs, :output)
    error = Map.get(attrs, :error)
    started_at = Map.fetch!(attrs, :started_at)
    completed_at = Map.fetch!(attrs, :completed_at)
    child_workflow_id = Map.get(attrs, :child_workflow_id)

    {columns, values} =
      operation_output_columns_and_values(
        workflow_id,
        function_id,
        output,
        error,
        function_name,
        started_at,
        completed_at,
        child_workflow_id
      )

    placeholders = 1..length(values) |> Enum.map_join(", ", &"$#{&1}")

    sql = """
    INSERT INTO #{table(config, "operation_outputs")} (#{Enum.join(columns, ", ")})
    VALUES (#{placeholders})
    ON CONFLICT (workflow_uuid, function_id) DO NOTHING
    """

    {:ok, %{num_rows: num_rows}} = config.db.query(config.conn, sql, values)
    {:inserted, num_rows > 0}
  end

  defp operation_output_columns_and_values(
         workflow_id,
         function_id,
         output,
         error,
         function_name,
         started_at,
         completed_at,
         nil
       ) do
    {
      ~w(workflow_uuid function_id output error function_name started_at_epoch_ms completed_at_epoch_ms serialization),
      [
        workflow_id,
        function_id,
        output,
        error,
        function_name,
        started_at,
        completed_at,
        Serialization.format_name()
      ]
    }
  end

  defp operation_output_columns_and_values(
         workflow_id,
         function_id,
         output,
         error,
         function_name,
         started_at,
         completed_at,
         child_workflow_id
       ) do
    {
      ~w(workflow_uuid function_id output error function_name started_at_epoch_ms completed_at_epoch_ms serialization child_workflow_id),
      [
        workflow_id,
        function_id,
        output,
        error,
        function_name,
        started_at,
        completed_at,
        Serialization.format_name(),
        child_workflow_id
      ]
    }
  end

  defp reconcile_existing_operation_output(config, attrs) do
    workflow_id = Map.fetch!(attrs, :workflow_id)
    function_id = Map.fetch!(attrs, :function_id)

    sql = """
    SELECT output, error, function_name, serialization, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms
    FROM #{table(config, "operation_outputs")}
    WHERE workflow_uuid = $1 AND function_id = $2
    """

    case config.db.query(config.conn, sql, [workflow_id, function_id]) do
      {:ok, %{rows: [row]}} ->
        reconcile_operation_output_row(workflow_id, function_id, attrs, row)

      {:ok, %{rows: []}} ->
        raise RuntimeError,
          message:
            "workflow #{workflow_id} step #{function_id}: conflicting checkpoint row was deleted concurrently"
    end
  end

  defp reconcile_operation_output_row(workflow_id, function_id, attrs, [
         stored_output,
         stored_error,
         stored_name,
         _serialization,
         stored_child_id,
         stored_started_at,
         stored_completed_at
       ]) do
    function_name = Map.fetch!(attrs, :function_name)

    same_write =
      function_name == stored_name and
        stored_output == Map.get(attrs, :output) and
        stored_error == Map.get(attrs, :error) and
        stored_child_id == Map.get(attrs, :child_workflow_id) and
        stored_started_at == Map.fetch!(attrs, :started_at) and
        stored_completed_at == Map.fetch!(attrs, :completed_at)

    cond do
      same_write ->
        :ok

      function_name != stored_name ->
        raise Dbos.UnexpectedStepError,
          workflow_id: workflow_id,
          function_id: function_id,
          expected: function_name,
          recorded: stored_name

      true ->
        raise RuntimeError,
          message:
            "workflow #{workflow_id} step #{function_id}: a concurrent execution already checkpointed this step"
    end
  end

  defp refresh_executor_id(%Config{executor_id: executor_id}, _workflow_id)
       when executor_id in [nil, ""], do: :ok

  defp refresh_executor_id(config, workflow_id) do
    sql = """
    UPDATE #{table(config, "workflow_status")} SET executor_id = $1
    WHERE workflow_uuid = $2 AND (executor_id IS NULL OR executor_id <> $1)
    """

    case config.db.query(config.conn, sql, [config.executor_id, workflow_id]) do
      {:ok, _result} ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "failed to refresh workflow executor ID after checkpoint: workflow_id=#{workflow_id} executor_id=#{config.executor_id} error=#{inspect(reason)}"
        )
    end
  end

  defp encode_json_or_nil(nil), do: nil
  defp encode_json_or_nil(term), do: JSON.encode!(term)

  defp encode_or_nil(nil), do: nil
  defp encode_or_nil(term), do: Serialization.encode(term)

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
