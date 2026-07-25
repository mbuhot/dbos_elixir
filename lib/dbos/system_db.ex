defmodule Dbos.SystemDb do
  @moduledoc """
  Reads and writes against the `dbos` system database tables. Every function takes a
  `Dbos.Config` first, so a call site never has to know which adapter or schema it's running
  against.

  Every statement goes through `Dbos.DB.Retry`, which retries transient connection-level failures
  on a bounded exponential backoff and never retries a statement issued on a connection already
  inside a transaction. Two statements opt out because a re-execution would duplicate their
  effect: the plain `INSERT` behind `insert_debounced_workflow/2`, and the offset-appending
  `INSERT` behind `write_stream/5`. `fork_workflow/4`'s transaction opts out for the same reason.
  A statement that still fails raises `Dbos.SystemDbError` carrying the statement and the
  underlying driver error.
  """

  require Logger

  alias Dbos.Config
  alias Dbos.DB.Retry
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
    delay_until_epoch_ms
  )

  @doc """
  Inserts a workflow directly onto a queue: `status = 'ENQUEUED'`, or `'DELAYED'` with
  `delay_until_epoch_ms` set if `params[:delay_ms]` is a positive integer. Idempotent on
  `workflow_uuid`. Raises `Dbos.QueueDeduplicatedError` if `params[:deduplication_id]` is already
  held by another workflow on the same queue.
  """
  def insert_enqueued_workflow(%Config{} = config, params) do
    workflow_id = Map.get(params, :workflow_id) || Uuid.v4()
    now = System.os_time(:millisecond)
    delay_ms = Map.get(params, :delay_ms)
    {status, delay_until_epoch_ms} = enqueue_status(delay_ms, now)

    values = [
      workflow_id,
      Status.to_string(status),
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
      Serialization.format_name(),
      delay_until_epoch_ms
    ]

    placeholders = 1..length(@enqueue_columns) |> Enum.map_join(", ", &"$#{&1}")

    sql = """
    INSERT INTO #{table(config, "workflow_status")} (#{Enum.join(@enqueue_columns, ", ")})
    VALUES (#{placeholders})
    ON CONFLICT (workflow_uuid) DO UPDATE SET updated_at = EXCLUDED.updated_at
    """

    case query_result(config, sql, values) do
      {:ok, _result} ->
        {:ok, workflow_id}

      {:error, error} ->
        handle_enqueue_error(error, workflow_id, params)
    end
  end

  defp enqueue_status(delay_ms, now) when is_integer(delay_ms) and delay_ms > 0,
    do: {:delayed, now + delay_ms}

  defp enqueue_status(_delay_ms, _now), do: {:enqueued, nil}

  defp handle_enqueue_error(error, workflow_id, params) do
    if dedup_unique_violation?(error) do
      raise Dbos.QueueDeduplicatedError,
        workflow_id: workflow_id,
        queue_name: Map.fetch!(params, :queue_name),
        deduplication_id: Map.get(params, :deduplication_id)
    else
      raise error
    end
  end

  defp dedup_unique_violation?(%{postgres: %{code: :unique_violation}}), do: true
  defp dedup_unique_violation?(_error), do: false

  @doc "The workflow id currently holding `(queue_name, deduplication_id)`'s slot, or `nil` if it's free."
  def get_deduplicated_workflow(%Config{} = config, queue_name, deduplication_id) do
    sql = """
    SELECT workflow_uuid FROM #{table(config, "workflow_status")}
    WHERE queue_name = $1 AND deduplication_id = $2
    """

    case query(config, sql, [queue_name, deduplication_id]) do
      {:ok, %{rows: [[workflow_id]]}} -> workflow_id
      {:ok, %{rows: []}} -> nil
    end
  end

  @doc "Fetches one workflow's status row by id."
  def get_workflow_status(%Config{} = config, workflow_id) do
    sql =
      "SELECT #{select_list(WorkflowStatus)} FROM #{table(config, "workflow_status")} WHERE workflow_uuid = $1"

    case query(config, sql, [workflow_id]) do
      {:ok, %{rows: [row]}} -> {:ok, WorkflowStatus.from_row(row)}
      {:ok, %{rows: []}} -> {:error, :not_found}
    end
  end

  @doc "Lists workflows matching the given filters; `:status`, `:name`, `:queue_name`, `:executor_id`, and `:application_version` each accept a single value or a list."
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

    {:ok, result} = query(config, sql, all_params)
    {:ok, Enum.map(result.rows, &WorkflowStatus.from_row/1)}
  end

  @doc "Returns a workflow's checkpointed steps, ordered by `function_id`."
  def get_workflow_steps(%Config{} = config, workflow_id) do
    sql = """
    SELECT #{select_list(StepInfo)} FROM #{table(config, "operation_outputs")}
    WHERE workflow_uuid = $1
    ORDER BY function_id ASC
    """

    {:ok, result} = query(config, sql, [workflow_id])
    {:ok, Enum.map(result.rows, &StepInfo.from_row/1)}
  end

  @doc """
  Returns a workflow's outcome: `{:ok, term}`, `{:error, exception}`, `{:error,
  %Dbos.WorkflowCancelledError{}}`, `{:error, %Dbos.MaxRecoveryAttemptsExceededError{}}`, or
  `:pending` while still running. Distinguishes `:cancelled` and
  `:max_recovery_attempts_exceeded` as their own outcomes.
  """
  def get_workflow_result(%Config{} = config, workflow_id) do
    case get_workflow_status(config, workflow_id) do
      {:error, :not_found} ->
        {:error, :not_found}

      {:ok, %WorkflowStatus{status: :success, output: output}} ->
        {:ok, output}

      {:ok, %WorkflowStatus{status: :error, error: error}} ->
        {:error, error}

      {:ok, %WorkflowStatus{status: :cancelled}} ->
        {:error, %Dbos.WorkflowCancelledError{workflow_id: workflow_id}}

      {:ok, %WorkflowStatus{status: :max_recovery_attempts_exceeded, recovery_attempts: attempts}} ->
        {:error,
         %Dbos.MaxRecoveryAttemptsExceededError{workflow_id: workflow_id, attempts: attempts}}

      {:ok, %WorkflowStatus{}} ->
        :pending
    end
  end

  @doc """
  Upserts a `workflow_status` row: `INSERT ... ON CONFLICT (workflow_uuid) DO UPDATE`.
  `opts[:increment_attempts]` gates whether a re-entrant call bumps `recovery_attempts` (only
  when the incoming status is not `:enqueued`/`:delayed`); `opts[:max_retries]` gates the
  `MAX_RECOVERY_ATTEMPTS_EXCEEDED` transition. `attrs[:owner_xid]` defaults to a fresh UUID; it
  is written only on first insert and is returned unchanged on every later call for the same
  workflow id.

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
      transaction(config, [], fn conn ->
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
  The ordered operation-execution checks: workflow existence,
  cancellation, whether the step already ran, and (if so) that its recorded `function_name`
  matches. Returns `:none` (not yet run), `{:replay, output}`, or `{:replay_failure, failure}`
  with both decoded via `Dbos.Serialization.decode/1`.
  """
  def check_operation_execution(%Config{} = config, workflow_id, function_id, function_name) do
    {:ok, result} =
      transaction(config, [], fn conn ->
        tx_config = %{config | conn: conn}
        status = fetch_workflow_status_for_check!(tx_config, workflow_id)
        ensure_not_cancelled!(status, workflow_id)
        check_recorded_step(tx_config, workflow_id, function_id, function_name)
      end)

    result
  end

  @doc """
  Checkpoints a step outcome: `output`/`error` are already-encoded
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
  Writes a workflow's terminal outcome: `attrs[:status]` is `:success`
  or `:error`, `attrs[:output]`/`attrs[:error]` are already-encoded strings or `nil`. Clears
  `deduplication_id` only; `queue_name` and `started_at_epoch_ms` are left as-is. Refuses to
  overwrite a row already `:success`/`:error`/`:cancelled`; if the row was
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

    {:ok, %{num_rows: num_rows}} = query(config, sql, values)

    if num_rows == 0 do
      handle_update_outcome_conflict(config, workflow_id)
    else
      :ok
    end
  end

  @doc """
  Checks whether a child workflow has already been recorded at `(parent_workflow_id,
  parent_step_id)`. Returns `:none` if no
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

    case query(config, sql, [parent_workflow_id, parent_step_id]) do
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
  Peeks at `(workflow_id, function_id)` in `operation_outputs`: no row means this is a new
  workflow or one that has not yet reached this point, so it inserts `function_name` there and
  returns `true`; a row whose `function_name` already matches means a replay of an
  already-patched run, also `true`; a row with a different `function_name` means an execution
  that already ran past this point under the old code, returns `false` and inserts nothing.
  """
  def patch(%Config{} = config, workflow_id, function_id, function_name) do
    {:ok, patched?} =
      transaction(config, [], fn conn ->
        tx_config = %{config | conn: conn}
        check_or_insert_patch(tx_config, workflow_id, function_id, function_name)
      end)

    patched?
  end

  defp check_or_insert_patch(config, workflow_id, function_id, function_name) do
    sql = """
    SELECT function_name FROM #{table(config, "operation_outputs")}
    WHERE workflow_uuid = $1 AND function_id = $2
    """

    case query(config, sql, [workflow_id, function_id]) do
      {:ok, %{rows: []}} ->
        insert_patch_marker(config, workflow_id, function_id, function_name)
        true

      {:ok, %{rows: [[recorded_name]]}} ->
        recorded_name == function_name
    end
  end

  defp insert_patch_marker(config, workflow_id, function_id, function_name) do
    sql = """
    INSERT INTO #{table(config, "operation_outputs")} (workflow_uuid, function_id, function_name)
    VALUES ($1, $2, $3)
    """

    {:ok, _result} = query(config, sql, [workflow_id, function_id, function_name])
  end

  @doc """
  Puts a queued `PENDING` workflow back to `ENQUEUED`, clearing `started_at_epoch_ms`.
  Returns `:cleared`, or `:not_cleared` if the
  row was not `PENDING` with a `queue_name` (e.g. another executor already claimed it).
  """
  def clear_queue_assignment(%Config{} = config, workflow_id) do
    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET status = $1, started_at_epoch_ms = NULL
        WHERE workflow_uuid = $2 AND queue_name IS NOT NULL AND status = $3
    """

    {:ok, %{num_rows: num_rows}} =
      query(config, sql, [
        Status.to_string(:enqueued),
        workflow_id,
        Status.to_string(:pending)
      ])

    if num_rows > 0, do: :cleared, else: :not_cleared
  end

  @doc """
  Atomically reassigns up to `opts[:batch_size]` non-queued `PENDING` rows owned by any of
  `dead_executor_ids` to `config.executor_id`. `opts[:batch_size]` defaults to unbounded. Filters
  by `config.application_version` when set, since this executor can only usefully re-run code
  matching its own registered version. Queued rows are
  excluded — the caller clears their queue assignment via
  `list_queued_pending_workflow_ids/2`/`clear_queue_assignment/2` instead. `FOR UPDATE SKIP
  LOCKED` lets concurrent callers claim disjoint batches without blocking on each other, the same
  pattern `dequeue_candidate_ids/4` uses. Returns the reassigned rows as `Dbos.WorkflowStatus`
  structs.
  """
  def reclaim_pending_workflows(%Config{} = config, dead_executor_ids, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size)
    {version_clause, version_params, next_index} = application_version_clause(config, 4)
    {limit_clause, limit_params} = reclaim_limit_clause(batch_size, next_index)

    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET executor_id = $1
        WHERE workflow_uuid IN (
          SELECT workflow_uuid FROM #{table(config, "workflow_status")}
          WHERE executor_id = ANY($2) AND status = $3 AND queue_name IS NULL
            #{version_clause}
          ORDER BY created_at ASC
          #{limit_clause}
          FOR UPDATE SKIP LOCKED
        )
    RETURNING #{select_list(WorkflowStatus)}
    """

    params =
      [config.executor_id, dead_executor_ids, Status.to_string(:pending)] ++
        version_params ++ limit_params

    {:ok, result} = query(config, sql, params)
    Enum.map(result.rows, &WorkflowStatus.from_row/1)
  end

  defp reclaim_limit_clause(nil, _index), do: {"", []}
  defp reclaim_limit_clause(batch_size, index), do: {"LIMIT $#{index}", [batch_size]}

  defp application_version_clause(%{application_version: nil}, index), do: {"", [], index}

  defp application_version_clause(%{application_version: version}, index),
    do: {"AND application_version = $#{index}", [version], index + 1}

  @doc """
  The ids of non-queued `PENDING` rows still owned by any of `dead_executor_ids` — the same set
  `reclaim_pending_workflows/3` claims from, without taking any lock. A row appears here after a
  reclaim pass exactly when that pass skipped it, either because a concurrent executor holds its
  lock or because a backend that has not finished being torn down still does.
  """
  def list_reclaimable_pending_workflow_ids(%Config{} = config, dead_executor_ids) do
    {version_clause, version_params, _next_index} = application_version_clause(config, 3)

    sql = """
    SELECT workflow_uuid FROM #{table(config, "workflow_status")}
    WHERE executor_id = ANY($1) AND status = $2 AND queue_name IS NULL
      #{version_clause}
    """

    params = [dead_executor_ids, Status.to_string(:pending)] ++ version_params
    {:ok, result} = query(config, sql, params)
    Enum.map(result.rows, fn [id] -> id end)
  end

  @doc """
  The workflow ids of queued `PENDING` rows owned by any of `dead_executor_ids`.
  The caller clears each via `clear_queue_assignment/2` so the queue redistributes
  it, rather than re-invoking it directly.
  """
  def list_queued_pending_workflow_ids(%Config{} = config, dead_executor_ids) do
    sql = """
    SELECT workflow_uuid FROM #{table(config, "workflow_status")}
    WHERE executor_id = ANY($1) AND status = $2 AND queue_name IS NOT NULL
    """

    {:ok, result} =
      query(config, sql, [dead_executor_ids, Status.to_string(:pending)])

    Enum.map(result.rows, fn [id] -> id end)
  end

  @doc """
  The distinct `executor_id`s among `PENDING` rows whose `updated_at` is at least
  `threshold_ms` old, for `Dbos.Cluster.OrphanSweep`.
  """
  def list_stale_pending_executor_ids(%Config{} = config, threshold_ms) do
    sql = """
    SELECT DISTINCT executor_id FROM #{table(config, "workflow_status")}
    WHERE status = $1 AND executor_id IS NOT NULL AND updated_at <= $2
    """

    cutoff_ms = System.os_time(:millisecond) - threshold_ms
    {:ok, result} = query(config, sql, [Status.to_string(:pending), cutoff_ms])
    Enum.map(result.rows, fn [id] -> id end)
  end

  @doc """
  Cancels every workflow in `workflow_ids`: a data-modifying CTE that updates every row not
  already `SUCCESS`/`ERROR`/`CANCELLED` to `CANCELLED` (nulling `started_at_epoch_ms`,
  `queue_name`, `deduplication_id`; setting `completed_at`), then returns every id that existed
  at all — including ids already terminal, which are left untouched but still reported as
  "existing".
  """
  def cancel_workflows(%Config{} = config, workflow_ids) do
    now = System.os_time(:millisecond)

    sql = """
    WITH existing AS (
      SELECT workflow_uuid FROM #{table(config, "workflow_status")} WHERE workflow_uuid = ANY($3)
    ), updated AS (
      UPDATE #{table(config, "workflow_status")}
          SET status = $1, updated_at = $2, completed_at = $2, started_at_epoch_ms = NULL,
              queue_name = NULL, deduplication_id = NULL
          WHERE workflow_uuid = ANY($3) AND status NOT IN ($4, $5, $6)
          RETURNING workflow_uuid
    )
    SELECT workflow_uuid FROM existing
    """

    params = [
      Status.to_string(:cancelled),
      now,
      workflow_ids,
      Status.to_string(:success),
      Status.to_string(:error),
      Status.to_string(:cancelled)
    ]

    {:ok, result} = query(config, sql, params)
    Enum.map(result.rows, fn [id] -> id end)
  end

  @doc """
  Cancels every `PENDING`/`ENQUEUED`/`DELAYED` workflow created at or before `cutoff_ms` — the
  operation backing `POST /dbos-global-timeout`. Returns the cancelled ids.
  """
  def cancel_all_before(%Config{} = config, cutoff_ms) do
    sql = """
    SELECT workflow_uuid FROM #{table(config, "workflow_status")}
    WHERE created_at <= $1 AND status IN ($2, $3, $4)
    """

    params = [
      cutoff_ms,
      Status.to_string(:pending),
      Status.to_string(:enqueued),
      Status.to_string(:delayed)
    ]

    {:ok, result} = query(config, sql, params)
    workflow_ids = Enum.map(result.rows, fn [id] -> id end)
    if workflow_ids == [], do: [], else: cancel_workflows(config, workflow_ids)
  end

  @max_descendant_depth 1000

  @doc "The ids of workflows whose `parent_workflow_id` is `workflow_id` (one level)."
  def child_workflow_ids(%Config{} = config, workflow_id) do
    sql = """
    SELECT workflow_uuid FROM #{table(config, "workflow_status")} WHERE parent_workflow_id = $1
    """

    {:ok, result} = query(config, sql, [workflow_id])
    Enum.map(result.rows, fn [id] -> id end)
  end

  @doc """
  Every descendant workflow id under `workflow_id`, breadth-first over `parent_workflow_id`.
  Already-visited ids are never re-queued, so a cycle in the parent link (which should not occur,
  but is not trusted) cannot loop forever; the walk also gives up after
  #{@max_descendant_depth} levels, returning whatever it has collected so far.
  """
  def descendant_workflow_ids(%Config{} = config, workflow_id) do
    walk_descendants(config, [workflow_id], MapSet.new([workflow_id]), 0, [])
  end

  defp walk_descendants(_config, [], _visited, _depth, acc), do: Enum.reverse(acc)

  defp walk_descendants(_config, _queue, _visited, depth, acc)
       when depth >= @max_descendant_depth,
       do: Enum.reverse(acc)

  defp walk_descendants(config, queue, visited, depth, acc) do
    children =
      queue
      |> Enum.flat_map(&child_workflow_ids(config, &1))
      |> Enum.uniq()
      |> Enum.reject(&MapSet.member?(visited, &1))

    visited = Enum.reduce(children, visited, &MapSet.put(&2, &1))
    walk_descendants(config, children, visited, depth + 1, Enum.reverse(children) ++ acc)
  end

  @doc """
  Resumes every workflow in `workflow_ids`: re-enqueues onto `opts[:queue_name]` (default
  `Dbos.Queue.internal_queue_name/0`), resetting `recovery_attempts` to `0` and clearing
  `workflow_deadline_epoch_ms`, `deduplication_id`, `started_at_epoch_ms`, `completed_at`. Rows
  already `SUCCESS`/`ERROR` are excluded by the guard and left untouched, but are still reported
  as "existing": resuming an already-terminal (success/error) workflow is a silent no-op. Returns
  the existing ids.
  """
  def resume_workflows(%Config{} = config, workflow_ids, opts \\ []) do
    queue_name = Keyword.get(opts, :queue_name, Dbos.Queue.internal_queue_name())
    now = System.os_time(:millisecond)

    sql = """
    WITH existing AS (
      SELECT workflow_uuid FROM #{table(config, "workflow_status")} WHERE workflow_uuid = ANY($5)
    ), updated AS (
      UPDATE #{table(config, "workflow_status")}
          SET status = $1, queue_name = $2, recovery_attempts = $3,
              workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
              started_at_epoch_ms = NULL, updated_at = $4, completed_at = NULL
          WHERE workflow_uuid = ANY($5) AND status NOT IN ($6, $7)
          RETURNING workflow_uuid
    )
    SELECT workflow_uuid FROM existing
    """

    params = [
      Status.to_string(:enqueued),
      queue_name,
      0,
      now,
      workflow_ids,
      Status.to_string(:success),
      Status.to_string(:error)
    ]

    {:ok, result} = query(config, sql, params)
    Enum.map(result.rows, fn [id] -> id end)
  end

  @doc "Whether `workflow_id`'s status is currently `CANCELLED`."
  def workflow_cancelled?(%Config{} = config, workflow_id) do
    sql = "SELECT status FROM #{table(config, "workflow_status")} WHERE workflow_uuid = $1"

    case query(config, sql, [workflow_id]) do
      {:ok, %{rows: [[status]]}} -> status == Status.to_string(:cancelled)
      {:ok, %{rows: []}} -> false
    end
  end

  @doc """
  Resolves this workflow's durable deadline: if a
  `workflow_timeout_ms` is set but no `workflow_deadline_epoch_ms` yet, computes and durably
  persists `now + timeout` (guarded so a race between concurrent resolvers only ever persists
  once); otherwise returns whatever deadline is already recorded. Returns `nil` if no timeout is
  set at all.
  """
  def resolve_workflow_deadline(%Config{} = config, workflow_id) do
    sql = """
    SELECT workflow_timeout_ms, workflow_deadline_epoch_ms
    FROM #{table(config, "workflow_status")} WHERE workflow_uuid = $1
    """

    case query(config, sql, [workflow_id]) do
      {:ok, %{rows: [[_timeout, deadline]]}} when is_integer(deadline) ->
        deadline

      {:ok, %{rows: [[timeout, nil]]}} when is_integer(timeout) ->
        compute_and_persist_deadline(config, workflow_id, timeout)

      {:ok, %{rows: [[nil, nil]]}} ->
        nil

      {:ok, %{rows: []}} ->
        nil
    end
  end

  defp compute_and_persist_deadline(config, workflow_id, timeout_ms) do
    deadline_ms = System.os_time(:millisecond) + timeout_ms

    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET workflow_deadline_epoch_ms = $1
        WHERE workflow_uuid = $2 AND workflow_deadline_epoch_ms IS NULL
        RETURNING workflow_deadline_epoch_ms
    """

    case query(config, sql, [deadline_ms, workflow_id]) do
      {:ok, %{rows: [[persisted]]}} ->
        persisted

      {:ok, %{rows: []}} ->
        resolve_workflow_deadline(config, workflow_id)
    end
  end

  @doc """
  Forks `original_workflow_id` from step `start_step` into a new workflow id
  (`opts[:new_workflow_id]`, default a fresh random UUID): copies
  `operation_outputs`/`workflow_events_history`/`streams` rows with
  `function_id < start_step`, recomputes `workflow_events` from the latest history row per key
  before the fork point, marks the original `was_forked_from = TRUE`, and enqueues the fork onto
  `opts[:queue_name]` (default the internal queue) so it re-runs starting at `start_step`.
  `opts[:application_version]` overrides the copied application version. Raises
  `Dbos.NonExistentWorkflowError` if `original_workflow_id` has no row.
  """
  def fork_workflow(%Config{} = config, original_workflow_id, start_step, opts \\ []) do
    new_workflow_id = Keyword.get_lazy(opts, :new_workflow_id, &Uuid.v4/0)
    queue_name = Keyword.get(opts, :queue_name, Dbos.Queue.internal_queue_name())

    {:ok, new_workflow_id} =
      transaction(
        config,
        [],
        fn conn ->
          tx_config = %{config | conn: conn}
          original = fetch_forkable_workflow!(tx_config, original_workflow_id)

          insert_forked_workflow(
            tx_config,
            original_workflow_id,
            original,
            new_workflow_id,
            queue_name,
            Keyword.get(opts, :application_version)
          )

          if start_step > 0 do
            copy_operation_outputs(tx_config, original_workflow_id, new_workflow_id, start_step)

            copy_workflow_events_history(
              tx_config,
              original_workflow_id,
              new_workflow_id,
              start_step
            )

            copy_workflow_events(tx_config, original_workflow_id, new_workflow_id, start_step)
            copy_streams(tx_config, original_workflow_id, new_workflow_id, start_step)
          end

          mark_was_forked_from(tx_config, original_workflow_id)
          new_workflow_id
        end,
        retry: false
      )

    new_workflow_id
  end

  defp fetch_forkable_workflow!(config, workflow_id) do
    sql = """
    SELECT name, authenticated_user, assumed_role, authenticated_roles, application_version,
           application_id, inputs, serialization, class_name, config_name, attributes
    FROM #{table(config, "workflow_status")} WHERE workflow_uuid = $1
    """

    case query(config, sql, [workflow_id]) do
      {:ok, %{rows: [row]}} -> row
      {:ok, %{rows: []}} -> raise Dbos.NonExistentWorkflowError, workflow_id: workflow_id
    end
  end

  defp insert_forked_workflow(
         config,
         original_workflow_id,
         [
           name,
           authenticated_user,
           assumed_role,
           authenticated_roles,
           application_version,
           application_id,
           inputs,
           serialization,
           class_name,
           config_name,
           attributes
         ],
         new_workflow_id,
         queue_name,
         application_version_override
       ) do
    now = System.os_time(:millisecond)

    sql = """
    INSERT INTO #{table(config, "workflow_status")}
        (workflow_uuid, status, name, queue_name, authenticated_user, assumed_role,
         authenticated_roles, executor_id, application_version, application_id, created_at,
         recovery_attempts, updated_at, inputs, owner_xid, class_name, config_name, serialization,
         attributes, forked_from)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
    """

    params = [
      new_workflow_id,
      Status.to_string(:enqueued),
      name,
      queue_name,
      authenticated_user,
      assumed_role,
      authenticated_roles,
      config.executor_id,
      application_version_override || application_version,
      application_id,
      now,
      0,
      now,
      inputs,
      Uuid.v4(),
      class_name,
      config_name,
      serialization,
      attributes,
      original_workflow_id
    ]

    {:ok, _result} = query(config, sql, params)
    :ok
  end

  defp copy_operation_outputs(config, original_id, new_id, start_step) do
    sql = """
    INSERT INTO #{table(config, "operation_outputs")}
        (workflow_uuid, function_id, function_name, output, error, child_workflow_id,
         started_at_epoch_ms, completed_at_epoch_ms, serialization)
    SELECT $2, function_id, function_name, output, error, child_workflow_id,
           started_at_epoch_ms, completed_at_epoch_ms, serialization
    FROM #{table(config, "operation_outputs")}
    WHERE workflow_uuid = $1 AND function_id < $3
    """

    {:ok, _result} = query(config, sql, [original_id, new_id, start_step])
    :ok
  end

  defp copy_workflow_events_history(config, original_id, new_id, start_step) do
    sql = """
    INSERT INTO #{table(config, "workflow_events_history")}
        (workflow_uuid, function_id, key, value, serialization)
    SELECT $2, function_id, key, value, serialization
    FROM #{table(config, "workflow_events_history")}
    WHERE workflow_uuid = $1 AND function_id < $3
    """

    {:ok, _result} = query(config, sql, [original_id, new_id, start_step])
    :ok
  end

  defp copy_workflow_events(config, original_id, new_id, start_step) do
    sql = """
    INSERT INTO #{table(config, "workflow_events")} (workflow_uuid, key, value, serialization)
    SELECT $2, h.key, h.value, h.serialization
    FROM (
      SELECT key, value, serialization,
             ROW_NUMBER() OVER (PARTITION BY key ORDER BY function_id DESC) AS rn
      FROM #{table(config, "workflow_events_history")}
      WHERE workflow_uuid = $1 AND function_id < $3
    ) h
    WHERE h.rn = 1
    """

    {:ok, _result} = query(config, sql, [original_id, new_id, start_step])
    :ok
  end

  defp copy_streams(config, original_id, new_id, start_step) do
    sql = """
    INSERT INTO #{table(config, "streams")}
        (workflow_uuid, key, value, "offset", function_id, serialization)
    SELECT $2, key, value, "offset", function_id, serialization
    FROM #{table(config, "streams")}
    WHERE workflow_uuid = $1 AND function_id < $3
    """

    {:ok, _result} = query(config, sql, [original_id, new_id, start_step])
    :ok
  end

  defp mark_was_forked_from(config, workflow_id) do
    sql = """
    UPDATE #{table(config, "workflow_status")} SET was_forked_from = TRUE WHERE workflow_uuid = $1
    """

    {:ok, _result} = query(config, sql, [workflow_id])
    :ok
  end

  @doc """
  Deletes every `workflow_status` row (cascading to its `operation_outputs` etc.) older than an
  effective cutoff — the operation backing the admin `/dbos-garbage-collect` route.
  `opts[:cutoff_epoch_timestamp_ms]` and/or `opts[:rows_threshold]` (keep at least this many of
  the newest rows) may be given; the effective cutoff is the max of both. `PENDING`/`ENQUEUED`/
  `DELAYED` rows are never deleted. Returns the number of rows deleted.
  """
  def garbage_collect_workflows(%Config{} = config, opts \\ []) do
    case effective_gc_cutoff(config, opts) do
      nil -> 0
      cutoff_ms -> delete_workflows_before(config, cutoff_ms)
    end
  end

  defp effective_gc_cutoff(config, opts) do
    cutoff = Keyword.get(opts, :cutoff_epoch_timestamp_ms)
    threshold_cutoff = rows_threshold_cutoff(config, Keyword.get(opts, :rows_threshold))

    [cutoff, threshold_cutoff]
    |> Enum.reject(&is_nil/1)
    |> case do
      [] -> nil
      values -> Enum.max(values)
    end
  end

  defp rows_threshold_cutoff(_config, nil), do: nil

  defp rows_threshold_cutoff(config, rows_threshold) do
    sql = """
    SELECT created_at FROM #{table(config, "workflow_status")}
    ORDER BY created_at DESC LIMIT 1 OFFSET $1
    """

    case query(config, sql, [rows_threshold - 1]) do
      {:ok, %{rows: [[created_at]]}} -> created_at
      {:ok, %{rows: []}} -> nil
    end
  end

  defp delete_workflows_before(config, cutoff_ms) do
    sql = """
    DELETE FROM #{table(config, "workflow_status")}
    WHERE created_at < $1 AND status NOT IN ($2, $3, $4)
    """

    params = [
      cutoff_ms,
      Status.to_string(:pending),
      Status.to_string(:enqueued),
      Status.to_string(:delayed)
    ]

    {:ok, %{num_rows: num_rows}} = query(config, sql, params)
    num_rows
  end

  @debounce_columns ~w(
    workflow_uuid status inputs name queue_name deduplication_id application_version
    created_at updated_at recovery_attempts delay_until_epoch_ms debounce_deadline_epoch_ms
    is_debounced serialization
  )

  @doc """
  Inserts a fresh debounced workflow: always `DELAYED` (even for a zero-length bounce window),
  `is_debounced = TRUE`, `deduplication_id = params[:debounce_key]`.
  """
  def insert_debounced_workflow(%Config{} = config, params) do
    workflow_id = Map.get(params, :workflow_id) || Uuid.v4()
    now = System.os_time(:millisecond)

    values = [
      workflow_id,
      Status.to_string(:delayed),
      Serialization.encode(Map.fetch!(params, :inputs)),
      Map.fetch!(params, :name),
      Map.fetch!(params, :queue_name),
      Map.fetch!(params, :debounce_key),
      Map.get(params, :application_version, config.application_version),
      now,
      now,
      0,
      Map.fetch!(params, :delay_until_epoch_ms),
      Map.get(params, :debounce_deadline_epoch_ms),
      true,
      Serialization.format_name()
    ]

    placeholders = 1..length(@debounce_columns) |> Enum.map_join(", ", &"$#{&1}")

    sql = """
    INSERT INTO #{table(config, "workflow_status")} (#{Enum.join(@debounce_columns, ", ")})
    VALUES (#{placeholders})
    """

    case query_result_once(config, sql, values) do
      {:ok, _result} ->
        {:ok, workflow_id}

      {:error, error} ->
        handle_enqueue_error(
          error,
          workflow_id,
          %{
            queue_name: Map.fetch!(params, :queue_name),
            deduplication_id: Map.fetch!(params, :debounce_key)
          }
        )
    end
  end

  @doc """
  Bounces an existing debounced, still-`DELAYED` workflow held at `(name, queue_name,
  debounce_key)`: extends `delay_until_epoch_ms` (capped at `debounce_deadline_epoch_ms` if one
  is set) and replaces `inputs`.
  Returns `{:bounced, workflow_id}` if a row matched, or `:not_found` if none did (the caller
  must then fall back to `get_debounce_holder/3`).
  """
  def bounce_debounced_workflow(
        %Config{} = config,
        name,
        queue_name,
        debounce_key,
        inputs,
        requested_delay_until_epoch_ms
      ) do
    now = System.os_time(:millisecond)

    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET delay_until_epoch_ms = CASE
              WHEN debounce_deadline_epoch_ms IS NOT NULL AND debounce_deadline_epoch_ms < $1
              THEN debounce_deadline_epoch_ms
              ELSE $1
            END,
            inputs = $2, serialization = $3, updated_at = $4
        WHERE name = $5 AND queue_name = $6 AND deduplication_id = $7
          AND status = $8 AND is_debounced = TRUE
        RETURNING workflow_uuid
    """

    params = [
      requested_delay_until_epoch_ms,
      Serialization.encode(inputs),
      Serialization.format_name(),
      now,
      name,
      queue_name,
      debounce_key,
      Status.to_string(:delayed)
    ]

    case query(config, sql, params) do
      {:ok, %{rows: [[workflow_id]]}} -> {:bounced, workflow_id}
      {:ok, %{rows: []}} -> :not_found
    end
  end

  @doc """
  Looks up whoever currently holds `(queue_name, debounce_key)`'s deduplication slot.
  Returns `:none` if the slot is free, or
  `{:holder, workflow_id, is_debounced, name}`.
  """
  def get_debounce_holder(%Config{} = config, queue_name, debounce_key) do
    sql = """
    SELECT workflow_uuid, is_debounced, name FROM #{table(config, "workflow_status")}
    WHERE queue_name = $1 AND deduplication_id = $2
    """

    case query(config, sql, [queue_name, debounce_key]) do
      {:ok, %{rows: [[workflow_id, is_debounced, name]]}} ->
        {:holder, workflow_id, is_debounced, name}

      {:ok, %{rows: []}} ->
        :none
    end
  end

  @doc """
  Registers/updates a schedule's static definition in `workflow_schedules`.
  Idempotent on `schedule_name`: an existing row's `last_fired_at`/`status` are left untouched
  (an operator-paused schedule, or one already mid-catch-up, survives a redeploy unchanged) —
  every other column is overwritten from the caller's declared definition. Returns
  `{last_fired_at_epoch_ms | nil, status}`.
  """
  def register_schedule(%Config{} = config, attrs) do
    sql = """
    INSERT INTO #{table(config, "workflow_schedules")}
        (schedule_id, schedule_name, workflow_name, workflow_class_name, schedule, context,
         automatic_backfill, cron_timezone, queue_name)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (schedule_name) DO UPDATE SET
        workflow_name = EXCLUDED.workflow_name,
        workflow_class_name = EXCLUDED.workflow_class_name,
        schedule = EXCLUDED.schedule,
        context = EXCLUDED.context,
        automatic_backfill = EXCLUDED.automatic_backfill,
        cron_timezone = EXCLUDED.cron_timezone,
        queue_name = EXCLUDED.queue_name
    RETURNING last_fired_at, status
    """

    params = [
      Map.get_lazy(attrs, :schedule_id, &Uuid.v4/0),
      Map.fetch!(attrs, :schedule_name),
      Map.fetch!(attrs, :workflow_name),
      Map.get(attrs, :workflow_class_name),
      Map.fetch!(attrs, :schedule),
      Map.fetch!(attrs, :context),
      Map.get(attrs, :automatic_backfill, false),
      Map.get(attrs, :cron_timezone),
      Map.get(attrs, :queue_name)
    ]

    {:ok, %{rows: [[last_fired_at, status]]}} = query(config, sql, params)
    {parse_epoch_ms_text(last_fired_at), status}
  end

  @doc "Every `ACTIVE` schedule's definition, per `Dbos.Scheduler`'s reconcile loop."
  def list_active_schedules(%Config{} = config) do
    sql = """
    SELECT schedule_name, workflow_name, schedule, context, automatic_backfill, cron_timezone,
           queue_name, last_fired_at
    FROM #{table(config, "workflow_schedules")} WHERE status = 'ACTIVE'
    """

    {:ok, result} = query(config, sql, [])
    Enum.map(result.rows, &schedule_from_row/1)
  end

  defp schedule_from_row([
         name,
         workflow_name,
         cron,
         context,
         automatic_backfill,
         cron_timezone,
         queue_name,
         last_fired_at
       ]) do
    %{
      schedule_name: name,
      workflow_name: workflow_name,
      cron: cron,
      context: Serialization.decode(context),
      automatic_backfill: automatic_backfill,
      cron_timezone: cron_timezone,
      queue_name: queue_name,
      last_fired_at: parse_epoch_ms_text(last_fired_at)
    }
  end

  @doc """
  Records the most recent fire time for a schedule. Informational/for-recovery only — the
  running scheduler's own in-memory floor is what actually drives due-occurrence computation
  tick to tick; this is what a fresh process reads back as its catch-up floor when
  `automatic_backfill` is set.
  """
  def update_schedule_last_fired_at(%Config{} = config, schedule_name, epoch_ms) do
    sql =
      "UPDATE #{table(config, "workflow_schedules")} SET last_fired_at = $1 WHERE schedule_name = $2"

    {:ok, _result} =
      query(config, sql, [Integer.to_string(epoch_ms), schedule_name])

    :ok
  end

  defp parse_epoch_ms_text(nil), do: nil
  defp parse_epoch_ms_text(text), do: String.to_integer(text)

  @doc "Registers `version_name` as a known application version, if not already present. Idempotent."
  def create_application_version(%Config{} = config, version_name) do
    sql = """
    INSERT INTO #{table(config, "application_versions")} (version_id, version_name, version_timestamp, created_at)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (version_name) DO NOTHING
    """

    now = System.os_time(:millisecond)
    {:ok, _result} = query(config, sql, [Uuid.v4(), version_name, now, now])
    :ok
  end

  @doc "The most recently registered application version, or `:none` if none are registered."
  def get_latest_application_version(%Config{} = config) do
    sql = """
    SELECT version_name FROM #{table(config, "application_versions")}
    ORDER BY version_timestamp DESC LIMIT 1
    """

    case query(config, sql, []) do
      {:ok, %{rows: [[name]]}} -> {:ok, name}
      {:ok, %{rows: []}} -> :none
    end
  end

  @doc """
  Promotes every `DELAYED` workflow whose `delay_until_epoch_ms` has passed to `ENQUEUED`. Run
  once per reconcile tick by `Dbos.Queue.Sup`, globally across all queues.
  """
  def transition_delayed_workflows(%Config{} = config) do
    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET status = $1, updated_at = $2,
            deduplication_id = CASE WHEN is_debounced THEN NULL ELSE deduplication_id END
        WHERE status = $3 AND delay_until_epoch_ms <= $2
    """

    now = System.os_time(:millisecond)

    {:ok, _result} =
      query(config, sql, [
        Status.to_string(:enqueued),
        now,
        Status.to_string(:delayed)
      ])

    :ok
  end

  @doc """
  Registers or updates a queue's persisted configuration in the `queues` table: an existing row
  is overwritten only if this executor's application version is the latest registered version,
  or if no version has been registered yet. Returns the row actually
  persisted (which may differ from `queue` if an existing row was left untouched).
  """
  def register_queue(%Config{} = config, %Dbos.Queue{} = queue) do
    update_existing =
      case get_latest_application_version(config) do
        :none -> true
        {:ok, latest} -> latest == config.application_version
      end

    now = System.os_time(:millisecond)

    insert_sql = """
    INSERT INTO #{table(config, "queues")}
        (queue_id, name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
         priority_enabled, partition_queue, polling_interval_sec, created_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (name) DO NOTHING
    """

    {rate_limit_max, rate_limit_period_sec} = queue_rate_limit_columns(queue.rate_limit)

    {:ok, %{num_rows: inserted_rows}} =
      query(config, insert_sql, [
        Uuid.v4(),
        queue.name,
        queue.global_concurrency,
        queue.worker_concurrency,
        rate_limit_max,
        rate_limit_period_sec,
        queue.priority_enabled,
        queue.partition_queue,
        queue.base_polling_interval_ms / 1000.0,
        now,
        now
      ])

    if inserted_rows == 0 and update_existing do
      update_queue_row(config, queue)
    end

    get_queue(config, queue.name)
  end

  defp update_queue_row(config, queue) do
    sql = """
    UPDATE #{table(config, "queues")}
        SET concurrency = $1, worker_concurrency = $2, rate_limit_max = $3,
            rate_limit_period_sec = $4, priority_enabled = $5, partition_queue = $6,
            polling_interval_sec = $7, updated_at = $8
        WHERE name = $9
    """

    {rate_limit_max, rate_limit_period_sec} = queue_rate_limit_columns(queue.rate_limit)

    {:ok, _result} =
      query(config, sql, [
        queue.global_concurrency,
        queue.worker_concurrency,
        rate_limit_max,
        rate_limit_period_sec,
        queue.priority_enabled,
        queue.partition_queue,
        queue.base_polling_interval_ms / 1000.0,
        System.os_time(:millisecond),
        queue.name
      ])

    :ok
  end

  defp queue_rate_limit_columns(nil), do: {nil, nil}

  defp queue_rate_limit_columns(%{limit: limit, period_ms: period_ms}),
    do: {limit, period_ms / 1000.0}

  @doc "Fetches one queue's persisted configuration, or `:not_found`."
  def get_queue(%Config{} = config, name) do
    sql = """
    SELECT name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
           priority_enabled, partition_queue, polling_interval_sec
    FROM #{table(config, "queues")} WHERE name = $1
    """

    case query(config, sql, [name]) do
      {:ok, %{rows: [row]}} -> {:ok, queue_from_row(row)}
      {:ok, %{rows: []}} -> :not_found
    end
  end

  @doc "Every persisted queue's configuration."
  def list_queues(%Config{} = config) do
    sql = """
    SELECT name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
           priority_enabled, partition_queue, polling_interval_sec
    FROM #{table(config, "queues")}
    """

    {:ok, result} = query(config, sql, [])
    {:ok, Enum.map(result.rows, &queue_from_row/1)}
  end

  defp queue_from_row([
         name,
         concurrency,
         worker_concurrency,
         rate_limit_max,
         rate_limit_period_sec,
         priority_enabled,
         partition_queue,
         polling_interval_sec
       ]) do
    %Dbos.Queue{
      name: name,
      global_concurrency: concurrency,
      worker_concurrency: worker_concurrency,
      rate_limit: queue_rate_limit_from_row(rate_limit_max, rate_limit_period_sec),
      priority_enabled: priority_enabled,
      partition_queue: partition_queue,
      base_polling_interval_ms: round(polling_interval_sec * 1000.0)
    }
  end

  defp queue_rate_limit_from_row(nil, _period_sec), do: nil

  defp queue_rate_limit_from_row(limit, period_sec),
    do: %{limit: limit, period_ms: round(period_sec * 1000.0)}

  @doc """
  The distinct, non-null partition keys among `ENQUEUED` workflows on `queue_name`.
  """
  def get_queue_partitions(%Config{} = config, queue_name) do
    sql = """
    SELECT DISTINCT queue_partition_key FROM #{table(config, "workflow_status")}
    WHERE queue_name = $1 AND status = $2 AND queue_partition_key IS NOT NULL
    """

    {:ok, result} = query(config, sql, [queue_name, Status.to_string(:enqueued)])
    Enum.map(result.rows, fn [key] -> key end)
  end

  @doc """
  Whether `error` is a Postgres row-lock contention failure (`NOWAIT` finding a held lock), the
  only error `Dbos.Queue.Runner` treats as retryable backoff rather than a logged failure.
  """
  def contention_error?(%{postgres: %{code: :lock_not_available}}), do: true
  def contention_error?(_error), do: false

  @doc """
  Claims up to a concurrency- and rate-limit-derived number of `ENQUEUED` workflows from `queue`
  for this executor, transitioning them to `PENDING`. `opts`: `:partition_key` (default `nil`), `:local_running_count` (default
  `0`, the caller's in-process count of workflows already running for this queue/partition).
  Returns a list of `%{workflow_id:, name:, inputs:, config_name:}`, `inputs` already decoded.
  """
  def dequeue_workflows(%Config{} = config, %Dbos.Queue{} = queue, opts \\ []) do
    partition_key = Keyword.get(opts, :partition_key)
    local_running_count = Keyword.get(opts, :local_running_count, 0)

    isolation =
      if queue.global_concurrency || queue.rate_limit, do: :repeatable_read, else: :read_committed

    result =
      transaction(
        config,
        [isolation: isolation],
        fn conn ->
          tx_config = %{config | conn: conn}
          do_dequeue_workflows(tx_config, queue, partition_key, local_running_count)
        end,
        retry_conflicts: true
      )

    case result do
      {:ok, workflows} -> workflows
      {:error, :nothing_claimed} -> []
    end
  end

  defp do_dequeue_workflows(config, queue, partition_key, local_running_count) do
    with {:ok, num_recent} <- rate_limiter_precheck(config, queue, partition_key),
         {:ok, max_tasks} <- concurrency_limit(config, queue, partition_key, local_running_count) do
      candidate_ids = dequeue_candidate_ids(config, queue, partition_key, max_tasks)
      claimed = claim_candidates(config, queue, candidate_ids, num_recent)

      if claimed == [] do
        config.db.rollback(config.conn, :nothing_claimed)
      else
        claimed
      end
    else
      :rate_limited -> config.db.rollback(config.conn, :nothing_claimed)
      :no_capacity -> config.db.rollback(config.conn, :nothing_claimed)
    end
  end

  defp rate_limiter_precheck(_config, %Dbos.Queue{rate_limit: nil}, _partition_key), do: {:ok, 0}

  defp rate_limiter_precheck(config, %Dbos.Queue{rate_limit: rate_limit} = queue, partition_key) do
    cutoff_ms = System.os_time(:millisecond) - rate_limit.period_ms

    {partition_sql, partition_params} = partition_clause(partition_key, 5)

    sql = """
    SELECT COUNT(*) FROM #{table(config, "workflow_status")}
    WHERE queue_name = $1 AND rate_limited = TRUE
      AND status NOT IN ($2, $3) AND started_at_epoch_ms > $4
      #{partition_sql}
    """

    params =
      [queue.name, Status.to_string(:enqueued), Status.to_string(:delayed), cutoff_ms] ++
        partition_params

    {:ok, %{rows: [[num_recent]]}} = query(config, sql, params)

    if num_recent >= rate_limit.limit, do: :rate_limited, else: {:ok, num_recent}
  end

  defp concurrency_limit(config, queue, partition_key, local_running_count) do
    max_tasks =
      -1
      |> narrow_by_worker_concurrency(queue, local_running_count)
      |> narrow_by_global_concurrency(config, queue, partition_key)

    if max_tasks == 0, do: :no_capacity, else: {:ok, max_tasks}
  end

  defp narrow_by_worker_concurrency(max_tasks, %Dbos.Queue{worker_concurrency: nil}, _local),
    do: max_tasks

  defp narrow_by_worker_concurrency(max_tasks, %Dbos.Queue{worker_concurrency: limit}, local) do
    available = max(limit - local, 0)
    if max_tasks < 0 or available < max_tasks, do: available, else: max_tasks
  end

  defp narrow_by_global_concurrency(
         max_tasks,
         _config,
         %Dbos.Queue{global_concurrency: nil},
         _pk
       ),
       do: max_tasks

  defp narrow_by_global_concurrency(
         max_tasks,
         config,
         %Dbos.Queue{global_concurrency: limit} = queue,
         partition_key
       ) do
    {partition_sql, partition_params} = partition_clause(partition_key, 3)

    sql = """
    SELECT COUNT(*) FROM #{table(config, "workflow_status")}
    WHERE queue_name = $1 AND status = $2 #{partition_sql}
    """

    {:ok, %{rows: [[pending_count]]}} =
      query(config, sql, [queue.name, Status.to_string(:pending)] ++ partition_params)

    available = max(limit - pending_count, 0)
    if max_tasks < 0 or available < max_tasks, do: available, else: max_tasks
  end

  defp partition_clause(nil, _param_index), do: {"", []}
  defp partition_clause("", _param_index), do: {"", []}

  defp partition_clause(partition_key, param_index),
    do: {"AND queue_partition_key = $#{param_index}", [partition_key]}

  defp dequeue_candidate_ids(config, queue, partition_key, max_tasks) do
    is_latest_version = latest_version?(config)
    application_version = config.application_version

    version_clause =
      if is_latest_version,
        do: "(application_version = $3 OR application_version IS NULL)",
        else: "application_version = $3"

    {partition_sql, partition_params} = partition_clause(partition_key, 4)

    lock_clause =
      if queue.global_concurrency == nil, do: "FOR UPDATE SKIP LOCKED", else: "FOR UPDATE NOWAIT"

    limit_clause = if max_tasks >= 0, do: "LIMIT #{max_tasks}", else: ""

    sql = """
    SELECT workflow_uuid FROM #{table(config, "workflow_status")}
    WHERE queue_name = $1 AND status = $2 AND #{version_clause}
      #{partition_sql}
    ORDER BY priority ASC, created_at ASC
    #{lock_clause}
    #{limit_clause}
    """

    params = [queue.name, Status.to_string(:enqueued), application_version] ++ partition_params

    {:ok, result} = query(config, sql, params)
    Enum.map(result.rows, fn [id] -> id end)
  end

  defp latest_version?(config) do
    case get_latest_application_version(config) do
      :none -> true
      {:ok, latest} -> latest == config.application_version
    end
  end

  defp claim_candidates(config, queue, candidate_ids, num_recent) do
    rate_limited = queue.rate_limit != nil

    {claimed, _count} =
      Enum.reduce_while(candidate_ids, {[], num_recent}, fn id, {claimed, count} ->
        if rate_limited and count >= queue.rate_limit.limit do
          {:halt, {claimed, count}}
        else
          case claim_one(config, id, rate_limited) do
            nil -> {:cont, {claimed, count}}
            workflow -> {:cont, {[workflow | claimed], count + 1}}
          end
        end
      end)

    Enum.reverse(claimed)
  end

  defp claim_one(config, workflow_id, rate_limited) do
    now = System.os_time(:millisecond)

    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET status = $1, application_version = $2, executor_id = $3, started_at_epoch_ms = $4,
            rate_limited = $5,
            workflow_deadline_epoch_ms = CASE
                WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                THEN $4 + workflow_timeout_ms
                ELSE workflow_deadline_epoch_ms
            END
        WHERE workflow_uuid = $6 AND status = $7
        RETURNING name, inputs, serialization, config_name
    """

    params = [
      Status.to_string(:pending),
      config.application_version,
      config.executor_id,
      now,
      rate_limited,
      workflow_id,
      Status.to_string(:enqueued)
    ]

    case query(config, sql, params) do
      {:ok, %{rows: [[name, inputs, _serialization, config_name]]}} ->
        %{
          workflow_id: workflow_id,
          name: name,
          inputs: decode_or_nil(inputs),
          config_name: config_name
        }

      {:ok, %{rows: []}} ->
        nil
    end
  end

  defp handle_update_outcome_conflict(config, workflow_id) do
    sql = "SELECT status FROM #{table(config, "workflow_status")} WHERE workflow_uuid = $1"
    cancelled = Status.to_string(:cancelled)

    case query(config, sql, [workflow_id]) do
      {:ok, %{rows: []}} ->
        :ok

      {:ok, %{rows: [[^cancelled]]}} ->
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

    {:ok, %{rows: [row]}} = query(config, sql, values)
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
      query(config, sql, [
        Status.to_string(:max_recovery_attempts_exceeded),
        workflow_id,
        Status.to_string(:pending)
      ])

    :ok
  end

  defp fetch_workflow_status_for_check!(config, workflow_id) do
    sql = "SELECT status FROM #{table(config, "workflow_status")} WHERE workflow_uuid = $1"

    case query(config, sql, [workflow_id]) do
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

    case query(config, sql, [workflow_id, function_id]) do
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

    {:ok, %{num_rows: num_rows}} = query(config, sql, values)
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

    case query(config, sql, [workflow_id, function_id]) do
      {:ok, %{rows: [row]}} ->
        reconcile_operation_output_row(workflow_id, function_id, attrs, row)

      {:ok, %{rows: []}} ->
        raise Dbos.ConcurrentCheckpointConflictError,
          workflow_id: workflow_id,
          function_id: function_id,
          reason: "conflicting checkpoint row was deleted concurrently"
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
        raise Dbos.ConcurrentCheckpointConflictError,
          workflow_id: workflow_id,
          function_id: function_id,
          reason: "a concurrent execution already checkpointed this step"
    end
  end

  defp refresh_executor_id(%Config{executor_id: executor_id}, _workflow_id)
       when executor_id in [nil, ""], do: :ok

  defp refresh_executor_id(config, workflow_id) do
    sql = """
    UPDATE #{table(config, "workflow_status")} SET executor_id = $1
    WHERE workflow_uuid = $2 AND (executor_id IS NULL OR executor_id <> $1)
    """

    case query_result(config, sql, [config.executor_id, workflow_id]) do
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

  defp decode_or_nil(nil), do: nil
  defp decode_or_nil(binary), do: Serialization.decode(binary)

  @null_topic "__null__topic__"
  @stream_closed_marker :__dbos_stream_closed__

  @doc "The sentinel topic `send`/`recv` substitute for `nil`."
  def null_topic, do: @null_topic

  @doc """
  Writes a message into `notifications`. Raises
  `Dbos.NonExistentWorkflowError` if `destination_id` has no `workflow_status` row (the
  `destination_uuid` foreign key violation). Sending to a terminal workflow is not rejected —
  only existence is checked.
  """
  def send_notification(%Config{} = config, destination_id, topic, message) do
    sql = """
    INSERT INTO #{table(config, "notifications")}
        (destination_uuid, topic, message, serialization, message_uuid, created_at_epoch_ms)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (message_uuid) DO NOTHING
    """

    values = [
      destination_id,
      topic,
      Serialization.encode(message),
      Serialization.format_name(),
      Uuid.v4(),
      System.os_time(:millisecond)
    ]

    case query_result(config, sql, values) do
      {:ok, _result} ->
        :ok

      {:error, %{postgres: %{code: :foreign_key_violation}}} ->
        raise Dbos.NonExistentWorkflowError, workflow_id: destination_id

      {:error, error} ->
        raise Dbos.SystemDbError, sql: sql, params: values, cause: error
    end
  end

  @doc "Whether an unconsumed message is waiting for `(destination_id, topic)`. The `recv` recheck query."
  def notification_pending?(%Config{} = config, destination_id, topic) do
    sql = """
    SELECT EXISTS (
      SELECT 1 FROM #{table(config, "notifications")}
      WHERE destination_uuid = $1 AND topic = $2 AND consumed = false
    )
    """

    {:ok, %{rows: [[exists]]}} = query(config, sql, [destination_id, topic])
    exists
  end

  @doc """
  Consumes the oldest unconsumed message for `(destination_id, topic)`: selects the oldest by
  `created_at_epoch_ms`, updates by `message_uuid` (never a `DELETE`, never keyed by timestamp).
  Returns `{:ok, term}`
  or `:none`.
  """
  def consume_notification(%Config{} = config, destination_id, topic) do
    sql = """
    WITH oldest_entry AS (
      SELECT message_uuid
      FROM #{table(config, "notifications")}
      WHERE destination_uuid = $1 AND topic = $2 AND consumed = false
      ORDER BY created_at_epoch_ms ASC
      LIMIT 1
    )
    UPDATE #{table(config, "notifications")}
    SET consumed = true
    WHERE message_uuid = (SELECT message_uuid FROM oldest_entry)
    RETURNING message
    """

    case query(config, sql, [destination_id, topic]) do
      {:ok, %{rows: [[message]]}} -> {:ok, Serialization.decode(message)}
      {:ok, %{rows: []}} -> :none
    end
  end

  @doc """
  Upserts `workflow_events` (per-key, last write wins) and inserts into
  `workflow_events_history` (per-step, keyed by `function_id`). Returns `value` unchanged.
  """
  def set_event_value(%Config{} = config, workflow_id, function_id, key, value) do
    encoded = Serialization.encode(value)
    serialization = Serialization.format_name()

    upsert_sql = """
    INSERT INTO #{table(config, "workflow_events")} (workflow_uuid, key, value, serialization)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (workflow_uuid, key)
        DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization
    """

    {:ok, _result} =
      query(config, upsert_sql, [workflow_id, key, encoded, serialization])

    history_sql = """
    INSERT INTO #{table(config, "workflow_events_history")} (workflow_uuid, function_id, key, value, serialization)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (workflow_uuid, function_id, key)
        DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization
    """

    {:ok, _result} =
      query(config, history_sql, [
        workflow_id,
        function_id,
        key,
        encoded,
        serialization
      ])

    value
  end

  @doc "Reads `workflow_events`'s current value for `(workflow_id, key)`."
  def get_event_value(%Config{} = config, workflow_id, key) do
    sql =
      "SELECT value FROM #{table(config, "workflow_events")} WHERE workflow_uuid = $1 AND key = $2"

    case query(config, sql, [workflow_id, key]) do
      {:ok, %{rows: [[value]]}} -> {:ok, Serialization.decode(value)}
      {:ok, %{rows: []}} -> :none
    end
  end

  @doc "The encoded stream-closed sentinel: an atom, since ETF is not cross-language."
  def stream_closed_marker, do: @stream_closed_marker

  @doc """
  Appends `value` to stream `key` at the next sequential offset (`MAX(offset)+1`, or `0` if
  none, computed in the same statement as the insert). Returns `{:error, :stream_closed}` without
  inserting if the close sentinel was already written for this `(workflow_id, key)`.
  """
  def write_stream(%Config{} = config, workflow_id, function_id, key, value) do
    check_sql = """
    SELECT 1 FROM #{table(config, "streams")}
    WHERE workflow_uuid = $1 AND key = $2 AND value = $3 LIMIT 1
    """

    sentinel = Serialization.encode(@stream_closed_marker)

    case query(config, check_sql, [workflow_id, key, sentinel]) do
      {:ok, %{rows: [_ | _]}} ->
        {:error, :stream_closed}

      {:ok, %{rows: []}} ->
        insert_sql = """
        INSERT INTO #{table(config, "streams")} (workflow_uuid, key, value, "offset", function_id, serialization)
        SELECT $1, $2, $3, COALESCE(
            (SELECT MAX("offset") FROM #{table(config, "streams")} WHERE workflow_uuid = $1 AND key = $2), -1
        ) + 1, $4, $5
        """

        {:ok, _result} =
          query_once(config, insert_sql, [
            workflow_id,
            key,
            Serialization.encode(value),
            function_id,
            Serialization.format_name()
          ])

        :ok
    end
  end

  @doc "Writes the close sentinel for stream `key`."
  def close_stream(%Config{} = config, workflow_id, function_id, key) do
    write_stream(config, workflow_id, function_id, key, @stream_closed_marker)
  end

  @doc """
  Reads stream `key` from `from_offset` (inclusive): stops at the close sentinel without
  including it. Returns `{values, next_offset, closed?}`.
  """
  def read_stream_page(%Config{} = config, workflow_id, key, from_offset) do
    sql = """
    SELECT value, "offset" FROM #{table(config, "streams")}
    WHERE workflow_uuid = $1 AND key = $2 AND "offset" >= $3
    ORDER BY "offset" ASC
    """

    {:ok, result} = query(config, sql, [workflow_id, key, from_offset])
    collect_stream_rows(result.rows, from_offset, [])
  end

  defp collect_stream_rows([], next_offset, acc), do: {Enum.reverse(acc), next_offset, false}

  defp collect_stream_rows([[value, offset] | rest], _next_offset, acc) do
    case Serialization.decode(value) do
      @stream_closed_marker -> {Enum.reverse(acc), offset + 1, true}
      decoded -> collect_stream_rows(rest, offset + 1, [decoded | acc])
    end
  end

  defp query(config, sql, params) do
    raise_on_error(Retry.query(config, sql, params), sql, params)
  end

  defp query_once(config, sql, params) do
    raise_on_error(Retry.query(config, sql, params, retry: false), sql, params)
  end

  defp query_result(config, sql, params), do: Retry.query(config, sql, params)

  defp query_result_once(config, sql, params), do: Retry.query(config, sql, params, retry: false)

  defp raise_on_error({:ok, result}, _sql, _params), do: {:ok, result}

  defp raise_on_error({:error, error}, sql, params),
    do: raise(Dbos.SystemDbError, sql: sql, params: params, cause: error)

  defp transaction(config, tx_opts, fun, opts \\ []) do
    case Retry.transaction(config, tx_opts, fun, opts) do
      {:error, error} ->
        if is_exception(error) do
          raise Dbos.SystemDbError, sql: "<transaction>", params: [], cause: error
        else
          {:error, error}
        end

      other ->
        other
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

    {clauses, params} =
      Enum.reduce(filters, {[], []}, fn {key, condition, cast}, {clauses, params} ->
        case Keyword.fetch(opts, key) do
          {:ok, value} ->
            param_index = length(params) + 1
            {casted_value, column_clause} = build_clause(condition, param_index, cast, value)
            {clauses ++ [column_clause], params ++ [casted_value]}

          :error ->
            {clauses, params}
        end
      end)

    clauses =
      if Keyword.get(opts, :queues_only, false),
        do: clauses ++ ["queue_name IS NOT NULL"],
        else: clauses

    where_sql = if clauses == [], do: "", else: "WHERE " <> Enum.join(clauses, " AND ")
    {where_sql, params}
  end

  defp build_clause(condition, param_index, cast, value) when is_list(value) do
    {Enum.map(value, cast), "#{condition} = ANY($#{param_index})"}
  end

  defp build_clause(condition, param_index, cast, value) do
    casted_value = cast.(value)

    column_clause =
      if String.contains?(condition, " ") do
        "#{condition} $#{param_index}"
      else
        "#{condition} = $#{param_index}"
      end

    {casted_value, column_clause}
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
