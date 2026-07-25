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
    delay_until_epoch_ms
  )

  @doc """
  Inserts a workflow directly onto a queue: `status = 'ENQUEUED'`, or `'DELAYED'` with
  `delay_until_epoch_ms` set if `params[:delay_ms]` is a positive integer, per
  `notes/queues.md` §7. Idempotent on `workflow_uuid`. Raises `Dbos.QueueDeduplicatedError` if
  `params[:deduplication_id]` is already held by another workflow on the same queue.
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

    case config.db.query(config.conn, sql, values) do
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

    case config.db.query(config.conn, sql, [queue_name, deduplication_id]) do
      {:ok, %{rows: [[workflow_id]]}} -> workflow_id
      {:ok, %{rows: []}} -> nil
    end
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

  @doc """
  Atomically reassigns up to `opts[:batch_size]` non-queued `PENDING` rows owned by any of
  `dead_executor_ids` to `config.executor_id`, per the Phase 3b dead-executor reclaim design
  (`DECISIONS.md`). `opts[:batch_size]` defaults to unbounded. Filters by
  `config.application_version` when set, exactly as `ListWorkflows` does upstream, since this
  executor can only usefully re-run code matching its own registered version. Queued rows are
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

    {:ok, result} = config.db.query(config.conn, sql, params)
    Enum.map(result.rows, &WorkflowStatus.from_row/1)
  end

  defp reclaim_limit_clause(nil, _index), do: {"", []}
  defp reclaim_limit_clause(batch_size, index), do: {"LIMIT $#{index}", [batch_size]}

  defp application_version_clause(%{application_version: nil}, index), do: {"", [], index}

  defp application_version_clause(%{application_version: version}, index),
    do: {"AND application_version = $#{index}", [version], index + 1}

  @doc """
  The workflow ids of queued `PENDING` rows owned by any of `dead_executor_ids`, per the Phase 3b
  reclaim design. The caller clears each via `clear_queue_assignment/2` so the queue redistributes
  it, rather than re-invoking it directly.
  """
  def list_queued_pending_workflow_ids(%Config{} = config, dead_executor_ids) do
    sql = """
    SELECT workflow_uuid FROM #{table(config, "workflow_status")}
    WHERE executor_id = ANY($1) AND status = $2 AND queue_name IS NOT NULL
    """

    {:ok, result} =
      config.db.query(config.conn, sql, [dead_executor_ids, Status.to_string(:pending)])

    Enum.map(result.rows, fn [id] -> id end)
  end

  @doc """
  The distinct `executor_id`s among `PENDING` rows whose `updated_at` is at least
  `threshold_ms` old, per the Phase 3b orphan sweep (`Dbos.Cluster.OrphanSweep`).
  """
  def list_stale_pending_executor_ids(%Config{} = config, threshold_ms) do
    sql = """
    SELECT DISTINCT executor_id FROM #{table(config, "workflow_status")}
    WHERE status = $1 AND executor_id IS NOT NULL AND updated_at <= $2
    """

    cutoff_ms = System.os_time(:millisecond) - threshold_ms
    {:ok, result} = config.db.query(config.conn, sql, [Status.to_string(:pending), cutoff_ms])
    Enum.map(result.rows, fn [id] -> id end)
  end

  @doc "Registers `version_name` as a known application version, if not already present. Idempotent."
  def create_application_version(%Config{} = config, version_name) do
    sql = """
    INSERT INTO #{table(config, "application_versions")} (version_id, version_name, version_timestamp, created_at)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (version_name) DO NOTHING
    """

    now = System.os_time(:millisecond)
    {:ok, _result} = config.db.query(config.conn, sql, [Uuid.v4(), version_name, now, now])
    :ok
  end

  @doc "The most recently registered application version, or `:none` if none are registered."
  def get_latest_application_version(%Config{} = config) do
    sql = """
    SELECT version_name FROM #{table(config, "application_versions")}
    ORDER BY version_timestamp DESC LIMIT 1
    """

    case config.db.query(config.conn, sql, []) do
      {:ok, %{rows: [[name]]}} -> {:ok, name}
      {:ok, %{rows: []}} -> :none
    end
  end

  @doc """
  Promotes every `DELAYED` workflow whose `delay_until_epoch_ms` has passed to `ENQUEUED`, per
  `notes/queues.md` §7 (`TransitionDelayedWorkflows`). Run once per reconcile tick by
  `Dbos.Queue.Sup`, globally across all queues.
  """
  def transition_delayed_workflows(%Config{} = config) do
    sql = """
    UPDATE #{table(config, "workflow_status")}
        SET status = $1, updated_at = $2
        WHERE status = $3 AND delay_until_epoch_ms <= $2
    """

    now = System.os_time(:millisecond)

    {:ok, _result} =
      config.db.query(config.conn, sql, [
        Status.to_string(:enqueued),
        now,
        Status.to_string(:delayed)
      ])

    :ok
  end

  @doc """
  Registers or updates a queue's persisted configuration in the `queues` table, per
  `notes/queues.md` §1. Mirrors upstream's default `QueueConflictUpdateIfLatestVersion`
  resolution: an existing row is overwritten only if this executor's application version is the
  latest registered version, or if no version has been registered yet. Returns the row actually
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
      config.db.query(config.conn, insert_sql, [
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
      config.db.query(config.conn, sql, [
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

    case config.db.query(config.conn, sql, [name]) do
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

    {:ok, result} = config.db.query(config.conn, sql, [])
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
  The distinct, non-null partition keys among `ENQUEUED` workflows on `queue_name`, per
  `notes/queues.md` §6 (`GetQueuePartitions`).
  """
  def get_queue_partitions(%Config{} = config, queue_name) do
    sql = """
    SELECT DISTINCT queue_partition_key FROM #{table(config, "workflow_status")}
    WHERE queue_name = $1 AND status = $2 AND queue_partition_key IS NOT NULL
    """

    {:ok, result} = config.db.query(config.conn, sql, [queue_name, Status.to_string(:enqueued)])
    Enum.map(result.rows, fn [key] -> key end)
  end

  @doc """
  Whether `error` is a Postgres row-lock contention failure (`NOWAIT` finding a held lock), the
  only error `Dbos.Queue.Runner` treats as retryable backoff rather than a logged failure, per
  `notes/queues.md` §2.
  """
  def contention_error?(%{postgres: %{code: :lock_not_available}}), do: true
  def contention_error?(_error), do: false

  @doc """
  Claims up to a concurrency- and rate-limit-derived number of `ENQUEUED` workflows from `queue`
  for this executor, transitioning them to `PENDING`. Literal port of `DequeueWorkflows`,
  `notes/queues.md` §2. `opts`: `:partition_key` (default `nil`), `:local_running_count` (default
  `0`, the caller's in-process count of workflows already running for this queue/partition).
  Returns a list of `%{workflow_id:, name:, inputs:, config_name:}`, `inputs` already decoded.
  """
  def dequeue_workflows(%Config{} = config, %Dbos.Queue{} = queue, opts \\ []) do
    partition_key = Keyword.get(opts, :partition_key)
    local_running_count = Keyword.get(opts, :local_running_count, 0)

    isolation =
      if queue.global_concurrency || queue.rate_limit, do: :repeatable_read, else: :read_committed

    result =
      config.db.transaction(config.conn, [isolation: isolation], fn conn ->
        tx_config = %{config | conn: conn}
        do_dequeue_workflows(tx_config, queue, partition_key, local_running_count)
      end)

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

    {:ok, %{rows: [[num_recent]]}} = config.db.query(config.conn, sql, params)

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
      config.db.query(
        config.conn,
        sql,
        [queue.name, Status.to_string(:pending)] ++ partition_params
      )

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

    {:ok, result} = config.db.query(config.conn, sql, params)
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

    case config.db.query(config.conn, sql, params) do
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

  defp decode_or_nil(nil), do: nil
  defp decode_or_nil(binary), do: Serialization.decode(binary)

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
