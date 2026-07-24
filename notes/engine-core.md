# DBOS Transact (Go) — Core Workflow Lifecycle

Reference: `reference/dbos-transact-golang/dbos/internal/sysdb/system_database.go` (sysdb) and
`reference/dbos-transact-golang/dbos/workflow.go` (engine). All line numbers below are `file.go:line`
in that reference tree as of the commit pinned for this port (`2a7705c`, "Re-stamp workflow
executor_id when successfully checkpointing a step (#411)").

Two tables matter for this document:

- `workflow_status` (one row per workflow instance) — full column list assembled from
  `migrations/1_initial_dbos_schema.sql` plus every later `ALTER TABLE` migration (see §9 for the
  complete list of migration files consulted).
- `operation_outputs` (one row per checkpointed step / child-workflow-spawn / getResult call),
  keyed by `(workflow_uuid, function_id)`.

All `%s` in SQL below is the schema prefix (e.g. `dbos.`), substituted by `s.RenderSQL`.

---

## 1. `InsertWorkflowStatus`

`system_database.go:1048-1294`. Requires a caller-supplied transaction (`input.Tx`); errors with
`"transaction is required for InsertWorkflowStatus"` if nil (`:1049-1051`).

### Full SQL

```sql
INSERT INTO %sworkflow_status (
    workflow_uuid,
    status,
    name,
    queue_name,
    authenticated_user,
    assumed_role,
    authenticated_roles,
    executor_id,
    application_version,
    application_id,
    created_at,
    recovery_attempts,
    updated_at,
    workflow_timeout_ms,
    workflow_deadline_epoch_ms,
    inputs,
    deduplication_id,
    priority,
    queue_partition_key,
    owner_xid,
    parent_workflow_id,
    class_name,
    config_name,
    serialization,
    delay_until_epoch_ms,
    attributes,
    schedule_name,
    debounce_deadline_epoch_ms,
    is_debounced
) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
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
```
(`:1129-1171`)

### Parameter order (exact, `:1189-1221`)

| # | Value |
|---|---|
| $1 | `Status.ID` (workflow UUID) |
| $2 | `Status.Status` |
| $3 | `Status.Name` |
| $4 | `Status.QueueName` |
| $5 | `Status.AuthenticatedUser` |
| $6 | `Status.AssumedRole` |
| $7 | `authenticatedRoles` — `json.Marshal(Status.AuthenticatedRoles)` (a JSON array stored in a TEXT column) |
| $8 | `Status.ExecutorID` |
| $9 | `applicationVersion` (nil if empty string) |
| $10 | `Status.ApplicationID` |
| $11 | `Status.CreatedAt.Round(time.Millisecond).UnixMilli()` — rounded "to slightly reduce the likelihood of collisions" |
| $12 | `attempts` — see below |
| $13 | `updatedAt.UnixMilli()` — `Status.UpdatedAt` if non-zero, else `time.Now()` |
| $14 | `timeoutMs` (nil unless `Status.Timeout > 0`; rounded to ms) |
| $15 | `deadline` (nil unless `Status.Deadline` non-zero; epoch ms) |
| $16 | `Status.Input` (already-encoded) |
| $17 | `deduplicationID` (nil if empty) |
| $18 | `Status.Priority` |
| $19 | `queuePartitionKey` (nil if empty) |
| $20 | `input.OwnerXID` (`*string`, may be nil) |
| $21 | `parentWorkflowID` (nil if empty) |
| $22 | `className` (nil if empty) |
| $23 | `Status.ConfigName` |
| $24 | `Status.Serialization` |
| $25 | `delayUntilEpochMs` (nil unless `Status.DelayUntil` non-zero) |
| $26 | `attributesJSON` (nil unless `Status.Attributes` non-empty; JSON-marshalled) |
| $27 | `scheduleName` (nil if empty) |
| $28 | `debounceDeadlineEpochMs` (nil unless `Status.DebounceDeadline` non-zero) |
| $29 | `Status.IsDebounced` |
| $30 | `models.WorkflowStatusEnqueued` ("ENQUEUED") — used only inside the CASE clauses |
| $31 | `models.WorkflowStatusDelayed` ("DELAYED") — used only inside the CASE clauses |
| $32 | `recoveryIncrement` — `1` if `input.IncrementAttempts`, else `0` |

### Initial values

`attempts` (bound to $12, the INSERT-branch value of `recovery_attempts`):
- `0` if the incoming status is `ENQUEUED` or `DELAYED` (`:1055-1057`)
- `1` otherwise (i.e. `PENDING`)

Note this initial value only applies on first insert (the INSERT branch of the upsert). It is
irrelevant on conflict.

### ON CONFLICT semantics (row already exists)

- **`recovery_attempts`**: incremented by `recoveryIncrement` (0 or 1) **iff** the incoming
  (`EXCLUDED`) status is *not* `ENQUEUED` and *not* `DELAYED`. Otherwise left unchanged. Since
  `recoveryIncrement` is only 1 when the Go caller sets `IncrementAttempts` — which happens only
  for a dequeue (`params.isDequeue`) or an explicit recovery (`params.isRecovery`), see
  `workflow.go:1412` — a plain re-entrant call to `RunWorkflow` for an already-`PENDING` workflow
  (e.g. losing a start race) does **not** bump `recovery_attempts`; only dequeuing off a queue or
  an explicit recovery pass does.
- **`updated_at`**: always overwritten to `EXCLUDED.updated_at` (the new call's timestamp).
- **`executor_id`**: overwritten to `EXCLUDED.executor_id` (the new caller's executor) *unless*
  the incoming status is `ENQUEUED` or `DELAYED`, in which case the existing `executor_id` is
  preserved. (Enqueuing/delaying doesn't claim ownership of a row that may already be executing
  elsewhere.)
- **`owner_xid` is never touched by the `DO UPDATE SET`.** It is only ever written by the original
  `INSERT` branch. `RETURNING owner_xid` therefore always returns the value stamped by whichever
  call *first* created the row — see §8.
- All other columns (`name`, `queue_name`, `inputs`, `serialization`, `attributes`, etc.) are left
  as originally inserted; a conflicting call's values for them are silently discarded except where
  used to detect mismatches (see below).

### Post-query validation and errors

- **Unique-violation on the dedup constraint** (`uq_workflow_status_queue_name_dedup_id` on
  `(queue_name, deduplication_id)`): mapped via `s.dialect.IsUniqueViolation(err)` to
  `models.NewQueueDeduplicatedError(workflowID, queueName, deduplicationID)` (`:1239-1245`,
  `ErrorCodeQueueDeduplicated`).
- **Name mismatch**: if the caller passed a non-empty `Status.Name` and the row's stored `name`
  differs, returns `models.NewUnexpectedWorkflowError` — "Workflow already exists with a different
  name..." (`:1259-1261`, `ErrorCodeUnexpectedWorkflow`).
- **Queue mismatch**: if the caller passed a non-empty `Status.QueueName` and it differs from the
  stored `queue_name`, same error type, "...already exists in a different queue..." (`:1262-1264`).
- **`MAX_RECOVERY_ATTEMPTS_EXCEEDED` transition** (`:1266-1291`): triggered when *all* of:
  - `result.Status` is not `SUCCESS` and not `ERROR` (i.e. still non-terminal), AND
  - `input.MaxRetries > 0`, AND
  - `result.Attempts > input.MaxRetries + 1`, AND
  - `!ownerXIDMatches` — the caller's `input.OwnerXID` does **not** equal the row's returned
    `owner_xid` (comparison treats both-nil as a match too, `:1235-1236`).

  When triggered, runs a second statement in the *same transaction*:
  ```sql
  UPDATE %sworkflow_status
             SET status = $1, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
             WHERE workflow_uuid = $2 AND status = $3
  ```
  with `$1 = MAX_RECOVERY_ATTEMPTS_EXCEEDED`, `$2 = workflowID`, `$3 = PENDING` (`:1272-1279`) —
  i.e. this only actually fires if the row is still `PENDING` at that instant. Columns nulled:
  `deduplication_id`, `started_at_epoch_ms`, `queue_name`. Note `executor_id`, `output`, `error`
  are **not** touched. The transaction is then **committed here** (not left for the caller), and
  the function returns `models.NewDeadLetterQueueError(workflowID, input.MaxRetries)`
  (`:1281-1291`, `ErrorCodeDeadLetterQueue`). If the `UPDATE` itself fails, that error is returned
  instead (transaction is not committed in that sub-branch — left to caller's rollback).

### Return shape (`InsertWorkflowResult`)

`Attempts`, `Status`, `Name`, `QueueName` (`*string`), `QueuePartitionKey` (`*string`), `Timeout`
(converted from `workflow_timeout_ms`, `time.Duration`, only set if `>0`), `WorkflowDeadline`
(converted from `workflow_deadline_epoch_ms`), `OwnerXID` (`string`, from the returned `*string`,
empty if null).

### Transaction requirement

Runs inside the caller's transaction only (no isolation level enforced here beyond whatever the
caller opened; `workflow.go` opens it with `TxOptions{}` — default/read-committed). Correctness
relies on the `ON CONFLICT` upsert being atomic, not on isolation level.

---

## 2. `CheckOperationExecution`

`system_database.go:2851-2925`. Signature: `(ctx, CheckOperationExecutionDBInput{WorkflowID,
StepID, StepName, Tx}) (*RecordedResult, error)`.

### Transaction

If `input.Tx` is nil, opens its own transaction with default `TxOptions{}` and always
`Rollback`s it at the end (`:2856-2864`) — comment: "We don't need to commit this transaction --
it is just useful for having READ COMMITTED across the reads". I.e. the two SELECTs below are
read-committed-consistent with each other, never mutate anything.

### Ordered checks

1. **Workflow status lookup**:
   ```sql
   SELECT status FROM %sworkflow_status WHERE workflow_uuid = $1
   ```
   (`:2867`, param: `WorkflowID`)
   - No row → `models.NewNonExistentWorkflowError(workflowID)` (`ErrorCodeNonExistentWorkflow`).
   - Any other error → wrapped generic error.
2. **Cancellation check**: if `status == CANCELLED` → `models.NewWorkflowCancelledError(workflowID,
   nil)` (`ErrorCodeWorkflowCancelled`) (`:2886-2888`). (Notably: *only* `CANCELLED` short-circuits
   here; `SUCCESS`/`ERROR`/`MAX_RECOVERY_ATTEMPTS_EXCEEDED` do not.)
3. **Step output lookup**:
   ```sql
   SELECT output, error, function_name, serialization
                             FROM %soperation_outputs
                             WHERE workflow_uuid = $1 AND function_id = $2
   ```
   (`:2870-2872`, params: `WorkflowID, StepID`)
   - No row → returns `(nil, nil)` — "not yet executed", caller proceeds to actually run the step.
   - Any other error → wrapped generic error.
4. **Determinism check**: if `input.StepName != recordedFunctionName` →
   `models.NewUnexpectedStepError(workflowID, stepID, StepName, recordedFunctionName)`
   (`ErrorCodeUnexpectedStep`) (`:2906-2909`).
5. Otherwise returns `&RecordedResult{Output, ErrStr, Serialization}` — `ErrStr` is nil unless the
   stored `error` column is non-null and non-empty (`:2915-2918`); `Serialization` defaults to `""`
   if the column is null.

### Return shape

`*RecordedResult{Output *string, ErrStr *string, Serialization string}`, or `nil` (not-yet-run),
or an error per above.

---

## 3. Recording a step outcome — `RecordOperationResult`

`system_database.go:2603-2719`, called from `RunAsStep` (`workflow.go:2547-2563`) and from the two
`GetResult`/`checkGetResultExecution` step-recording call sites (`workflow.go:2459` area /
`:322`,`:436`).

### SQL — insert

Column list is built dynamically; `child_workflow_id` is appended only if
`input.ChildWorkflowID != ""`:

```sql
INSERT INTO %soperation_outputs (workflow_uuid, function_id, output, error, function_name, started_at_epoch_ms, completed_at_epoch_ms, serialization[, child_workflow_id])
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8[, $9])
    ON CONFLICT (workflow_uuid, function_id) DO NOTHING
```
(`:2634-2648`)

Parameters in order: `WorkflowID, StepID, Output, ErrStr, StepName, startedAtMs (StartedAt.UnixMilli()), completedAtMs (CompletedAt.UnixMilli()), Serialization[, ChildWorkflowID]`.

`ON CONFLICT DO NOTHING` is deliberate: it "keeps a caller-owned transaction healthy so it can
still be used or rolled back cleanly after the conflict" instead of letting a raw unique-violation
abort the transaction (`:2627-2629`).

### Success path (row inserted, `RowsAffected() > 0`)

Calls `s.refreshExecutorID(ctx, querier, input.WorkflowID, input.ExecutorID)` (`:2663-2666`) —
**this is the pinned commit's behavior**, see below — then returns `nil`.

### Conflict path (0 rows affected — a row already exists at this `(workflow_uuid, function_id)`)

Re-reads the existing row:
```sql
SELECT output, error, function_name, serialization, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms
    FROM %soperation_outputs
    WHERE workflow_uuid = $1 AND function_id = $2
```
(`:2668-2670`)

- No row (should only happen if GC deleted it concurrently) → `models.NewWorkflowConflictIDError(workflowID)` (`ErrorCodeConflictingID`) (`:2681-2684`).
- Otherwise compare the stored row against the input, field by field, treating `NULL` and `""` as
  equal (`nullableStrEq`, `:2721-2724`):
  - `sameWrite` iff: `StepName == storedFunctionName` AND `Output`, `ErrStr`, `Serialization` all
    equal (nullable-string-equal) AND `ChildWorkflowID` equal AND both `started_at_epoch_ms` and
    `completed_at_epoch_ms` are non-null and equal to the input's timestamps.
    - If `sameWrite` → this is our own earlier write whose commit ack was lost; **return `nil`**
      (idempotent success). Note this does **not** call `refreshExecutorID` on this path.
  - Else if `StepName != storedFunctionName` → `models.NewUnexpectedStepError(workflowID, StepID,
    StepName, storedFunctionName)` (`ErrorCodeUnexpectedStep`) — determinism violation
    (`:2700-2702`).
  - Else → `models.NewWorkflowConflictIDError(workflowID)` (`ErrorCodeConflictingID`) — a
    concurrent execution of this workflow checkpointed the step first; the doc comment says
    callers must surface it as the step error "so the workflow-level handler parks this run in
    polling mode rather than racing the other execution step by step" (`:2617-2626`, `:2703-2705`).

### `started_at_epoch_ms` / `completed_at_epoch_ms` / `serialization`

- `started_at_epoch_ms`, `completed_at_epoch_ms`: epoch milliseconds, from `input.StartedAt` /
  `input.CompletedAt` (`time.Time`) captured by the caller immediately before/after invoking the
  step body (`workflow.go:2521,2541`: `stepStartTime := time.Now()` ... `stepCompletedTime :=
  time.Now()`). These are part of the idempotency comparison above — a retried checkpoint with
  different timestamps than the stored row is treated as a genuine conflict, not a benign retry.
- `serialization`: the name of the encoder used for this step's output (`ser.Name()`, e.g.
  `"DBOS_JSON"` or `"portable_json"` or a custom serializer's name), passed straight through as a
  column value; see §9.

### Re-stamping `executor_id` (pinned commit: `2a7705c`, "Re-stamp workflow executor_id when successfully checkpointing a step (#411)")

```go
func (s *SysDB) refreshExecutorID(ctx context.Context, querier Querier, workflowID, executorID string) {
    if executorID == "" { // Shouldn't happen!
        return
    }
    query := s.RenderSQL(`UPDATE %sworkflow_status SET executor_id = $1
        WHERE workflow_uuid = $2 AND (executor_id IS NULL OR executor_id <> $1)`,
        s.dialect.SchemaPrefix(s.schema))
    if _, err := querier.Exec(ctx, query, executorID, workflowID); err != nil {
        s.logger.Warn("failed to refresh workflow executor ID after checkpoint",
            "workflow_id", workflowID, "executor_id", executorID, "error", err)
    }
}
```
(`:2708-2719`)

Semantics: **only** when `RecordOperationResult` actually inserts a new row (i.e. this
executor "won" the checkpointing race for this step), it also stamps `workflow_status.executor_id`
to the current executor's ID — guarded by `executor_id IS NULL OR executor_id <> $1` so it's a
no-op UPDATE (no row touched) when already correct. This runs on the **same querier** (tx or pool)
as the `operation_outputs` insert, so it commits atomically with the checkpoint. Rationale from the
commit message: "When an executor 'wins' the checkpointing race, it should restamp the executor ID
column in the status table so reporting is accurate." Before this commit, `executor_id` in
`workflow_status` was only set at `InsertWorkflowStatus`/dequeue time and could go stale if a
*different* executor recovered and actually executed the steps. Failure to run this UPDATE is only
logged (`Warn`), never returned as an error — a porter must decide whether this fire-and-forget
semantics (a failed re-stamp is silently swallowed) is acceptable or should propagate.

Errors are **not** raised on `refreshExecutorID` failure; the outer `RecordOperationResult` call
still returns `nil` (success) even if the executor-ID UPDATE failed.

---

## 4. Workflow completion

### SUCCESS / ERROR — `UpdateWorkflowOutcome`

`system_database.go:1655-1702`. Called from `workflow.go:1669-1676` after the workflow function
returns (or panics-turned-into-error upstream).

```sql
UPDATE %sworkflow_status
          SET status = $1, output = $2, error = $3, updated_at = $4, completed_at = $4, deduplication_id = NULL
          WHERE workflow_uuid = $5 AND status NOT IN ($6, $7, $8)
```
Params: `$1 = Status (SUCCESS or ERROR)`, `$2 = Output (*string, encoded)`, `$3 = ErrStr (string,
serialized error, empty if none)`, `$4 = time.Now().UnixMilli()` (used for both `updated_at` and
`completed_at`), `$5 = WorkflowID`, `$6..$8 = CANCELLED, SUCCESS, ERROR` (`:1668-1678`).

Note: **queue_name and started_at_epoch_ms are NOT cleared here** — unlike `CancelWorkflows`
(§below) which does null them out. A porter should check whether that asymmetry is intentional or
a gap (see Gotchas).

`deduplication_id` **is** cleared unconditionally on every successful outcome write (frees the
dedup slot immediately on completion, regardless of SUCCESS or ERROR).

The guard `status NOT IN (CANCELLED, SUCCESS, ERROR)` means: writing the outcome is refused if the
row is already terminal. Behavior on refusal (`RowsAffected() == 0`):
- Re-reads `SELECT status FROM %sworkflow_status WHERE workflow_uuid = $1` (`:1689`).
  - No row → treated as success (silent no-op), returns `nil` (`:1692-1694`).
  - Current status is `CANCELLED` → returns `models.NewWorkflowCancelledError(workflowID, nil)`
    (`:1697-1699`) so the caller's outcome becomes "cancelled" rather than silently reporting a
    completion that was never recorded.
  - Otherwise (already `SUCCESS` or `ERROR`) → silent no-op, `nil` (a genuine idempotent retry: the
    outcome was already recorded by an earlier attempt whose ack was lost).

### Caller side (`workflow.go:1550-1696`)

Builds `status := WorkflowStatusSuccess`; on non-nil error, `status = WorkflowStatusError`
(`:1647-1650`). Encodes the workflow's `result` via `resolveEncoder(workflowCtx).Encode(result)`
regardless of success/error (an error-outcome workflow can still have a partial/typed result to
encode; typically the zero value). Serializes the error itself via `serializeWorkflowError(...)`
into `serializedErr` only `if err != nil` (`:1652-1664`). Calls `removeActive()` — removing the
workflow ID from the in-process active-workflow set — **before** calling `UpdateWorkflowOutcome`,
specifically so that once the outcome becomes durable, a concurrent resume/dequeue on this
workflow ID is not blocked by a stale local "active" entry (`:1665-1668`).

If `UpdateWorkflowOutcome` returns the cancellation error, the in-process outcome delivered to
`GetResult` callers is a cancellation (`models.NewWorkflowCancelledError(workflowID, err)`, wrapping
the original err) rather than the success/error outcome, with `cancelled: true` (`:1683-1687`).

---

## 5. Status values and every legal transition

Values (`internal/models/workflow_status.go:13-19`): `PENDING`, `ENQUEUED`, `DELAYED`, `SUCCESS`,
`ERROR`, `CANCELLED`, `MAX_RECOVERY_ATTEMPTS_EXCEEDED`.

| From | To | Where | Trigger |
|---|---|---|---|
| (new row) | `PENDING` | `InsertWorkflowStatus` INSERT branch | Direct (non-queued) workflow start |
| (new row) | `ENQUEUED` | `InsertWorkflowStatus` INSERT branch | Enqueue with no delay |
| (new row) | `DELAYED` | `InsertWorkflowStatus` INSERT branch | Enqueue with `DelayDuration > 0` |
| `DELAYED` | `ENQUEUED` | `TransitionDelayedWorkflows` (`:4377-4390`) | Background sweep once `delay_until_epoch_ms <= now`; also clears `deduplication_id` if `is_debounced` |
| `ENQUEUED` | `PENDING` | `DequeueWorkflows` (`:4672-4685`, `WHERE status = ENQUEUED`) | Queue consumer claims the workflow: sets `executor_id`, `application_version`, `started_at_epoch_ms`, `rate_limited`, computes `workflow_deadline_epoch_ms` from timeout if unset |
| `PENDING` | `ENQUEUED` | `ClearQueueAssignment` (`:4728-4750`, `WHERE status = PENDING AND queue_name IS NOT NULL`) | Executor gives back a claimed-but-not-started queued workflow (nulls `started_at_epoch_ms`) |
| `PENDING`/any non-terminal | `PENDING` (re-entrant) | `InsertWorkflowStatus` ON CONFLICT branch | Recovery / retry of an already-inserted row |
| `PENDING` | `MAX_RECOVERY_ATTEMPTS_EXCEEDED` | `InsertWorkflowStatus` (`:1272-1279`) | `recovery_attempts > MaxRetries+1` and owner_xid mismatch; nulls `deduplication_id`, `started_at_epoch_ms`, `queue_name` |
| non-terminal | `SUCCESS` | `UpdateWorkflowOutcome` | Workflow function returned without error |
| non-terminal | `ERROR` | `UpdateWorkflowOutcome` | Workflow function returned an error |
| non-terminal | `CANCELLED` | `CancelWorkflows` (`:1756-1895`, guard `status NOT IN (SUCCESS, ERROR, CANCELLED)`) | Explicit cancel (user or durable-deadline cancel via `context.AfterFunc`, `workflow.go:1536-1547`); nulls `started_at_epoch_ms`, `queue_name`, `deduplication_id`; sets `completed_at` |
| terminal or non-terminal (not `SUCCESS`/`ERROR`) | `ENQUEUED` | `ResumeWorkflows` (`:2094-2216`, guard `status NOT IN (SUCCESS, ERROR)`) | Resume (including a `CANCELLED` workflow): sets `queue_name` (default `models.InternalQueueName` if none given), `recovery_attempts = 0`, nulls `workflow_deadline_epoch_ms`, `deduplication_id`, `started_at_epoch_ms`; nulls `completed_at` |
| `DELAYED` (debounced) | `DELAYED` (extended) | `DebounceDelayedWorkflow` (`:4420-4495`) | A bounce arrives before the debounce window fires: extends `delay_until_epoch_ms` (capped at `debounce_deadline_epoch_ms`), rewrites `inputs`/`serialization` |
| — | `CANCELLED` (bulk) | `CancelAllBefore` (`:1998-2028`, not detailed above but same guard pattern) | Admin bulk-cancel of everything created before a cutoff |

Once `SUCCESS` or `ERROR`, no further transition is possible through any of these paths — every
mutating query above excludes those two statuses in its `WHERE`/guard clause. `CANCELLED` *can*
still transition (to `ENQUEUED` via resume), so it is not a true dead end, only `SUCCESS`/`ERROR`
(and, practically, `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, which has no forward transition coded here
either — resume's guard is `NOT IN (SUCCESS, ERROR)` so a `MAX_RECOVERY_ATTEMPTS_EXCEEDED` row
*can* be resumed).

---

## 6. `AwaitWorkflowResult` / GetResult

`system_database.go:2554-2601`.

```sql
SELECT status, output, error, recovery_attempts, serialization FROM %sworkflow_status WHERE workflow_uuid = $1
```

### Polling

- `pollInterval` argument; if `<= 0`, defaults to `sysdb.DBRetryInterval` (`:2557-2559`).
- Loops forever: checks `ctx.Done()` first each iteration (returns `ctx.Err()` if cancelled,
  `:2561-2565`), else runs the query.
- **Non-existent workflow**: `pgx.ErrNoRows` → `time.Sleep(pollInterval); continue` — it does
  **not** return an error for a workflow that doesn't exist (yet); it polls forever waiting for
  the row to appear (`:2573-2577`). A genuinely non-existent workflow ID therefore polls
  indefinitely (bounded only by context cancellation/timeout at the caller). This differs from
  `CheckOperationExecution`, which returns a hard `NonExistentWorkflowError` immediately.
- Any other query error → returned immediately, wrapped (`:2578`).

### Per-terminal-status behavior

- `SUCCESS` or `ERROR`: returns `&AwaitWorkflowResultOutput{Output, Serialization}` immediately;
  `ErrStr` set from the `error` column only if non-null/non-empty (`:2587-2592`). The caller
  (`workflowPollingHandle.GetResult`, `workflow.go:341-...`) turns a non-nil `ErrStr` into the
  workflow's real error via `deserializeWorkflowError`.
- `CANCELLED`: returns `(result, models.NewAwaitedWorkflowCancelledError(workflowID))`
  (`ErrorCodeAwaitedWorkflowCancelled`) — `result` is still populated (`Output`/`Serialization`
  from whatever's in the row, likely both empty) (`:2593-2594`).
- `MAX_RECOVERY_ATTEMPTS_EXCEEDED`: returns `(result,
  models.NewDeadLetterQueueError(workflowID, attempts-2))` (`:2595-2596`). The `-2` back-computes
  the original `maxRetries` from the stored `recovery_attempts` count (since the DLQ transition
  fires once `attempts > maxRetries + 1`, i.e. `attempts == maxRetries + 2` at minimum when it
  first trips — this is an approximation, not necessarily exact if further insert attempts bumped
  it further).
- Any other status (`PENDING`, `ENQUEUED`, `DELAYED`): sleeps `pollInterval` and loops.

### Caller-side (`workflowPollingHandle.GetResult`, `workflow.go:341-...`)

Wraps the sysdb call in `sysdb.RetryWithResult` (transient-error retry). Decodes `Output` via
`resolveDecoder[R](storedSerialization, customSerializer)` — an unrecognized serialization format
here is a **hard error**, returned as `"failed to resolve decoder: %w"` (`:401-404`). If called
from within another workflow (i.e. awaiting a child), records the outcome as a
`"DBOS.getResult"` step via `RecordOperationResult` so replay is deterministic — including a
special case where a cancelled child's cancellation is itself checkpointed as the step's error
(`:414-436`).

---

## 7. Read paths

### `ListWorkflows`

`system_database.go:1330-1653`. Base columns always selected:

```
workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
executor_id, created_at, updated_at, application_version, application_id,
recovery_attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms, started_at_epoch_ms,
deduplication_id, priority, queue_partition_key, forked_from, parent_workflow_id,
serialization, delay_until_epoch_ms, was_forked_from, completed_at, class_name, config_name,
attributes, schedule_name, debounce_deadline_epoch_ms, is_debounced
```
Plus `output, error` if `LoadOutput`; plus `inputs` if `LoadInput` (appended in that order, which
must match the `scanArgs` append order at `:1522-1527`).

Filters (each optional, AND-combined, `qb.addWhere*` builds parameterized `$n` placeholders):
`name IN (...)` (WorkflowName), `queue_name IN (...)`, `queue_name IS NOT NULL` (QueuesOnly),
`workflow_uuid LIKE ... % ANY` (WorkflowIDPrefix, prefix match), `workflow_uuid IN (...)`
(WorkflowIDs), `authenticated_user IN (...)`, `created_at >= ...` (StartTime, epoch ms),
`created_at <= ...` (EndTime), `status IN (...)`, `application_version IN (...)`, `executor_id IN
(...)`, `forked_from IN (...)`, `parent_workflow_id IN (...)`, `deduplication_id IN (...)`,
`schedule_name IN (...)`, `completed_at >= ...` (CompletedAfter), `completed_at <= ...`
(CompletedBefore), `started_at_epoch_ms >= ...` (DequeuedAfter — filters on
`started_at_epoch_ms`, the "when a workflow was dequeued and began executing" column, per the
code comment `:1404-1405`), `started_at_epoch_ms <= ...` (DequeuedBefore), `was_forked_from = ...`
(WasForkedFrom), `is_debounced = ...` (IsDebounced), `parent_workflow_id IS [NOT] NULL`
(HasParent), and `attributes @> $n::jsonb` (Attributes, JSONB containment via the GIN index — only
supported when `s.dialect.SupportsAttributesContainment()`, else a hard error naming Postgres as
the required backend, `:1425-1428`).

Ordering: `ORDER BY created_at DESC` if `SortDesc`, else `ORDER BY created_at ASC` (`:1447-1452`,
always — no secondary sort key, so ties on `created_at` are unordered between runs).

Pagination: `LIMIT $n` if `Limit` set; else if `Offset` set without `Limit`, a dialect-specific
"no limit" clause is inserted before the `OFFSET` (`dialectNoLimitClause`, needed because some
dialects require a `LIMIT` clause to accept `OFFSET`).

Runs on `input.Tx` if given, else `s.pool` (`:1469-1481`).

`GetStatus` (`workflow.go:121-172`) is implemented as `ListWorkflows` filtered to
`WorkflowIDs: [id]`; a zero-length result maps to `models.NewNonExistentWorkflowError(workflowID)`
(`:168-170`) — there is no dedicated `GetWorkflowStatus` query in this codebase.

### `GetWorkflowSteps`

`system_database.go:2946-3028`.

```sql
SELECT function_id, function_name, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization[, output]
          FROM %soperation_outputs
          WHERE workflow_uuid = $1
          ORDER BY function_id ASC
```
Plus `output` column appended if `LoadOutput`. `LIMIT $n` / `OFFSET $n` appended same pattern as
`ListWorkflows` (dialect no-limit-clause when only `Offset` given). No filter besides
`workflow_uuid` — always all steps for one workflow, ordered by `function_id` (i.e. step/checkpoint
sequence order).

### `GetWorkflowStatus`

No such function exists in the Go codebase; use `ListWorkflows` with `WorkflowIDs` as above.

---

## 8. `owner_xid`

Column added in `migrations/7_add_owner_xid.sql`: `ALTER TABLE %s.workflow_status ADD COLUMN
owner_xid TEXT DEFAULT NULL;` — a bare nullable TEXT column, no index.

**Written**: only by the `INSERT` branch of `InsertWorkflowStatus`'s upsert (bound to `$20`); the
`ON CONFLICT DO UPDATE SET` clause does **not** include `owner_xid`, so it is set exactly once, at
the moment a workflow row is first created, and never changes for the lifetime of that row
(barring a full row delete/recreate).

**Generated by the caller** (`workflow.go:1396`): `ownerXID := uuid.New().String()` — a fresh
random UUID generated fresh on *every* call to `RunWorkflow`/start, including every retry attempt
and every concurrent racer, then passed as `&ownerXID` into `InsertWorkflowStatusDBInput.OwnerXID`.

**Read back** via the `RETURNING owner_xid` clause into `InsertWorkflowResult.OwnerXID`, then
compared:

```go
ownerXIDMatches := (input.OwnerXID == nil && ownerXIDReturn == nil) ||
    (input.OwnerXID != nil && ownerXIDReturn != nil && *input.OwnerXID == *ownerXIDReturn)
```
(`system_database.go:1235-1236`)

**What it protects against**: it is the mechanism by which a caller distinguishes "I am the
execution that originally created this row" from "someone else created this row and I merely
raced/retried into it". Two consumers:

1. Inside `InsertWorkflowStatus` itself (`:1268-1269`): the `MAX_RECOVERY_ATTEMPTS_EXCEEDED`
   transition requires `!ownerXIDMatches` — i.e. it only fires when *this* call's random UUID is
   not the one stored on the row, meaning this call did not create the row and is (almost always)
   itself a *recovery* attempt that's now exceeding `MaxRetries`. Because `owner_xid` never
   changes after creation, this comparison is really "was this exact call, with this exact random
   UUID, the one that first inserted the row" — which for any call other than the very first ever
   insert will always be false. So in practice this guard is really testing "is this the original
   creating call, or any subsequent attempt" — the DLQ transition can only be triggered by
   non-originating (i.e., recovery/retry) calls.
2. In `workflow.go:1446-1451` (`shouldSkip` computation): a *fresh* start (not `isDequeue`, not
   `isRecovery`) that discovers `insertStatusResult.OwnerXID != ownerXID` (i.e., some *other* call,
   at *any* time in the past, already created and "owns" this workflow ID) skips actually running
   the workflow body locally and instead returns an `earlyReturnPollingHandle` that awaits the
   existing execution's result (`awaitExistingOutcome`/`AwaitWorkflowResult`). This is how the
   engine detects "another goroutine/process is already the true owner of this workflow ID" for
   plain (non-queued, non-recovery) `RunWorkflow` calls, so it doesn't try to execute the same
   workflow body twice within the same process for an ID collision. Recovery and dequeue paths
   explicitly bypass this guard (comment `:1590-1592`) since they're expected to take over
   execution of someone else's row.

---

## 9. The serialization boundary

### Columns holding serialized values

- `workflow_status.inputs` — the encoded workflow input.
- `workflow_status.output` — the encoded workflow result.
- `workflow_status.error` — the *serialized error string* (not raw Go error text — produced by
  `serializeWorkflowError`), stored as plain TEXT (not itself subject to the `serialization`
  column's format — see below).
- `workflow_status.serialization` — the format name used for `inputs`/`output` on that row.
- `operation_outputs.output` — the encoded step/checkpoint output (or child-workflow-getResult
  output).
- `operation_outputs.error` — serialized step error string.
- `operation_outputs.serialization` — format name used for that row's `output`.
- Also carried on `notifications`, `workflow_events`, `workflow_events_history`, and `streams`
  (each has its own `serialization` column per `migrations/11_add_serialization_columns.sql`) —
  out of scope for this document but same convention.

### How `serialization` is populated

Set directly from the encoder actually used for that write, by name (`Serializer.Name()`):
- `"DBOS_JSON"` — the default built-in JSON serializer (`serialization.go:66-71`); values are
  base64-encoded JSON (`serialization.go:73-94`, e.g. `base64.StdEncoding.EncodeToString(jsonBytes)`).
  A nil value encodes as the literal marker string `"__DBOS_NIL"` (`nilMarker`,
  `serialization.go:20,79-80`), *not* base64 — callers must special-case this literal when decoding.
- `"portable_json"` (`PortableSerializerName`, `serialization.go:23`) — used for cross-language
  ("portable") workflows; raw (non-base64) JSON text, nil encodes as the literal string `"null"`
  (`serialization.go:76-77`).
- Any custom serializer registered by the user — its own `Name()`, e.g. `"DBOS_GOB"` mentioned in
  the `Serializer` interface doc comment as an example (`serialization.go:46`).

`resolveEncoder(ctx)` (`serialization.go:275-283`) picks which encoder to use for a *write*:
portable workflow → user custom serializer → default `DBOS_JSON`, in that priority order.

`resolveDecoder[T](storedSerialization, customSer)` (`serialization.go:287-298`) picks the decoder
for a *typed* read (used by `GetResult`, `GetStatus`-from-inside-a-workflow, etc.): matches
`portable_json` first, then the user's custom serializer by name match, then treats `""` or
`"DBOS_JSON"` as the default JSON serializer.

### Unrecognized serialization format

Two different behaviors depending on call path:

1. **Typed decode paths** (`resolveDecoder`, used when the caller knows the target Go type — e.g.
   `GetResult`, `AwaitWorkflowResult`'s caller, `checkGetResultExecution`): returns a **hard
   error** — `fmt.Errorf("unknown serialization format %q", storedSerialization)`
   (`serialization.go:297`). This propagates up and fails the operation.
2. **Listing/display paths** (`decodeListingValue`, used by `ListWorkflows`/`GetWorkflowSteps` via
   `decodeWorkflowsInputOutput`, where there's no static target type): also returns an error
   internally (`serialization.go:330`, same message format), but the caller
   (`workflow.go:5158-5190`, `decodeWorkflowsInputOutput`) treats it as **non-fatal**: it logs
   `c.logger.Warn("failed to decode workflow input/output, storing raw value", ...)` and stores the
   raw encoded string in the `WorkflowStatus.Input`/`.Output` field rather than failing the whole
   listing call. This is an intentional asymmetry — a porter must replicate both behaviors, not
   just the error path.

---

## Gotchas for the porter

1. **`owner_xid` is write-once, not write-every-time.** The `ON CONFLICT DO UPDATE SET` in
   `InsertWorkflowStatus` deliberately omits `owner_xid` from its SET list. If a port naively adds
   `owner_xid = EXCLUDED.owner_xid` to make the upsert "symmetric" with the other columns, it will
   silently break the "who owns this workflow ID" race-detection logic in `workflow.go:1446-1451`
   and the DLQ-transition ownership check at `system_database.go:1268-1269`.

2. **`recovery_attempts` only increments under `IncrementAttempts`, and even then, only when the
   incoming status is neither `ENQUEUED` nor `DELAYED`.** A straightforward re-entrant call to
   start an already-existing `PENDING` workflow (e.g., a lost start race, not a dequeue or
   recovery) does *not* bump `recovery_attempts` — because the Go caller only sets
   `IncrementAttempts: true` for `params.isDequeue || params.isRecovery`
   (`workflow.go:1412`). Getting the increment condition and the `IncrementAttempts` gating wrong
   independently changes when `MAX_RECOVERY_ATTEMPTS_EXCEEDED` fires.

3. **`RecordOperationResult`'s "same write" idempotency check compares caller-supplied
   timestamps**, not just output/error/name. A port that re-derives `started_at`/`completed_at` at
   checkpoint-record time (rather than passing through the exact same timestamps captured at step
   start/end) will make every retry look like a *conflict* (`ErrorCodeConflictingID`) instead of a
   benign idempotent no-op, because the timestamps won't match the previously-stored row.

4. **`refreshExecutorID` only runs on the "we inserted a new operation_outputs row" branch, never
   on the idempotent-retry branch**, and its failure is swallowed (logged, not returned). Silently
   dropping this — or running it unconditionally on every `RecordOperationResult` call including
   retries — changes observable `executor_id` semantics and (in the "unconditional" case) adds
   extra unnecessary writes on every idempotent replay of a step.

5. **`UpdateWorkflowOutcome` does NOT clear `queue_name` or `started_at_epoch_ms`** on
   SUCCESS/ERROR, unlike `CancelWorkflows` (which nulls `started_at_epoch_ms`, `queue_name`,
   `deduplication_id`) and `ResumeWorkflows` (which nulls `workflow_deadline_epoch_ms`,
   `deduplication_id`, `started_at_epoch_ms`). Only `deduplication_id` is cleared on normal
   completion. This looks like it could be an oversight relative to the cancel/resume paths (a
   completed workflow keeps a stale `queue_name` forever), but it is the actual upstream behavior
   as of the pinned commit — port it as-is and flag it rather than "fixing" it silently, since
   downstream queries/filters (e.g. `ListWorkflows` `QueuesOnly`, `WithFilterQueueName`) may
   depend on a completed workflow still reporting the queue it ran on.

6. **`AwaitWorkflowResult` polls forever on a non-existent workflow ID** (treats `ErrNoRows` as
   "not there yet, keep polling") rather than erroring immediately, in contrast to
   `CheckOperationExecution` and `SetWorkflowAttributes`, which both return
   `NonExistentWorkflowError` immediately on a missing row. A port that "fixes" `AwaitWorkflowResult`
   to fail fast on a missing row will change behavior for legitimate cases (awaiting a workflow ID
   whose `InsertWorkflowStatus` transaction hasn't committed yet).

7. **Two different error behaviors for an unrecognized serialization format**: hard error in typed
   decode paths (`resolveDecoder`), but a logged warning + raw-string fallback in listing paths
   (`decodeListingValue` via `decodeWorkflowsInputOutput`). Collapsing these to one behavior
   changes either `ListWorkflows`'s resilience (if made to hard-fail) or `GetResult`'s correctness
   guarantee (if made to silently degrade).

8. **The nil marker `"__DBOS_NIL"` is a wire-frozen literal string**, and is different per
   serializer: default JSON uses `"__DBOS_NIL"` (unencoded, not base64), portable JSON uses the
   JSON literal `"null"`. A port must special-case both exactly, including the fact that the
   marker is *not* base64-encoded even though non-nil `DBOS_JSON` values always are — a decoder
   that unconditionally base64-decodes will crash or corrupt on nil values if it doesn't check for
   the marker string first.

9. **`error` column semantics differ between `nil` and `""`.** Multiple call sites (`nullableStrEq`,
   `AwaitWorkflowResult`, `CheckOperationExecution`) explicitly treat a stored empty-string error
   the same as SQL `NULL` (no error), and `UpdateWorkflowOutcome` always writes *some* string to
   `error` — `input.ErrStr` — even on `SUCCESS`, where it will typically be `""`. A port must not
   accidentally treat `""` as a truthy "this workflow errored" signal.

10. **`MAX_RECOVERY_ATTEMPTS_EXCEEDED`'s DB-side transaction commit happens inside
    `InsertWorkflowStatus` itself**, before the function returns its `DeadLetterQueueError` — the
    caller's own transaction lifecycle (`workflow.go`'s `insertWorkflowStatusTx` closure normally
    defers a `Rollback`) is irrelevant on this path since the commit already happened. A port must
    ensure equivalent code doesn't try to also commit/rollback the same transaction handle
    afterward (double-commit/rollback-after-commit errors).
