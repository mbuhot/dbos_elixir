# Recovery and Lifecycle — Go Reference

Source: `reference/dbos-transact-golang/dbos/recovery.go` (90 lines), `reference/dbos-transact-golang/dbos/internal/sysdb/system_database.go`, `reference/dbos-transact-golang/dbos/workflow.go`, `reference/dbos-transact-golang/dbos/admin_server.go`, `reference/dbos-transact-golang/dbos/dbos.go`.

## 1. `recoverPendingWorkflows` (`dbos/recovery.go:9-90`)

Signature: `func recoverPendingWorkflows(ctx *dbosContext, executorIDs []string) ([]WorkflowHandle[any], error)`.

### Query and filters (`:11-23`)
```go
appVersion := []string{}
if ctx.applicationVersion != "" {
    appVersion = []string{ctx.applicationVersion}
}
ctx.systemDB.ListWorkflows(ctx, sysdb.ListWorkflowsDBInput{
    Status:             []WorkflowStatusType{WorkflowStatusPending},
    ExecutorIDs:        executorIDs,
    ApplicationVersion: appVersion,
    LoadInput:          true,
})
```
`ListWorkflows` (`system_database.go:1330-1481`) builds this into (columns abbreviated):
```sql
SELECT workflow_uuid, status, name, ..., inputs
FROM %sworkflow_status
WHERE status = ANY($1) AND executor_id = ANY($2) AND application_version = ANY($3)
ORDER BY created_at ASC
```
via `qb.addWhereAny("status", ...)`, `addWhereAny("executor_id", ...)`, `addWhereAny("application_version", ...)` (`:1377-1381`, `:1383-1385`). Only `status = PENDING` is recovered here — `ENQUEUED`/`DELAYED` workflows are not returned by this call at all (they are picked up by the queue runner, not recovery). Filtering by `application_version` is skipped entirely (empty `ANY` array matches nothing meaningful in the input slice, so the filter clause is simply omitted) when `ctx.applicationVersion == ""`.

### Per-workflow handling (`:28-87`)
For each returned `workflow`:

1. **Queued workflows** (`workflow.QueueName != ""`) — a `PENDING` row that also carries a `queue_name` means it was dequeued and started executing, then the executor died before finishing. Recovery does **not** re-invoke the workflow function directly; instead it calls `ClearQueueAssignment(ctx, workflow.ID)` (`:30-32`), which:
   ```sql
   UPDATE %sworkflow_status
     SET status = $1, started_at_epoch_ms = NULL
     WHERE workflow_uuid = $2 AND queue_name IS NOT NULL AND status = $3
   ```
   (`system_database.go:4729-4733`, `$1=ENQUEUED, $3=PENDING`) — this puts the workflow back in `ENQUEUED` state with `started_at_epoch_ms` cleared, so the normal queue dequeue logic will pick it up again as if freshly enqueued. If the `UPDATE` affected zero rows (`cleared == false` — e.g. another executor already cleared/claimed it first), recovery skips it silently (no handle, no log) rather than treating that as an error. If cleared, a **polling handle** (`newWorkflowPollingHandle`) is appended to the result — recovery does not itself re-run the function body for queued workflows, it just returns a handle the caller can await once the queue runner dispatches it.
   The `continue` at `:40` skips straight to the next workflow regardless of error/cleared outcome — queued workflows never fall through to the registry lookup below.

2. **Non-queued workflows** — must be re-invoked directly:
   - **Registry lookup by name** (`:43-58`): the lookup name is the workflow's `Name`, qualified with `ConfigName` via `instanceQualifiedName` if the workflow is a configured instance (`:44-47`). It is looked up first in `ctx.workflowCustomNametoFQN` (custom name → fully-qualified name) then in `ctx.workflowRegistry` (FQN → `WorkflowRegistryEntry`).
   - **Unknown registry name behaviour**: if either lookup misses, recovery logs at `Error` level (`"Workflow not found in registry"` or `"Workflow function not found in registry"`) and `continue`s to the next workflow (`:49-51`, `:55-57`) — it does **not** abort the whole recovery batch, does **not** mark the workflow as failed/dead-lettered, and does **not** return an error from `recoverPendingWorkflows`. The unrecovered workflow simply stays `PENDING` in the database, to be picked up again on a future recovery pass (e.g. a redeploy that re-registers the function, or another executor that has it registered).
   - **Re-invocation** (`:65-85`): builds options `WithWorkflowID(workflow.ID)`, `withIsRecovery()`, plus the original `AuthenticatedUser`/`AssumedRole`/`AuthenticatedRoles` so any child workflows spawned during the recovered run inherit the same identity as the original run. Calls `registeredWorkflow.wrappedFunction(ctx, workflow.Input, workflow.Serialization, opts...)` — passing the still-encoded input directly; decoding to the target type happens inside the wrapper once the target type is known.
   - **Error handling on invocation** (`:78-85`): if the wrapped call returns `ErrDeadLetterQueue` (i.e. this workflow already exceeded `maxRetries` and `InsertWorkflowStatus` moved it to `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, see `system_database.go:1266-1291`), recovery logs a `Warn` and `continue`s — one dead-lettered workflow must not abort recovery of the rest of the batch. Any other error from the wrapped call **does** propagate and abort `recoverPendingWorkflows` with that error, returning `nil` handles for the whole batch (not just the failing one) — a port must preserve this "abort-all vs skip-one" distinction between dead-letter and other errors.

Called from three places: `dbos.go:774` (`recoverPendingWorkflows(c, []string{c.executorID})`, once at launch, before the queue runner starts — comment at `:772-773` notes this ordering is deliberate so recovered workflows aren't racing a fresh dequeue pass), `admin_server.go:230` (the `/dbos-workflow-recovery` HTTP endpoint, arbitrary caller-supplied `executorIDs`), and `conductor.go:480` (remote conductor-driven recovery).

## 2. Executor identity and application version

Both are resolved in `loadDBOSConfig`-style setup in `dbos/dbos.go`:

- **`ExecutorID`**: `dbosConfig.ExecutorID` from the caller-supplied config, overridden by env var `DBOS__VMID` if set (`dbos.go:135-137`), else defaulted to the literal string `"local"` if still empty (`:143-145`).
- **`ApplicationVersion`**: caller-supplied config value, forced to the literal `"PATCHING_ENABLED"` if `EnablePatching` is set and no version was given (`:126-129`), overridden by env var `DBOS__APPVERSION` if set (`:132-134`), else computed by `computeApplicationVersion()` if still empty (`:140-142`).
  - `computeApplicationVersion()` (`dbos.go:976-983`) calls `getBinaryHash()` (`:940-973`): resolves `os.Executable()`, follows symlinks (`filepath.EvalSymlinks`), verifies it is a regular file, then computes the **SHA-256 hash of the entire executable file** and returns its hex encoding. On any error it prints a warning and returns `""` (empty application version).

Both are read back off `ctx.executorID` / `ctx.applicationVersion` throughout the codebase (e.g. `recovery.go:14`, `admin_server.go:254`) and are the values written into `workflow_status.executor_id` / `workflow_status.application_version` at `InsertWorkflowStatus` time (`system_database.go:1197-1198`).

## 3. Cancel (`system_database.go:1756-1895`, `CancelWorkflows`)

Optionally expands the ID set to include all descendants first if `input.CancelChildren` (breadth-first via `GetWorkflowChildren`, `:1764-1777`). Two SQL shapes depending on dialect capability (`SupportsDataModifyingCTE`):

**Postgres / dialects with data-modifying CTEs** (`:1852-1861`):
```sql
WITH existing AS (
    SELECT workflow_uuid FROM %sworkflow_status WHERE workflow_uuid = ANY($3)
), updated AS (
    UPDATE %sworkflow_status
    SET status = $1, updated_at = $2, completed_at = $2, started_at_epoch_ms = NULL,
        queue_name = NULL, deduplication_id = NULL
    WHERE workflow_uuid = ANY($3) AND status NOT IN ($4, $5, $6)
    RETURNING workflow_uuid
)
SELECT workflow_uuid FROM existing
```
(`$1 = CANCELLED`, `$4,$5,$6 = SUCCESS, ERROR, CANCELLED` — already-terminal or already-cancelled workflows are left untouched, but their IDs are still returned by `existing` since they did exist). Dialects without data-modifying CTEs (sqlite) instead run the `UPDATE` then a separate `SELECT workflow_uuid FROM ... WHERE workflow_uuid = ANY($1)` inside the same (repeatable-read) transaction (`:1789-1850`).

`CancelAllBefore(cutoffTime)` (`:1998-2022`) is the bulk/global-timeout variant: lists every workflow in `PENDING`, `ENQUEUED`, or `DELAYED` with `created_at <= cutoffTime` (via `ListWorkflows{EndTime: cutoffTime, Status: [...]}`, `:2000-2003`) then calls `CancelWorkflows` on that ID set.

## 4. Resume (`system_database.go:2094-2216`, `ResumeWorkflows`)

Re-enqueues onto `queueName` (defaults to `models.InternalQueueName` if empty, `:2102-2105`). Postgres shape (`:2181-2191`):
```sql
WITH existing AS (
    SELECT workflow_uuid FROM %sworkflow_status WHERE workflow_uuid = ANY($5)
), updated AS (
    UPDATE %sworkflow_status
    SET status = $1, queue_name = $2, recovery_attempts = $3,
        workflow_deadline_epoch_ms = NULL, deduplication_id = NULL,
        started_at_epoch_ms = NULL, updated_at = $4, completed_at = NULL
    WHERE workflow_uuid = ANY($5) AND status NOT IN ($6, $7)
    RETURNING workflow_uuid
)
SELECT workflow_uuid FROM existing
```
(`$1 = ENQUEUED`, `$3 = 0` i.e. `recovery_attempts` reset to zero, `$6,$7 = SUCCESS, ERROR`). Resuming clears the deadline (so a fresh timeout window is computed on redispatch), clears `deduplication_id` (so the resumed run isn't blocked by its own former dedup key), and clears `completed_at`/`started_at_epoch_ms`. As the doc comment states (`:2091-2093`), IDs already in a terminal state are still considered "existing" (returned) even though the `UPDATE` does not touch them.

## 5. Fork (`system_database.go:2228-2546`, `ForkWorkflows` / `ForkFrom`)

### `ForkFrom` — computing the start step (`:2464-2546`)
Exactly one of four modes must be set: `FromLastFailure`, `FromLastStep`, `FromStep` (explicit int), `FromStepName`. When not `FromStep`, the start step per workflow is computed by:
```sql
SELECT workflow_uuid, <stepExpr>
FROM %soperation_outputs
WHERE workflow_uuid = ANY($1) [AND function_name = $2]
GROUP BY workflow_uuid
```
where `stepExpr` is:
- `FromLastFailure`: `COALESCE(MAX(CASE WHEN error IS NOT NULL THEN function_id END), MAX(function_id))` — the last step that recorded an error, falling back to the last step overall if none errored.
- `FromLastStep` / `FromStepName`: `MAX(function_id)` (with `FromStepName` adding `function_name = $2` to the `WHERE`, so it's really "the last occurrence of that named step").
A workflow with no matching rows raises an error (`"workflow %s has no step named '%s'"` or `"... has no steps"`) rather than silently forking from step 0.

### `ForkWorkflows` — the actual copy (`:2228-2447`)
1. Validates `len(StartSteps) == len(OriginalWorkflowIDs)` and (if given) `len(ForkedWorkflowIDs) == len(OriginalWorkflowIDs)`; generates a random `uuid.New().String()` for any `ForkedWorkflowIDs[i]` left empty (`:2240-2250`) — **this is how the new workflow ID is chosen**: caller-supplied or a fresh random UUID, never derived from the original ID.
2. Reads the original workflows' full status rows in the same transaction (`ListWorkflows` with `Tx: tx`) so the fork sees a consistent pre-fork snapshot; missing IDs raise `NonExistentWorkflowError` (`:2266-2283`).
3. Bulk-inserts one new `workflow_status` row per fork, in a single multi-row `INSERT`, with `status = ENQUEUED`, `queue_name` = target queue (or `InternalQueueName` if unset), `recovery_attempts = 0`, `forked_from = originalWorkflowID`, and everything else (`name`, `authenticated_user`, `assumed_role`, `authenticated_roles`, `application_version` — overridable per-call via `input.ApplicationVersion` — `application_id`, `inputs`, `serialization`, `class_name`, `config_name`, `attributes`) copied verbatim from the original (`:2296-2366`).
4. **For workflows forked from a step > 0** (`StartSteps[i] > 0`), three/four tables are copied using one `UNION ALL`-built `(orig_id, fork_id, start_step)` mapping so an arbitrary batch is a single statement per table (`:2371-2429`):
   - `operation_outputs`: `SELECT ... FROM mapping JOIN operation_outputs oo ON oo.workflow_uuid = m.orig_id AND oo.function_id < m.start_step` — **only steps strictly before the start step** are copied (the start step itself, and everything after, re-executes fresh on the fork).
   - `workflow_events_history`: same `function_id < m.start_step` join — copies every historical event write from before the fork point.
   - `workflow_events` (the *current-value* table): populated from the **latest** `workflow_events_history` row per key before the fork point, via `ROW_NUMBER() OVER (PARTITION BY m.fork_id, h.key ORDER BY h.function_id DESC) ... WHERE rn = 1` — i.e. only the last `setEvent` per key prior to `start_step` is copied into `workflow_events`, not every historical value.
   - `streams`: same `function_id < m.start_step` join, copying `(workflow_uuid, key, value, offset, function_id, serialization)` rows verbatim (offsets are preserved, not renumbered).
   - `StartSteps[i] == 0` workflows skip this whole block — nothing is copied, they just inherit the input and start completely fresh (equivalent to "restart from scratch with a new ID").
5. Marks every original workflow `was_forked_from = TRUE` (`:2432-2439`) — a boolean flag on the **original**, distinct from the new fork row's `forked_from` column which stores the original's ID. `was_forked_from` lets you find "workflows that have been forked from" without a join; `forked_from` lets you find "what this fork came from."
6. Execution resumes for the fork by re-running the workflow function from function/step index `start_step` onward: because `operation_outputs` rows below `start_step` were copied in, `CheckOperationExecution` (the replay short-circuit every step goes through) finds a recorded result for every step index `< start_step` and skips straight past them; the first step actually executed is `start_step` itself.

## 6. Timeouts / deadlines

- `workflow_status.workflow_timeout_ms` and `workflow_status.workflow_deadline_epoch_ms` are the two durable columns (migration 1 adds `workflow_timeout_ms`... actually both are present from migration 1's initial schema plus later ALTERs — see `system_database.go:1129-1171` `InsertWorkflowStatus` INSERT list, which writes both). `InsertWorkflowStatus` returns the resolved `Timeout`/`WorkflowDeadline` (`:1250-1257`).
- **Who enforces it**: not a separate reaper/watchdog process. Each running workflow computes its own effective deadline at start (`workflow.go:1521-1528`): if a durable timeout is set but no deadline yet, `deadline = now + timeout`; else use the already-recorded durable deadline (so recovery/resume/fork reuse the original deadline rather than restarting the clock). If a deadline results, the workflow's Go context is wrapped with `WithTimeout(workflowCtx, time.Until(durableDeadline))` (`:1530-1531`), and `context.AfterFunc(workflowCtx, workflowCancelFunction)` registers a callback that fires the instant that context is done — from a durable deadline expiring, an explicit user cancel, or a parent workflow's cancellation propagating down (`:1533-1547`).
- **Resulting status**: `workflowCancelFunction` (`:1536-1546`) calls `CancelWorkflows(ctx, {WorkflowIDs: [workflowID]})` — the same cancel path as an explicit `CancelWorkflow` call, i.e. the workflow ends up in `WorkflowStatusCancelled` (see §3's SQL), not a distinct "timed out" status.
- Because enforcement lives inside the executing process's own goroutine/context, a workflow whose executor process dies is **not** auto-cancelled by anyone watching the deadline — it stays `PENDING` until recovery (§1) picks it up on some executor and re-establishes a context deadline from the same durably-recorded `workflow_deadline_epoch_ms`.
- Enqueued-but-not-yet-dequeued workflows: a timeout set at enqueue time does **not** set a deadline yet — `workflow.go:1898` logs a warning ("enqueue timeout does not set a deadline: the timeout clock starts when the workflow is dequeued") — the deadline is computed only once the workflow is actually dequeued and begins executing.

## 7. Global timeout and garbage collection admin endpoints

- **`/dbos-global-timeout`** (`admin_server.go:318-342`): decodes `{"cutoff_epoch_timestamp_ms": int64}`, converts to a `time.Time`, and calls `ctx.systemDB.CancelAllBefore(ctx, cutoffTime)` (retried via `sysdb.Retry`) — i.e. it is a thin wrapper that cancels every `PENDING`/`ENQUEUED`/`DELAYED` workflow created at or before the cutoff (see §3). Returns `204 No Content` on success.
- **`/dbos-garbage-collect`** (`admin_server.go:295-316`): decodes `{"cutoff_epoch_timestamp_ms": *int64, "rows_threshold": *int}` but **the handler body is a stub** — it decodes the request and immediately returns `204 No Content` without calling anything. The actual call is commented out:
  ```go
  // TODO: Implement garbage collection
  // err := garbageCollect(ctx, inputs.CutoffEpochTimestampMs, inputs.RowsThreshold)
  ```
  The underlying `SystemDatabase.GarbageCollectWorkflows` (`system_database.go:2029-2083`) **is** fully implemented and does real work — it computes an effective cutoff (the max of the caller's `CutoffEpochTimestampMs` and, if `RowsThreshold` is set, the `created_at` of the Nth-newest row via `SELECT created_at FROM workflow_status ORDER BY created_at DESC LIMIT 1 OFFSET rowsThreshold-1`), then:
  ```sql
  DELETE FROM %sworkflow_status
    WHERE created_at < $1
      AND status NOT IN ($2, $3, $4)   -- PENDING, ENQUEUED, DELAYED excluded from deletion
  ```
  — but as of this reference snapshot, nothing in the HTTP layer invokes it. A Go-based caller must call `GarbageCollectWorkflows` directly (or through whatever public wrapper exists outside `admin_server.go`); the admin HTTP route does not expose it yet.

## 8. Admin HTTP API (`admin_server.go`, all under one `http.ServeMux`, `_ADMIN_SERVER_READ_HEADER_TIMEOUT = 5s`)

| Method + Path | Request body | Response |
|---|---|---|
| `GET /dbos-healthz` | none | `200`, `{"status":"healthy"}` |
| `POST /dbos-workflow-recovery` | JSON array of executor ID strings, e.g. `["executor-1","executor-2"]` | `200`, JSON array of recovered workflow ID strings |
| `GET /deactivate` | none | `200`, plain text `deactivated`; stops the workflow scheduler (cron) but does not wait for in-flight jobs; idempotent via `atomic.CompareAndSwap` |
| `GET /conductor` | none | `200`, `{"status":true}` |
| `GET /dbos-workflow-queues-metadata` | none | `200`, JSON array of queue metadata (the internal queue + every DB-backed queue from `ListQueues`) |
| `POST /dbos-garbage-collect` | `{"cutoff_epoch_timestamp_ms": int64\|null, "rows_threshold": int\|null}` | `204` — **no-op**, see §7 |
| `POST /dbos-global-timeout` | `{"cutoff_epoch_timestamp_ms": int64}` | `204` on success; `500` with error text on failure |
| `POST /workflows` | `listWorkflowsRequest` JSON (workflow_uuids, authenticated_user, start_time, end_time, status [string or array], application_version, workflow_name, limit, offset, sort_desc, workflow_id_prefix, load_input, load_output, queue_name) | `200`, JSON array of workflow objects (`toListWorkflowResponse` shape: WorkflowUUID, Status, WorkflowName, AuthenticatedUser, AssumedRole, AuthenticatedRoles, Output, ExecutorID, ApplicationVersion, ApplicationID, Attempts, QueueName, Timeout, DeduplicationID, Priority, QueuePartitionKey, Input, CreatedAt/UpdatedAt/WorkflowDeadlineEpochMS/StartedAt as stringified epoch-ms, Error as a JSON-encoded string) |
| `GET /workflows/{id}` | none | `200`, single workflow object (same shape as above); `404` if not found |
| `POST /queues` | same `listWorkflowsRequest` shape; if `status` is omitted, defaults to `ENQUEUED, PENDING, DELAYED`; always adds `QueuesOnly` filter | `200`, JSON array of workflow objects (same shape) |
| `GET /workflows/{id}/steps` | none | `200`, JSON array of `{function_id, function_name, child_workflow_id, started_at_epoch_ms?, completed_at_epoch_ms?, output, error?}` |
| `POST /workflows/{id}/cancel` | none | `204` on success; `500` on error |
| `POST /workflows/{id}/resume` | none | `204` on success; `500` on error |
| `POST /workflows/{id}/fork` | `{"start_step": uint\|null, "new_workflow_id": string\|null, "application_version": string\|null}` | `200`, `{"workflow_id": "<new-id>"}` |

All error responses use `http.Error` with `500` (or `400` for malformed JSON bodies, or `404` for a missing single workflow) and a plain-text message embedding the Go error via `fmt.Sprintf("...: %v", err)`.

## 9. Gotchas for the port

- **`recovery_attempts` / dead-letter is enforced inside `InsertWorkflowStatus`, not inside `recoverPendingWorkflows`.** Recovery just re-invokes the wrapped function; the dead-letter check (`recovery_attempts > maxRetries+1`) fires the *next* time that workflow's status row is upserted (`system_database.go:1266-1291`), and recovery only special-cases the resulting `ErrDeadLetterQueue` by skipping instead of aborting. A port must keep this check inside the status-insert path, not duplicate it in the recovery loop.
- **Queued vs. non-queued `PENDING` workflows take completely different recovery paths.** A port that always re-invokes the function directly (ignoring `queue_name`) would double-execute work the queue runner is about to dispatch again once `ClearQueueAssignment` flips it back to `ENQUEUED`.
- **Unknown registry name is a per-workflow skip-and-log, not a batch-abort** — but any *other* error from invoking a known, registered workflow *does* abort the whole batch and returns `nil` handles. These are easy to conflate; a port must replicate the asymmetry (`continue` for `ErrDeadLetterQueue`/unknown-name, `return nil, err` for everything else).
- **Fork copies `operation_outputs`/`workflow_events_history`/`streams` strictly `< start_step`**, never `<= start_step` — the start step itself always re-executes. An off-by-one here silently either re-runs a step that already had side effects recorded, or skips the step that was supposed to be the first one re-executed.
- **`workflow_events` on fork is populated from the latest history row per key, not copied directly from the live `workflow_events` table** — copying the live table instead would be wrong whenever the fork's `start_step` is earlier than the step that last updated a given key (the live table always reflects the *original* workflow's most recent write, which may be after the fork point).
- **The garbage-collect admin HTTP endpoint is currently a no-op stub** despite the underlying system-database method being fully implemented and having real `DELETE` semantics — a port basing itself only on the HTTP contract would silently implement "always succeeds, does nothing," which may or may not be the intended behavior to carry forward; flag this explicitly rather than assuming the stub is intentional final behavior.
- **Timeout enforcement is process-local (`context.AfterFunc` on the running goroutine), not database-driven.** A port that instead polls the database for expired deadlines needs to independently ensure it still routes through the exact same `CancelWorkflows` (not a distinct terminal status) — the observable end state (`WorkflowStatusCancelled`) must match, timeout and explicit cancel are otherwise indistinguishable in the stored data.
- **Executor ID defaults to the literal `"local"`, never empty** — code paths that filter `ListWorkflows{ExecutorIDs: [...]}` rely on this being a real value; passing an empty string through would silently match nothing (`= ANY($1)` with `$1` containing `""` only matches rows literally stored with an empty executor id).
