# Queueing and Dequeueing (ported from dbos-transact-golang)

Source of truth: `reference/dbos-transact-golang/dbos/queue.go` (768 lines) and
`DequeueWorkflows` in `reference/dbos-transact-golang/dbos/internal/sysdb/system_database.go`
(lines 4513-4726). All line numbers below are relative to
`reference/dbos-transact-golang/`.

## 1. Queue configuration surface

A queue's configuration is `workflowQueue` (`dbos/queue.go:20-32`), persisted as
`models.QueueConfig` (`dbos/internal/models/queue.go:17-28`) in the `queues`
table (`dbos/internal/sysdb/migrations/21_create_queues_table.sql`).

| Option (functional `QueueOption`) | Field | Type | Default | Effect |
|---|---|---|---|---|
| `WithWorkerConcurrency(n)` (`queue.go:210-214`) | `WorkerConcurrency` | `*int` | `nil` (unbounded) | Caps rows this **one executor** dequeues concurrently from the queue; enforced by subtracting the caller-supplied `LocalRunningCount` from the limit inside `DequeueWorkflows` (`system_database.go:4557-4565`). |
| `WithGlobalConcurrency(n)` (`queue.go:218-222`) | `GlobalConcurrency` | `*int` | `nil` (unbounded) | Caps total `PENDING` rows for the queue across all executors; enforced by a `COUNT(*) ... WHERE status = 'PENDING'` inside the same transaction as the dequeue SELECT (`system_database.go:4567-4591`). Also forces `NOWAIT` locking and snapshot isolation (see §2). |
| `WithRateLimiter(&RateLimiter{Limit, Period})` (`queue.go:234-238`) | `RateLimit` | `*RateLimiter{Limit int; Period time.Duration}` (`dbos/internal/models/queue.go:12-15`) | `nil` (no limit) | Caps workflow **starts** per rolling window; see §3. |
| `WithPriorityEnabled()` (`queue.go:226-230`) | `PriorityEnabled` | `bool` | `false` | Persisted flag surfaced to callers/Conductor (`conductor_protocol.go:687,698`); it does **not** gate the dequeue `ORDER BY` — that always sorts by `priority, created_at` regardless of this flag (see §5). |
| `WithPartitionQueue()` (`queue.go:244-248`) | `PartitionQueue` | `bool` | `false` | Enables partition-key dequeue and per-partition concurrency accounting; see §6. |
| `WithQueueBasePollingInterval(d)` (`queue.go:253-257`) | `basePollingInterval` | `time.Duration` | `models.DefaultBasePollingInterval` = 1s (`dbos/internal/models/queue.go:9`) | Floor and starting point of the runner's poll interval. |
| `WithQueueMaxPollingInterval(d)` (`queue.go:262-266`) | `maxPollingInterval` | `time.Duration` | `_DEFAULT_MAX_POLLING_INTERVAL` = 120s (`queue.go:16`) | Backoff ceiling. **Ignored for database-backed queues** — `RegisterQueue` warns and resets it to the default (`queue.go:333-339`); the runner instead derives it as `max(basePollingInterval, 120s)` on every reload (`queue.go:645`). |
| `WithQueueOnConflict(policy)` (`queue.go:270-274`) | `onConflict` (registration-only, not persisted) | `QueueConflictResolution` | `QueueConflictUpdateIfLatestVersion` | Controls whether `RegisterQueue` overwrites an existing row of the same name; see below. |

Max attempts / retries are a **workflow**-level concept (`MaxRetries` in
`workflowOptions`, `workflow.go:826`), not a queue option — queues have no
max-attempts field.

Validation (`validateQueueConfig`, `queue.go:278-294`):
- `WorkerConcurrency > GlobalConcurrency` → `InvalidOptionError`.
- `basePollingInterval <= 0` → `InvalidOptionError`.
- `RateLimit.Limit <= 0` or `RateLimit.Period <= 0` → `InvalidOptionError`.

`QueueConflictResolution` (`queue.go:192-203`):
- `QueueConflictUpdateIfLatestVersion` (default): looks up the latest registered
  application version; overwrites only if the calling process's version is that
  latest version, or if no versions are registered yet (first-ever process).
- `QueueConflictAlwaysUpdate`: always overwrites.
- `QueueConflictNeverUpdate`: never overwrites an existing row.

`RegisterQueue` (`queue.go:309-388`) rejects the reserved name
`_dbos_internal_queue` (`models.InternalQueueName`, `dbos/internal/models/queue.go:6`),
then upserts via `UpsertQueue` and re-reads the persisted row so the returned
`Queue` handle reflects what's actually in the table (not just what was
requested).

Runtime updates: `Queue.Set*` methods (`SetGlobalConcurrency`,
`SetWorkerConcurrency`, `SetRateLimit`, `SetPriorityEnabled`,
`SetPartitionQueue`, `SetPollingInterval`, `queue.go:110-156`) only work on
database-backed queues; each does a read-modify-write of the row under
snapshot isolation (`UpdateQueueConfig`, `system_database.go:4974` on, using
`s.dialect.SnapshotIsolation()`), re-validates, and writes back. Live workers
pick up changes on their next reconcile tick (every 1s, see §9) — no restart
needed.

## 2. `DequeueWorkflows` — pseudocode

Signature (`DequeueWorkflowsInput`, `system_database.go:4505-4511`):
```
Queue              models.QueueConfig
ExecutorID         string
ApplicationVersion string
QueuePartitionKey  string   // "" for non-partitioned queues
LocalRunningCount  int      // caller-supplied; see §4
```

```
function DequeueWorkflows(input):
    # --- isolation choice (system_database.go:4514-4521) ---
    snapshot = (input.Queue.GlobalConcurrency != nil) OR (input.Queue.RateLimit != nil)
    iso = dialect.QueueDequeueIsolation(snapshot)
    #   Postgres: snapshot -> REPEATABLE READ, else READ COMMITTED  (dialect.go:200-205)
    #   SQLite:   always IsoLevelDefault (single-writer)            (dialect.go:340)
    tx = BEGIN TRANSACTION ISOLATION LEVEL iso
    # deferred ROLLBACK unless committed below

    # --- 1. Rate limiter pre-check (system_database.go:4525-4552) ---
    if input.Queue.RateLimit != nil:
        cutoffTimeMs = now() - input.Queue.RateLimit.Period   (as epoch ms)
        numRecentQueries = SELECT COUNT(*) FROM workflow_status
            WHERE queue_name = input.Queue.Name
              AND rate_limited = TRUE
              AND status NOT IN ('ENQUEUED','DELAYED')
              AND started_at_epoch_ms > cutoffTimeMs
              [AND queue_partition_key = input.QueuePartitionKey]   -- only if partition key given
        if numRecentQueries >= input.Queue.RateLimit.Limit:
            return []   # commit not needed; tx rolled back by defer

    # --- 2. Concurrency-derived LIMIT (system_database.go:4554-4595) ---
    maxTasks = -1   # -1 means "unbounded"
    if input.Queue.WorkerConcurrency != nil:
        available = max(WorkerConcurrency - input.LocalRunningCount, 0)
        maxTasks = available if (maxTasks < 0 or available < maxTasks) else maxTasks
        # (logs a warning, does not error, if LocalRunningCount > WorkerConcurrency)

    if input.Queue.GlobalConcurrency != nil:
        globalCount = SELECT COUNT(*) FROM workflow_status
            WHERE queue_name = input.Queue.Name AND status = 'PENDING'
              [AND queue_partition_key = input.QueuePartitionKey]
        availableTasks = max(GlobalConcurrency - globalCount, 0)
        maxTasks = availableTasks if (maxTasks < 0 or availableTasks < maxTasks) else maxTasks
        # (logs a warning, does not error, if globalCount > GlobalConcurrency)

    if maxTasks == 0:
        return []  # (nil slice, nil error in Go)

    # --- 3. Application-version predicate (system_database.go:4600-4613) ---
    isLatestVersion = true
    latest = GetLatestApplicationVersion(tx)   # tx-scoped read
    if latest found:
        isLatestVersion = (latest.Name == input.ApplicationVersion)
    else if error is "no application versions registered yet":
        isLatestVersion stays true   # bootstrap: this worker counts as latest
    else:
        return error

    versionClause =
        "application_version = $ApplicationVersion"                                  if NOT isLatestVersion
        "(application_version = $ApplicationVersion OR application_version IS NULL)"  if isLatestVersion
    # NULL application_version rows are workflows enqueued before any version was
    # recorded, or by a client that never set one; only the latest-version worker
    # will claim them.

    # --- 4. SELECT candidate IDs (system_database.go:4615-4646) ---
    query = "SELECT workflow_uuid FROM workflow_status
             WHERE queue_name = $QueueName
               AND status = 'ENQUEUED'
               AND " + versionClause
             [+ " AND queue_partition_key = $QueuePartitionKey"]   -- only if partition key given
             + " ORDER BY priority ASC, created_at ASC"

    if input.Queue.GlobalConcurrency == nil:
        query += " " + dialect.LockSkipLocked()   # Postgres: "FOR UPDATE SKIP LOCKED"; SQLite: "" (no-op)
    else:
        query += " " + dialect.LockNoWait()        # Postgres: "FOR UPDATE NOWAIT";      SQLite: "" (no-op)

    if maxTasks >= 0:
        query += " LIMIT " + maxTasks

    dequeuedIDs = execute query, collect workflow_uuid column
    # (each row-scan loop iteration checks ctx.Done() first and aborts the whole
    #  call with ctx.Err() if the caller's context was cancelled mid-scan)

    # --- 5. UPDATE claims each row one at a time (system_database.go:4671-4716) ---
    retWorkflows = []
    for id in dequeuedIDs:
        if input.Queue.RateLimit != nil:
            if len(retWorkflows) + numRecentQueries >= input.Queue.RateLimit.Limit:
                break   # stop claiming once the rate limit would be exceeded
        row = UPDATE workflow_status
              SET status = 'PENDING',
                  application_version = input.ApplicationVersion,
                  executor_id = input.ExecutorID,
                  started_at_epoch_ms = now_ms,
                  rate_limited = (input.Queue.RateLimit != nil),
                  workflow_deadline_epoch_ms = CASE
                      WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                      THEN now_ms + workflow_timeout_ms
                      ELSE workflow_deadline_epoch_ms
                  END
              WHERE workflow_uuid = id AND status = 'ENQUEUED'
              RETURNING name, inputs, serialization, config_name
        if row not found (0 rows -- someone else claimed it first):
            continue   # skip silently, do not fail the whole call
        retWorkflows.append({Id: id, Name, Input: inputs, Serialization, ConfigName})

    # --- 6. Commit only if something was claimed (system_database.go:4718-4723) ---
    if len(retWorkflows) > 0:
        COMMIT
    # else: falls through to the deferred ROLLBACK — avoids WAL bloat / XID advance
    # on empty polls.

    return retWorkflows
```

Return shape — `[]DequeuedWorkflow` (`system_database.go:4497-4503`):
```
type DequeuedWorkflow struct {
    Id            string
    Name          string
    Input         *string  // encoded input, nil-able
    Serialization string
    ConfigName    *string  // set only for configured-instance workflows
}
```

### Notes on the `NOWAIT` branch
`NOWAIT` is used precisely when `GlobalConcurrency != nil`. Two concurrent
dequeuers racing for the same rows will have one succeed and the other's
`SELECT ... FOR UPDATE NOWAIT` raise a lock-not-available error immediately
(no blocking) — this is treated as a contention/retryable error by the caller
(`ctx.systemDB.IsContentionError(err)`, `queue.go:759`) and simply retried on
the next poll rather than surfaced. `SKIP LOCKED` (used when
`GlobalConcurrency == nil`) instead silently excludes rows another transaction
already holds, so both dequeuers can make progress on disjoint rows in the
same round.

## 3. Rate limiting

Columns (added by `33_add_rate_limited.sql`, `21_create_queues_table.sql`):
- `workflow_status.rate_limited BOOLEAN NOT NULL DEFAULT FALSE` — set to `TRUE`
  on every row dequeued from a queue that has a rate limiter configured
  (`system_database.go:4702`, param `$5` of the UPDATE). Rows dequeued from a
  queue without a rate limiter are never marked.
- `queues.rate_limit_max INTEGER`, `queues.rate_limit_period_sec DOUBLE PRECISION`
  (`21_create_queues_table.sql:11-12`) store `RateLimit.Limit` /
  `RateLimit.Period.Seconds()`.
- Index: `idx_workflow_status_rate_limited (queue_name, started_at_epoch_ms) WHERE rate_limited = TRUE`
  (`34_create_rate_limited_index.sql`) — supports the count query below.

Window computation: the window is a **rolling** window computed fresh on every
dequeue call, not a fixed bucket. `cutoffTimeMs = now() - Period` (as epoch
ms). The count query:
```sql
SELECT COUNT(*)
FROM workflow_status
WHERE queue_name = $1
  AND rate_limited = TRUE
  AND status NOT IN ($2, $3)   -- excludes ENQUEUED, DELAYED
  AND started_at_epoch_ms > $4
  [AND queue_partition_key = $5]
```
(`system_database.go:4530-4542`). Excluding `ENQUEUED`/`DELAYED` means only
workflows that have actually **started** (moved past `started_at_epoch_ms`
being set, i.e. `PENDING` and beyond, including terminal states) count against
the limit — a workflow counts against its window for the workflow's whole
lifetime as long as it started within `Period`, not just while running.

Enforcement is two-layered:
1. **Pre-check** (`system_database.go:4549-4551`): if `numRecentQueries >=
   Limit` before even looking at candidate rows, the whole call returns `[]`
   immediately — no SELECT/UPDATE against `workflow_status` candidates runs at
   all this round.
2. **Per-row cap during the UPDATE loop** (`system_database.go:4689-4693`): as
   rows are claimed one at a time, the loop stops (`break`) as soon as
   `len(retWorkflows) + numRecentQueries >= Limit`, so a single dequeue call
   never claims more than `Limit - numRecentQueries` rows even if `maxTasks`
   (the concurrency-derived LIMIT) would have allowed more.

At the limit: excess `ENQUEUED` rows are simply left in place; the queue
runner will see `hasBackoffError == false` (a rate limit hit is not a
contention error) and will scale the poll interval down toward the base
interval on this "successful" (zero-result) iteration, per the normal
scale-back logic in `runQueue` (`queue.go:719-728`) — there is no dedicated
backoff for being rate-limited.

## 4. Global vs worker (local) concurrency

- **Worker (local) concurrency** (`WorkerConcurrency`): enforced purely
  in-process by computing `available = max(WorkerConcurrency -
  LocalRunningCount, 0)` and using it as (part of) the SQL `LIMIT`
  (`system_database.go:4557-4565`). No database column tracks "workflows
  running on this executor" — it is derived from `LocalRunningCount`, which the
  caller (the queue runner) computes as:
  ```go
  ctx.countActiveWorkflowsForQueue(queue.Name, partitionKey)
  ```
  (`queue.go:754`), implemented at `workflow.go:792-806`: it scans an
  in-memory `sync.Map` (`activeWorkflowIDs`) of `{queueName,
  queuePartitionKey} -> count`, populated whenever a workflow is currently
  executing on this process for that queue+partition. This is a live,
  in-memory count of workflows this specific process is actively running —
  not a database read, and not persisted. Because `snapshot` isolation is
  **not** forced by worker concurrency alone (see §2 isolation rule), worker
  concurrency does not need cross-process consistency: it's a purely local
  cap.
- **Global concurrency** (`GlobalConcurrency`): enforced by counting
  `workflow_status` rows with `status = 'PENDING'` for the queue (optionally
  filtered by partition key) inside the *same* dequeue transaction
  (`system_database.go:4567-4591`), which is why global concurrency forces
  `REPEATABLE READ` (snapshot) isolation and `FOR UPDATE NOWAIT` locking —
  every executor must see a consistent view of how many `PENDING` rows exist
  system-wide to avoid two racing executors both under-counting and
  over-claiming.
- Both limits, if set, are combined by taking the smaller of the two
  `available`/`availableTasks` values as `maxTasks` (`system_database.go:4562-4564,4588-4590`);
  either can independently drive `maxTasks` to `0`, short-circuiting the call.

## 5. Priority semantics

- Field: `WorkflowStatus.Priority int` (`dbos/internal/models/workflow_status.go:43`,
  comment: "lower numbers have higher priority").
- Column: `workflow_status.priority INTEGER NOT NULL DEFAULT 0`
  (`1_initial_dbos_schema.sql:25`).
- Set via `WithPriority(uint)` (direct workflow start, `workflow.go:904-908`) or
  `WithEnqueuePriority(uint)` (`workflow.go:1737-1741`); both take an
  **unsigned** value, so the caller-visible range is `0 .. math.MaxInt`
  (validated at `workflow.go:1330-1332` and `workflow.go:1893-1894` — a
  priority `> math.MaxInt` is rejected with an error, not clamped). Default
  when unset is the zero value, `0` — the **highest** priority under
  "lower number wins."
- Direction confirmed by the dequeue `ORDER BY priority ASC, created_at ASC`
  (`system_database.go:4628`): rows with the smallest `priority` value are
  scanned (and hence claimed, subject to `LIMIT`) first; among equal
  priorities, older `created_at` wins (FIFO within a priority tier).
- `PriorityEnabled` on the queue is **not** consulted in `DequeueWorkflows` at
  all — the `ORDER BY priority ASC, created_at ASC` clause is unconditional.
  In practice this means priority ordering is always active regardless of the
  flag; `PriorityEnabled` appears to function purely as a declared/advertised
  capability (e.g. surfaced to Conductor, `conductor_protocol.go:687,698`)
  rather than a behavioral gate. Port faithfully: always sort by
  `(priority, created_at)`.
- Debounced workflows cannot set a priority — `rejectConflictingDebounceOptions`
  rejects a non-zero `Priority` with `InvalidOptionError`
  (`debouncer.go:135-137`), because debounce owns the workflow's identity via
  the deduplication key, not scheduling order.
- Index `idx_workflow_status_in_flight (queue_name, status, priority,
  created_at) WHERE status IN ('ENQUEUED','PENDING')`
  (`32_create_in_flight_index.sql`) is shaped to exactly match the dequeue
  query's predicates and `ORDER BY` so the planner can use it directly.

## 6. Partition keys

- Column: `workflow_status.queue_partition_key TEXT` (nullable), added by
  `2_add_queue_partition_key.sql`.
- Enqueue-time: `WithEnqueueQueuePartitionKey` sets `QueuePartitionKey` on the
  workflow status row; mutually exclusive with a deduplication ID — enqueue
  rejects both being set (`workflow.go:1868-1870`,
  `models.NewInvalidOptionError("partition key and deduplication ID cannot be used together")`),
  and the debouncer separately rejects a partition key because "partitioned
  queues do not support deduplication, which debouncing requires"
  (`debouncer.go:133-134`... i.e. `rejectConflictingDebounceOptions`).
- Discovery: `GetQueuePartitions(queueName)` (`system_database.go:4753-4780`)
  runs `SELECT DISTINCT queue_partition_key FROM workflow_status WHERE
  queue_name = $1 AND status = 'ENQUEUED' AND queue_partition_key IS NOT
  NULL` — i.e. the set of live partitions is derived from what's currently
  enqueued, not from a separate partitions table.
- Dequeue effect: when `queue.PartitionQueue` is true, the runner
  (`queue.go:657-671`) calls `GetQueuePartitions` each iteration and calls
  `DequeueWorkflows` **once per partition key**
  (`queue.go:676-682`), each call passing that partition key as
  `input.QueuePartitionKey`. `DequeueWorkflows` then adds `AND
  queue_partition_key = $N` to: the rate-limiter count query
  (`system_database.go:4539-4542`), the global-concurrency pending count
  (`system_database.go:4574-4577`), and the candidate SELECT
  (`system_database.go:4623-4626`). For non-partitioned queues, or the
  no-partitions-yet case, `partitionKeys = []string{""}` and no partition
  filter is applied (`len(input.QueuePartitionKey) > 0` guards each branch).
- Concurrency accounting is **per partition**: global concurrency's `PENDING`
  count and worker concurrency's `LocalRunningCount`
  (`countActiveWorkflowsForQueue(queueName, partitionKey)`, `workflow.go:792`)
  are both scoped to `(queueName, partitionKey)`, so each partition gets an
  independent concurrency budget rather than sharing the queue's limit.
- Gotcha (documented in `SetPartitionQueue`, `queue.go:130-147`): flipping an
  existing unpartitioned queue to partitioned **abandons** workflows already
  enqueued without a partition key (their `queue_partition_key IS NULL`), since
  a partitioned queue's runner only ever iterates the partition keys returned
  by `GetQueuePartitions` (which requires `queue_partition_key IS NOT NULL`) —
  those rows are never dequeued again. A `Warn` log is emitted but nothing
  automatically migrates them.

## 7. `DELAYED` status and `delay_until`

- Column: `workflow_status.delay_until_epoch_ms BIGINT DEFAULT NULL`
  (`16_add_delay_until.sql`), with partial index
  `idx_workflow_status_delayed (delay_until_epoch_ms) WHERE status = 'DELAYED'`.
- **Set by**: the enqueue path. If `params.DelayDuration > 0` (direct-start
  path, `workflow.go:1296-1302`) or `params.delayDuration > 0` (Enqueue path,
  `workflow.go:1921-1927`), the initial status is `DELAYED` instead of
  `ENQUEUED`, and `delay_until = time.Now().Add(delayDuration)`. A **debounced**
  enqueue is always `DELAYED` even for a zero delay
  (`workflow.go:1921-1927`: `params.delayDuration > 0 || params.isDebounced`),
  because the debounce key is only released on the `DELAYED -> ENQUEUED`
  transition (see §8).
- **Promoted by**: `TransitionDelayedWorkflows` (`system_database.go:4377-4390`):
  ```sql
  UPDATE workflow_status
  SET status = 'ENQUEUED', updated_at = $now_ms,
      deduplication_id = CASE WHEN is_debounced THEN NULL ELSE deduplication_id END
  WHERE status = 'DELAYED'
    AND delay_until_epoch_ms <= $now_ms
  ```
  This both promotes the status and — for debounced rows only — clears
  `deduplication_id`, releasing the debounce key for reuse the instant the
  workflow becomes dequeueable.
- **Scheduling**: not a per-queue timer. The queue-runner supervisor loop
  (`queueRunner.run`, `queue.go:509-557`) calls
  `ctx.systemDB.TransitionDelayedWorkflows(ctx)` once per **reconcile tick**
  (every `reconcileInterval = 1 * time.Second`, `queue.go:522`), globally
  across all queues, before rebuilding the listen set and (re)spawning worker
  goroutines. So delay promotion latency is up to ~1s beyond the requested
  `delay_until`, independent of any individual queue's polling interval.

## 8. Debounce

Columns (`42_add_debounce_columns.sql`):
```sql
ALTER TABLE workflow_status ADD COLUMN IF NOT EXISTS "debounce_deadline_epoch_ms" BIGINT DEFAULT NULL;
ALTER TABLE workflow_status ADD COLUMN IF NOT EXISTS "is_debounced" BOOLEAN NOT NULL DEFAULT FALSE;
```
Semantics: a debounced workflow is enqueued `DELAYED`, using the debounce key
as its `deduplication_id` (so the existing unique index
`uq_workflow_status_dedup_id (queue_name, deduplication_id) WHERE
deduplication_id IS NOT NULL` doubles as the debounce mutual-exclusion lock).
`is_debounced = TRUE` marks that this row's `deduplication_id` is a debounce
key rather than a plain dedup key, so it should be cleared (not just checked)
on promotion. `debounce_deadline_epoch_ms` is an optional hard ceiling: repeated
"bounces" extend `delay_until_epoch_ms` but it is capped there.

Options rejected outright for a debounced workflow
(`rejectConflictingDebounceOptions`, `debouncer.go:126-146`): a queue override
(the queue is fixed on the debouncer), a deduplication ID, a delay, a
priority, a partition key, and a non-default deduplication policy — each
returns `models.NewInvalidOptionError` with an explanation of which
debounce-owned field conflicts.

Bounce SQL — `debounceDelayedWorkflowInternal` (`system_database.go:4441-4495`):
```sql
UPDATE workflow_status
SET delay_until_epoch_ms = CASE
      WHEN debounce_deadline_epoch_ms IS NOT NULL AND debounce_deadline_epoch_ms < $1
      THEN debounce_deadline_epoch_ms
      ELSE $1
    END,
    inputs = $2, serialization = $3, updated_at = $4
WHERE name = $5 AND queue_name = $6 AND deduplication_id = $7
  AND status = $8 AND is_debounced = TRUE
RETURNING workflow_uuid
```
(`$1` = new `delay_until` as epoch ms, `$8` = `'DELAYED'`). `CASE` is used
instead of `LEAST`/`MIN` for Postgres/SQLite portability. Matching on `name`
in addition to `queue_name + deduplication_id` prevents a debounce-key
collision between two *different* workflow names/keys (e.g. `"a"+"b-c"` vs
`"a-b"+"c"` concatenating to the same string) from overwriting the wrong
workflow's inputs.

If the `UPDATE` affects no row, the code distinguishes two cases by a
follow-up `SELECT workflow_uuid, is_debounced, name FROM workflow_status WHERE
queue_name = $1 AND deduplication_id = $2`:
1. No row at all → the key is unheld; caller should enqueue fresh
   (`&DebounceResult{}`, `system_database.go:4483-4486`).
2. A row exists but wasn't matched by the bounce UPDATE → returned as
   `HolderWorkflowID`/`HolderIsDebounced`/`HolderWorkflowName`
   (`system_database.go:4490-4494`) so the caller can tell apart "the key is
   held by a plain (non-debounced) dedup enqueue — surface a conflict" from
   "held by a same-key debounced workflow that flipped status mid-race —
   retry" (see `classifyBounce`, `debouncer.go:158` on, and `bounceAction`
   enum `debouncer.go:152-157`: `bounceReturn`, `bounceEnqueue`,
   `bounceRaise`, `bounceRetry`).

## 9. Queue runner loop (`queue.go`)

Top-level supervisor: `queueRunner.run(ctx)` (`queue.go:509-557`).
- Every `reconcileInterval = 1s` (`queue.go:522`):
  1. Calls `TransitionDelayedWorkflows` globally (§7).
  2. Rebuilds the listen set via `queuesToListen` (`queue.go:564-607`): merges
     the always-present internal queue (`_dbos_internal_queue`) with
     database-backed queues from `ListQueues`, filtered by an optional
     explicit listen-set (`ListenQueues`) — an empty listen set means "listen
     to everything." On a transient error listing queues, it falls back to the
     last-known snapshot rather than tearing everything down.
  3. For each queue in that set lacking a live worker goroutine (tracked via a
     `map[string]chan struct{}` of "done" channels), spawns one via
     `go qr.runQueue(ctx, q)`.
  4. Sleeps until `reconcileInterval` or context cancellation.
- On shutdown (`ctx.Done()`), the loop exits, then `run`'s deferred cleanup
  waits on `queueGoroutinesWg` (all worker goroutines) before signalling
  `completionChan` — a clean, ordered shutdown; no workflow claim is left
  half-updated because each worker checks `ctx.Done()` at its own sleep point
  and mid-dequeue-scan loop (`system_database.go:4654-4658`).

Per-queue worker: `runQueue(ctx, queue)` (`queue.go:626-743`), one goroutine
per queue, looping until cancelled:
1. If database-backed, reloads the queue's fresh config every iteration
   (`currentQueueConfig`, published by the supervisor's `queuesToListen`); if
   the queue has disappeared from the reconciled set, the worker exits (the
   supervisor will respawn it if the queue reappears). `maxPollingInterval` is
   always re-derived as `max(basePollingInterval, 120s)` since it isn't
   persisted (`queue.go:645`), and the *current* interval is re-clamped into
   `[basePollingInterval, maxPollingInterval]` on every reload
   (`queue.go:648`).
2. Computes partition keys: `[""]` for non-partitioned queues, or the live set
   from `GetQueuePartitions` for partitioned ones (`queue.go:656-671`). A
   failure listing partitions sets `skipDequeue = true` for this iteration and,
   if it's a contention error, marks `hasBackoffError = true`.
3. For each partition key, calls `dequeueWorkflows` (`queue.go:747-768`), which
   wraps `ctx.systemDB.DequeueWorkflows` in the retry helper
   (`sysdb.RetryWithResult`) and:
   - on a contention error, sets `hasBackoffError = true` and returns
     `(nil, true)` — "continue to next iteration" (i.e. per-partition errors
     don't abort the other partitions' results already gathered, but this
     partition's error is swallowed after being flagged for backoff),
   - on a non-contention error, logs it and likewise returns `(nil, true)`,
   - on success returns `(workflows, false)`.
4. Dispatches every dequeued workflow: resolves the registered function by
   name (qualified with config name for configured-instance workflows via
   `instanceQualifiedName`), then calls
   `registeredWorkflow.wrappedFunction(ctx, workflow.Input,
   workflow.Serialization, WithWorkflowID(workflow.Id), withIsDequeue())`
   synchronously in the worker goroutine (not concurrently per workflow — the
   loop is sequential over `dequeuedWorkflows`, `queue.go:687-716`). A
   dispatch/registry-lookup failure is only logged; it does not affect polling
   backoff.
5. Adjusts `currentPollingInterval`:
   - on `hasBackoffError`: exponential backoff, `interval *= backoffFactor
     (2.0)`, capped at `queue.maxPollingInterval` (`queue.go:720-723`).
   - otherwise: scale back, `interval *= scalebackFactor (0.9)`, floored at
     `queue.basePollingInterval` (`queue.go:724-727`).
   - jitter is applied multiplicatively every iteration regardless of branch:
     `jitter = jitterMin(0.95) + rand()*(jitterMax(1.05)-jitterMin(0.95))`,
     `sleep = interval * jitter` (`queue.go:730-732`) — so actual sleep is
     always ±5% of the computed interval.
6. Sleeps `sleepDuration`, or returns immediately on `ctx.Done()`.

`ClearQueueAssignment(workflowID)` (`system_database.go:4728-4750`):
```sql
UPDATE workflow_status
SET status = 'ENQUEUED', started_at_epoch_ms = NULL
WHERE workflow_uuid = $1
  AND queue_name IS NOT NULL
  AND status = 'PENDING'
```
Reverts a claimed-but-not-actually-started workflow back onto its queue
(clearing `started_at_epoch_ms` so it doesn't count toward rate-limiter
windows incorrectly next time it's dequeued and re-stamped). Returns whether a
row was actually affected (`false` if the workflow was no longer `PENDING`,
e.g. it already completed or was already re-cleared) — the caller should
treat `false` as "nothing to do," not an error.

## 10. Deduplication (`dedup_id`)

- Column: `workflow_status.deduplication_id` (added earlier in the schema
  history, before the migrations covered here).
- Constraint evolution: originally a full unique constraint
  `uq_workflow_status_queue_name_dedup_id` on `(queue_name,
  deduplication_id)`; replaced by a **partial** unique index
  `uq_workflow_status_dedup_id` on the same columns `WHERE deduplication_id IS
  NOT NULL` (`27_create_partial_dedup_id_index.sql`, described internally as
  "Migration 23" in its own header comment — filename numbering and the
  in-file "Migration N" comment numbering have drifted apart across renames;
  trust the filename/its position in the migration run order, not the
  comment's number), with the old constraint dropped in
  `28_drop_dedup_id_constraint.sql` (dialect-specific: Postgres uses `ALTER
  TABLE ... DROP CONSTRAINT IF EXISTS`; there is a parallel
  `28_drop_dedup_id_constraint_cockroach.sql` because CockroachDB implements
  unique constraints as indexes and rejects `DROP CONSTRAINT` for them). A
  partial index (rather than a full unique constraint) allows unlimited rows
  with `deduplication_id IS NULL` (the common case — most enqueues don't
  dedup) while still enforcing uniqueness among rows that do set one, scoped
  per queue.
- Conflict behaviour: the insert (`system_database.go:1237-1247`) catches a
  unique-violation via `s.dialect.IsUniqueViolation(err)` and converts it to
  `models.NewQueueDeduplicatedError(workflowID, queueName, deduplicationID)`,
  matched by callers via `errors.Is(err, ErrQueueDeduplicated)`
  (`dbos/errors.go:15-16`, `ErrorCodeQueueDeduplicated`).
- Caller-visible behaviour depends on `DeduplicationPolicy`
  (`workflow.go:808-819`):
  - `DeduplicationPolicyReject` (default, zero value): the enqueue/start call
    returns the `ErrQueueDeduplicated` error directly.
  - `DeduplicationPolicyReturnExisting`: on `ErrQueueDeduplicated`, the caller
    looks up the existing holder via `GetDeduplicatedWorkflow(queueName,
    deduplicationID)` and returns a polling handle to it instead of an error
    (`workflow.go:1998-2010`, `insertEnqueuedWorkflow`). If the lookup finds no
    holder (the dedup slot was freed — e.g. the holder transitioned out —
    between the failed insert and the lookup), the whole enqueue is retried in
    a loop (`workflow.go:1985-1988`, "the dedup slot was freed before the
    holder lookup; enqueue again") rather than erroring — this loop has no
    bound other than eventually winning the race or the process being
    cancelled.
- Partition keys and deduplication IDs are mutually exclusive at enqueue time
  (validated, not merely undefined behavior) — see §6.

## Gotchas for the porter

Every place below is somewhere the Go implementation tolerates a duplicate
attempt, a lost claim, or a silently-degraded pass rather than failing loudly.
A port must preserve — not "fix" — these, or workflows will double-execute or
silently stall differently than upstream:

1. **Return-existing dedup retry loop is unbounded**
   (`workflow.go:1985-1988`, `2007-2010`): if the dedup key's holder vanishes
   between the failed insert and the follow-up lookup, the caller loops and
   retries the whole enqueue with no backoff and no attempt cap. Under
   sustained contention on one key this can spin.
2. **Row-claim races are absorbed as no-ops, not errors**
   (`system_database.go:4705-4709`): after the SELECT gathers candidate IDs,
   the per-row `UPDATE ... WHERE status = 'ENQUEUED' RETURNING ...` may affect
   zero rows if another executor claimed the row first (this can happen even
   under `SKIP LOCKED`/`NOWAIT` if the row was claimed in the narrow window
   between the SELECT and the UPDATE for `SKIP LOCKED` mode, since `SKIP
   LOCKED` releases the row-level locking guarantee the `NOWAIT`/global-
   concurrency path relies on). The code `continue`s past it silently instead
   of surfacing the lost claim.
3. **Dispatch failures never retried by the queue runner itself**
   (`queue.go:713-715`): `wrappedFunction(...)` erroring only logs — the
   runner does not requeue, retry, or backoff on it. Whether the underlying
   workflow's own execution/retry semantics (durable step retry, recovery on
   crash) paper over this is a different layer; the *dequeue loop itself* just
   drops the error on the floor for that iteration.
4. **`ClearQueueAssignment` returning `false` is silently treated as fine**
   (`system_database.go:4728-4750`): callers must not treat "no row affected"
   as an error — but conversely, if a caller assumed it always reverts a
   `PENDING` row and doesn't check the boolean, a workflow that raced to
   completion right as `ClearQueueAssignment` ran would be silently left
   completed while the caller believes it was requeued.
5. **`insertEnqueuedWorkflow`'s dedup fallback can return a stale ID with an
   error still set** (`workflow.go:1996-2000`): note the pattern `return
   *existingID, err` — a non-nil `err` accompanied by a non-empty ID is used
   deliberately as a signal ("dedup happened, here's who owns it") rather than
   Go's usual "err != nil implies ignore other return values" convention. A
   port must not discard the ID just because an error is also present.
6. **Rate-limiter window is a live re-count, not a token bucket**: every
   dequeue call re-executes the `COUNT(*)` query fresh
   (`system_database.go:4530-4544`). This is correct but expensive at scale
   (no cached/decremented counter), and it's evaluated once per partition per
   poll for partitioned queues — N partitions means N separate rate-limit
   count queries per polling tick, each independently comparing to the same
   `Limit` (i.e. the limit is **not** divided across partitions; each
   partition gets the full `Limit` against its own count, since the query is
   filtered `AND queue_partition_key = $N`). Confirm this is the intended
   semantics before porting, since "partitioned queue with a rate limiter"
   means the *effective* aggregate rate limit is `Limit × (number of active
   partitions)`, not `Limit` total.
7. **`WorkerConcurrency`/`GlobalConcurrency` violations are logged, not
   enforced retroactively** (`system_database.go:4559-4561, 4585-4587`): if
   `LocalRunningCount` (or the global `PENDING` count) already exceeds the
   configured limit — e.g. because the limit was just lowered via `Set*` while
   workflows were in flight — the code computes `max(limit - count, 0)` (i.e.
   claims zero more) and only *warns*; it does not attempt to cancel or
   preempt the already-overcommitted workflows. Overcommitment silently
   resolves itself only as those workflows finish.
8. **Debounce deduplication_id release depends on the 1-second global
   reconcile tick, not the queue's own polling interval**
   (`queue.go:522-529`, `system_database.go:4377-4390`): a debounce key is
   only actually freed (`deduplication_id` set to `NULL`) when
   `TransitionDelayedWorkflows` runs and flips `DELAYED -> ENQUEUED`. A caller
   racing to reuse the same debounce key immediately after a delay nominally
   expires can still transiently see the key as held for up to ~1s.
9. **Partition abandonment on `SetPartitionQueue(true)` is silent apart from a
   log line** (`queue.go:130-147`): pre-existing unpartitioned enqueues (with
   `queue_partition_key IS NULL`) become permanently undiscoverable by
   `GetQueuePartitions` (which requires `IS NOT NULL`) the moment the queue
   flips to partitioned — there is no migration, alerting beyond the one
   `Warn` log, or safeguard preventing this at the SQL layer.

## Report to caller (compact summary)

- **Isolation**: `REPEATABLE READ` iff `GlobalConcurrency != nil OR RateLimit
  != nil`, else `READ COMMITTED` (`system_database.go:4514-4517`,
  `dialect.go:200-205`). SQLite always uses its default level regardless
  (`dialect.go:340`) since it's single-writer.
- **Locking**: `SELECT ... FOR UPDATE SKIP LOCKED` when `GlobalConcurrency ==
  nil`; `SELECT ... FOR UPDATE NOWAIT` when `GlobalConcurrency != nil`
  (`system_database.go:4630-4640`). SQLite dialect returns `""` for both
  (no row locking clause emitted).
- **LIMIT computation**: `maxTasks` starts at `-1` (unbounded); each of
  `WorkerConcurrency - LocalRunningCount` and `GlobalConcurrency -
  currentPending` (each floored at 0) narrows it to the smaller value if set;
  `maxTasks == 0` short-circuits to returning no rows before any SELECT for
  candidates runs.
- **Version predicate**: `application_version = $current` always; loosened to
  `(application_version = $current OR application_version IS NULL)` only when
  this executor's version equals the latest registered application version
  (or no versions are registered yet) — i.e. only the "latest" worker claims
  legacy/unversioned rows, preventing an old worker from picking up
  version-less work meant to run on new code.
- **Ordering**: unconditionally `ORDER BY priority ASC, created_at ASC` — the
  `PriorityEnabled` queue flag is not consulted by the dequeue query at all.
- **Claim**: candidate IDs are gathered by a SELECT, then claimed one-by-one
  via `UPDATE ... WHERE status='ENQUEUED' RETURNING ...`; the whole
  transaction only commits if at least one row was actually claimed.

Upstream behaviours that read as possible bugs / sharp edges (also listed
above as porter gotchas, called out here for visibility):
- Rate limiting on a **partitioned** queue applies `Limit` independently per
  partition rather than across the queue as a whole, meaning effective
  throughput scales with partition count — worth confirming with DBOS
  maintainers whether that's intentional before assuming it's a defect to fix
  in the port.
- The `DeduplicationPolicyReturnExisting` retry-on-vanished-holder loop
  (`workflow.go:1985-1988`) has no retry limit or backoff.
- `WorkerConcurrency`/`GlobalConcurrency` overshoot (via a lowered live
  config) is only logged, never actively corrected.
