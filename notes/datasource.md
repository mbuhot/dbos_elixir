# Transactional Steps (`RunAsTransaction`) — Go Reference

Source: `reference/dbos-transact-golang/dbos/datasource.go` (561 lines), cross-referenced against `dbos/workflow.go` (`RunAsStep`, `runAsTxn`, `prepareStepExecution`, `executeStepWithRetry`) and `dbos/internal/sysdb/retry.go` / `system_database.go` (retry primitives).

## 1. The completion table

Lives in the **user's own database** (whatever engine backs the `DataSource`), not the DBOS system database — one instance per data source. Name and key (`datasource.go:26`, `:160-185`):

```
const transactionCompletionTable = "transaction_completion"
```

Postgres/CockroachDB DDL (`:173-184`):
```sql
CREATE SCHEMA IF NOT EXISTS "dbos"
CREATE TABLE IF NOT EXISTS "dbos".transaction_completion (
	workflow_id TEXT NOT NULL,
	step_id INT NOT NULL,
	output TEXT,
	error TEXT,
	serialization TEXT,
	created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
	PRIMARY KEY (workflow_id, step_id)
)
```
SQLite DDL (`:163-171`, no schema):
```sql
CREATE TABLE IF NOT EXISTS transaction_completion (
	workflow_id TEXT NOT NULL,
	step_id INTEGER NOT NULL,
	output TEXT,
	error TEXT,
	serialization TEXT,
	created_at INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (workflow_id, step_id)
)
```
Key: `(workflow_id, step_id)` — the same composite key convention as the system database's `operation_outputs`, so a step is durably identified the same way in both tables.

**Provisioning is idempotent and permission-aware**: `ensureCompletionTable` (`:244-266`) first calls `completionTableInstalled` (`:219-237`, a `SELECT EXISTS(...)` against `information_schema.tables`, or `sqlite_master` for SQLite) and skips all DDL if the table is already there — so a least-privilege DML-only role works against a table the application pre-created in its own migrations. Only a genuinely missing table triggers `CREATE SCHEMA`/`CREATE TABLE`, and a failure there returns an actionable error telling the user to pre-provision the table or grant `CREATE` (`:259-262`).

## 2. `RunAsTransaction` — the three layers

Entry point: `(c *dbosContext) RunAsTransaction` (`:365-561`), called from the generic wrapper `RunAsTransaction[R]` (`:331-356`).

### Layer 0 — nesting guard (before anything else)
```go
if ws, ok := c.Value(workflowStateKey).(*workflowState); ok && ws != nil && ws.isWithinTransaction {
    return nil, models.NewStepExecutionError(ws.workflowID, stepOpts.stepName,
        fmt.Errorf("cannot call RunAsTransaction within a transaction"))
}
```
(`:366-374`) — checked before the `sameAsSystemDB` short-circuit, so it applies universally. See §5 for the full nesting matrix.

### Layer 1 — system-database checkpoint (`operation_outputs`), the replay fast path
```go
recordedOutput, err := sysdb.RetryWithResult(c, func() (*sysdb.RecordedResult, error) {
    return c.systemDB.CheckOperationExecution(uncancellableCtx, sysdb.CheckOperationExecutionDBInput{
        WorkflowID: stepState.workflowID, StepID: stepState.stepID, StepName: stepOpts.stepName,
    })
}, ...)
```
(`:441-450`). If this step ID was already recorded — by a fully-completed prior attempt of *this exact* `RunAsTransaction` call (including its Layer-1 checkpoint write) — the recorded output/error is returned immediately (`stepCheckpointedOutcome`, `:451-454`) and neither the user's `fn` nor the user database is touched again. **This closes the crash window after Layer 1 has been written** (i.e. after the whole `RunAsTransaction` call previously completed its bookkeeping) — a straightforward "already fully done" replay.

### Layer 2 — user-database checkpoint (`transaction_completion`), the "txn1 committed but txn2 didn't" fast path
```go
completion, err := sysdb.RetryWithResult(c, func() (*completionRecord, error) {
    return ds.checkCompletion(uncancellableCtx, ds.pool, stepState.workflowID, stepState.stepID)
}, ...)
```
(`:459-464`), where `checkCompletion` is:
```sql
SELECT output, error, serialization FROM %s WHERE workflow_id = $1 AND step_id = $2
```
(`:280-297`, table = `qualifiedCompletionTable()`). If a row is found here (but Layer 1 above found nothing), it means the user's transaction (`fn` + its `INSERT INTO transaction_completion`) **already committed on a previous attempt**, but the process crashed before checkpointing that fact into the system database. Recovery must **not re-run `fn`** — the row it wrote is already durably committed in the user's database, and re-running it would duplicate side effects. Instead, the stored `output`/`error` is replayed straight into the system-database checkpoint:
```go
checkpoint(completion.output, completion.errStr, replaySer, stepStartTime) // -> RecordOperationResult
return stepCheckpointedOutcome{value: completion.output, serialization: replaySer}, deserializeWorkflowError(completion.errStr)
```
(`:465-476`) — **this closes the crash window between txn1 (user DB commit) and txn2 (system DB checkpoint)**: the exact gap the two-database split creates. The replay uses the serialization codec **recorded in the row**, not the caller's current default codec (`:466-470`), so a version upgrade of the default serializer between the original run and the replay doesn't corrupt the decode.

### Layer 3 — fresh execution (only when both Layer 1 and Layer 2 found nothing)
`runTxnOnce` (`:488-511`) is one attempt:
1. `tx := ds.pool.BeginTx(uncancellableCtx, txOpts)` (isolation resolved in §4) on the **user's** pool.
2. Run `fn(stepCtx, tx)` — the user's application writes.
3. `ser.Encode(output)`.
4. `ds.recordCompletion(uncancellableCtx, tx, workflowID, stepID, encoded, nil, ser.Name())` — **inside the same `tx`** as the user's writes:
   ```sql
   INSERT INTO %s (workflow_id, step_id, output, error, serialization, created_at) VALUES ($1, $2, $3, $4, $5, $6)
   ```
   (`:301-312`). A duplicate `(workflow_id, step_id)` surfaces as `models.NewWorkflowConflictIDError` via the dialect's unique-violation detection.
5. `tx.Commit(uncancellableCtx)` — **this single commit is what makes the user's application writes and the durability row atomic** (txn1). If the commit fails or `fn` errors, the deferred `tx.Rollback` undoes both together — there is no possibility of the app write committing without its durability row, or vice versa.

After a successful (or exhausted-retry) attempt, `RunAsTransaction` writes txn2 — the system-database checkpoint — via `checkpoint(...)` → `RecordOperationResult` into `operation_outputs` (`:553-558`), completing Layer 1 for future replays. **Ordering**: user-DB commit (txn1) always happens strictly before the system-DB checkpoint (txn2) — never the reverse — which is exactly why Layer 2 (checking `transaction_completion` before assuming a fresh run is needed) must exist: a crash can land after txn1 commits but before txn2 does, and recovery must detect that via Layer 2, not by re-running `fn`.

On a failed transaction attempt (`stepError != nil` after retries), the failure is **also** best-effort mirrored into the user database as a standalone insert on the pool (not inside `fn`'s already-rolled-back transaction), written **before** the system-DB checkpoint to preserve "Layer 1 checked before Layer 2" recovery order (`:544-551`) — this failure mirror is explicitly best-effort: a failure to write it only logs a warning ("the system database remains the source of truth"), it never fails the call.

## 3. The `sameAsSystemDB` short-circuit

Detected once, at `NewDataSource` time:
```go
if sysdb.SameEngine(ds.pool, c.systemDB.Pool()) {
    ds.sameAsSystemDB = true
    ...
    return ds, nil
}
```
(`:134-138`) — condition: the data source's underlying pool handle is the literal same engine object as the DBOS system database's own pool (i.e. the application chose to store its own tables in the same Postgres instance/pool DBOS itself uses). When true, dialect resolution and `ensureCompletionTable` are skipped entirely — there is no `transaction_completion` table at all for this data source.

At call time, `RunAsTransaction` checks this flag **first, before Layer 0's nesting guard would otherwise matter for the two-database path**:
```go
if ds.sameAsSystemDB {
    return c.runAsTxn(dbosCtx, fn, opts...)
}
```
(`:376-380`). `runAsTxn` (`workflow.go:2568-2599`, delegating to `c.runAsTxn`) is the same single-transaction primitive used by every other "special step" that must be atomic with a system-database write (`Send`, `SetEvent`, `WriteStream`, etc.) — **one transaction, on the system database's own pool, that runs `fn` and records the `operation_outputs` checkpoint together**. Because there is exactly one database and one transaction, there is no txn1/txn2 split and thus no `transaction_completion` table, no Layer 2 check, and no separate hard-retry checkpoint step — the checkpoint write is part of the same atomic commit as the user's work.

## 4. Isolation level handling

Default is **Read Committed** for every transaction opened by `RunAsTransaction` (`:393` "invoked inside a step" path, `:479` fresh-execution path): `txOpts := TxOptions{IsoLevel: IsoLevelReadCommitted}`. Overridable per call via the step option `WithTxIsolation(level IsoLevel)` (`workflow.go:2226-2229`), which sets `stepOpts.txIsoLevel`; when set, `RunAsTransaction` uses `*stepOpts.txIsoLevel` instead of the Read Committed default (`:394-395`, `:480-482`). `IsoLevel` is a portable enum (`sysdb/dbq.go:53-59`: `IsoLevelDefault`, `IsoLevelReadCommitted`, `IsoLevelRepeatableRead`, `IsoLevelSerializable`) mapped to each dialect's native transaction-isolation syntax at the `BeginTx` call site. (Other system-database internal transactions — cancel/resume list-then-update, queue dequeue — use `dialect.SnapshotIsolation()`/`QueueDequeueIsolation()` instead, which is a separate, unrelated isolation policy not user-configurable through `RunAsTransaction`.)

## 5. Nesting rules

| Nesting | Outcome |
|---|---|
| **transaction-in-transaction** (`RunAsTransaction` called from inside another `RunAsTransaction`'s `fn`) | **Rejected** at runtime, unconditionally: `ws.isWithinTransaction` is checked at the very top of `RunAsTransaction` (`:368`) and returns `StepExecutionError("cannot call RunAsTransaction within a transaction")`. |
| **transaction-in-step** (`RunAsTransaction` called from inside a `RunAsStep` body) | **Allowed**, but degraded: `prep.IsWithinStep == true` routes to a distinct branch (`:390-411`) that opens a real transaction on the user's pool, runs `fn`, commits — but **records no `transaction_completion` row and no separate `operation_outputs` checkpoint of its own**. It durably rides on the enclosing step's own checkpoint instead. A crash between this inner commit and the enclosing step's own checkpoint would re-run the *entire enclosing step* (including this inner transaction) on recovery — so `fn` must be safe to re-execute in that scenario, unlike the top-level fresh-execution path which is idempotent via Layer 1/2. |
| **step-in-transaction** (calling `RunAsStep`, `Send`, `SetEvent`, etc. from inside a `RunAsTransaction`'s `fn`) | **Not reachable through the public API by construction**, not via a runtime check: `fn`'s declared parameter type is a plain `context.Context` (the `Txn[R]` type is `func(ctx context.Context, tx Tx) (R, error)`), which does not expose the `dbos.Context`/`dbos.Client` methods (`RunAsStep`, `Send`, etc.) require. There is no explicit `isWithinTransaction` check in `prepareStepExecution` (used by `RunAsStep`) or in `Send`/`Recv`/`SetEvent`/etc. (which only check `isWithinStep`) — the type signature is the only thing preventing this nesting. A port using a dynamically-typed or duck-typed context must add an explicit runtime check here, since the Go type-level guard has no direct equivalent. |
| **step-in-step**, **transaction-in-step calling another transaction while still inside the first's `fn`**, etc. | Not applicable/covered above; `prepareStepExecution`'s `wfState.isWithinStep` check is what makes any second `RunAsStep`/`RunAsTransaction` call from inside a step body collapse to "just call `fn` directly, no new checkpoint" (`:2322-2324`, and the `datasource.go:390-411` branch) rather than nesting a second durability record. |

## 6. Hard-retry policy on the post-commit system checkpoint

The `checkpoint` closure used by both the Layer-2-replay path (`:471`) and the fresh-execution path (`:553`) calls:
```go
sysdb.Retry(c, func() error {
    return c.systemDB.RecordOperationResult(uncancellableCtx, dbInput)
}, sysdb.WithRetrierLogger(c.logger))
```
`sysdb.Retry` (`system_database.go:5970-6038`) with **no `maxRetries` override** uses the default `retryConfig`:
```go
maxRetries:    -1,                 // infinite
baseDelay:     100 * time.Millisecond,
maxDelay:      30 * time.Second,
backoffFactor: 2.0,
jitterMin:     0.95,
jitterMax:     1.05,
retryConditionChain: []func(error, *slog.Logger) bool{
    PostgresDialect{}.IsRetryable,
    SqliteDialect{}.IsRetryable,
},
```
(`:5971-5982`) — i.e. **exponential backoff starting at 100ms, doubling each attempt, capped at 30s, with ±5% jitter, retried indefinitely** (`maxRetries: -1` means the attempt-count gate in `decide` is never triggered, `:6014-6016`), but **only for errors the dialect classifies as retryable** (transient connection failures, not application/integrity errors — `decide` returns `(false, err)` immediately for a non-retryable error, `:6008-6013`). The call is also wrapped in `WithoutCancel(c)` context (`uncancellableCtx`, computed once near the top of `RunAsTransaction`, `:413`) — **the checkpoint write is deliberately immune to the caller's context being cancelled**, so a workflow cancellation or deadline firing mid-checkpoint cannot abandon a write that the user's transaction has already committed and is now relying on for correctness.

**If it ultimately fails** (a genuinely non-retryable database error, e.g. a schema mismatch or the system database being permanently unreachable in a way the dialect doesn't classify as transient): the error is wrapped as `models.NewStepExecutionError(...)`. In the fresh-execution path, if the transaction body itself also errored (`stepError != nil`), the checkpoint error is joined with it via `errors.Join` before being wrapped (`:553-558`) — so the caller sees both failures, not just the checkpoint one. There is no fallback path that treats "user transaction committed but system checkpoint permanently failed" as success — the call returns an error even though the user's application data is already durably committed, which means: **on the next recovery attempt, Layer 2 (`transaction_completion`) is what prevents `fn` from re-running**, since Layer 1 was never successfully written. This is precisely why Layer 2 exists as an independent, non-optional check rather than an optimization — without it, a permanently-failed checkpoint after a successfully-committed user transaction would cause `fn` to re-execute and duplicate its side effects on the next recovery pass.

## 7. Gotchas for the port

- **Layer ordering must be Layer 1 (`operation_outputs`) checked before Layer 2 (`transaction_completion`), never the reverse**, and both must be checked before assuming a fresh execution is needed. Checking Layer 2 first (or skipping Layer 1) would replay a stale/wrong output whenever the system-database checkpoint *did* succeed on a previous run but the port re-derives a completion row from the user DB anyway — Layer 1 is authoritative; Layer 2 exists only to cover the specific gap Layer 1 cannot yet know about.
- **The Layer-2 replay path still writes the Layer 1 checkpoint before returning** (`:471`) — a port that only returns the replayed value without also completing the system-database checkpoint will re-hit Layer 2 (not Layer 1) on every subsequent recovery attempt forever, never actually closing the gap.
- **The failure mirror into `transaction_completion` (error case) is best-effort and non-fatal on write failure** — treating it as fatal would turn an already-correctly-failed workflow step into a *different* failure (a spurious "recording transaction completion" error rather than the true application error), and would block on a write into the user's database that isn't required for correctness (the system database remains authoritative for the failure).
- **Serialization codec for Layer 2 replay comes from the stored row, not the current default codec** (`:467-470`) — hardcoding "always use the current default serializer" breaks replay across a serializer version change made between the original attempt and the crash-recovery attempt.
- **`sameAsSystemDB` collapses to a completely different code path** (`runAsTxn`, one transaction, no `transaction_completion` table at all) — a port must not assume every `DataSource` has a completion table; the check must happen before any completion-table SQL is attempted, or those queries will fail against a schema that was deliberately never created.
- **Transaction-in-step silently forgoes idempotent replay for the inner transaction** — a port must document (or refuse, if it wants stronger guarantees) that `fn` bodies invoked this way must tolerate re-execution, since there's no Layer-1/Layer-2 protection specific to that inner call; only the enclosing step's own checkpoint governs replay.
- **The inner "invoked inside a step" transaction path (`:390-411`) uses `WithoutCancel(c)` for its `BeginTx` context but is otherwise a plain best-effort commit** — no retry loop wraps `runTxnOnce`-equivalent logic there (unlike the fresh-execution path's `runTxnResilient`), so a transient connection error inside this specific nesting mode propagates directly as a step error rather than being retried transparently. A port should preserve this asymmetry rather than assuming all transaction execution paths get the same retry treatment.
- **The outer user-facing step retry (`maxRetries`/predicate) wraps only `runTxnResilient`** (the fresh-execution attempt), never the Layer 1/Layer 2 checks — a failed Layer 1/2 lookup (e.g. a transient connection drop while checking) is retried by the *inner*, infinite, connection-only retry (`sysdb.RetryWithResult` at `:441` and `:459`), not counted against the user's `maxRetries` budget. Conflating these two retry layers would make transient infrastructure hiccups consume the user's limited retry budget, which the design explicitly avoids (see the comment at `:513-517` about not compounding retries).
