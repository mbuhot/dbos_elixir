# Performance

Both this engine and the upstream Go implementation are bound by Postgres round-trips, so the number
of statements per workflow is the measurement that travels: it is the same on any machine, and it is
what a network hop multiplies.

Run the harness with:

```
mix test.bench
```

It reports; it does not assert wall-clock thresholds. The bounds it does assert are loose sanity
checks (an idle engine settling below two round-trips a second, a two-step workflow costing under
thirty round-trips).

## Round-trips per workflow

A two-step workflow started directly and awaited, counted through a `Dbos.DB` adapter that tallies
every statement:

| Statement | Per workflow |
|---|---|
| `INSERT operation_outputs` | 2 |
| `SELECT output, error, function_name, serialization` | 2 |
| `SELECT status FROM workflow_status` | 2 |
| `UPDATE workflow_status SET executor_id` | 2 |
| `SELECT <full row> FROM workflow_status` | 2 |
| `INSERT workflow_status` | 1 |
| `SELECT workflow_timeout_ms, workflow_deadline_epoch_ms` | 1 |
| `UPDATE workflow_status` (outcome) | 1 |
| `BEGIN`/`COMMIT` (3 transactions) | 6 |
| **Total** | **19** |

The step path is statement-for-statement what upstream issues: a checkpoint is one guarded `INSERT`
plus an `executor_id` re-stamp, and a replay check is two `SELECT`s inside one transaction.

Two statements have no upstream counterpart:

- **The full-row status reads.** `Dbos.await/2` resolves an outcome through the database, so a
  workflow running in the same VM is still read twice — once finding it `PENDING`, once finding the
  outcome. Upstream hands a local caller a channel. Each read is every column, including `inputs`
  and `output`.
- **The deadline resolution.** `resolve_workflow_deadline/2` reads `workflow_timeout_ms` even for a
  workflow that has no timeout, though the insert already returned that column.

## Where the time goes

Measured against Postgres over a local socket, so these are floors — a network hop scales with the
round-trip counts above.

| Workload | Result |
|---|---|
| Two-step workflows, started and awaited one at a time | ~3.9 ms each, ~255/s |
| 500 queued workflows, `worker_concurrency: 20`, 1 s polling | ~32/s |
| 500 queued workflows, `worker_concurrency: 20`, 50 ms polling | ~230/s |

## The queue throughput ceiling

A runner claims at most its free capacity per pass, and a completing workflow notifies nobody, so the
next claim waits for the next tick. Throughput per engine is bounded by:

```
worker_concurrency / base_polling_interval
```

The two queue rows above are the same workload at two intervals, and the 7× spread between them is
that bound. Notifications cover arrival, so a queue that is fed steadily runs at full speed; a burst
larger than one batch drains a batch per tick.

## Idle cost

An engine with one queue and a `LISTEN` connection, doing nothing:

| Window | Round-trips per second |
|---|---|
| First 5 s | ~4 |
| Following 25 s | ~1 |

The rate decays because an idle tick that found an empty queue backs off toward the ceiling, which is
safe once notifications are the primary wake-up. The residue is the dequeue attempt itself — a
version lookup and a candidate `SELECT` inside one transaction — plus a lease renewal.

Upstream scales its interval back to the base on every successful pass, backing off only on
contention, so an idle queue there costs a dequeue and a config reconcile every second indefinitely.

## What the notification triggers cost

`workflow_status` carries two triggers this engine adds: one announcing arrivals on a queue, one
announcing every status change. Timed as an insert-plus-outcome pair, alternating rounds with the
triggers enabled and disabled:

| Condition | Added per workflow |
|---|---|
| A `LISTEN`er attached | ~0.04–0.08 ms |
| No `LISTEN`er | ~0.03–0.10 ms |

Under 3% of a whole workflow, and the measurement sits close enough to the noise floor that the
rounds disagree on which arm is faster. It buys the idle savings above, and it is what lets a
cancellation reach a workflow parked on another node.

## The published DBOS benchmark

`dbos-inc/dbos-workflow-benchmarks` compares DBOS against AWS Step Functions on one workload: a
workflow that invokes a transaction `i` times, timed server-side around the start-and-await, reported
as a latency distribution over `n` iterations. The transaction reads a counter for a name and writes
it back. That workload is ported here, statement for statement, against the same `dbos_hello` table,
with the application tables in the same database as the system tables — the arrangement their DBOS
Cloud deployment uses.

Round-trips are exactly linear in the transaction count, which makes them the regression detector:

| Transactions per workflow | Round-trips |
|---|---|
| 1 | 15 |
| 2 | 23 |
| 5 | 47 |
| 10 | 87 |
| 20 | 167 |

`7 + 8i`, holding to the decimal across runs. The eight cover the engine's statements; the body's own
two go through the `Repo` directly, so a connection carries ten per transaction.

| Statement | Per transaction |
|---|---|
| `SELECT status FROM workflow_status` | 1 |
| `SELECT output, error, function_name, serialization` | 1 |
| `INSERT operation_outputs` | 1 |
| `UPDATE workflow_status SET executor_id` | 1 |
| `BEGIN`/`COMMIT` (the check, then the body) | 4 |

Wall-clock, measured locally, is a floor rather than a comparison against any published figure:

| Transactions | Median | p95 |
|---|---|---|
| 1 | ~3.3 ms | ~5 ms |
| 5 | ~9 ms | ~11 ms |
| 20 | ~32 ms | ~40 ms |

Fitted through the medians: **~1.5 ms per transaction**, on ~1.6 ms of fixed cost per workflow.

## Comparing against upstream

Upstream ships no benchmarks in the Go SDK and no profiling tests. Its `chaos_tests` run ten thousand
workflows with a chaos monkey restarting Postgres underneath, asserting every result is correct and
timing nothing. So the comparison is per-statement, read from its SQL, rather than a number to race:

| Path | Upstream | Here |
|---|---|---|
| Step replay check | `BEGIN` + 2 `SELECT` + `ROLLBACK` | `BEGIN` + 2 `SELECT` + `COMMIT` |
| Step checkpoint | guarded `INSERT` + `executor_id` re-stamp | same |
| Workflow insert | `INSERT ... ON CONFLICT ... RETURNING` in the caller's transaction | same, plus `ex_workflow_version` |
| Dequeue | version lookup + candidate `SELECT` + claim `UPDATE`, one transaction | same |
| Success outcome | one guarded `UPDATE` | same |
| Error outcome | one guarded `UPDATE` | `UPDATE` and the unwind enqueue, one transaction |

A transaction is the one path with a structural difference. Upstream, when the application and system
tables share a database, runs the replay check, the body and the checkpoint in a single transaction,
bracketing the body in a `SAVEPOINT` so a failure discards its writes and leaves the transaction
usable. `deftransaction` commits the replay check on its own, then runs the body and its checkpoint
together. Both land on ten round-trips per transaction — the savepoint pair against the extra
`BEGIN`/`COMMIT` — and both make the body atomic with the checkpoint that records it, which is the
guarantee a transaction exists for.
