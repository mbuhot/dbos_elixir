# Notifications, Events, Streams — Go Reference

Source: `reference/dbos-transact-golang/dbos/internal/sysdb/system_database.go` (`sysdb` package), `reference/dbos-transact-golang/dbos/workflow.go`, migrations under `reference/dbos-transact-golang/dbos/internal/sysdb/migrations/`.

## 1. LISTEN/NOTIFY channels

Three fixed channel names (`system_database.go:354-356`):

```go
_DBOS_NOTIFICATIONS_CHANNEL   = "dbos_notifications_channel"
_DBOS_WORKFLOW_EVENTS_CHANNEL = "dbos_workflow_events_channel"
_DBOS_STREAMS_CHANNEL         = "dbos_streams_channel"
```

Each is driven by an `AFTER INSERT` trigger installed by migrations. On CockroachDB (no `LISTEN/NOTIFY` support) these triggers are skipped entirely and the poller fallback is used instead (`system_database.go:384-388`, `:418-425`).

### notifications (migration 1, `1_initial_dbos_schema_listen_notify.sql:1-14`)
```sql
CREATE OR REPLACE FUNCTION %s.notifications_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.destination_uuid || '::' || NEW.topic;
BEGIN
    PERFORM pg_notify('dbos_notifications_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dbos_notifications_trigger
AFTER INSERT ON %s.notifications
FOR EACH ROW EXECUTE FUNCTION %s.notifications_function();
```
Payload = `destination_uuid || '::' || topic`.

### workflow_events (migration 1, `1_initial_dbos_schema_listen_notify.sql:16-29`)
```sql
CREATE OR REPLACE FUNCTION %s.workflow_events_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_workflow_events_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER dbos_workflow_events_trigger
AFTER INSERT ON %s.workflow_events
FOR EACH ROW EXECUTE FUNCTION %s.workflow_events_function();
```
Payload = `workflow_uuid || '::' || key`.

### streams (migration 39, `39_create_streams_trigger.sql`)
```sql
CREATE OR REPLACE FUNCTION %s.streams_function() RETURNS TRIGGER AS $$
DECLARE
    payload text := NEW.workflow_uuid || '::' || NEW.key;
BEGIN
    PERFORM pg_notify('dbos_streams_channel', payload);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER FUNCTION %s.streams_function() SET search_path = pg_catalog, pg_temp;

DROP TRIGGER IF EXISTS dbos_streams_trigger ON %s.streams;
CREATE TRIGGER dbos_streams_trigger
AFTER INSERT ON %s.streams
FOR EACH ROW EXECUTE FUNCTION %s.streams_function();
```
Payload = `workflow_uuid || '::' || key`. This trigger is added only in migration 39, applied on top of every earlier `INSERT` into `streams` (writes before migration 39 in an existing deployment obviously predate the trigger, but that's a historical detail, not a runtime path).

## 2. Listener process design

One dedicated goroutine, `notificationListenerLoop` (`system_database.go:3544-3659`), owns a single dedicated pooled connection for the process's whole lifetime (until reconnect is required):

- `acquire()` (`:3555-3585`) checks out a `pgxpool.Conn`, opens a transaction, issues `LISTEN <channel>` for all three channel names, then commits. All three LISTENs share one connection — Postgres multiplexes multiple channels onto one `LISTEN`ing backend.
- The loop (`:3599-3658`) calls `poolConn.Conn().WaitForNotification(ctx)` in a blocking loop. On each notification it dispatches by `n.Channel` to one of three in-process `notifyRegistry` instances: `RecvNotifier`, `EventNotifier`, `streamNotifier` (`:3650-3657`).
- Reconnect handling: if the connection is closed (`:3611-3634`), it releases and re-acquires (with exponential backoff via `WaitForRetry`/`ConnectionRetryBackoff`), then calls `s.RecvNotifier.notifyAll()` and `s.EventNotifier.notifyAll()` (NOT `streamNotifier.notifyAll()` — streams gap is closed by the reader's own polling fallback, see §5) so every waiter re-probes the table for a value whose notification may have been dropped during the reconnect window (`:3630-3634`).
- Other transient errors: backoff and retry on the same connection (`:3636-3642`); a successful notification decrements the backoff counter (`:3645-3648`).
- Context cancellation causes clean exit (`:3603-3609`, `:3615-3618`).

### In-process waiter registration (`notifyRegistry`, `:3792-3903`)

A `notifyRegistry` is a `map[payload string]map[chan struct{}]struct{}` guarded by a mutex — payload is the same `"id::topic"` / `"id::key"` string used as the NOTIFY payload.

- `subscribe(payload)` — allocate a buffered (`cap=1`) channel, register it under `payload`. Multiple waiters can share the same payload (used for `getEvent`, streams).
- `subscribeExclusive(payload)` — same, but fails (`ok=false`) if a waiter is already registered for the payload. Used for `recv`: only one workflow may `Recv` a given `(destinationID, topic)` at a time (`system_database.go:3953-3959`, returns `WorkflowConflictIDError`).
- `notify(payload)` wakes every channel registered under that payload with a non-blocking send (`select ... default:` — coalesces bursts into one pending wake, `:3846-3855`).
- `notifyAll()` wakes every channel under every payload — used only on listener reconnect.
- `unsubscribe` drops a single waiter and removes the payload entry once empty.

`notificationWait` (`:3912-3947`) is the shared wait loop used by both recv and getEvent: it blocks on `<-ch` or the deadline, and on each wake re-invokes a `recheck` callback (a `SELECT EXISTS(...)` query) to authoritatively decide whether the awaited condition now holds — the channel wake is only a *hint* to re-poll, never itself proof that the row exists. This makes the design proof against notifications being coalesced, delayed, or (during a reconnect gap) lost.

### Polling fallback (`system_database.go:1 (const), :3661-3739`)

When `ListenNotifyPool()` returns nil (CockroachDB, or any dialect without `SupportsListenNotify()`), `Launch` (`:955-973`) starts `notificationPollerLoop` instead of the listener loop. It ticks every **1 second** (`ticker := time.NewTicker(1 * time.Second)`, `:3668`) and, on each tick, calls `pollNotifications` and `pollEvents`:

- `pollNotifications` (`:3683-3710`) iterates every payload currently registered in `RecvNotifier` (i.e. every active `Recv` waiter), runs `SELECT EXISTS(SELECT 1 FROM %snotifications WHERE destination_uuid = $1 AND topic = $2 AND consumed = false)`, and if true, calls `RecvNotifier.notify(payload)` to wake the waiter (which will then recheck itself via its own `recheck` closure).
- `pollEvents` (`:3712-3739`) does the analogous thing against `workflow_events` for `EventNotifier` payloads.
- There is no equivalent poller for streams; the reader itself falls back to a **1 second** bounded wait using the shared `DBRetryInterval` constant (`system_database.go:365`, used at `workflow.go:3721`) inside its own read loop (see §5) rather than a central poller thread.

Both pieces (LISTEN or poll) are started exactly once, from `Launch` (`system_database.go:955-973`), and both write to the *same* `notifyRegistry` instances, so callers of `StartRecvListener`/`StartEventListener`/`StreamWakeChannel` are unaffected by which transport is active.

## 3. `send`

`NullTopic` sentinel (`system_database.go:3741`):
```go
const NullTopic = "__null__topic__"
```
`Send` (`workflow.go:3011-3059`) substitutes this whenever the caller passes an empty topic (`system_database.go:3760-3764`); `GetAllNotifications` normalizes it back to a nil `Topic` for observability output (`:4291-4293`).

Insert SQL (`system_database.go:3769`):
```sql
INSERT INTO %snotifications (destination_uuid, topic, message, serialization, message_uuid, created_at_epoch_ms)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (message_uuid) DO NOTHING
```
- `message_uuid` is a random UUID by default, or (with `WithIdempotencyKey(key)`) is deterministically `fmt.Sprintf("%s::%s", idempotencyKey, destinationID)` (`:3771-3773`) so a retried `Send` with the same key is a no-op on replay — the `ON CONFLICT (message_uuid) DO NOTHING` is what makes this idempotent (`:3766-3768`).
- Sending to a **non-existent** destination workflow: the `notifications.destination_uuid` FK to `workflow_status(workflow_uuid)` fails and the driver reports a foreign-key violation, translated to `models.NewNonExistentWorkflowError(input.DestinationID)` (`:3783-3786`). There is no special check for a **terminal** workflow — the FK only checks existence, so `Send` to a `SUCCESS`/`ERROR` workflow succeeds and the message simply sits unconsumed forever (the receiving workflow, being terminal, will never call `Recv` again).
- `Send` is itself a checkpointed step when called from inside a workflow (runs inside `runAsTxn`, step name `"DBOS.send"`), and is explicitly forbidden from being called inside a step (`workflow.go:3017-3019`, returns `StepExecutionError`).

## 4. `recv` — full sequence (`workflow.go:3082-3172`)

1. **Guard**: must be called from within a workflow, and not from within a step (`:3084-3089`).
2. Two step IDs are pre-allocated up front, `stepID` then `sleepStepID` (`:3093-3094`), so the recorded operation layout is identical whether or not the internal timeout-sleep step actually executes (relevant for replay determinism across recovery/fork).
3. **Early-exit / idempotency check**: `CheckOperationExecution` for `stepID` under step name `"DBOS.recv"` (`:3101-3113`). If this step was already checkpointed by a prior attempt (recovery replaying a workflow that had already consumed its message), the checkpointed output/error is returned immediately — no re-registration, no re-wait.
4. **Register as receiver**: `StartRecvListener(ctx, workflowID, topic)` (`:3116`) — calls `RecvNotifier.subscribeExclusive` (fails with `WorkflowConflictIDError` if another `Recv` is already registered for this exact `(workflowID, topic)`), THEN runs the `recheck` query:
   ```sql
   SELECT EXISTS (SELECT 1 FROM %snotifications WHERE destination_uuid = $1 AND topic = $2 AND consumed = false)
   ```
   Subscribing *before* querying is deliberate: it closes the crash/race window where a `Send` could complete (and NOTIFY fire) in the gap between "waiter not yet registered" and "waiter now registered" — because the wake channel has capacity 1, a notify that lands between subscribe and the recheck query is buffered and will still be observed by the subsequent wait loop even though the initial recheck (racing the same insert) might return either true or false.
5. If the message was **already pending** at step 4 (`waiter.Pending == true`), skip the timeout-sleep checkpoint and the wait entirely — go straight to step 7.
6. Otherwise, checkpoint the timeout deadline as its own durable **`"DBOS.sleep"`** step (`:3124-3131`): `deadlineMs := time.Now().Add(timeout).UnixMilli()`, persisted via `runAsTxn`. On replay this returns the *originally recorded* deadline, so a recovered workflow only waits the remaining time, not the full timeout again. Then `waiter.Wait(deadline)` blocks (via the shared `notificationWait` loop, §2) until either the recheck query finds a message or the deadline passes (`timeoutOccurred = true`).
7. **Consume + checkpoint the `recv` result in a single transaction** (`runAsTxn`, step name `"DBOS.recv"`, id `stepID`): inside that transaction, call `ConsumeMessage`. If `message == nil && timeoutOccurred`, the step's checkpointed error is `TimeoutError`. If another executor already completed and checkpointed step `stepID` first, `runAsTxn` short-circuits and returns the previously recorded result instead of consuming again — so at most one execution attempt actually calls `ConsumeMessage` for a given step id.

### `ConsumeMessage` (`system_database.go:3986-4008`)
```sql
WITH oldest_entry AS (
    SELECT message_uuid
    FROM %snotifications
    WHERE destination_uuid = $1 AND topic = $2 AND consumed = false
    ORDER BY created_at_epoch_ms ASC
    LIMIT 1
)
UPDATE %snotifications
SET consumed = true
WHERE message_uuid = (SELECT message_uuid FROM oldest_entry)
RETURNING message, serialization
```
Confirmed: oldest by `created_at_epoch_ms ASC LIMIT 1`, and the `UPDATE` matches by `message_uuid` (not `created_at_epoch_ms`) — the code comment explains why: "created_at_epoch_ms can match multiple rows when inserts occur in the same millisecond" (`:3987`), so keying the `UPDATE` off the CTE-selected `message_uuid` guarantees exactly one row flips even under millisecond collisions. **Never a `DELETE`** — rows persist forever with `consumed = true`, which is what backs `GetAllNotifications` observability.

## 5. `setEvent` / `getEvent`

### `SetEvent` (`system_database.go:4019-4053`, called from `workflow.go:3251-3284`)
Two writes in the same transaction (the enclosing `runAsTxn` for step `"DBOS.setEvent"`):
```sql
INSERT INTO %sworkflow_events (workflow_uuid, key, value, serialization)
VALUES ($1, $2, $3, $4)
ON CONFLICT (workflow_uuid, key)
    DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization
```
```sql
INSERT INTO %sworkflow_events_history (workflow_uuid, function_id, key, value, serialization)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (workflow_uuid, function_id, key)
    DO UPDATE SET value = EXCLUDED.value, serialization = EXCLUDED.serialization
```
`workflow_events` is upserted **in place per key** (last write wins, `SetEvent` with the same key overwrites) — this is the table `GetEventValue`/`GetEvent` read from. `workflow_events_history` (migration 6, `6_add_workflow_events_history.sql`) is keyed by `(workflow_uuid, function_id, key)` — i.e. per *step*, not per key — so it preserves every distinct `SetEvent` call site's value across workflow forks/replays (`function_id` is the step ID of the enclosing `setEvent` call). Its purpose: when a workflow is forked (see `notes/recovery.md`), execution resumes at a later step, and any `setEvent` calls at earlier steps must be able to report their originally-recorded value again without re-executing — `workflow_events_history` is what supplies that per-step value on replay/fork, while `workflow_events` only ever holds each key's *current* value. Migration 6 DDL:
```sql
CREATE TABLE %s.workflow_events_history (
    workflow_uuid TEXT NOT NULL,
    function_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (workflow_uuid, function_id, key),
    FOREIGN KEY (workflow_uuid) REFERENCES %s.workflow_status(workflow_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
);
```
(Migration 11, `11_add_serialization_columns.sql`, later adds a `serialization` column to several tables including this one — not shown here verbatim since not requested, but referenced by the `INSERT ... serialization` above.)

### `GetEvent` (`workflow.go:3308-3429`)
Works both inside and outside a workflow:
- **Inside a workflow**: checkpointed like `recv` — pre-allocates `stepID` then `sleepStepID`, does the `CheckOperationExecution` early-exit for step `"DBOS.getEvent"`, registers via `StartEventListener` (a plain `subscribe`, NOT exclusive — multiple workflows/callers may wait on the same `(targetWorkflowID, key)` concurrently, unlike `recv`), checkpoints the timeout deadline as a `"DBOS.sleep"` step if not already pending, waits, then reads+checkpoints the value inside one transaction (`runAsTxn`, step `"DBOS.getEvent"`).
- **Outside a workflow** (`workflow.go:3377-3395`): still registers via `StartEventListener` and waits the same way, but the value read afterward (`GetEventValue`) is a plain retried query with **no step checkpoint at all** — there is no workflow context to checkpoint into. This is the difference the task calls out: inside a workflow, the read is durable/replayable; outside, it is a one-shot best-effort read with retry-on-transient-error only.

`GetEventValue` (`system_database.go:4088-4100`):
```sql
SELECT value, serialization FROM %sworkflow_events WHERE workflow_uuid = $1 AND key = $2
```

## 6. Streams

### `WriteStream` (`system_database.go:4128-4173`, called from `workflow.go:3515-3548`)
Checkpointed step `"DBOS.writeStream"`. Two queries against `%sstreams`:
```sql
SELECT 1 FROM %sstreams
WHERE workflow_uuid = $1 AND key = $2 AND value = $3 LIMIT 1   -- checkClosedQuery, $3 = StreamClosedSentinel
```
```sql
INSERT INTO %sstreams (workflow_uuid, key, value, "offset", function_id, serialization)
SELECT $1, $2, $3, COALESCE(
    (SELECT MAX("offset") FROM %sstreams WHERE workflow_uuid = $1 AND key = $2), -1
) + 1, $4, $5
```
If the closed-sentinel row already exists for this `(workflow, key)`, `WriteStream` returns an error (`"stream '%s' is already closed"`) instead of inserting — writes after close are rejected, not appended. Offset assignment is `MAX(offset)+1` (or `0` if none) computed **inside the same statement/transaction** as the insert, so ordering is enforced by the enclosing transaction's isolation, not by the trigger.

### `CloseStream` (`workflow.go:3930-3946`)
Just calls `WriteStream` with `Value = &StreamClosedSentinel` where `StreamClosedSentinel = "__DBOS_STREAM_CLOSED__"` (`system_database.go:359`). The sentinel is a normal row occupying the next offset — it is filtered out of read results (both `ReadStream` and `GetAllStreamEntries`) but is what makes `closed` become `true`.

### `ReadStream` (`system_database.go:4177-4221`)
```sql
SELECT value, "offset", serialization FROM %sstreams
WHERE workflow_uuid = $1 AND key = $2 AND "offset" >= $3
ORDER BY "offset" ASC
```
Scans rows in offset order; stops and reports `closed=true` as soon as it hits the sentinel value (never included in the returned entries).

### Reader resume semantics (`workflow.go:3592-3729`, `(c *dbosContext) readStream`)
This is the long-poll loop backing the public `ReadStream`/`ReadStreamAsync`:
1. Registers on `StreamWakeChannel(workflowID, key)` — a shared (non-exclusive) `streamNotifier` subscription; multiple readers of the same stream share one wake channel, and whichever reader's cleanup fires first drops the registration for all of them (comment at `:3616-3621`).
2. Loop: drain any stale wake, call `ReadStream` from `currentOffset`, forward each returned value on the output channel, advance `currentOffset = entry.Offset + 1`.
3. If the sentinel was hit, emit `Closed: true` and stop.
4. If `snapshot` mode (`WithReadStreamSnapshot()`), stop after one drain pass regardless of closed/open state — used for a non-blocking "give me what's there now" read.
5. Otherwise, check the *producing workflow's* status (`ListWorkflows`). If it is no longer `PENDING`/`ENQUEUED` (i.e. terminal), set `finalRead = true` and loop once more (rather than stopping immediately) — this closes a race where the workflow committed a final write and went terminal between the read and the status check; the extra pass drains anything committed in that gap before reporting closed.
6. If nothing new was read and the workflow is still active, block on `select { <-c.Done(), <-wakeCh, <-time.After(DBRetryInterval) }` — `DBRetryInterval = 1 * time.Second` (`system_database.go:365`) is the bounded polling fallback that also protects against a missed/coalesced NOTIFY, since workflow completion itself fires no stream notification and must be discovered by polling `ListWorkflows`.

## 7. Gotchas / deviation risks for the port

- **`NullTopic` must be applied consistently on both write and read paths.** `Send`/`Recv` both default an empty topic to `NullTopic` independently (`workflow.go:3095-3097` for Recv, `system_database.go:3761-3764` for Send) — a port that defaults only one side will silently split messages into two different "topics" and both `Send` and `Recv` will appear to work while never seeing each other's traffic.
- **`ConsumeMessage` must key its `UPDATE` by `message_uuid`, not by `(destination_uuid, topic, created_at_epoch_ms)`.** Millisecond-collision inserts make a timestamp-keyed update ambiguous (could update 0 or >1 rows depending on DB semantics); the CTE-then-UPDATE-by-uuid pattern is what guarantees exactly one row transitions.
- **Subscribe-before-recheck ordering in `recv`/`getEvent`.** If a port checks the table before registering the waiter, a `Send`/`SetEvent` landing in that exact gap is invisible: no row was found by the check, and the notify (which already fired) is never observed because the waiter wasn't registered yet. The buffered wake channel only helps if subscription happens first.
- **`recv` requires exclusivity; `getEvent` does not.** A port that reuses one generic "waiter registry" for both must preserve `subscribeExclusive` semantics for recv (conflicting concurrent `Recv` on the same `(workflowID, topic)` must fail fast with a conflict error) while allowing arbitrary fan-out for `getEvent`.
- **Timeout is checkpointed as a deadline, not a duration.** Recording `time.Now().Add(timeout)` as an absolute deadline step means recovery replays the *remaining* wait; a port that re-records the full `timeout` duration on every recovery attempt will make a recovering workflow wait far longer than the original caller intended (and could loop forever if it keeps re-crashing near the same point).
- **Sending to a terminal workflow is silently accepted.** Only the FK on `destination_uuid` is checked (existence), never workflow status — a port that additionally rejects sends to terminal workflows would diverge from observed behavior (and than break any use case that intentionally leaves a message for later manual inspection via `GetAllNotifications`).
- **Streams: writes after close must be rejected, not silently appended or silently dropped** (`WriteStream`'s closed-sentinel check happens before the insert, and returns an error — it does not swallow the write).
- **Stream reader's "final read after inactive" pass is required for correctness**, not an optimization — omitting it introduces a real race that drops the last value(s) written immediately before workflow completion.
- **CockroachDB gets no `streamNotifier.notifyAll()` on any code path** — because migration 39 (the streams trigger) is skipped there entirely, so there is never a NOTIFY to miss; the reader's own `DBRetryInterval` poll is the only signal source in that deployment, and a port must ensure the equivalent poll path exists whenever `LISTEN/NOTIFY` is unavailable, mirroring `notificationPollerLoop`'s 1-second cadence.
- **`workflow_events` vs `workflow_events_history` have different keys** (`(workflow_uuid, key)` vs `(workflow_uuid, function_id, key)`) — collapsing them into a single table would lose the fork/replay per-step provenance that `workflow_events_history` exists to preserve.
