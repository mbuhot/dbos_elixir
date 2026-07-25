# FAQ

Troubleshooting, in question-per-heading form. Each entry: the symptom, the cause, the fix.

## My workflow fails to compile with "is nondeterministic and breaks replay"

**Symptom:** a `CompileError` naming a call like `:rand.uniform/0`, `DateTime.utc_now/0`,
`Process.sleep/1`, `receive`, `Task.async/1`, or a direct call to your `:repo` module, pointing at
the file and line inside a `defworkflow` body.

**Cause:** `Dbos.Determinism` walks every `defworkflow` body at compile time and rejects
constructs that would produce a different value (or block in a way the engine can't checkpoint)
on a replay than they did the first time. See `docs/determinism.md` for the full banned list and
why each entry is on it.

**Fix:** move the flagged call inside a `defstep`/`deftransaction` — its result becomes a
checkpointed output, and the workflow body reads that output on replay. The nondeterministic
operation itself never runs again. For `receive`, use `Dbos.recv_message/2` or `Dbos.sleep/1`.
For a direct repo call, wrap it in `deftransaction` so it commits atomically with its
checkpoint.

## What does `Dbos.UnexpectedStepError` mean?

**Symptom:** raised at runtime, naming a `workflow_id`, `function_id`, an `expected` step name,
and a `recorded` one.

**Cause:** the step the current code is about to run at this position does not match the step
name recorded in `operation_outputs` for that same `(workflow_uuid, function_id)` — a replay
whose step sequence has drifted from what actually happened during the original run. Usually
this means the workflow's code changed (a step reordered, renamed, or added/removed) while an
instance of it was still in flight. See `guides/tutorials/upgrading-workflows.md`.

**Fix:** there's no way to repair the mismatch in place — the checkpoint and the code disagree
about what happened at that step. Restore the code that produced the original layout long enough
for the workflow to finish (or fork it from before the point of divergence with `Dbos.fork/3`,
if you know exactly where that is), then apply future changes through a version bump or a new
workflow name — an in-place edit leaves the mismatch unresolved.

## My workflow is stuck `PENDING` and never progresses

**Symptom:** `Dbos.status/2` (or a direct query) shows `status: :pending` indefinitely; no
process is running it.

**Cause**, in order of likelihood:

1. **Nobody has this workflow's name registered.** `Dbos.Recovery`/reclaim log a warning
   ("workflow ... is not registered on this executor; skipping recovery") and move on without
   raising — check your logs for it. `workflows:` on `Dbos.Supervisor` must include the
   module (or the exact `{name, {module, fun, arity}}` tuple).
2. **`application_version` mismatch.** A non-queued `PENDING` row is only reclaimed by an
   executor whose own `config.application_version` matches the row's (`reclaim_pending_workflows/3`
   filters on it when set). If every live executor is running a different version than the one
   that started this workflow, nothing will ever pick it up. See
   `guides/tutorials/upgrading-workflows.md`.
3. **It's actually running, just slow or blocked.** A step body that's genuinely long-running, or
   a `defworkflow` blocked in `Dbos.sleep/1`/`Dbos.recv_message/2` with a long remaining wait,
   looks identical to "stuck" from the status alone — check `operation_outputs` for that
   workflow to see how far it's actually gotten.

**Fix:** register the missing name and restart, or align `application_version`, or call
`Dbos.Recovery.reclaim/3` (or `POST /dbos-workflow-recovery` on the admin server) explicitly
naming the executor id it's stuck under.

## What does `MAX_RECOVERY_ATTEMPTS_EXCEEDED` mean?

**Symptom:** `Dbos.status/2` returns this status; `Dbos.result/2` / `Dbos.await/2` return
`{:error, %Dbos.MaxRecoveryAttemptsExceededError{}}`.

**Cause:** the workflow's `recovery_attempts` climbed past `Dbos.Config.max_recovery_attempts`
(default `3`) — every recovery/reclaim pass that redispatches it bumps this counter, so a
workflow whose process crashes immediately on every attempt — the signature of a bug in the
workflow body, since a transient failure would eventually succeed — eventually gets moved
here, ending the retry loop.

**Fix:** this status will not run again on its own. Fix whatever's actually crashing it, then
call `Dbos.resume/2` — it clears the queue assignment and deadline, resets `recovery_attempts` to
`0`, and re-enqueues it from its last checkpoint.

## The notification listener fell back to polling

**Symptom:** a log warning at boot mentioning the dedicated `LISTEN` connection couldn't be
established; `Dbos.Notifications.mode/1` returns `:poll` even though you configured
`notifications: :listen` (the default).

**Cause:** either no connection options could be derived (a bare `Dbos.DB.Postgrex` pool with no
`notifications_conn_opts:` given), or the connection attempt itself failed.

**Fix:** pass `notifications_conn_opts:` explicitly if you're on the Postgrex adapter, or confirm
`Dbos.DB.Ecto`'s repo config is reachable at boot. This is not fatal — waits still work, on a
1-second polling cadence. Check whether that's intentional or an unnoticed degradation. Full
detail: `guides/integrating-dbos.md`, "The dedicated LISTEN connection".

## The engine refuses to start: migration version mismatch

**Symptom:** a raised error at boot naming a `dbos_migrations.version` that isn't the expected
one (`42`), or saying the table doesn't exist at all.

**Cause:** `Dbos.Migrator.verify!/1` (run by `migrations: :verify`, the default, and also the
first check `:create_if_absent` makes) refuses to start against a schema at the wrong version —
running against tables whose shape doesn't match what the engine expects would silently
checkpoint into the wrong columns.

**Fix:** run `mix dbos.gen.migration` and apply the resulting migration through
`mix ecto.migrate` (or, for local dev only, switch to `migrations: :create_if_absent`, which
applies `priv/schema/dbos_schema.sql` verbatim if verification fails). Never point production at
a schema you haven't migrated deliberately.

## A workflow will not cancel

**Symptom:** `Dbos.cancel/2` returns `:ok`, `workflow_status.status` becomes `CANCELLED`, but the
running process keeps going.

**Cause:** cancellation is cooperative — a workflow observes it at its next durable operation.
A workflow blocked in a durable wait (`sleep`/`recv_message`/`get_event`) with a live process
on this engine is woken immediately.
A workflow actively executing a step's body only notices at its **next step boundary** —
`check_operation_execution` is where the cancelled status is actually observed and stops the
workflow. A step whose body runs for a long time (or loops without ever calling another durable
operation) has no boundary to hit until it returns.

**Fix:** cancellation will take effect at the next step call; if the current step's body can run
indefinitely, add a durable operation (or break it into smaller steps) so there's a boundary to
observe the cancellation at, or accept that cancellation only prevents *future* steps — the
one already in progress still runs to completion.

## Deduplication errors: `Dbos.QueueDeduplicatedError`

**Symptom:** `Dbos.enqueue/3` raises this, naming a `workflow_id`, `queue_name`, and
`deduplication_id`.

**Cause:** `deduplication_id` on a queue acts as a unique slot — a workflow already holding that
`(queue_name, deduplication_id)` pair exists and hasn't finished (or been re-enqueued past the
point where the slot frees), and this call tried to claim the same slot.

**Fix:** either treat the raise as "already enqueued, nothing to do" (catch it, or check
`Dbos.SystemDb.get_deduplicated_workflow/3` first to see who holds it), or pick a deduplication
id that's actually unique to this attempt. Note `deduplication_id` and `partition_key` are
mutually exclusive on the same enqueue call.

## A `Task` inside a workflow silently loses its checkpoint

**Symptom:** durable operations called from inside a `Task.async`/`Task.start` re-run in full on
every replay. They are never checkpointed — no error, just repeated side effects.

**Cause:** inside a `defworkflow` body, this is actually a compile error — `Dbos.Determinism`
bans `Task.async`, `Task.await`, `Task.async_stream` and `Task.start` outright, since a spawned `Task` does not inherit
the workflow context (`Dbos.Runtime.current_workflow_id/0` has nothing to return inside it) and
anything durable called from within it silently skips checkpointing. The checker only walks
`defworkflow` bodies, though — the exact same problem can happen unnoticed inside a `defstep`'s
own body, which isn't checked the same way, if that step itself spawns a `Task` and calls a
durable operation from inside it.

**Fix:** never spawn a bare `Task` around a durable operation, whether in a workflow body (where
it won't compile) or a step body (where it will, and will misbehave). Use a step for concurrent
work that doesn't itself need to be durable, or a child workflow (`Dbos.start/3` from inside a
workflow) for concurrent work that does.
