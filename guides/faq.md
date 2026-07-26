# FAQ

Troubleshooting, in question-per-heading form. Each entry: the symptom, the cause, the fix.
Background for every entry here lives in the rest of the guides.

## My workflow fails to compile with "is nondeterministic and breaks replay"

**Symptom:** a `CompileError` naming a call like `:rand.uniform/0`, `DateTime.utc_now/0`,
`Process.sleep/1`, `receive`, `Task.async/1`, or a direct call to your `:repo` module, pointing at
the file and line inside a `defworkflow` body.

**Cause:** The determinism checker walks every `defworkflow` body at compile time and rejects
constructs that would produce a different value (or block in a way the engine can't checkpoint)
on a replay than they did the first time.

**Fix:** move the flagged call inside a `defstep`/`deftransaction` — its result becomes a
checkpointed output, and the workflow body reads that output on replay. The nondeterministic
operation itself never runs again. For `receive`, use `Dbos.recv_message/2` or `Dbos.sleep/1`.
For a direct repo call, wrap it in `deftransaction` so it commits atomically with its
checkpoint.

The macro sees only the literal `do` block. A banned construct reached through a same-module
helper compiles cleanly, and `mix credo` reports it — the check walks the local call graph out of
every workflow, step and transaction body, using the same banned-construct tables.

## What does `Dbos.UnexpectedStepError` mean?

**Symptom:** raised at runtime, naming a `workflow_id`, `function_id`, an `expected` step name,
and a `recorded` one.

**Cause:** the step the current code is about to run at this position does not match the step
name recorded in `operation_outputs` for that same `(workflow_uuid, function_id)` — a replay
whose step sequence has drifted from what actually happened during the original run. Usually
this means the workflow's code changed (a step reordered, renamed, or added/removed) while an
instance of it was still in flight.

**Fix:** the checkpoint and the code disagree about what happened at that step, and the mismatch
cannot be repaired in place. Restore the code that produced the original layout long enough for
the workflow to finish, or fork the workflow from before the point of divergence with
`Dbos.fork/3` if you know exactly where that is.

Going forward, guard new code inside the body with `Dbos.patch/1`: executions that already ran
past the call site keep their recorded step-id sequence and skip the guarded code, while new
executions take it. Once the old executions have drained, `Dbos.deprecate_patch/1` retires the
guard. Larger changes want a version bump or a new workflow name.

## My workflow is stuck `PENDING` and never progresses

**Symptom:** `Dbos.status/2` (or a direct query) shows `status: :pending` indefinitely; no
process is running it.

**Cause**, in order of likelihood:

1. **Nobody has this workflow's name registered.** `Dbos.Recovery`/reclaim log a warning
   ("workflow ... is not registered on this executor; skipping recovery") and move on without
   raising — check your logs for it. Pass `otp_app:` to `Dbos.Supervisor` so every module in the
   application defining a `defworkflow` is discovered, and use `workflows:` for modules that live
   in a dependency.
2. **`application_version` mismatch.** A non-queued `PENDING` row is only reclaimed by an
   executor whose own `config.application_version` matches the row's
   (`reclaim_pending_workflows/4` filters on it when set). If every live executor is running a
   different version than the one that started this workflow, nothing will ever pick it up.
   Every reclaim pass that finishes its batch says so, one warning per name and version:

   ```
   [warning] dbos: leaving 12 PENDING workflow(s) named "process_order/1"
   (version "v1", this executor "v2") unclaimed: version_mismatch; for example 018f...
   ```

   The same groups arrive as `[:dbos, :recovery, :declined]`, measuring `%{count: n}` with
   `%{engine:, name:, row_version:, executor_version:, reason:}`. `:reason` is one of
   `:name_not_registered` (cause 1 above), `:version_mismatch` (this cause), or
   `:locked_elsewhere` (a peer held the row — transient). The count is a gauge: a population
   nothing can claim re-reports on every lease sweep until an operator moves it.
3. **It's actually running, just slow or blocked.** A step body that's genuinely long-running, or
   a `defworkflow` blocked in `Dbos.sleep/1`/`Dbos.recv_message/2` with a long remaining wait,
   looks identical to "stuck" from the status alone — check `operation_outputs` for that
   workflow to see how far it's actually gotten.

**Fix:** register the missing module and restart, or align `application_version`, or call
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
`Dbos.DB.Ecto`'s repo config is reachable at boot. Waits still work on a 1-second polling
cadence, so startup continues either way. Check whether the fallback is intentional or an
unnoticed degradation.

## The engine refuses to start: migration version mismatch

**Symptom:** a raised error at boot naming a `dbos_migrations.version` that isn't the expected
one (`42`), or saying the table doesn't exist at all.

**Cause:** `Dbos.Migrator.verify!/1`, run by `migrations: :verify` (the default), refuses to start
against a schema at the wrong version. Tables whose shape doesn't match what the engine expects
would silently checkpoint into the wrong columns.

**Fix:** run `mix dbos.gen.migration` and apply the resulting migration through `mix ecto.migrate`,
as an explicit step in your own migration sequence.

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
query the workflow row first to see who holds it), or pick a deduplication
id that's actually unique to this attempt. Note `deduplication_id` and `partition_key` are
mutually exclusive on the same enqueue call.

## My enqueued workflow never runs in a test

**Symptom:** `Dbos.enqueue/3` returns a handle, the row is in `workflow_status`, and the test
times out waiting for a result.

**Cause:** under `testing: :manual` the row is inserted and left alone; the queue runners that
would claim it are among the processes these modes deliberately do not start, which is what keeps
everything on the caller's own connection and therefore inside `Ecto.Adapters.SQL.Sandbox`.

**Fix:** call `Dbos.Testing.drain_queue/2` or `Dbos.Testing.drain_all/1` at the point in the test
where the work should happen. Use `testing: :inline` for the enqueue to run synchronously inside
the call itself.

## My `Task.async` call will not compile inside a workflow or a step

**Symptom:** a `CompileError` naming `Task.async`, `Task.await`, `Task.async_stream`,
`Task.start`, `Task.start_link`, `spawn`, `spawn_link`, or `spawn_monitor`, in a `defworkflow`,
`defstep`, or `deftransaction` body.

**Cause:** a spawned process starts with none of the workflow context the process dictionary
carries (`Dbos.Runtime.current_workflow_id/0` has nothing to return inside it), so a durable
operation called from within it takes the passthrough path and skips its checkpoint entirely.
The determinism checker rejects the construct at compile time in both workflow bodies (`check!/2`)
and step and transaction bodies (`check_step!/2`).

**Fix:** run the work inline in the step, or use a child workflow (`Dbos.start/3` from inside a
workflow) for concurrency that itself needs to be durable. A process spawned through a
same-module helper compiles, and `mix credo` reports it.
