# TODO

The work queue. Each entry is something we intend to do. Resolved items are deleted, not ticked.

## Nested steps corrupt the function_id sequence

A `defstep` called from inside another step's body consumes a `function_id`.
`Runtime.next_function_id/0` is a bare counter, and there is no `StepInStepError` to stop it — only
`StepInTransactionError` exists.

```
original run:  outer=1, inner=2, next=3
replay:        outer=1 replays from its checkpoint, its body never runs,
               inner never consumes id 2
               next=2  ← reads another step's recorded value
```

Every step after the nesting point shifts by one and replays the wrong checkpoint. Silent, and
reachable today by factoring a `defstep` call into another `defstep`. No test covers nested steps.

Raise `Dbos.StepInStepError` at runtime, mirroring `StepInTransactionError`, and add step-body →
step-body as a reachability check in the determinism compiler (the edge is already in its graph).

## Saga compensation

`compensate:` on a step, recorded onto the step's checkpoint row, unwound in reverse by a separate
durable workflow. Design and rationale in `notes/saga-compensation-design.md`.

Phases:

1. Extension migration 3 (`operation_outputs.ex_compensation`), the `compensate:` option on
   `defstep`/`deftransaction`, the `@before_compile` contract check, and the record written at
   checkpoint time.
2. The compensation workflow: reverse walk, `DBOS.` filtering, undo-per-step, fail-fast, and
   `[:dbos, :compensation, :stuck]`.
3. Triggers: automatic enqueue in the terminal transaction for the exception path, and
   `Dbos.abort/1`.
4. `CANCELLING` status, the process-side transition, and lease-sweep pickup.
5. Recursion into `getResult`/`enqueue`/`forkWorkflow` descendants, awaited.
6. `compensate:` on `send_message`/`set_event`/`write_stream`.
7. `widget_store` reworked as the saga demo, covering both the failure and the
   nothing-to-unwind branches.

## Per-workflow application version

`SystemDb.reclaim_pending_workflows/4` filters `AND application_version = $n`, so a version
mismatch reclaims nothing — the workflow is orphaned permanently. `DBOS__APPVERSION` is commonly a
git SHA, and even the computed default digests every workflow module together, so changing one
workflow strands in-flight instances of all of them.

`notes/per-workflow-version-design.md` has the design.

**Phase 0 is done**: a drained reclaim pass reports every row it left behind — one
`Logger.warning` and one `[:dbos, :recovery, :declined]` event per `{name, version, reason}`
group, with `reason` in `:name_not_registered | :version_mismatch | :locked_elsewhere`.

**Phase 1 is done**: extension migration 2 adds `executor_leases.ex_capabilities`, a nullable
JSONB array of the `{name, version}` pairs each executor accepts, republished on every lease
renewal. `Dbos.Recovery.orphans/1` joins `PENDING` rows against it to answer the fleet-wide
question one executor's declined report cannot, grouped by `{name, application_version}` with a
reason of `:no_live_executors | :name_not_registered | :version_mismatch`. Served by
`GET /dbos-orphans`, `mix dbos.orphans`, and `[:dbos, :recovery, :orphaned]`. A live lease that
has not published capabilities suppresses the report rather than producing false orphans.

**What remains** is a declared version, nothing computed:

1. `version:` on `defworkflow`, carried in the registry.
2. An extension migration adding `workflow_status.ex_workflow_version` and the reclaim index.
3. Stamping it at insert, claim and fork.
4. The `unnest` reclaim predicate matching `(name, version)` pairs, with `NULL` matching `NULL`
   so an undeclared workflow is claimable by any executor registering its name.
5. `ex_capabilities` publishing the declared version rather than `application_version`.

