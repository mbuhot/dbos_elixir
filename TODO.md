# TODO

The work queue. Each entry is something we intend to do. Resolved items are deleted, not ticked.

## Per-workflow application version

`SystemDb.reclaim_pending_workflows/4` filters `AND application_version = $n`, so a version
mismatch reclaims nothing — the workflow is orphaned permanently. `DBOS__APPVERSION` is commonly a
git SHA, and even the computed default digests every workflow module together, so changing one
workflow strands in-flight instances of all of them.

`notes/per-workflow-version-design.md` has the design and its phasing.

**Phase 0 is done**: a drained reclaim pass reports every row it left behind — one
`Logger.warning` and one `[:dbos, :recovery, :declined]` event per `{name, version, reason}`
group, with `reason` in `:name_not_registered | :version_mismatch | :locked_elsewhere`. Folding
that event into `Dbos.Telemetry` and `docs/telemetry.md` is outstanding.

**Phases 1-5 remain**: the `ex_workflow_version` column and `ex_capabilities` on leases,
`Dbos.Recovery.orphans/1` and `mix dbos.orphans`, the workflow-skeleton digest, the per-workflow
reclaim predicate, and prefix verification with `Dbos.Recovery.adopt/3`.

## Whole-application determinism checker

`notes/determinism-tracer-design.md` has the design: a `Mix.Task.Compiler` that installs a
compilation tracer, builds a function-level call graph, and reports transitive violations as
warnings. Keeps the macro AST walk as the hard failure, deletes the Credo check.

Phase 0 is a gate: confirm `env.function` attributes calls inside a `defworkflow` body captured
with `Macro.escape/1` and re-injected at `@before_compile`, and that line metadata survives the
round trip. A no answer invalidates the design.

## Primitives the Phoenix integration wanted

Surfaced building `sample_apps/live_approvals`:

- `Dbos.resume/2` is a no-op on `SUCCESS`/`ERROR`, so a workflow that errored while parked has no
  public route back. The only way through is a raw `UPDATE` to `PENDING`.
