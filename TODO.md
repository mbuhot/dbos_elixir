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
group, with `reason` in `:name_not_registered | :version_mismatch | :locked_elsewhere`.

**Phases 1-5 remain**: the `ex_workflow_version` column and `ex_capabilities` on leases,
`Dbos.Recovery.orphans/1` and `mix dbos.orphans`, the workflow-skeleton digest, the per-workflow
reclaim predicate, and prefix verification with `Dbos.Recovery.adopt/3`.

## Whole-application determinism checker

`Mix.Tasks.Compile.Dbos` ships phases 1-3 and 5 of `notes/determinism-tracer-design.md`: the
tracer, the ETS state and manifest, entry points from the macros, forward BFS with witness
chains, `@dbos_deterministic` and the project `trusted:` list. The Credo check is gone.

Phase 4 remains: the `:on_module` abstract-code scan for `receive` and other special forms, which
the macro AST walk currently catches only when written literally in a body.

Two gaps are recorded in §8 of the design note: this repo cannot trace a compile run that also
recompiles the tracer's own modules, and a stale or missing manifest under-reports until the next
full recompile.
