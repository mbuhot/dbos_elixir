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

**Phase 1 is done**: extension migration 2 adds `executor_leases.ex_capabilities`, a nullable
JSONB array of the `{name, version}` pairs each executor accepts, republished on every lease
renewal. `Dbos.Recovery.orphans/1` joins `PENDING` rows against it to answer the fleet-wide
question one executor's declined report cannot, grouped by `{name, application_version}` with a
reason of `:no_live_executors | :name_not_registered | :version_mismatch`. Served by
`GET /dbos-orphans`, `mix dbos.orphans`, and `[:dbos, :recovery, :orphaned]`. A live lease that
has not published capabilities suppresses the report rather than producing false orphans.

**Phases 2-5 remain**: the `ex_workflow_version` column, the workflow-skeleton digest, the
per-workflow reclaim predicate, and prefix verification with `Dbos.Recovery.adopt/3`.

