# TODO

The work queue. Each entry is something we intend to do. Resolved items are deleted, not ticked.

## Remove the clustering angle

`Dbos.Cluster` and `Dbos.Cluster.NodeWatcher` exist to shorten dead-executor detection by
scheduling a sweep when `:nodedown` fires. Lease TTL is 60s and the orphan sweep interval is 300s,
so the saving is real but it applies only where distributed Erlang is running.

Dropping `orphan_sweep.interval_ms` to 15-30s buys the same latency for every topology, including
one-pod-per-deployment where `:nodedown` never fires at all. Delete `Dbos.Cluster`,
`Dbos.Cluster.NodeWatcher`, the `cluster:` supervisor option and `cluster_group` config; keep
leases and the sweep; rename `Dbos.Cluster.OrphanSweep` once the namespace goes.

Touches `guides/production-checklist.md`, which currently tells the reader to weigh
`cluster.enabled`, and `docs/clustering.md`.

## Per-workflow application version

`SystemDb.reclaim_pending_workflows/4` filters `AND application_version = $n`, so a version
mismatch reclaims nothing and logs nothing — the workflow is orphaned permanently and silently.
`DBOS__APPVERSION` is commonly a git SHA, and even the computed default digests every workflow
module together, so changing one workflow strands in-flight instances of all of them.

Version each workflow by a digest of its own reachable code and match reclaim on that. "Reachable"
is the hard part: a digest over the entry module alone misses a helper change that breaks replay.
The call graph in the determinism checker below is the same index.

Needs a design pass before implementation — it changes a column and the reclaim contract.

## Whole-application determinism checker

`notes/determinism-tracer-design.md` has the design: a `Mix.Task.Compiler` that installs a
compilation tracer, builds a function-level call graph, and reports transitive violations as
warnings. Keeps the macro AST walk as the hard failure, deletes the Credo check.

Phase 0 is a gate: confirm `env.function` attributes calls inside a `defworkflow` body captured
with `Macro.escape/1` and re-injected at `@before_compile`, and that line metadata survives the
round trip. A no answer invalidates the design.

## Drop `priority_enabled` from the queue options

It gates nothing. Dequeue orders `priority ASC, created_at ASC` unconditionally; the flag is only
persisted and rendered by the admin server. Take it out of `Dbos.Queue.new/2` and the docs, keep
the column so the schema stays at migration version 42.

## Primitives the Phoenix integration wanted

Surfaced building `sample_apps/live_approvals`:

- `Dbos.Notifications.subscribe_event/3` and `subscribe_stream/3` are keyed on
  `(workflow_id, key)`, so bridging progress to `Phoenix.PubSub` needs one registration per
  in-flight workflow. An engine-wide subscription would make a single bridge process viable.
- Blocking waits emit no telemetry span. `[:dbos, :step, :stop]` covers step execution, so a
  telemetry-driven UI cannot observe a workflow parked on a human, which is the state a
  human-in-the-loop UI most needs.
- `Dbos.resume/2` is a no-op on `SUCCESS`/`ERROR`, so a workflow that errored while parked has no
  public route back. The only way through is a raw `UPDATE` to `PENDING`.

## `Dbos.Migration` disappears when Ecto arrives after Dbos

`lib/dbos/migration.ex` is wrapped in `if Code.ensure_loaded?(Ecto.Migration)`, evaluated when
`dbos` compiles. An application that adds `dbos` first and `ecto_sql` second has a compiled `dbos`
with no `Dbos.Migration` in it, and `mix ecto.migrate` fails with `UndefinedFunctionError` on a
migration that looks correct. `mix deps.compile dbos --force` fixes it.

Hit three times converting the sample apps. Every new user installing `dbos` into an app that
already has Ecto is fine; the trap is the other order, and the error names the wrong culprit.
Options: make the module unconditional and raise a clear message at call time, or detect the
condition in `mix dbos.gen.migration` and say what to run.

## Integration suite fails under local Docker

Four tests fail on an arm64 Mac: a stale-image `Dbos.Version` `MatchError`, then `:badrpc` and ETS
lookup failures between node1, node2 and the runner after a clean rebuild. Unknown whether this is
local-only. The nightly workflow added in `.github/workflows/integration.yml` will report on it.
