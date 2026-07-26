# TODO

The work queue. Each entry is something we intend to do. Resolved items are deleted, not ticked.

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

## Primitives the Phoenix integration wanted

Surfaced building `sample_apps/live_approvals`:

- `Dbos.resume/2` is a no-op on `SUCCESS`/`ERROR`, so a workflow that errored while parked has no
  public route back. The only way through is a raw `UPDATE` to `PENDING`.

## Integration suite fails under local Docker

Four tests fail on an arm64 Mac: a stale-image `Dbos.Version` `MatchError`, then `:badrpc` and ETS
lookup failures between node1, node2 and the runner after a clean rebuild. Unknown whether this is
local-only. The nightly workflow added in `.github/workflows/integration.yml` will report on it.
