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

- `Dbos.Notifications.subscribe_event/3` and `subscribe_stream/3` are keyed on
  `(workflow_id, key)`, so bridging progress to `Phoenix.PubSub` needs one registration per
  in-flight workflow. An engine-wide subscription would make a single bridge process viable.
- Blocking waits emit no telemetry span. `[:dbos, :step, :stop]` covers step execution, so a
  telemetry-driven UI cannot observe a workflow parked on a human, which is the state a
  human-in-the-loop UI most needs.
- `Dbos.resume/2` is a no-op on `SUCCESS`/`ERROR`, so a workflow that errored while parked has no
  public route back. The only way through is a raw `UPDATE` to `PENDING`.

## Integration suite fails under local Docker

Four tests fail on an arm64 Mac: a stale-image `Dbos.Version` `MatchError`, then `:badrpc` and ETS
lookup failures between node1, node2 and the runner after a clean rebuild. Unknown whether this is
local-only. The nightly workflow added in `.github/workflows/integration.yml` will report on it.

## Make the options dispatcher the primary call form

`defworkflow review(id)` generates `review/1` and `review/2`, the second taking a keyword list.
It routes through `Dbos.Macros.dispatch_workflow/3` to `Dbos.start/3`, which reads a fixed set of
keys, so `queue_name:` and `delay_ms:` are silently discarded and the workflow starts immediately.

Route through `Dbos.enqueue/3` when `opts` carries `:queue_name`, and `Dbos.start/3` otherwise, so
one call shape covers starting, queueing and delaying. Raise on unknown keys and on incompatible
combinations (`delay_ms:` without `queue_name:`, `partition_key:` with `deduplication_id:`).

Inside a workflow the call stays start-and-await, matching the bare call it varies. `Dbos.enqueue/3`
remains available by name for enqueue-and-continue; it already checkpoints itself, so it needs no
wrapping step.

Lead the guides with this form. `Dbos.start/3` and `Dbos.enqueue/3` become the escape hatch for
dispatching by name.
