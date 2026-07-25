# TODO

Known gaps, ordered by how much they matter. Each entry says what is wrong, why it matters, and
what fixing it involves. Items resolved should be deleted, not ticked.

## Correctness

### The checker cannot see helper functions
The macro only ever sees the literal `do` block. A violation inside a private helper the body
calls is invisible:

```elixir
defworkflow process(id), name: "process" do
  fan_out(id)
end

defp fan_out(id), do: Task.async_stream(...)   # invisible
```

Two sample-app authors hit this and inlined loop logic to keep it under the checker's eye, which
is a bad thing to force on users. A Credo check can do the reachability analysis a macro cannot.
Keep the compile-time bans for the un-skippable cases; add Credo for what needs a call graph.

### External side effects are at-least-once
A step that performs an effect and crashes before its checkpoint commits runs that effect again.
Leases narrow the window; nothing closes it. Documented in `docs/determinism.md` and the README.
Listed here so it is not mistaken for an oversight — a step that must never repeat needs an
idempotency key of its own.

## Testing modes

### `workflow_timeout_ms` is not sandbox-safe
`Dbos.Runtime.arm_deadline/2` spawns an unsupervised `Task` that later calls `Dbos.cancel/2`. It
escapes the calling process, so it races the sandbox's connection ownership. Timeout-driven
behaviour cannot be tested under `testing: :inline`.

### Streams are not handled in testing modes
`write_stream`/`read_stream` still route through `Dbos.Notifications`, which is not started in
testing modes. A test touching a stream fails confusingly. They need the same short-circuit
`sleep`, `recv_message` and `get_event` already have.

## Missing surface

### `DeprecatePatch`
`Dbos.patch/1` exists; its counterpart, which retires a patch marker once every in-flight workflow
predating it has drained, does not. Same conditional step-id shape as `patch`.

### `Dbos.debounce`
`Dbos.Debouncer.debounce/4` takes a raw `Dbos.Config` and a string workflow name, so it sits
outside the ergonomics of `Dbos.enqueue/3`. Wants a wrapper taking a capture and options.

### `list_workflows` filters
Roughly fourteen filters are unimplemented: workflow ids, id prefix, authenticated user, forked
from, parent workflow id, deduplication id, completed before/after, dequeued before/after, has
parent, attributes containment, schedule name, is-debounced, and the load-input/output toggles.
Each is a new column predicate. The admin API and any console are limited by their absence.

### Patch guards
The reference gates patching behind config and rejects a patch called from inside a step. Neither
is ported.

## Operational

### Fencing tokens
A reclaim does not invalidate the previous owner's writes. The lease makes a false verdict
self-limiting for durable state, since an executor that cannot renew also cannot checkpoint, but a
fencing token would reject a stale execution's checkpoint outright at its next attempt. Worth it
only if lease expiry proves too coarse in practice.

### Docker integration suite is manual
`test/integration` covers hard node kill, recovery ownership, concurrent start and queue
competition across two real BEAM nodes. It runs only when invoked by hand. It should run in CI on
a schedule, since it is the only coverage of genuine multi-node behaviour.

### Admin server has no authentication
Documented in the production checklist. Deliberate, and it means the port must never be exposed.

## Housekeeping

### Suite time
About 44 seconds for 452 tests. Several tests use fixed sleeps where polling would do; the
scheduler and fork tests dominate. Some sleeps are load-bearing — durable sleep resuming with only
the remainder, rate limiting over a window — and must stay.

### `docs_config.js`
ex_doc always emits a `<script src="docs_config.js">` tag that only HexDocs supplies. An empty
file is shipped through the `assets` option to silence the 404. Revisit if ex_doc stops emitting
it.

### Sample app dependencies
Each of the ten sample apps vendors its own `deps/` and `_build/`. Both are gitignored, so this
costs disk rather than repository size.
