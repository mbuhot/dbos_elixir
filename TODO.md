# TODO

Known gaps, ordered by how much they matter. Each entry says what is wrong, why it matters, and
what fixing it involves. Items resolved should be deleted, not ticked.

## Correctness

### The checker sees one module at a time
`Credo.Check.Warning.DbosDeterminism` follows local calls out of a workflow body, so a violation
in a same-module helper is caught. A helper in *another* module, a call through `apply/3`, and a
call through an unresolvable function value all remain invisible. Repo-call detection is also
absent from the Credo path, since it needs a `Macro.Env`.

### External side effects are at-least-once
A step that performs an effect and crashes before its checkpoint commits runs that effect again.
Leases narrow the window; nothing closes it. Documented in `docs/determinism.md` and the README.
Listed here so it is not mistaken for an oversight — a step that must never repeat needs an
idempotency key of its own.

## Operational

### Fencing tokens
A reclaim does not invalidate the previous owner's writes. The lease makes a false verdict
self-limiting for durable state, since an executor that cannot renew also cannot checkpoint, but a
fencing token would reject a stale execution's checkpoint outright at its next attempt. Worth it
only if lease expiry proves too coarse in practice.

### Admin server has no authentication
Documented in the production checklist. Deliberate, and it means the port must never be exposed.

## Housekeeping

### Suite time
About 53 seconds for 529 tests, nearly all of it sequential. Several tests use fixed sleeps where
polling would do; the scheduler and fork tests dominate. Some sleeps are load-bearing — durable
sleep resuming with only the remainder, rate limiting over a window, a parked wait that must
outlast its own setup — and must stay.

### `docs_config.js`
ex_doc always emits a `<script src="docs_config.js">` tag that only HexDocs supplies. An empty
file is shipped through the `assets` option to silence the 404. Revisit if ex_doc stops emitting
it.

### Sample app dependencies
Each of the ten sample apps vendors its own `deps/` and `_build/`. Both are gitignored, so this
costs disk rather than repository size.
