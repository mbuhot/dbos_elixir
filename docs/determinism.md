# The determinism contract

## The core idea

A workflow body re-executes from the top after a crash or restart. Steps that already
completed do not run again — they return the value already recorded for them. This only
produces a correct result if the body takes the same path and calls the same steps in the
same order every time.

## Why this is stricter than it looks

Step **arguments are never stored**. Only two things are persisted:

- `workflow_status.inputs` — the workflow's own input, once.
- Each step's **output** — keyed by workflow ID and a step position.

A step's arguments on replay are whatever the surrounding code computes them to be. If
that computation drifts, the step is called with different arguments, but the engine has
no record of the original arguments to compare against. It matches on position and step
name only, finds a recorded output, and returns it — silently.

### Worked example

```elixir
defworkflow charge_customer(order_id) do
  order = step(:load_order, order_id)
  charge = step(:charge_card, order.customer_id, order.total)
  step(:send_receipt, order.customer_id, charge.id)
end
```

Say `load_order` is a plain function (not a step) that reads `order.total` from a
mutable price table, and prices change between the first attempt and a crash-recovery
replay:

1. First run: `order.total` is `100`. `charge_card` runs for real, charges `100`,
   records output `%{charge_id: "ch_1", amount: 100}` at step position 1.
2. Crash after `charge_card`, before `send_receipt`.
3. Replay: prices changed, `order.total` is now `150`. `charge_card` is called with
   `150`, but position 1 already has a recorded output. The engine returns the **old**
   `%{charge_id: "ch_1", amount: 100}` without charging anything.
4. `send_receipt` runs with `charge.id = "ch_1"`, `amount = 100` — the customer's receipt
   says `100` even though the intended charge was `150`.

No error. No log line. The workflow completes "successfully" with a silently stale
value. This is why every input to a step must itself come from a prior step's recorded
output or the workflow's own input — never from a live read of mutable state.

## Banned in a workflow body

| Construct | Why it breaks replay | Use instead |
|---|---|---|
| `:rand.*` | Different random value every run | Generate randomness inside a step; the value becomes the step's recorded output |
| `DateTime.utc_now`, `NaiveDateTime.utc_now`, `Date.utc_today` | Wall-clock value differs on replay | A step that returns the current time |
| `System.system_time`, `os_time`, `monotonic_time`, `unique_integer` | Same problem — a fresh value every call | A step, or the workflow/step IDs the engine already gives you |
| `Process.sleep` | Blocks the process; the wait is neither durable nor resumable | The engine's durable sleep operation |
| `receive` | Waits on messages the engine cannot record or replay | The engine's durable send/recv operations |
| `spawn`, `spawn_link` | New process has no workflow context; loses checkpointing | Steps, or child workflows |
| `Task.async`, `Task.await`, `Task.async_stream` | See callout below — silent context loss | A step, or a child workflow if you need concurrency |
| `send/2` | Ordinary messages are not durable and vanish on crash | The engine's durable send |
| `make_ref` | Fresh, non-reproducible value | Not needed — use step/workflow IDs |
| `self()`-dependent logic | Process identity changes across replay (new process each run) | Nothing process-identity-shaped belongs in workflow logic |
| `node()`-dependent logic | Which node picks up recovery is not deterministic | Push node-specific work into a step |
| Direct repo calls outside a transactional step | Side effect with no checkpoint; re-runs for real on every replay | A transactional step |
| Reading mutable module/application state (ETS, `Application.get_env` for values that can change, global counters, in-memory caches) | Value can differ between original run and replay | A step that reads and returns the value |

## Task is the quiet one

A `Task` does not inherit the calling process's process dictionary. This engine tracks
"am I inside a workflow, inside a step" using the process dictionary. A step called from
inside a `Task.async` runs in a process that has none of that context.

Result: the step takes the **passthrough path** — it just runs the function body
directly, as if it were an ordinary function call outside any workflow. No checkpoint is
written. No error is raised. The call looks like it worked. On replay, that same code
runs again in full, with none of its side effects protected by idempotency.

This is the easiest bug to introduce and the hardest to notice, because everything
appears to succeed on the first run. Do not call steps from inside a `Task`. If you need
concurrency, use child workflows.

## What's allowed and encouraged

- Pure computation.
- Pattern matching and branching on the **results of steps** (values already recorded).
- Calling other pure functions.
- Pushing every source of nondeterminism — time, randomness, I/O, external state — into a
  step, so it gets recorded once and replayed as a fixed value forever after.

A workflow body should read like a deterministic script over already-known values. All
the "what does the world look like right now" logic belongs inside steps.

## Step IDs are a contract

Every durable operation — a step call, a sleep, a send, a receive, a `setEvent`, a
`getEvent`, a stream write, a `getResult`, an enqueue, a child workflow call, a fork, a
patch check — consumes the next number from a per-workflow counter, starting at 0. That
sequence of numbers is how the engine matches "the 3rd durable thing this workflow does"
on replay to "the 3rd durable thing this workflow did" originally.

The counter is never itself persisted. It is rebuilt from scratch, starting at 0, every
time the workflow body runs — first attempt, replay, or recovery. Reproducing the exact
same sequence of operations is what keeps replay correct.

The classic bug: a branch that allocates a different number of IDs than another branch.
If the original run took the `true` branch (consuming an extra step ID) and a replay
somehow takes the `false` branch, every operation after the branch shifts by one
position and gets matched against the wrong recorded row. If the step names happen to
differ, the engine raises a hard error. If two steps at those mismatched positions
happen to share the same name, the engine returns the **wrong step's cached output** with
no error at all.

### IDs consumed per operation

| Operation | IDs consumed | Notes |
|---|---|---|
| Step call | 1 | Allocated before checking whether it already ran |
| Sleep | 1 | |
| Send (from inside a workflow) | 1 | Sending from outside a workflow consumes 0 |
| Receive (`recv`) | 2 | One for the receive itself, one for an internal timeout-tracking sleep — both reserved up front, even if no wait ends up happening |
| `setEvent` | 1 | |
| `getEvent` (from inside a workflow) | 2 | Same shape as receive: event + internal timeout sleep, both reserved up front |
| Stream write / close | 1 each | |
| `getResult` (blocking wait on a handle, from inside a workflow) | 1 | Allocated **after** the wait completes, not before — the only operation with this shape |
| Enqueue (from inside a workflow) | 1 | Enqueueing from outside a workflow consumes 0 |
| Starting/enqueueing a child workflow | 1 (parent side) | Consumed very early, before any error path — the child's own workflow ID is derived from this number, so it must be stable |
| Fork | 1 | |
| Patch check | 0 or 1 | Only consumes an ID if the patch actually applies on this run (see below) |

The receive/getEvent "reserve 2 up front, even if the second is never used" rule exists
specifically so that both the "message was already there" path and the "had to wait"
path consume the same total, keeping every operation after them at a stable position
regardless of timing.

## Evolving a workflow safely

Once a workflow has in-flight instances recorded, its code cannot simply be edited in
place — a running instance replays the *old* control flow against a database of *old*
recorded steps. Three ways to change behavior:

| Option | When to use | Effect on in-flight workflows |
|---|---|---|
| Bump the application version | The workflow's code changed in a way that only matters for **new** workflow starts (e.g. deploying alongside unrelated changes) | In-flight workflows recover under the version they started with; new workflows use the new code |
| Use a patch | You need to change behavior for workflows that are **currently in flight**, in a way that must not shift step IDs for work already recorded | The patch check consumes 0 IDs on the "not yet patched, old code path" branch and 1 ID once the patch actually takes effect for that workflow — this asymmetry is exactly what lets old and new code coexist without breaking ID alignment |
| Write a new workflow name | The change is big enough that the old and new versions have fundamentally different step sequences | Old in-flight workflows keep running under the old name/code indefinitely; new work is routed to the new name |

Rule of thumb: if you are tempted to add an `if` branch to a workflow body that changes
which steps run, reach for a patch, not a bare conditional. A bare conditional whose
truth value can differ between original execution and replay is the "branch allocates
different IDs" bug from the previous section.

## Serialized values are a schema

Step and workflow outputs are stored as encoded Erlang terms. The struct or data shape a
step returns **on its first run** is the shape that must still decode correctly whenever
that workflow is replayed, no matter how much later.

Changing a struct's fields — renaming a key, removing a field, changing a type — between
the run that recorded a value and a later replay that reads it back is a breaking change
of the same kind as an incompatible database migration on a live table.

Rule: treat every struct that a step can return as a versioned, append-only schema.

Safe pattern:

- Adding a new field with a default is safe — old recorded values decode fine.
- Renaming or removing a field is not safe while any workflow that recorded the old
  shape might still be replayed. Add a new field, and drop the old
  one only after you are certain no in-flight workflow can still reference it.
- Changing a field's type (e.g. integer to string) is not safe for the same reason.
- If a shape must change incompatibly, treat it like a workflow-behavior change: give the
  step a new name (a new "column" in `operation_outputs`) so old recorded rows keep being
  read under the old struct and new runs write the new one.
