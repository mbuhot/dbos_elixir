# The determinism contract

## The guarantee: at-least-once side effects, exactly-once checkpoints

A checkpoint — a step's recorded output, a workflow's recorded outcome — is written exactly once,
and replay always returns the recorded value. A **side effect** is at-least-once: a crash between
a step performing its real-world effect (charging a card, sending an email, calling a service) and
that step's checkpoint committing means replay re-runs the step and performs the effect again.
Executor leases and `owner_xid` narrow that window; nothing closes it to zero.

A step whose side effect must not repeat needs its own idempotency key at that boundary — a
payment gateway's idempotency-key parameter, an email provider's message-id, a deduplication key on
the receiving system.

## The core idea

A workflow body re-executes from the top after a crash or restart. Steps that already completed
return the value already recorded for them. That is only correct if the body takes the same path
and calls the same steps in the same order every time.

## Why this is stricter than it looks

Step **arguments are never stored**. Two things are persisted: the workflow's own input
(`workflow_status.inputs`), once, and each step's **output**, keyed by workflow id and step
position.

A step's arguments on replay are whatever the surrounding code computes them to be. The engine
matches on position and step name only, finds a recorded output, and returns it.

```elixir
defworkflow charge_customer(order_id) do
  order = step(:load_order, order_id)
  charge = step(:charge_card, order.customer_id, order.total)
  step(:send_receipt, order.customer_id, charge.id)
end
```

Suppose `load_order` is a plain function reading `order.total` from a mutable price table:

1. First run: `order.total` is `100`. `charge_card` charges `100` and records
   `%{charge_id: "ch_1", amount: 100}` at step position 1.
2. Crash after `charge_card`, before `send_receipt`.
3. Replay: prices changed, `order.total` is now `150`. `charge_card` is called with `150`, position
   1 already has a recorded output, and the engine returns the old `%{charge_id: "ch_1", amount:
   100}` without charging anything.
4. `send_receipt` runs with `amount = 100`. The workflow completes "successfully" with a stale
   value, no error and no log line.

Every input to a step must come from a prior step's recorded output or the workflow's own input.

## Banned in a workflow body

| Construct | Why it breaks replay | Use instead |
|---|---|---|
| `:rand.*` | Different random value every run | Generate randomness inside a step, so the value becomes the step's recorded output |
| `DateTime.utc_now`, `NaiveDateTime.utc_now`, `Date.utc_today` | Wall-clock value differs on replay | A step that returns the current time |
| `System.system_time`, `os_time`, `monotonic_time`, `unique_integer` | A fresh value every call | A step, or the workflow/step ids the engine already gives you |
| `Process.sleep` | Blocks the process; the wait is neither durable nor resumable | `Dbos.sleep/1` |
| `receive` | Waits on messages the engine cannot record or replay | `Dbos.send_message/4`, `Dbos.recv_message/2` |
| `spawn`, `spawn_link`, `spawn_monitor` | New process has no workflow context; loses checkpointing | Steps, or child workflows |
| `Task.async`, `Task.await`, `Task.async_stream`, `Task.start` | Silent context loss — see below | A step, or a child workflow for concurrency |
| `send/2` | Ordinary messages are not durable and vanish on crash | `Dbos.send_message/4` |
| `make_ref` | Fresh, non-reproducible value | The step/workflow ids the engine already gives you |
| A direct call to the repo passed to `use Dbos` | A side effect with no checkpoint; re-runs for real on every replay | `deftransaction`, so it commits atomically with its checkpoint |
| `self()`-dependent logic | Process identity changes across replay | Keep process identity out of workflow logic |
| `node()`-dependent logic | Which node picks up recovery is not deterministic | Push node-specific work into a step |
| Reading mutable module or application state (ETS, changeable `Application.get_env` values, global counters, in-memory caches) | Value can differ between the original run and replay | A step that reads and returns the value |

## Task is the quiet one

A `Task` does not inherit the calling process's process dictionary, which is where the engine
tracks "am I inside a workflow, inside a step". A step called from inside a `Task.async` runs with
none of that context, so it takes the **passthrough path**: the function body runs directly, no
checkpoint is written, no error is raised, and the call looks like it worked. On replay the same
code runs again in full with none of its side effects protected.

This is the easiest bug to introduce and the hardest to notice. For concurrency, use child
workflows.

## A step body is not a workflow body

A step is where nondeterminism belongs. `DateTime.utc_now`, `:rand.uniform`, a database read, an
HTTP call — all of it is ordinary code inside a step, and none of it is checked there.
`Process.sleep` is the right tool for a blocking wait in a step; a bare `receive` runs in the
workflow's own process and loses no context; `send/2`, `make_ref`, and direct repo calls stay put.

Only two families are banned in a `defstep`/`deftransaction` body, both because they hand execution
to a process that starts with an empty process dictionary:

| Construct in a step | Use instead |
|---|---|
| `spawn`, `spawn_link`, `spawn_monitor` | Run the work inline in this step, or split it into its own step |
| `Task.async`, `Task.await`, `Task.async_stream`, `Task.start`, `Task.start_link` | Run the work inline, or use a step or child workflow for real concurrency |

## Where the contract is enforced

Three layers, sharing one banned-construct table, so they always agree.

| Layer | Sees | On a violation |
|---|---|---|
| The `defworkflow`/`defstep`/`deftransaction` macros | The literal `do` block handed to the macro | `CompileError` naming the call, its file and line, and the fix |
| `Mix.Tasks.Compile.Dbos` | Every function in the application, reached forward from every workflow, step and transaction body | A compiler warning carrying the whole chain from the entry point to the banned call |
| Code review | `self()`, `node()`, ETS reads, mutable `Application.get_env` values | Guidance |

### The whole-application compiler

Add it ahead of the standard compilers, in `:dev` and `:test`:

```elixir
def project do
  [
    compilers: [:dbos] ++ Mix.compilers(),
    ...
  ]
end
```

A compilation tracer records a call graph of resolved `{module, function, arity}` nodes while
`compile.elixir` runs, and the compiler then walks it forward from each entry point. A finding
reads:

```
warning: nondeterministic call reachable from a workflow body

  MyApp.Orders.place/3  (workflow "place")
    → MyApp.Pricing.quote/1  lib/my_app/orders.ex:23
    → MyApp.Pricing.jitter/1  lib/my_app/pricing.ex:4
    → :rand.uniform/1  lib/my_app/pricing.ex:6

  This is nondeterministic and breaks replay on recovery. Generate the random value inside a
  step so it is checkpointed and replayed as a fixed value.

  lib/my_app/pricing.ex:6
```

The diagnostic sits at the banned call, so an editor sends you to the fix; the chain shows why
the checker believes a workflow reaches it. `mix compile --warnings-as-errors` makes findings fail
the build, which is the recommended CI setting.

Descent stops at every step and transaction body — a step is where nondeterminism belongs — at
`Dbos` engine internals, and at every dependency. A dependency is an opaque leaf: its calls are
matched against the rule table, its internals are never traced.

### Silencing a finding

One function, in a module that has `use Dbos`:

```elixir
@dbos_deterministic "reads a compile-time-frozen config map"
def lookup_region(code), do: :persistent_term.get({:regions, code})
```

A module or MFA the project does not own, in `mix.exs`:

```elixir
dbos: [trusted: [MyApp.PureHelpers, {Some.Dep, :fetch_config, 1}]]
```

Either one makes the function a leaf: it is not followed, and its own calls are not reported. An
annotation or list entry that silences nothing is itself reported, so the list does not rot.

### What the compiler cannot see

The analysis is an over-approximation of what runs and an under-approximation of what is
reachable. Treat it as a large, cheap improvement in recall, and not as a guarantee.

| Construct | Behaviour |
|---|---|
| `apply/3` with a computed module | Reported as a blind spot, never followed |
| A call through a variable module (`mod.fetch(id)`), protocol dispatch, a behaviour callback | Invisible |
| A function value received as an argument and called | Invisible at the callee; the capture at the call site is followed |
| A branch that never runs | Reported anyway |
| Workflows defined in a compiled dependency | Not checked — their call graph belongs to another compile run |
| A macro that expands to a banned call | Reported at the macro's line |

Warnings, never errors: an inference that turns out wrong must not break a build.

## `mix dbos.explain`

`mix dbos.explain Mod.function/arity` prints a `defworkflow`'s statically-derivable step-id
sequence — which durable operation each step id maps to — and flags a `case`/`cond`/`if` whose
branches consume a different number of ids. Where the sequence depends on something it cannot
resolve, it reports that position as indeterminate and makes no guess.

## Step IDs are a contract

Every durable operation consumes the next number from a per-workflow counter, starting at 0. That
sequence is how the engine matches "the 3rd durable thing this workflow does" on replay to "the 3rd
durable thing this workflow did" originally.

The counter is never persisted. It is rebuilt from scratch, starting at 0, every time the workflow
body runs.

The classic bug is a branch that allocates a different number of ids than another branch. If the
original run took the `true` branch and a replay takes the `false` branch, every operation after it
shifts by one position and is matched against the wrong recorded row. Mismatched step names raise a
hard error; two steps that happen to share a name return the wrong cached output silently.

| Operation | IDs consumed | Notes |
|---|---|---|
| Step call | 1 | Allocated before checking whether it already ran |
| Sleep | 1 | |
| Send | 1 | Sending from outside a workflow consumes 0 |
| Receive (`recv`) | 2 | The receive plus an internal timeout-tracking sleep, both reserved up front |
| `setEvent` | 1 | |
| `getEvent` | 2 | Same shape as receive; from outside a workflow, 0 |
| Stream write / close | 1 each | |
| `getResult` (blocking wait on a handle) | 1 | Allocated **after** the wait completes — the only operation with this shape. A timed-out wait consumes none |
| Enqueue | 1 | Enqueueing from outside a workflow consumes 0 |
| Starting or enqueueing a child workflow | 1 (parent side) | Consumed very early, before any error path — the child's workflow id is derived from it, so it must be stable |
| Fork | 1 | |
| Patch check | 0 or 1 | 1 only when the patch applies on this run |
| Patch retirement | 0 or 1 | 1 only when this run recorded the patch marker |

Receive and `getEvent` reserve both ids up front so that the "message was already there" path and
the "had to wait" path consume the same total, keeping every later operation at a stable position
regardless of timing.

## Evolving a workflow safely

A workflow with in-flight instances cannot be edited in place: a running instance replays the old
control flow against a database of old recorded steps. Three ways to change behaviour:

| Option | When to use | Effect on in-flight workflows |
|---|---|---|
| Bump the application version | The change only matters for **new** workflow starts | In-flight workflows recover under the version they started with; new workflows use the new code |
| Use a patch (`Dbos.patch/1`) | You must change behaviour for workflows **currently in flight** without shifting step ids for work already recorded | The patch check consumes 0 ids on the old code path and 1 once the patch takes effect for that workflow, which is what lets old and new code coexist |
| Write a new workflow name | Old and new versions have fundamentally different step sequences | Old in-flight workflows keep running under the old name; new work routes to the new name |

If you are tempted to add an `if` to a workflow body that changes which steps run, reach for a
patch. A bare conditional whose truth value can differ between the original execution and a replay
is the "branch allocates different ids" bug.

## Serialized values are a schema

Step and workflow outputs are stored as encoded Erlang terms. The struct or data shape a step
returns **on its first run** must still decode correctly whenever that workflow is replayed, no
matter how much later. Treat every struct a step can return as a versioned, append-only schema.

| Change | Safe? |
|---|---|
| Adding a field with a default | Yes — old recorded values decode fine |
| Renaming or removing a field | Only once no workflow that recorded the old shape can still be replayed |
| Changing a field's type | Same rule |
| An unavoidable incompatible change | Give the step a new name, so old recorded rows keep being read under the old struct and new runs write the new one |
