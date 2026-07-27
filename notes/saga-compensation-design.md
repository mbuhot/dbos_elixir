# Saga Compensation — Design

Crash-safe compensation for `dbos_elixir`, driven off durable step history.

## The problem

A durable workflow that crashes is replayed forward. A workflow whose *business transaction*
fails needs the opposite: the effects of steps that already succeeded must be reversed. Upstream
DBOS offers the informal "wrap the body in try/catch and undo in reverse" pattern, which loses the
unwind if the process dies partway through it.

## Principles

1. **The undo list is durable step history.** Exactly the steps that checkpointed a completed
   result. A branch that never ran is not in history and needs no undo.

2. **The undo input is the step's checkpointed return value**, plus whatever arguments the step
   froze into its compensation record. Step inputs are persisted per-step, opt-in, by naming them
   in `compensate:`.

3. **The unwind is its own durable workflow**, one per workflow unwound. It checkpoints each undo,
   recovers like anything else, and has independent status and retries. This ends the "what
   compensates the compensator" recursion.

4. **Rollback is for the error path.** A `PENDING` workflow left by a node crash is *recovered*.
   Compensation is triggered by a terminal business failure.

5. **A committed transaction cannot be un-committed by the library.** The compensating action is a
   new statement that semantically negates the old one. Only the domain knows what that means.

## The DSL

`compensate:` takes a capture of a `defstep` in the same module. `&1` is the failed step's
checkpointed return value; everything else is frozen by value at checkpoint time.

```elixir
defstep reserve_stock(product_id, quantity),
  compensate: &release_stock(product_id, quantity, &1) do
  Inventory.reserve(product_id, quantity)
end

defstep release_stock(product_id, quantity, _reservation_id) do
  Inventory.release(product_id, quantity)
end
```

Accepted on `defstep`, `deftransaction`, and on the durable primitives with external effect:
`Dbos.send_message/4`, `Dbos.set_event/3`, `Dbos.write_stream/3`.

### Compile-time contract

Checked in `@before_compile`, because `@dbos_steps` accumulates as macros expand and a
`compensate:` may name a step defined further down the file.

| Rule | Failure |
|---|---|
| Target is a literal capture | `CompileError` |
| Target is a `defstep`/`deftransaction` in the same module | `CompileError` naming the target |
| Target arity matches the bound arguments plus `&1` | `CompileError` |

The target is a step, so its body is already determinism-checked and already an entry point for
the whole-application compiler.

## Storage

An extension migration adds one column:

```sql
ALTER TABLE "dbos".operation_outputs ADD COLUMN IF NOT EXISTS ex_compensation TEXT;
```

ETF, base64-encoded, like `output` and `error`. It holds a map so later configuration is additive:

```elixir
%{undo: {WidgetStore.Checkout, :release_stock, [product_id, quantity, :__checkpoint__]}}
```

`:__checkpoint__` marks where the stored `output` is substituted at unwind time, so `&1` may sit at
any position.

The row becomes a self-describing rollback recipe. No registry, no name lookup, and immune to a
later rename of the workflow module.

## Triggers

| Trigger | Parent status | Compensator enqueued by |
|---|---|---|
| Uncaught exception | `ERROR` | The workflow process, same transaction |
| `Dbos.abort/1` | `ERROR` | The workflow process, same transaction |
| `Dbos.cancel/2` | `CANCELLING` → `CANCELLED` | The workflow process, same transaction as `CANCELLED` |

Automatic: declaring `compensate:` on a step is the opt-in. The compensator is enqueued only when
history holds at least one compensable row.

### `CANCELLING`

`CANCELLED` is terminal, so a cancelled-but-not-unwound row is indistinguishable from a
cancelled-and-unwound one and no existing machinery can see it has work left. `CANCELLING` is
non-terminal and visible to the sweep.

```
Dbos.cancel/2
  └─ status = CANCELLING, wake the local process

workflow process
  ├─ in-flight step completes and checkpoints   (its effect is undoable)
  ├─ next check_operation_execution sees CANCELLING and stops the forward path
  └─ commit: status = CANCELLED + compensator enqueued

executor dies first
  └─ lease sweep finds CANCELLING with an expired lease and enqueues the compensator
```

The in-flight step *must* still checkpoint. Refusing that write would lose the record of an effect
that already happened, and the unwind would miss it.

The `ERROR` path needs no equivalent state: the process is alive and commits the status and the
enqueue together, so the compensator always exists whenever the parent is `ERROR`. "Is the unwind
done?" is answered by that workflow's own status.

## The unwind

One compensation workflow per workflow to be unwound, taking that workflow's id as its input.
Deterministic id `"#{target_id}-compensate"`, with `parent_workflow_id` set. It walks that one
workflow's `operation_outputs` in descending `function_id`, running each recorded undo as its own
durable step.

Its input is a target id rather than "the workflow that failed", because a workflow is also
unwound when it succeeded and something above it failed.

### One history each

A step cannot start, enqueue, fork, await or cancel a workflow — each of those takes a step id of
its own, and a step is one checkpoint (`Dbos.OperationInStepError`). So everything the unwind does
to another workflow happens in the compensator's *body*, and the undos alone are its steps.

That settles how the unwind crosses a workflow boundary: it does not read another workflow's rows.
On reaching a row that spawned a descendant, the compensator starts *that* workflow's compensator
as a child and awaits it. Depth is ordinary workflow nesting, so a partially-finished deep unwind
recovers on the machinery that already exists for parent and child, and each level retries and
resumes on its own.

The walk is also complete by construction: only a workflow body can spawn a workflow, so every
descendant a workflow has appears in that workflow's own history. No step can have spawned one
behind the walk's back.

### Engine-internal steps

Every built-in durable operation records a `function_name` prefixed `DBOS.`
(`Dbos.StepNames`). Skipped by default, with three exceptions, each naming a descendant:

| `function_name` | Where the descendant's id is |
|---|---|
| `DBOS.getResult` | `child_workflow_id` |
| `DBOS.enqueue` | the step's `output` |
| `DBOS.forkWorkflow` | the step's `output` |

`enqueue` and `forkWorkflow` do not set `child_workflow_id`; they return the new id as the step's
output.

### Descendants

A descendant may be in any state when the unwind reaches it, and every state resolves to "ask that
workflow to unwind itself":

| State | Action |
|---|---|
| `ENQUEUED`, `DELAYED` | → `CANCELLED`. No history to unwind, and it must not start later. |
| `PENDING` | → `CANCELLING`. It unwinds itself through the cancellation path. |
| `SUCCESS` | Start its compensator and await it. |
| `ERROR`, `CANCELLED`, `CANCELLING` | Skip. Already unwinding or unwound. |

Descendants are awaited before the walk continues past them. Their effects happened later, so they
are reversed first. Parallel sibling unwinds are given up deliberately: the error path is where
predictable ordering matters most.

`"#{target_id}-compensate"` being derived from the target means starting a compensator is
idempotent: a replay of the walk, and a descendant that reached its own compensator through its own
failure, converge on one workflow id rather than two unwinds of the same history.

### When an undo fails

Fail fast. The compensator stops at the first undo that exhausts its retries and lands in `ERROR`.

Because each undo is its own checkpoint, the resume point is exact: everything before the failure
is done and everything after is untouched. Resuming is `Dbos.fork/3` at the failed undo's step id,
not `Dbos.retry/2` — a failed step checkpoints its failure, so re-running the same workflow replays
that failure instead of retrying the undo. The parent stays in its non-terminal state, so the sweep
and the admin views still show outstanding work.

Emits `[:dbos, :compensation, :stuck]`, carrying that step id so an operator has the fork point. A
stuck compensator is a louder alarm than a stuck forward workflow: confirmed side effects are
outstanding with no automatic path to reversal.

## Rejected

**`on_error`** (Reactor's second callback, for a step that errored without completing). Its only
available input is the exception, since a failed step has no return value. A `rescue` in the step
body has strictly more context — it sees the local bindings, including any id obtained before the
failure. And a node that dies mid-step writes no row at all, so the step is re-executed on
recovery rather than compensated, which is the better outcome.

Cleanup inside a step's `rescue` is plain code either way: a nested `defstep` call folds into the
enclosing step and writes no checkpoint of its own, so it is never something the unwind can see.

**A name-keyed `undo/2` convention.** `function_name` defaults to `"fun/arity"` with no module
qualification, and a workflow freely calls steps defined in other modules, so one history can hold
two rows named `"reserve/1"` from different modules.

## Limits

- Compensation crosses workflow boundaries only through recorded history. A workflow reached by
  neither `getResult`, `enqueue`, nor `forkWorkflow` is invisible.
- One `workflow_status` row per level of nesting unwound, since each level runs its own
  compensator.
- The compensating MFA is resolved from a database row, so the unwind path itself is beyond the
  determinism compiler's reach. Each undo's *body* is checked at its definition site.
- Undo actions are at-least-once and must be idempotent.
- Bound arguments must be serialisable.
