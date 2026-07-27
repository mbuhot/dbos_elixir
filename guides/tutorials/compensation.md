# Compensation

A workflow that crashes is replayed forward. A workflow whose *business transaction* fails needs
the opposite: the effects of the steps that already succeeded have to be reversed.

Declare how each step undoes itself, and the engine does the reversing:

```elixir
defworkflow book_trip(trip_id), name: "MyApp.Travel.book_trip" do
  reserve_seat(trip_id)
  charge_card(trip_id)
  issue_ticket(trip_id)
end

defstep reserve_seat(trip_id), compensate: &release_seat(trip_id, &1) do
  Airline.reserve(trip_id)
end

defstep release_seat(trip_id, _seat) do
  Airline.release(trip_id)
end
```

If anything after `reserve_seat` raises, `release_seat` runs. The workflow body describes only the
path you want; the reversal is derived from what actually happened.

## The `compensate:` option

`compensate:` takes a capture of a `defstep` or `deftransaction` **in the same module**.

| In the capture | Means |
|---|---|
| `&1` | this step's checkpointed return value |
| any other argument | frozen by value when this step checkpoints |

```elixir
defstep reserve_stock(product_id, quantity),
  compensate: &release_stock(product_id, quantity, &1) do
  Inventory.reserve(product_id, quantity)
end
```

`&release_stock/1` is shorthand for `&release_stock(&1)`.

The recipe is written onto the step's own checkpoint row, so it survives renaming the module that
declared it, and the unwind needs no registry to find it.

Three rules, all checked at compile time: the target is a literal capture, it is a step in the same
module, and its arity matches the arguments given. `&1` must be a whole argument — take the value
apart in the compensating step's body, not in the capture.

## What gets reversed

Only steps that **completed**. A step is in the history because it checkpointed a result, and a
step that failed checkpointed a failure, which carries no compensation — there is nothing to put
back. A branch never taken is not in the history at all.

Nothing else needs declaring. Pure computation in the workflow body has nothing to undo, and a step
that only reads has nothing to undo either.

## What triggers it

| Trigger | The workflow reaches | Unwinds |
|---|---|---|
| An uncaught exception | `ERROR` | yes |
| `Dbos.abort/1` | `ERROR` | yes |
| `Dbos.cancel/2` | `CANCELLING` → `CANCELLED` | yes |
| Returning normally | `SUCCESS` | no |

Declaring `compensate:` on a step is the entire opt-in. The unwind is enqueued in the same
transaction as the terminal status, so a workflow is never `ERROR` with effects outstanding and
nothing on its way to reverse them.

`Dbos.abort/1` is for a business transaction that cannot go on though nothing has broken — a
declined payment, a rejected approval:

```elixir
case await_payment() do
  :paid -> :ok
  other -> Dbos.abort({:payment_not_confirmed, other})
end
```

It reads as a decision rather than a bug, and it works from a step as well as from the body.

### Cancelling

`Dbos.cancel/2` on a workflow with effects to reverse puts it in `CANCELLING` — a non-terminal
status — rather than `CANCELLED`. It stops at its next checkpoint, unwinds, and only then becomes
`CANCELLED`. `Dbos.await/2` blocks through all of it.

`CANCELLED` is terminal, so it cannot say whether the effects were ever reversed. `CANCELLING` can,
which is what lets the engine finish a cancellation whose executor died halfway through.

## The unwind is a workflow

Each unwind is a durable workflow of its own, `"<workflow_id>-compensate"`, and each undo is one of
its steps.

That is what makes an interrupted unwind resume rather than restart: the undos already done keep
their checkpoints. It also ends the "what compensates the compensator" question — nothing does, and
nothing needs to.

Start one by hand with `Dbos.unwind/2`. Its id is derived from its target, so calling it twice
converges on one unwind rather than reversing the same history twice.

```elixir
{:ok, handle} = Dbos.unwind(workflow_id)
{:ok, undos_run} = Dbos.await(handle)
```

## Child workflows

A step that spawned another workflow is reversed by unwinding *that* workflow, not by reaching into
its history. Each workflow's effects are its own to reverse, so the walk hands a descendant to its
own compensator and waits for it:

| The descendant is | The unwind |
|---|---|
| still running | cancels it, and it unwinds itself |
| `SUCCESS` with effects | starts its compensator and waits |
| `ENQUEUED` or `DELAYED` | cancels it — it has no history, and must not start later |
| already unwinding or unwound | leaves it alone |

Descendants are reversed before the steps that came before them, and one at a time. Their effects
happened later, and the error path is where predictable ordering matters most.

## When an undo fails

The unwind stops at the first undo that exhausts its retries, and lands in `ERROR`. Everything
before it is done; everything after is untouched.

`[:dbos, :compensation, :stuck]` fires, measuring `%{reversed: n, outstanding: n}` and carrying the
`step_id` to resume from. Treat it as louder than a stuck forward workflow: confirmed side effects
are outstanding with no automatic path to reversing them.

Resume with `Dbos.fork/3` at that step, once the downstream system is repaired:

```elixir
Dbos.fork(unwind_id, stuck_step_id)
```

Not `Dbos.retry/2` — a failed step checkpoints its failure, so re-running the same workflow replays
that failure instead of retrying the undo.

## Undoing an event, a message, or a stream write

`Dbos.send_message/4`, `Dbos.set_event/3` and `Dbos.write_stream/3` cannot be wrapped in a
compensable step, because a step may not call them. They take the option directly instead:

```elixir
Dbos.set_event("state", "shipped", compensate: &MyApp.Orders.retract_state/1)
Dbos.write_stream("events", item, compensate: {MyApp.Feed, :retract, ["events", :__checkpoint__]})
```

These are functions rather than macros, so the capture DSL cannot apply. The forms are
`&Module.fun/1` or an explicit `{module, function, args}` with `:__checkpoint__` marking where the
checkpointed value belongs.

## What to keep in mind

- **Undos run at least once.** Make them idempotent, keyed on something stable like the workflow id.
- **A committed write cannot be un-committed.** The compensating action is a new statement that
  negates the old one; only your domain knows what that means.
- **Arguments frozen into a recipe must be serialisable**, like any other checkpointed value.
- **Cleanup inside a step's own `rescue` is plain code.** A `defstep` called from there folds into
  the enclosing step and writes no checkpoint, so the unwind cannot see it.
