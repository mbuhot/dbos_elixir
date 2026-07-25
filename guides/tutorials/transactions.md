# Transactional steps

A transactional step is a step that also writes to your own database tables through Ecto. It
solves one specific problem: a database write and the checkpoint that records it must commit
together, or not at all.

## The problem

A plain `defstep` that calls `MyApp.Repo.insert!/1` checkpoints its *result* after the insert
already committed. Between those two things there is a real crash window: the insert commits,
the process dies before the checkpoint write lands, and recovery replays the step — inserting the
row a second time, or worse, running whatever side effect the step performed twice. Making the
write itself un-checkpointed doesn't help either: a workflow can only reproduce the write
determinstically if replay is guaranteed not to repeat it.

`deftransaction` closes this window by putting the user's write and its checkpoint in the *same*
database transaction, so they commit or roll back together — there is no state where one exists
without the other.

## `deftransaction`

```elixir
defmodule MyApp.Checkout do
  use Dbos, repo: MyApp.Repo

  defworkflow process_order(order_id, amount), name: "process_order" do
    charge = charge_card(order_id, amount)
    record_receipt(order_id, charge)
  end

  defstep charge_card(order_id, amount) do
    MyApp.PaymentGateway.charge!(order_id, amount)
  end

  deftransaction record_receipt(order_id, charge) do
    MyApp.Repo.insert!(%MyApp.Receipt{order_id: order_id, charge_id: charge.charge_id})
  end
end
```

Naming and options work the same as `defstep` — the name defaults to `"function_name/arity"`
(module excluded), override with `name:`. The one transaction-specific option is `isolation:`.

## The Ecto enlistment rule

`deftransaction`'s body runs inside one call to `Dbos.transaction/3`, which opens the transaction
with `Repo.transaction/2`, always through the pool it's given. Any
call your body makes through the same `Repo` module — `MyApp.Repo.insert!/1`,
`MyApp.Repo.update!/1`, a raw `MyApp.Repo.query!/2` — enlists on that same connection
automatically, the same way any two `Repo` calls nested inside an ordinary
`Repo.transaction/2` do. You don't need to thread a connection argument through by hand.

Going around the pool — a hand-rolled `Postgrex.transaction/3` against the raw connection, or a
second, unrelated `Repo` — does not enlist, and reintroduces the two-write atomicity gap
`deftransaction` exists to close.

`use Dbos, repo: MyApp.Repo` also makes a direct `MyApp.Repo` call from inside a workflow body a
compile error — see `docs/determinism.md` — precisely so that a write like the one above only
ever happens wrapped in a `deftransaction`.

## Isolation levels

`opts[:isolation]`: `:read_committed`, `:repeatable_read`, or `:serializable`. Unset uses
Postgres's own default (read committed).

```elixir
deftransaction reserve_seat(showing_id, seat_number), isolation: :serializable do
  MyApp.Repo.insert!(%MyApp.Reservation{showing_id: showing_id, seat_number: seat_number})
end
```

## Nesting rules

| Nesting | Outcome |
|---|---|
| `deftransaction` called from inside another `deftransaction`'s body | Rejected: raises `Dbos.NestedTransactionError`. |
| A `defstep` (or any durable operation built on `Dbos.Runtime.run_step/3` — `Dbos.send_message`, `Dbos.recv_message`, `Dbos.set_event`, ...) called from inside a `deftransaction`'s body | Rejected: raises `Dbos.StepInTransactionError`. |
| `deftransaction` called from inside a plain `defstep`'s body | Allowed. Runs a real transaction and commits it, but records **no separate checkpoint row** of its own — it rides on the enclosing step's own checkpoint. A replay that re-runs the enclosing step re-runs this inner transaction too, so its body must tolerate re-execution in that case. |

## What happens on failure

If the body raises, the transaction rolls back — the write and the checkpoint both undo together,
same as a commit closes both together. The exception is then checkpointed as the step's recorded
failure and re-raised, exactly like a plain step's failure. This covers a body that genuinely
raised, distinct from the process simply crashing mid-transaction. Replaying that workflow fails
the same way again — it never silently retries past that failure.

## Why one transaction is enough

The engine keeps its own tables in the same database your application already uses, configured
as `db: {Dbos.DB.Ecto, MyApp.Repo}`.

One database means one transaction. `deftransaction` opens a single `Repo.transaction/2` holding
both writes:

| Write | What it is |
|---|---|
| Your `Repo` calls in the body | The business data |
| The step's checkpoint | The record that this step completed |

They commit together or roll back together. That single commit is the whole atomicity guarantee —
there is no second commit and no window between commits.

A deployment that put the engine's tables in a separate database would need a completion table
and a two-phase commit order to get the same property. Sharing one database removes the need.
