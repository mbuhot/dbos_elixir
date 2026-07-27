# Transactional steps

A transactional step writes to your own database tables through Ecto and commits its checkpoint in
the same database transaction as those writes.

## The problem it solves

A plain `defstep` that calls `MyApp.Repo.insert!/1` checkpoints its result after the insert has
already committed. There is a real crash window between the two: the insert commits, the process
dies before the checkpoint write lands, and recovery replays the step, inserting the row a second
time.

`deftransaction` closes the window by putting the write and its checkpoint in one transaction, so
they commit or roll back together.

## `deftransaction`

```elixir
defmodule MyApp.Checkout do
  use Dbos, repo: MyApp.Repo

  defworkflow process_order(order_id, amount), name: "MyApp.Checkout.process_order" do
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

Naming works the same as `defstep`: the name defaults to `"function_name/arity"` with the module
excluded, overridable with `name:`. The one transaction-specific option is `isolation:`.

`Dbos.transaction/3` is the inline form, for a one-off that does not warrant its own function.

## The Ecto enlistment rule

`deftransaction`'s body runs inside one `Repo.transaction/2` on the pool the engine is configured
with. Any call your body makes through that same `Repo` module — `insert!/1`, `update!/1`, a raw
`query!/2` — enlists on that connection automatically. You never thread a connection argument
through by hand.

Going around that pool — a hand-rolled `Postgrex.transaction/3`, or a second unrelated `Repo` —
reopens the atomicity gap.

`use Dbos, repo: MyApp.Repo` makes a direct `MyApp.Repo` call from inside a workflow body a
compile error, so a write only ever happens wrapped in a `deftransaction`.

## Isolation levels

`opts[:isolation]`: `:read_committed`, `:repeatable_read`, or `:serializable`. Left unset, the
adapter's own default applies (read committed).

```elixir
deftransaction reserve_seat(showing_id, seat_number), isolation: :serializable do
  MyApp.Repo.insert!(%MyApp.Reservation{showing_id: showing_id, seat_number: seat_number})
end
```

## Nesting rules

| Nesting | Outcome |
|---|---|
| `deftransaction` inside another `deftransaction`'s body | Raises `Dbos.NestedTransactionError`. |
| A `defstep` inside a `deftransaction`'s body | Allowed, and folded in: it runs as ordinary code, takes no step id and writes no checkpoint of its own. Its side effect happens inside the open database transaction, so a later rollback cannot undo it — keep external calls out of a transaction body. |
| Any other durable operation (`Dbos.send_message/4`, `Dbos.recv_message/3`, `Dbos.set_event/3`, starting or awaiting a workflow, ...) inside a `deftransaction`'s body | Raises `Dbos.OperationInStepError`. |
| `deftransaction` inside a plain `defstep`'s body | Allowed. Runs a real transaction and commits it, recording no checkpoint row of its own — it rides on the enclosing step's checkpoint. A replay that re-runs the enclosing step re-runs this inner transaction, so its body must tolerate re-execution. |


## What happens on failure

A raised exception rolls the transaction back — the write and the checkpoint undo together — and
propagates out of the transactional step. The step is left with no recorded outcome, so a later
run of this workflow (recovery after a crash, `Dbos.resume/2`, `Dbos.fork/3`) executes the body
again from the top. A plain `defstep` behaves differently here: its failure is itself checkpointed
and re-raised verbatim on every replay.

Uncaught, the exception reaches the workflow body and the workflow is recorded `ERROR`.

`isolation:` is the only option a transactional step acts on.

## Undoing a transaction

`compensate:` works as it does on `defstep` — see [Compensation](compensation.md). The compensating
action is a new write that negates the old one; a committed transaction cannot be un-committed.
