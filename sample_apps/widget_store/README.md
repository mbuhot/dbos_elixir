# Widget Store — Fault-Tolerant Checkout

A storefront that sells one product. `WidgetStore.Checkout.checkout/3` is a `Dbos` workflow
that reserves inventory, asks a payment gateway to charge the customer, and waits for a
payment confirmation that arrives out of band — a webhook in a real app, `mix
widget_store.demo confirm` here.

## What it demonstrates

**Durability**: a crash at any point during checkout — after the inventory write, mid-charge, or
while durably parked waiting for payment — never double-decrements inventory, never double-charges,
and never loses a refund. Restarting the application resumes the workflow from its last checkpoint
instead of starting over.

**Compensation**: the workflow body describes only the happy path. Each step that changes something
declares how to change it back, and a checkout that cannot go on calls `Dbos.abort/1`. The engine
then runs those undos in reverse — in a durable workflow of its own, so an interrupted unwind
resumes rather than restarting.

| Step | Kind | Undone by |
|---|---|---|
| `reserve_and_create_order/3` | `deftransaction` | `release_and_cancel_order/4` — puts the stock back and marks the order `CANCELLED`, in one write |
| `charge_customer/2` | `defstep`, 3 retries | `refund_charge/2` — refunds the gateway, keyed on the order id so a retry is harmless |
| Waiting for payment | `Dbos.recv_message/2` | nothing to undo; a decline or a timeout is what triggers the unwind |
| `dispatch_order/1` | `deftransaction` | nothing after it can fail, so it needs no undo |

An order that cannot be filled raises instead: the reservation step checkpoints a *failure*, which
records no compensation, so the checkout fails with an empty unwind and no compensator is started
at all.

## Running it

```sh
mix deps.get
createdb widget_store_dev   # once
mix widget_store.demo start
```

This seeds a product, starts a checkout, and after the inventory reservation and the charge
request both complete, **crashes the BEAM with `System.halt/1`** while the workflow is durably
parked waiting for a payment confirmation that hasn't arrived.

```sh
mix widget_store.demo confirm
```

This restarts the application. `Dbos.Recovery` replays every `PENDING` workflow from its last
checkpoint before the task does anything else: `reserve_and_create_order` and `charge_customer`
return their recorded outputs rather than running again, and the inventory decrement and charge
request are not repeated. Only then does the task send the payment confirmation the workflow is
still waiting on, and the checkout finishes — dispatched, exactly once.

## What to kill mid-run to see recovery work

Run `mix widget_store.demo start`, then before running `confirm`, inspect the database directly:

```sql
select * from products;   -- inventory already decremented
select * from orders;     -- one row, status 'pending'
select workflow_id, status from dbos.workflow_status where workflow_id = 'demo-order-1';
-- status is 'pending' — the process died, but nothing here was lost
```

Then run `confirm` and watch the same order finish as `dispatched`, with the product's
inventory unchanged from what `start` already committed.

Or run `decline` instead, and watch the saga unwind: the charge is refunded, the stock goes back,
and the order ends `cancelled`.

```sql
select workflow_uuid, status from dbos.workflow_status
 where workflow_uuid like 'demo-order-1%';
-- demo-order-1              ERROR
-- demo-order-1-compensate   SUCCESS

select function_id, function_name from dbos.operation_outputs
 where workflow_uuid = 'demo-order-1-compensate' order by function_id;
-- 0  refund_charge/2
-- 1  release_and_cancel_order/4
```

## Tests

```sh
mix test
```

`test/widget_store/checkout_test.exs` proves the exactly-once property directly: it kills the
workflow's own process mid-checkout with `Process.exit(pid, :kill)`, confirms the workflow's
status is still `PENDING` in `dbos.workflow_status`, drives recovery with
`Dbos.Recovery.recover_pending/1`, and asserts the order finishes with inventory decremented by
exactly the ordered quantity — not twice.

Three further tests cover the saga: a declined payment refunds and restores stock, the unwind
records each undo as its own checkpointed step in reverse order, and an out-of-stock order fails
with no compensator started.
