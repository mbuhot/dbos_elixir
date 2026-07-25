# Widget Store — Fault-Tolerant Checkout

A storefront that sells one product. `WidgetStore.Checkout.checkout/3` is a `Dbos` workflow
that reserves inventory, asks a payment gateway to charge the customer, and waits for a
payment confirmation that arrives out of band — a webhook in a real app, `mix
widget_store.demo confirm` here.

## What it demonstrates

**Durability property**: a crash at any point during checkout — after the inventory write,
mid-charge, or while durably parked waiting for payment — never double-decrements inventory,
never double-charges, and never loses a refund. Restarting the application resumes the
workflow from its last checkpoint instead of starting over.

| Step | Kind | Why |
|---|---|---|
| `reserve_and_create_order/3` | `deftransaction` | Decrements `products.inventory` and inserts the `orders` row in one Postgres transaction — the write and its `Dbos` checkpoint commit together, or neither does. |
| `charge_customer/2` | `defstep`, 3 retries | Calls the payment gateway; a transient failure retries instead of failing the whole order. |
| Waiting for payment | `Dbos.recv_message/2` | Durably parks — no process pinned in memory for the whole wait — until a message arrives on the `"payment"` topic, or 30s pass. |
| `dispatch_order/1` / `refund_and_restore/3` | `deftransaction` | Marks the order `DISPATCHED`, or restores inventory and marks it `CANCELLED`. |

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

## Tests

```sh
mix test
```

`test/widget_store/checkout_test.exs` proves the exactly-once property directly: it kills the
workflow's own process mid-checkout with `Process.exit(pid, :kill)`, confirms the workflow's
status is still `PENDING` in `dbos.workflow_status`, drives recovery with
`Dbos.Recovery.recover_pending/1`, and asserts the order finishes with inventory decremented by
exactly the ordered quantity — not twice. Two further tests cover a declined/timed-out payment
(refund and restore) and an out-of-stock order (rejected without touching inventory).
