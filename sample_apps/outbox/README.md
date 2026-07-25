# Outbox — Transactional Outbox Pattern

Placing an order has two effects that must both happen or neither: the `orders` row is
written, and an external system (a broker, a webhook, a downstream API — here,
`Outbox.ExternalSystem`, a stub) is told about it. `Outbox.Workflows` shows the pattern that
makes that atomic without a distributed transaction.

## The naive version, and why it's wrong

```elixir
def place_order(customer, item, quantity) do
  order = Repo.insert!(%Order{customer: customer, item: item, quantity: quantity})
  ExternalSystem.publish("order_placed", %{order_id: order.id})
  order.id
end
```

The database write commits. Then the process crashes, or the network call to
`ExternalSystem` times out, or the BEAM node dies before the `publish` call returns. The order
exists; nothing downstream ever hears about it. There is no retry, because nothing recorded
that a publish was owed — the information "this order needs an event" lived only in the
now-finished call stack.

## The pattern

| Step | Kind | What it does |
|---|---|---|
| `insert_order_with_outbox/3` | `deftransaction` | Inserts the `orders` row and an `outbox_events` row (`status: "pending"`) in one Postgres transaction. Either both are committed, or neither is. |
| `drain_outbox/2` | `defworkflow`, cron-scheduled every 2s | Lists every `"pending"` event, publishes it, marks it `"sent"` — as three separate durable steps. |
| `publish_event/1` | `defstep`, 5 retries | Calls `Outbox.ExternalSystem.publish/2`; a transient failure retries instead of losing the event. |
| `mark_event_sent/1` | `deftransaction` | Flips `status` to `"sent"` only after `publish_event` actually returned successfully. |

Nothing here is an explicit "outbox table drained by a poller you have to keep alive
yourself" — `drain_outbox` **is** the poller, and `Dbos` durably checkpoints its progress. If
the process dies partway through a drain, the next scheduled run picks up wherever the
`operation_outputs` checkpoints left off.

## The guarantee, stated honestly

**At-least-once delivery.** If the process dies after `publish_event` succeeds but before
`mark_event_sent` commits, the event is still `"pending"` in the database — the next drain
finds it and publishes it again. A slow drain overlapping with the next scheduled tick has the
same effect: two `drain_outbox` runs can both see the same `"pending"` row and both publish it
before either marks it `"sent"`. `Outbox.ExternalSystem`, standing in for whatever this
actually notifies, must be idempotent on its end (a unique event id, an upsert, a dedup
table) — this pattern guarantees the event is never silently dropped, not that it is
delivered exactly once.

## Running it

```sh
mix deps.get
createdb outbox_dev   # once
mix ecto.migrate
iex -S mix
```

```elixir
iex> Outbox.Workflows.place_order("alice", "widget", 3)
{:ok, %Dbos.WorkflowHandle{...}}
```

Within 2 seconds `drain_outbox`'s next scheduled tick finds the pending event and publishes
it — watch the log line from `Outbox.ExternalSystem`.

To see the failure path: `Outbox.ExternalSystem.fail_next("order_placed")` arms the very next
publish attempt to raise. Place an order, and watch `publish_event` retry (with backoff) until
it succeeds — the event is never lost, only delayed.

## What to kill mid-run to see recovery work

Place an order, then before the next scheduled drain fires, `kill -9` the BEAM. Restart with
`iex -S mix`. The order and its outbox row are both there (they committed together, or not at
all), still `"pending"` — the next scheduled drain publishes it exactly as if nothing had
happened.

## Tests

```sh
mix test
```

`test/outbox/workflows_test.exs`:

- proves an order is never committed without its outbox row
- arms `Outbox.ExternalSystem.fail_next/1` and proves a failed publish is retried within the
  same drain (`ExternalSystem.attempts/1` goes to 2, not 1) rather than the event being lost
- proves draining twice never re-sends an event already marked `"sent"`
