# Steps

A step is the unit of work a workflow checkpoints. Once a step's output is recorded, every future
replay of that workflow returns the recorded output at that position, leaving the body
unexecuted.

```elixir
defmodule MyApp.Checkout do
  use Dbos

  defworkflow process_order(order_id, amount), name: "MyApp.Checkout.process_order" do
    charge_card(order_id, amount)
  end

  defstep charge_card(order_id, amount) do
    MyApp.PaymentGateway.charge!(order_id, amount)
  end
end
```

## What belongs in a step

Everything nondeterministic, and every side effect: an HTTP call, a call to another service, a
read of the current time, a random number, a file read, a database write (through
`deftransaction` — see [Transactions](transactions.md)).

Pure computation over the workflow's own inputs and prior steps' outputs stays in the workflow
body:

```elixir
defworkflow process_order(order_id, amount), name: "MyApp.Checkout.process_order" do
  charge = charge_card(order_id, amount)
  total_with_tax = charge.amount * 1.1
  record_receipt(order_id, total_with_tax)
end
```

`total_with_tax` is a deterministic function of `charge.amount`, which is itself an already
checkpointed value, so replay reproduces it byte-for-byte.

## Side effects are at-least-once, checkpoints are exactly-once

A step's recorded output is exactly-once: after its checkpoint commits, replay returns that value
without calling the body again. The body's own side effect sits outside that guarantee — a crash
between the side effect happening and the checkpoint commit means the next replay calls the body
again and performs the side effect a second time.

A step whose side effect must never repeat (charging a card, sending a notification, calling a
non-idempotent downstream API) needs an idempotency key at that boundary: pass the workflow id, or
a value derived from the workflow's input, as the key the downstream system deduplicates on.

## Naming

A step's name defaults to `"function_name/arity"`, with the module excluded:

```elixir
defstep charge_card(order_id, amount) do
  ...
end
```

checkpoints under `"charge_card/2"`. Moving the function to a different module, or renaming
`MyApp.Checkout` itself, leaves an in-flight workflow's recorded checkpoints resolvable; the
function's own name and arity are what must stay stable. Override with `name:`:

```elixir
defstep reserve_stock(order_id), name: "custom_reserve_name" do
  order_id
end
```

Two steps sharing a function name and arity across modules used by the same workflow collide under
the default naming — give one an explicit `name:`.

## Inline steps

`Dbos.step/2` (or `Dbos.step/3` with retry options) runs a one-off durable step without declaring
a function for it:

```elixir
Enum.each(1..3, fn i ->
  Dbos.step("notify/1", fn -> MyApp.Notifier.send(i) end)
end)
```

Reach for it when a named function would be pure ceremony. `defstep` is the better fit when the
step has a stable identity worth naming and reusing.

## What a step may contain

A step may call another step. The inner call is folded into the caller: it runs as ordinary code,
takes no step id, and writes no checkpoint of its own. Factoring one step out of another is
therefore safe, and the extracted function keeps working whether it is reached from a workflow
body, from another step, or from a `deftransaction` — a transaction is a step that commits its
checkpoint together with its write, so the same rule governs both.

Everything else durable belongs in the workflow body, and raises `Dbos.OperationInStepError`
from inside either body:

| Refused inside a step | Where it belongs |
|---|---|
| `Dbos.start/3`, `Dbos.enqueue/3`, `Dbos.fork/3`, awaiting a workflow | The workflow body |
| `Dbos.send_message/4`, `Dbos.recv_message/2` | The workflow body |
| `Dbos.set_event/2`, `Dbos.get_event/3` | The workflow body |
| `Dbos.sleep/1`, stream writes | The workflow body |

The reason is the step-id sequence. Each of those is a durable operation that takes an id of its
own, but a step is one checkpoint: replay returns it from that checkpoint without running its
body, so an id consumed inside would never be consumed again and every later step would shift
onto the wrong one. A nested *step* is exempt because it takes no id at all.

This mirrors upstream DBOS, where a step body receives a plain context rather than a DBOS one,
making every operation in that table unrepresentable inside it.

## Undoing a step

`compensate:` names the step that reverses this one, so a workflow that fails later can be unwound:

```elixir
defstep reserve_stock(product_id, quantity),
  compensate: &release_stock(product_id, quantity, &1) do
  Inventory.reserve(product_id, quantity)
end
```

`&1` is this step's checkpointed return value. See [Compensation](compensation.md).

## Retries

By default a step does not retry — `max_retries: 0` — and a raised exception propagates straight
to the workflow. Opt in with retry options on `defstep`:

```elixir
defstep charge_card(order_id, amount),
  max_retries: 3,
  base_interval_ms: 200,
  backoff_factor: 2.0,
  max_interval_ms: 5_000 do
  MyApp.PaymentGateway.charge!(order_id, amount)
end
```

The delay before attempt `n` (1-indexed) is `base_interval_ms * backoff_factor ^ (n - 1)`, capped
at `max_interval_ms`. With the example above:

| Attempt | Delay before it |
|---|---|
| 1 | — |
| 2 | 200ms |
| 3 | 400ms |
| 4 | 800ms |

`max_retries: 3` means three retries *after* the first attempt — four attempts total. Once the
budget is exhausted, the last failure is wrapped in `Dbos.MaxStepRetriesExceededError`, raised, and
checkpointed as the step's recorded failure.

| Option | Default |
|---|---|
| `max_retries` | `0` |
| `base_interval_ms` | `100` |
| `backoff_factor` | `2.0` |
| `max_interval_ms` | `5000` |

## What is checkpointed

On success: the step name, its output, and start/completion timestamps. On failure: the step name
and the exception. All of it recorded against the step's position in call order.

Step **arguments are never stored.** This stays invisible until a step is called with different
arguments on a replay:

```elixir
defworkflow charge_customer(order_id), name: "MyApp.Checkout.charge_customer" do
  order = load_order(order_id)
  charge_card(order.customer_id, order.total)
end
```

If `load_order` reads a price that can change between the original run and a crash-recovery
replay, `charge_card` is called with a different `order.total` on replay. The engine finds a
recorded output at that position and returns it: the workflow succeeds with the old charge amount
silently substituted.

The fix is structural — every argument passed to a step comes from the workflow's own input or a
prior step's recorded output. See [the determinism contract](../../docs/determinism.md) for the full worked example.
