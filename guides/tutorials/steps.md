# Steps

A step is the unit of work a workflow checkpoints. Once a step's output is recorded, every future
replay of that workflow returns the recorded output at that position. The step's body never runs
again. `defstep` is how you declare one.

```elixir
defmodule MyApp.Checkout do
  use Dbos

  defworkflow process_order(order_id, amount), name: "process_order" do
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
`deftransaction` — see `guides/tutorials/transactions.md`).

## Side effects are at-least-once, checkpoints are exactly-once

A step's recorded output is exactly-once: once its checkpoint commits, replay always returns that
same value without calling the step body again. The step body's own side effect is not covered by
that guarantee — a crash between the side effect happening and the checkpoint commit means the
next replay calls the body again, performing the side effect a second time.

A step whose side effect must never repeat (charging a card, sending a notification, calling a
non-idempotent downstream API) needs an idempotency key at that boundary: pass the step's own
`function_id`/workflow id, or a value derived from the workflow's input, as the idempotency key the
downstream system itself deduplicates on. The engine's checkpointing does not substitute for this
— it protects the *recorded result*, not the *external call*.

## What does not belong in a step

Pure computation that only touches the workflow's own inputs and prior steps' outputs — it can
stay directly in the workflow body:

```elixir
defworkflow process_order(order_id, amount), name: "process_order" do
  charge = charge_card(order_id, amount)
  total_with_tax = charge.amount * 1.1
  record_receipt(order_id, total_with_tax)
end
```

`total_with_tax` doesn't need a step: it's a deterministic function of `charge.amount`, which is
itself already a checkpointed value. Wrapping it in a step would just add a checkpoint row for
something replay already reproduces byte-for-byte.

## Naming

A step's name defaults to `"function_name/arity"` — the module is deliberately excluded:

```elixir
defstep charge_card(order_id, amount) do
  ...
end
```

is checkpointed under the name `"charge_card/2"` alone. This means moving `charge_card/2` to a
different module, or renaming `MyApp.Checkout` itself, leaves an in-flight workflow's recorded
checkpoints resolvable by name — only the function's own name and arity have to stay stable;
where it lives can change freely. Override the default with `name:`:

```elixir
defstep reserve_stock(order_id), name: "custom_reserve_name" do
  order_id
end
```

Two different steps that happen to share a function name and arity across modules used by the
same workflow would collide under the default naming — give one an explicit `name:` if that ever
comes up.

## Inline steps

`defstep` expands to `Dbos.Runtime.run_step/3`, which is itself callable directly for a one-off
that doesn't warrant its own named function — inside a loop, for instance:

```elixir
Enum.each(1..3, fn i ->
  Dbos.Runtime.run_step("notify/1", [], fn -> MyApp.Notifier.send(i) end)
end)
```

Prefer `defstep` when the step has a stable identity worth naming and reusing; reach for
`Dbos.Runtime.run_step/3` only when a named function would be pure ceremony.

## Retries

By default a step does **not** retry — `max_retries: 0` — a raised exception propagates straight
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
| 1 | — (first attempt, no delay) |
| 2 | 200ms |
| 3 | 400ms |
| 4 | 800ms |

`max_retries: 3` means three retries *after* the first attempt — four attempts total. Once the
budget is exhausted, the last failure is wrapped in `Dbos.MaxStepRetriesExceededError` and raised,
in place of the original exception, and checkpointed as the step's recorded failure.

Defaults, if any option is omitted: `max_retries: 0`, `base_interval_ms: 100`,
`backoff_factor: 2.0`, `max_interval_ms: 5000`.

## What is checkpointed

On success: `function_name`, the encoded `output`, and `started_at`/`completed_at` timestamps. On
failure: `function_name` and the encoded exception, in place of `output`. All of it keyed by
`(workflow_id, function_id)` — the step's position in call order.

## What is not checkpointed: step arguments

The step's **arguments are never stored.** Only its recorded output is. This is easy to overlook
because it's invisible until a step is called with different arguments on a replay:

```elixir
defworkflow charge_customer(order_id) do
  order = load_order(order_id)
  charge_card(order.customer_id, order.total)
end
```

If `load_order` reads a price that can change between the original run and a crash-recovery
replay, `charge_card` is called with a *different* `order.total` on replay — but the engine only
matches on `(workflow_id, function_id)`, finds a recorded output already sitting at that position,
and returns it. No error, no log line: the workflow "succeeds" with the **old** charge amount
silently substituted for the new one.

The fix is structural: every argument passed to a step must itself come from the workflow's own
input or a prior step's recorded output, sourced there and never from a live read of mutable
state performed directly in the workflow body. See `docs/determinism.md` for the full worked
example and the complete list of constructs `defworkflow` rejects at compile time.
