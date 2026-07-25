# Workflows

A workflow is a durable function. Its steps checkpoint to Postgres as they complete, so a crash,
a deploy, or a killed node resumes the workflow from its last checkpoint. It never starts over.
`defworkflow` is how you declare one.

## Guarantees

- **Once started, a workflow runs to completion** (or a terminal failure) even across process
  and node restarts, as long as some engine with that workflow registered eventually comes back
  up.
- **Each step runs at most once for its recorded output** — a step that already checkpointed
  returns that output on every future replay. Its body never executes again.
- **An exception a step raises is itself checkpointed and re-raised on replay** — a workflow that
  failed for a real reason fails the same way again. It never silently retries past the failure.

These guarantees hold only if the workflow body is deterministic. See
[the determinism contract, in summary](#the-determinism-contract-in-summary) below.

## `defworkflow`

```elixir
defmodule MyApp.Checkout do
  use Dbos

  defworkflow process_order(order_id, amount), name: "process_order" do
    charge = charge_card(order_id, amount)
    record_receipt(order_id, charge)
  end
end
```

`name:` is required. Recovery dispatches a `PENDING` workflow row by its `name` column. Moving
`process_order/2` to a different module, or renaming the function, leaves an in-flight workflow
row resolvable as long as `name:` doesn't change. Compiling without `name:` is a `CompileError`.

A workflow head cannot carry a `when` guard — recovery must map one name to exactly one
deterministic body, so branching goes inside the body:

```elixir
# rejected at compile time
defworkflow process_order(order_id) when is_binary(order_id), name: "process_order" do
  ...
end
```

Default arguments are supported:

```elixir
defworkflow greet(name \\ "world"), name: "greet" do
  "hello, #{name}"
end
```

## Starting a workflow: bare call, `Dbos.start/3`, or `Dbos.enqueue/3`

A bare call to a `defworkflow`-defined function is already durable, and its return type depends
on where you call it from:

| Call site | What happens | Return value |
|---|---|---|
| Outside a workflow (a controller, a test, `iex`) | Starts a root workflow, does not wait for it | `{:ok, %Dbos.WorkflowHandle{}}` |
| Inside another workflow's body | Runs as a child workflow, blocks until it finishes | The child's unwrapped result |
| No engine started | — | raises `Dbos.NotStartedError` |

```elixir
{:ok, handle} = MyApp.Checkout.process_order("order-1", 4999)
{:ok, result} = Dbos.await(handle)
```

`Dbos.start/3` does the same thing by workflow name (or a `&Mod.fun/n` capture), skipping the
generated dispatcher function, and accepts `:workflow_id`, `:engine`, `:deduplication_id`,
`:priority`, `:application_version`:

```elixir
{:ok, handle} =
  Dbos.start("process_order", ["order-1", 4999], workflow_id: "checkout-order-1")
```

`Dbos.enqueue/3` starts a workflow by placing it on a named queue for later dispatch — a
`Dbos.Queue.Runner` claims it, subject to the queue's concurrency, rate limit, and priority
rules. Requires `:queue_name`; also accepts `:workflow_id`, `:priority`,
`:deduplication_id`, `:partition_key`, `:delay_ms`, `:application_version`:

```elixir
{:ok, handle} =
  Dbos.enqueue("ship_order", [order_id], queue_name: "shipping", priority: 1)
```

Called from inside a workflow, `Dbos.enqueue/3` is itself a checkpointed step (`"DBOS.enqueue"`)
— a replay finds the enqueue already recorded and does not enqueue a second copy.

## Workflow ids

Every workflow has an id — a string, random (`Dbos.Uuid.v4/0`-generated) by default. Supply your
own with `:workflow_id` on `Dbos.start/3` or `Dbos.enqueue/3` to make starting the *same* logical
operation twice a no-op: starting a workflow id that already has a row simply returns a handle to
the existing run. A second one never starts.

```elixir
Dbos.start("process_order", [order_id, amount], workflow_id: "checkout-#{order_id}")
```

## Handles and awaiting

`Dbos.start/3` and `Dbos.enqueue/3` both return `{:ok, %Dbos.WorkflowHandle{engine:, workflow_id:}}`
immediately, without blocking for the workflow's lifetime. `Dbos.await/2` blocks until that
workflow reaches a terminal status:

```elixir
case Dbos.await(handle, timeout_ms: 30_000) do
  {:ok, output} -> output
  {:error, exception} -> raise exception
  {:error, :timeout} -> :still_running
end
```

Called from inside a workflow, `Dbos.await/2` is a checkpointed step (`"DBOS.getResult"`) — the
wait itself is never checkpointed, only the outcome, once it's known, so replaying a still-waiting
await waits again; there's no recorded value yet to replay.

## Child workflows

A bare call, or `Dbos.start/3`, made from inside a workflow body is a **child workflow**: it
starts as its own row, is awaited inline, and its unwrapped result comes back as an ordinary
return value.

```elixir
defworkflow parent_flow(order_id), name: "parent_flow" do
  process_order(order_id, 4999)
end
```

A child's id defaults to `"<parent_id>-<parent_step_id>"` — deterministic from the parent's own
id and the step position the child was started at, so replaying the parent finds the same child
id already recorded (`check_child_workflow`) and does not start it a second time.

## The determinism contract, in summary

A workflow body must take the same path and call the same steps with the same arguments every
time it replays. Step **arguments are never stored** — only each step's *output* is — so a step
called with different arguments on replay silently returns the stale recorded output for its
position. No error surfaces. `defworkflow` rejects the mechanically-detectable violations
(`:rand.*`, `DateTime.utc_now/0`, bare `receive`, `Task.async`, ...) at compile time. Full
contract, the worked example of a silently stale replay, and the complete banned-construct table:
`docs/determinism.md`.

## Step ids: a contract the body must reproduce

Every durable operation inside a workflow consumes one or more step ids, in call order. On
replay, the engine matches recorded checkpoints to the *position* a call falls at — so the body
must allocate ids in the same sequence every time. A `case`/`cond`/`if` whose branches consume a
different number of ids is the classic way to break this invisibly.

| Operation | Ids consumed |
|---|---|
| `defstep`/`deftransaction` call | 1 |
| `Dbos.sleep/1` | 1 |
| `Dbos.send_message/4` | 1 |
| `Dbos.recv_message/3` | 2 (`DBOS.recv` + an internal `DBOS.sleep`) |
| `Dbos.set_event/3` | 1 |
| `Dbos.get_event/4` | 2 (`DBOS.getEvent` + an internal `DBOS.sleep`) |
| `Dbos.write_stream/3` | 1 |
| `Dbos.close_stream/2` | 1 |
| `Dbos.enqueue/3` | 1 |
| `Dbos.fork/3` | 1 |
| `Dbos.status/2` | 1 |
| `Dbos.await/2` | 1 |
| Bare call to another `defworkflow` (child workflow) | 2 (start + `DBOS.getResult`) |

## `mix dbos.explain`

`mix dbos.explain Module.function/arity` prints the step-id sequence `Dbos.Explain` can derive
statically for a `defworkflow` body, and flags any `case`/`cond`/`if` whose branches consume an
uneven number of ids:

```
$ mix dbos.explain MyApp.Checkout.process_order/2
workflow "process_order" (MyApp.Checkout):
  id 0: step reserve_stock/1 ("reserve_stock/1")
  id 1: step charge_card/2 ("charge_card/2")
  id 2: step record_receipt/2 ("record_receipt/2")
```

A call this analysis cannot resolve to a local step, a same-module child workflow, or a `Dbos.*`
primitive — a comprehension, a captured function, a call into another module — is reported as
`CANNOT BE STATICALLY DETERMINED`, without a guess. An uneven branch is called out
explicitly:

```
case at id 2: UNEVEN ID ALLOCATION ACROSS BRANCHES — replaying a different branch than the one
originally taken will misalign every step id after this point. Use a patch here — a bare
conditional breaks replay.
```

Run it on any workflow before shipping a change to its body — it is the fastest way to see
whether a new branch just broke replay.
