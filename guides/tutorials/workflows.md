# Workflows

A workflow is a durable function. Its steps checkpoint to Postgres as they complete, so a crash,
a deploy, or a killed node resumes the workflow from its last checkpoint.

## Guarantees

- **Once started, a workflow runs to completion** (or a terminal failure) across process and node
  restarts, as long as some engine with that workflow registered comes back up.
- **A step that already checkpointed returns its recorded output on every future replay.** Its
  body stays unexecuted.
- **An exception a step raises is checkpointed and re-raised on replay.** A workflow that failed
  for a real reason fails the same way again.

These guarantees hold while the workflow body is deterministic — see the summary below.

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

`name:` is required — recovery dispatches a `PENDING` workflow row by its `name` column. Moving
`process_order/2` to a different module, or renaming the function, leaves an in-flight workflow
row resolvable as long as `name:` stays the same. Compiling without `name:` is a `CompileError`.

A workflow head cannot carry a `when` guard: recovery maps one name to exactly one deterministic
body, so branching goes inside the body.

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

## Starting a workflow

A bare call to a `defworkflow`-defined function is already durable. Its return type depends on
where you call it from:

| Call site | What happens | Return value |
|---|---|---|
| Outside a workflow (a controller, a test, `iex`) | Starts a root workflow, does not wait for it | `{:ok, %Dbos.WorkflowHandle{}}` |
| Inside another workflow's body | Runs as a child workflow, blocks until it finishes | The child's unwrapped result |
| No engine started | — | raises `Dbos.NotStartedError` |

```elixir
{:ok, handle} = MyApp.Checkout.process_order("order-1", 4999)
{:ok, result} = Dbos.await(handle)
```

`Dbos.start/3` does the same thing by workflow name (or a `&Mod.fun/n` capture), and accepts
`:workflow_id`, `:engine`, `:deduplication_id`, `:priority`, `:timeout_ms`,
`:application_version`:

```elixir
{:ok, handle} =
  Dbos.start("process_order", ["order-1", 4999], workflow_id: "checkout-order-1")
```

`Dbos.enqueue/3` starts a workflow by placing it on a named queue for later dispatch, subject to
that queue's concurrency, rate limit, and priority rules — see `guides/tutorials/queues.md`.

```elixir
{:ok, handle} =
  Dbos.enqueue("ship_order", [order_id], queue_name: "shipping", priority: 1)
```

## Workflow ids

Every workflow has a string id, random by default. Supply your own with `:workflow_id` to make
starting the *same* logical operation twice a no-op: starting a workflow id that already has a
row returns a handle to the existing run.

```elixir
Dbos.start("process_order", [order_id, amount], workflow_id: "checkout-#{order_id}")
```

## Handles and awaiting

`Dbos.start/3` and `Dbos.enqueue/3` return `{:ok, %Dbos.WorkflowHandle{engine:, workflow_id:}}`
immediately. `Dbos.await/2` blocks until that workflow reaches a terminal status:

```elixir
case Dbos.await(handle, timeout_ms: 30_000) do
  {:ok, output} -> output
  {:error, exception} -> raise exception
  {:error, :timeout} -> :still_running
end
```

Called from inside a workflow, `Dbos.await/2` checkpoints the outcome once it is known. A
`{:error, :timeout}` outcome is left uncheckpointed, so replaying a still-waiting await waits
again.

## Child workflows

A bare call, or `Dbos.start/3`, made from inside a workflow body is a **child workflow**: it
starts as its own row, is awaited inline, and its unwrapped result comes back as an ordinary
return value.

```elixir
defworkflow parent_flow(order_id), name: "parent_flow" do
  process_order(order_id, 4999)
end
```

A child's id defaults to `"<parent_id>-<parent_step_id>"`, derived from the parent's own id and
the step position the child started at, so replaying the parent finds the same child id already
recorded and reuses it.

## The determinism contract, in summary

A workflow body must take the same path and call the same steps with the same arguments every
time it replays. Step **arguments are never stored** — only each step's *output* is — so a step
called with different arguments on replay returns the recorded output for its position, silently.
`defworkflow` rejects the mechanically-detectable violations (`:rand.*`, `DateTime.utc_now/0`,
`System.os_time/0`, `Process.sleep/1`, bare `receive`, `send/2`, `spawn`, `Task.*`, `make_ref/0`,
and — with `use Dbos, repo: MyApp.Repo` — a direct `Repo` call) at compile time. The full
contract, a worked example of a silently stale replay, and the complete banned-construct table
live in `docs/determinism.md`.

## Step ids

Every durable operation inside a workflow consumes one or more step ids, in call order. On
replay, the engine matches recorded checkpoints to the *position* a call falls at, so the body
must allocate ids in the same sequence every time. A `case`/`cond`/`if` whose branches consume a
different number of ids is the classic way to break this invisibly.

| Operation | Ids consumed |
|---|---|
| `defstep`/`deftransaction` call, `Dbos.step/2`, `Dbos.transaction/3` | 1 |
| `Dbos.sleep/1` | 1 |
| `Dbos.send_message/4` | 1 |
| `Dbos.recv_message/3` | 2 (`DBOS.recv` + an internal sleep) |
| `Dbos.set_event/3` | 1 |
| `Dbos.get_event/4` | 2 (`DBOS.getEvent` + an internal sleep) |
| `Dbos.write_stream/3`, `Dbos.close_stream/2` | 1 |
| `Dbos.enqueue/3`, `Dbos.debounce/3` | 1 |
| `Dbos.start/3` | 1 |
| `Dbos.await/2` | 1 |
| `Dbos.status/2`, `Dbos.cancel/2`, `Dbos.resume/2`, `Dbos.fork/3` | 1 |
| Bare call to another `defworkflow` (child workflow) | 2 (start + `DBOS.getResult`) |
| `Dbos.patch/1` | 0 or 1, decided at runtime |

## `mix dbos.explain`

`mix dbos.explain Module.function/arity` prints the step-id sequence it derives
statically from a `defworkflow` body:

```
$ mix dbos.explain MyApp.Checkout.process_order/2
workflow "process_order" (MyApp.Checkout):
  id 0: step reserve_stock/1 ("reserve_stock/1")
  id 1: step charge_card/2 ("charge_card/2")
  id 2: step record_receipt/2 ("record_receipt/2")
```

A call the analysis cannot resolve to a local step, a same-module child workflow, or a `Dbos.*`
primitive — a comprehension, a captured function, a call into another module — is reported as
`CANNOT BE STATICALLY DETERMINED`. A branch whose arms allocate different numbers of ids is
called out explicitly:

```
case at id 2: UNEVEN ID ALLOCATION ACROSS BRANCHES — replaying a different branch than the one
originally taken will misalign every step id after this point. Use a patch instead of a bare
conditional here.
```

Run it on a workflow before shipping a change to its body — it is the fastest way to see whether
a new branch just broke replay.
