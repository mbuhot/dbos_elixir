# Dbos

A durable execution engine for Elixir, ported from [DBOS Transact](https://github.com/dbos-inc/dbos-transact-golang).

## The problem

A normal function call is gone the moment the process crashes. `Dbos` makes a function call
**durable**: every step it takes is checkpointed in your existing Postgres database, so a crash,
a deploy, or a killed node resumes exactly where it left off — no queue infrastructure, no
second database, no separate worker fleet.

```mermaid
flowchart LR
    A[workflow starts] --> B[step 1: reserve stock]
    B --> C[step 2: charge card]
    C -->|crash here| X((💥))
    X -.->|recovery replays from checkpoints| C2[step 2 result replayed from checkpoint]
    C2 --> D[step 3: ship order]
```

## What lives where

| Piece | What it gives you |
|---|---|
| Workflows, steps, transactions | `defworkflow`/`defstep`/`deftransaction` — durable functions |
| Queues | Concurrency limits, rate limits, priority, partitioning, delayed/debounced enqueue |
| Recovery | Crash/restart resumes from the last checkpoint, on this node or another |
| Scheduler | Cron-driven workflows, backed by the database, no separate scheduler process elsewhere |
| Admin server | An HTTP API to inspect, cancel, resume, and fork running workflows |
| Telemetry | `:telemetry` spans for workflow/step/queue/recovery events — see `docs/telemetry.md` |

## Quickstart

Add the dependency:

```elixir
def deps do
  [{:dbos, "~> 0.1.0"}]
end
```

Write a workflow:

```elixir
defmodule MyApp.Checkout do
  use Dbos, repo: MyApp.Repo

  defworkflow process_order(order_id, amount), name: "process_order" do
    charge = charge_card(order_id, amount)
    record_receipt(order_id, charge)
  end

  defstep charge_card(order_id, amount) do
    PaymentGateway.charge!(order_id, amount)
  end

  deftransaction record_receipt(order_id, charge) do
    Ecto.Multi.new()
    |> Ecto.Multi.insert(:receipt, %Receipt{order_id: order_id, charge_id: charge.id})
    |> MyApp.Repo.transaction()
  end
end
```

Start the engine in your supervision tree:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      MyApp.Repo,
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Ecto, MyApp.Repo},
       workflows: [MyApp.Checkout],
       migrations: :create_if_absent}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

Run your first durable workflow — a bare call to `process_order/2` is already durable. Called
from ordinary code (a controller, a test, `iex`), it starts the workflow and returns a handle
immediately, without blocking for however long the workflow takes to run:

```elixir
{:ok, handle} = MyApp.Checkout.process_order("order-123", 4999)
{:ok, result} = Dbos.await(handle)
```

The same call made from inside another workflow's body is a child workflow instead: it blocks
for the child's result and returns the unwrapped value directly, with no handle in sight —

```elixir
defworkflow parent_flow(order_id), name: "parent_flow" do
  receipt = process_order(order_id, 4999)
  receipt
end
```

Kill the BEAM mid-charge and restart: `process_order/2` resumes from whichever step last
checkpointed, without re-charging a card that already succeeded.

A capture of the workflow function (`&MyApp.Checkout.process_order/2`) resolves the same way a
name string does, everywhere a workflow name or capture is accepted (`Dbos.start/3`,
`Dbos.enqueue/3`).

### Start options

`defworkflow` also generates a second, one-arity-higher dispatcher taking every argument
explicitly plus a trailing options list — the way to pin a `workflow_id` (making a repeated
start idempotent), set `priority`, or a `deduplication_id`:

```elixir
MyApp.Checkout.process_order("order-123", 4999, workflow_id: "order-123-checkout")
```

### An inline step

`Dbos.step/2` checkpoints a one-off step without a named `defstep`:

```elixir
defworkflow process_order(order_id, amount), name: "process_order" do
  Dbos.step("notify_ops", fn -> OpsChannel.notify(order_id) end)
end
```

### Cancelling a workflow tree

`Dbos.cancel/2` cancels one workflow; `cancel_children: true` walks its child tree and cancels
every descendant too:

```elixir
Dbos.cancel(workflow_id, cancel_children: true)
```

## Supervision tree

```mermaid
flowchart TB
    Sup["Dbos.Supervisor (one per engine, namespaced by :name)"]
    Sup --> Reg[Dbos.Registry]
    Sup --> WSup[Dbos.WorkflowSup]
    Sup --> Rec[Dbos.Recovery]
    Sup --> Q[Dbos.Queue.Sup]
    Sup --> Sched[Dbos.Scheduler]
    Sup --> Notif[Dbos.Notifications]
    Sup -.optional.-> Cluster[Dbos.Cluster + NodeWatcher]
    Sup -.optional.-> Admin[Dbos.AdminServer]
```

Multiple engines can run in one BEAM (each given a distinct `:name`) — every process above is
namespaced under it. See `Dbos.Supervisor`'s module docs for every option, and:

- `docs/clustering.md` — dead-node work reclaim, opt-in, built on `:pg`/`:net_kernel`.
- `docs/telemetry.md` — every emitted `:telemetry` span.
- `docs/testing.md` — how the two test suites (unit/acceptance vs. Docker-based node-kill) work.
- `docs/interop-migration.md` — what is and isn't readable by a non-Elixir process.

## The determinism contract, in one paragraph

A workflow body re-executes from the top after a crash; steps that already completed return
their recorded output instead of running again. This is only correct if the body takes the same
path and calls steps with the same arguments every time it replays — a workflow computing a step
argument from a live read of mutable state (the current time, a random number, a database row
that can change between runs) risks the step returning a **stale** recorded result on replay
instead of an error, because step arguments are never themselves persisted. `defworkflow` catches
the common mistakes at compile time. Full contract, worked example, and the banned-construct
table: `docs/determinism.md`.

## Testing

`mix test` runs the unit/acceptance suite against a local Postgres. `mix test.integration` runs
a Docker-based suite that kills a whole node and checks a second node recovers it. See
`docs/testing.md`.
