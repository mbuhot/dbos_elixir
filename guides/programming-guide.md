# Learn Dbos

This guide builds one small application — an order checkout service — adding one durable
building block at a time. Each stage runs on its own. By the end the service survives a
mid-charge crash without double-charging a customer.

## Setup

Add the dependencies:

```elixir
def deps do
  [
    {:dbos, "~> 0.1.0"},
    {:ecto_sql, "~> 3.13"},
    {:postgrex, "~> 0.21"}
  ]
end
```

`Dbos` checkpoints into the Postgres database your application already uses, under a `dbos`
schema alongside your own tables. Install that schema as a migration in your own sequence:

```
mix dbos.gen.migration
mix ecto.migrate
```

## Your first workflow

A **workflow** is a durable function: if the process running it crashes partway through,
restarting the application resumes it from its last checkpoint. A **step** is the unit that gets
checkpointed — call it once, and its recorded result replaces the call on every future replay.

```elixir
defmodule MyApp.Checkout do
  use Dbos

  defworkflow process_order(order_id, amount), name: "MyApp.Checkout.process_order" do
    reservation = reserve_stock(order_id)
    charge = charge_card(order_id, amount)
    %{reservation: reservation, charge: charge}
  end

  defstep reserve_stock(order_id) do
    MyApp.Inventory.reserve!(order_id)
  end

  defstep charge_card(order_id, amount) do
    MyApp.PaymentGateway.charge!(order_id, amount)
  end
end
```

`defworkflow` requires an explicit `name:`. Recovery re-dispatches a crashed workflow by looking
up this name, so it must be unique across your application and stable across deploys. Qualifying
it with the defining module keeps both properties easy to hold.

Wire the engine into your supervision tree, after your repo:

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      MyApp.Repo,
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Ecto, MyApp.Repo},
       otp_app: :my_app}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

`otp_app: :my_app` finds every workflow module compiled into that application, so
`MyApp.Checkout` is registered by being compiled.

A bare call to `process_order/2` is already durable. Called from ordinary code — a controller, a
test, `iex` — it starts the workflow and returns a handle immediately:

```elixir
{:ok, handle} = MyApp.Checkout.process_order("order-1", 4999)
{:ok, result} = Dbos.await(handle)
```

If a step raises, the workflow crashes with it. The next section adds retries.

## Adding retries to a step

`reserve_stock` calls an inventory service over the network — worth retrying on a transient
failure:

```elixir
defstep reserve_stock(order_id), max_retries: 3, base_interval_ms: 200, backoff_factor: 2.0 do
  MyApp.Inventory.reserve!(order_id)
end
```

Three retries with a 200ms base interval doubling each attempt: 200ms, 400ms, 800ms, then the
step's exception is re-raised wrapped in `Dbos.MaxStepRetriesExceededError` and the workflow
fails.

## A transaction: writing to your own database

`charge_card` calls an external gateway, so the charge itself can't be part of a database
transaction. Recording the resulting receipt in your own tables should always happen together
with its checkpoint: a receipt row with no matching checkpoint, or a checkpoint with no receipt
row, is a state your application logic never asked for.

`deftransaction` opens one database transaction holding both the user's write and the step's own
checkpoint:

```elixir
defmodule MyApp.Checkout do
  use Dbos, repo: MyApp.Repo

  defworkflow process_order(order_id, amount), name: "MyApp.Checkout.process_order" do
    reservation = reserve_stock(order_id)
    charge = charge_card(order_id, amount)
    receipt = record_receipt(order_id, charge)
    %{reservation: reservation, charge: charge, receipt: receipt}
  end

  deftransaction record_receipt(order_id, charge) do
    MyApp.Repo.insert!(%MyApp.Receipt{order_id: order_id, charge_id: charge.charge_id})
  end
end
```

`use Dbos, repo: MyApp.Repo` also turns on a compile-time check: a direct call to `MyApp.Repo`
from inside `process_order`'s body is a compile error, since an un-checkpointed write there
would re-run on every replay.

## A queue: running work with a concurrency limit

Shipping calls a slow carrier API. Enqueue it onto a queue that caps how many ship calls run at
once:

```elixir
{Dbos.Supervisor,
 name: Dbos,
 db: {Dbos.DB.Ecto, MyApp.Repo},
 otp_app: :my_app,
 queues: [Dbos.Queue.new("shipping", worker_concurrency: 5)]}
```

```elixir
defworkflow process_order(order_id, amount), name: "MyApp.Checkout.process_order" do
  reservation = reserve_stock(order_id)
  charge = charge_card(order_id, amount)
  receipt = record_receipt(order_id, charge)
  {:ok, ship_handle} = Dbos.enqueue("MyApp.Checkout.ship_order", [order_id], queue_name: "shipping")
  %{reservation: reservation, charge: charge, receipt: receipt, shipment: ship_handle}
end

defworkflow ship_order(order_id), name: "MyApp.Checkout.ship_order" do
  MyApp.Carrier.ship!(order_id)
end
```

`worker_concurrency: 5` lets each running instance dispatch at most 5 `ship_order` workflows at
a time, however many orders are enqueued. Use `global_concurrency:` for a ceiling shared across
the whole cluster.

`Dbos.enqueue/3` is itself a checkpointed step when called from inside a workflow — a replay of
`process_order` finds the enqueue already recorded and skips it. Here it hands back a handle and
lets `process_order` finish while the shipment waits its turn. Write `ship_order(order_id,
queue_name: "shipping")` instead when the order is not done until the carrier call is: that queues
the child the same way and blocks for its result.

For bursty work where only the last call matters, `Dbos.debounce/3` collapses repeated enqueues
of the same key into one delayed workflow.

## Durable communication: waiting for a human

A large order needs manual approval before it ships. The workflow blocks on a message; a long
wait durably parks and rehydrates rather than pinning a BEAM process:

```elixir
defworkflow process_order(order_id, amount), name: "MyApp.Checkout.process_order" do
  reservation = reserve_stock(order_id)
  charge = charge_card(order_id, amount)
  receipt = record_receipt(order_id, charge)

  if amount > 10_000 do
    :approved = Dbos.recv_message("approval", :timer.hours(24))
  end

  {:ok, ship_handle} = Dbos.enqueue("MyApp.Checkout.ship_order", [order_id], queue_name: "shipping")
  %{reservation: reservation, charge: charge, receipt: receipt, shipment: ship_handle}
end
```

An admin action or webhook handler approves it from outside, addressing the workflow by id. Pin
that id at start time so the sender knows it:

```elixir
{:ok, handle} = MyApp.Checkout.process_order("order-1", 40_000, workflow_id: "order-1")

Dbos.send_message("order-1", "approval", :approved)
```

Every `defworkflow` gets a second dispatcher at one arity higher taking this trailing options
list. It covers the whole dispatch surface: `workflow_id:` to pin the id, `queue_name:` to route
the workflow onto a queue, `delay_ms:`, `priority:`, `timeout_ms:`. An option the dispatch cannot
honour raises `Dbos.InvalidWorkflowOptionError` at the call.

## Crash and resume

Start `process_order/2` and kill the BEAM right after `charge_card` checkpoints but before
`record_receipt` runs. Restart the application. `Dbos.Supervisor` starts `Dbos.Recovery`, which
re-dispatches every workflow still `PENDING` for this engine before your application accepts its
first request.

`process_order` runs again from the top:

| Step | On replay |
|---|---|
| `reserve_stock` | Returns its recorded output |
| `charge_card` | Returns its recorded output — the card is not charged twice |
| `record_receipt` | Runs for real; it never got as far as checkpointing |

Nothing in `MyApp.Checkout` had to detect or handle the crash. This is the guarantee
`defworkflow` gives you, as long as the workflow body stays deterministic.

## Where to go next

- [Workflows](tutorials/workflows.md) — workflow ids, handles, child workflows, `mix dbos.explain`.
- [Steps](tutorials/steps.md) — what belongs in a step, retries, step arguments on replay.
- [Transactions](tutorials/transactions.md) — isolation levels and nesting rules.
- [Queues](tutorials/queues.md) — concurrency, rate limits, priorities, debouncing.
- [Workflow Communication](tutorials/workflow-communication.md) — messages, events, and streams.
- [Testing](tutorials/testing.md) — sandbox setup, queue tests, crash-and-recover tests.
- [Determinism](../docs/determinism.md) — the determinism contract a workflow body must hold to.
