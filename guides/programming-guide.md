# Learn Dbos

This guide builds one small application — an order checkout service — adding one durable
building block at a time. Each stage is runnable on its own. By the end the service survives a
mid-charge crash without double-charging a customer, and you'll know where to go for depth on
any one piece.

## Setup

Add `dbos` and `ecto_sql`/`postgrex` to your app, and give it an Ecto repo:

```elixir
def deps do
  [
    {:dbos, "~> 0.1.0"},
    {:ecto_sql, "~> 3.13"},
    {:postgrex, "~> 0.21"}
  ]
end
```

```elixir
defmodule MyApp.Repo do
  use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.Postgres
end
```

`Dbos` checkpoints into the same Postgres database your application already uses — a `dbos`
schema alongside your own tables, no second database to run.

## Your first workflow

A **workflow** is a durable function: if the process running it crashes partway through,
restarting the application resumes it from its last checkpoint. It never starts over. A
**step** is the unit that gets checkpointed — call it once, and its recorded result replaces the
call on every future replay.

```elixir
defmodule MyApp.Checkout do
  use Dbos

  defworkflow process_order(order_id, amount), name: "process_order" do
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
up this name — a stable identity for the workflow across deploys, independent of the module or
function it was defined in.

Wire the engine into your supervision tree:

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

Run it. A bare call to `process_order/2` is already durable — called from ordinary code (a
controller, a test, `iex`) it starts the workflow and returns a handle immediately:

```elixir
{:ok, handle} = MyApp.Checkout.process_order("order-1", 4999)
{:ok, result} = Dbos.await(handle)
```

If a step raises, the workflow crashes with it — nothing here retries yet. The next section adds
that.

## Adding retries to a step

`reserve_stock` calls an inventory service over the network — worth retrying on a transient
failure to avoid failing the whole order:

```elixir
defstep reserve_stock(order_id), max_retries: 3, base_interval_ms: 200, backoff_factor: 2.0 do
  MyApp.Inventory.reserve!(order_id)
end
```

Three retries with a 200ms base interval doubling each attempt: 200ms, 400ms, 800ms, then the
step's exception is re-raised (wrapped in `Dbos.MaxStepRetriesExceededError`) and the workflow
fails. See `guides/tutorials/steps.md` for the full backoff curve and defaults.

## A transaction: writing to your own database

`charge_card` calls an external gateway — the charge itself can't be part of a database
transaction. But recording the resulting receipt in your own tables should never happen without
also being checkpointed, and vice versa: a receipt row with no matching checkpoint (or a
checkpoint with no receipt row) is a state your application logic didn't ask for.

`deftransaction` opens one database transaction that holds both the user's write and the step's
own checkpoint:

```elixir
defmodule MyApp.Checkout do
  use Dbos, repo: MyApp.Repo

  defworkflow process_order(order_id, amount), name: "process_order" do
    reservation = reserve_stock(order_id)
    charge = charge_card(order_id, amount)
    receipt = record_receipt(order_id, charge)
    %{reservation: reservation, charge: charge, receipt: receipt}
  end

  # ...

  deftransaction record_receipt(order_id, charge) do
    MyApp.Repo.insert!(%MyApp.Receipt{order_id: order_id, charge_id: charge.charge_id})
  end
end
```

`use Dbos, repo: MyApp.Repo` also turns on a compile-time check: a direct call to `MyApp.Repo`
from inside `process_order`'s body (bypassing `deftransaction`) is now a compile error, since an
un-checkpointed write there would re-run on every replay. See `guides/tutorials/transactions.md`.

## A queue: running work with a concurrency limit

Shipping calls a slow carrier API. Running it inline would hold up the workflow, so enqueue it
onto a queue that limits how many ship calls run at once:

```elixir
defmodule MyApp.Application do
  def start(_type, _args) do
    children = [
      MyApp.Repo,
      {Dbos.Supervisor,
       name: Dbos,
       db: {Dbos.DB.Ecto, MyApp.Repo},
       workflows: [MyApp.Checkout],
       queues: [Dbos.Queue.new("shipping", worker_concurrency: 5)],
       migrations: :create_if_absent}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

```elixir
defworkflow process_order(order_id, amount), name: "process_order" do
  reservation = reserve_stock(order_id)
  charge = charge_card(order_id, amount)
  receipt = record_receipt(order_id, charge)
  {:ok, ship_handle} = Dbos.enqueue("ship_order", [order_id], queue_name: "shipping")
  %{reservation: reservation, charge: charge, receipt: receipt, shipment: ship_handle}
end

defworkflow ship_order(order_id), name: "ship_order" do
  MyApp.Carrier.ship!(order_id)
end
```

At most 5 `ship_order` workflows run at a time across the whole cluster, however many orders are
enqueued. `Dbos.enqueue/3` is itself a checkpointed step when called from inside a workflow — a
replay of `process_order` finds the enqueue already recorded and does not enqueue a second
shipment.

## Durable communication: waiting for a human

Suppose a large order needs manual approval before it ships. The workflow can block on a message
without holding a BEAM process pinned for however long that takes — a long wait durably parks
and rehydrates:

```elixir
defworkflow process_order(order_id, amount), name: "process_order" do
  reservation = reserve_stock(order_id)
  charge = charge_card(order_id, amount)
  receipt = record_receipt(order_id, charge)

  if amount > 10_000 do
    :approved = Dbos.recv_message("approval", :timer.hours(24))
  end

  {:ok, ship_handle} = Dbos.enqueue("ship_order", [order_id], queue_name: "shipping")
  %{reservation: reservation, charge: charge, receipt: receipt, shipment: ship_handle}
end
```

Something else — an admin action, a webhook handler — approves it from outside the workflow:

```elixir
Dbos.send_message(order_id, "approval", :approved)
```

See `guides/tutorials/workflows.md` for `set_event`/`get_event` and streams, the other two
durable communication primitives.

## Crash and resume

Start `process_order/2`, and kill the BEAM right after `charge_card` checkpoints but before
`record_receipt` runs — a `Process.exit` in a test, a `kill -9` on the node in a demo. Restart
the application. `Dbos.Supervisor` starts `Dbos.Recovery`, which re-dispatches every workflow
still `PENDING` for this engine automatically, before your application accepts its first request.

`process_order` runs again from the top:

- `reserve_stock` and `charge_card` return their recorded outputs. Neither re-runs, so the
  card is not charged twice.
- `record_receipt` runs for real, since it never got as far as checkpointing.

Nothing in `MyApp.Checkout` had to detect or handle the crash. This is the guarantee `defworkflow`
gives you, as long as the workflow body stays deterministic — see the determinism contract in
`docs/determinism.md`, and `mix dbos.explain` in `guides/tutorials/workflows.md` for a way to
check a workflow's step layout before you ship it.

## Where to go next

- `guides/tutorials/workflows.md` — workflow ids, handles, child workflows, the determinism
  contract, `mix dbos.explain`.
- `guides/tutorials/steps.md` — what belongs in a step, retries, what step arguments do (and do
  not) guarantee on replay.
- `guides/tutorials/transactions.md` — the two-write atomicity problem `deftransaction` solves,
  isolation levels, nesting rules.
