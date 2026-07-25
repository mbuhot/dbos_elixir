# Get Started

By the end of this page you'll have a workflow running against a real Postgres database,
you'll kill the BEAM in the middle of it, and you'll watch it pick up exactly where it left
off.

## 1. Add the dependency

```elixir
def deps do
  [{:dbos, "~> 0.1.0"}]
end
```

`Dbos` needs a Postgres connection. It reuses your application's own — either a bare
`Postgrex` pool (`Dbos.DB.Postgrex`) or an existing `Ecto.Repo` (`Dbos.DB.Ecto`). This guide
uses `Ecto.Repo`, since most Phoenix and OTP apps already have one.

## 2. Create the schema

`Dbos` keeps its own tables, under their own Postgres schema (`dbos` by default), separate
from your application's tables. In development, let it create that schema for you the first
time it starts — that's what `migrations: :create_if_absent` below does. (In production you
verify a schema that's already there; see `guides/integrating-dbos.md`.)

## 3. Write a workflow

```elixir
defmodule MyApp.Checkout do
  use Dbos, repo: MyApp.Repo

  defworkflow process_order(order_id, amount), name: "process_order" do
    reservation = reserve_stock(order_id)
    charge = charge_card(order_id, amount)
    %{reservation: reservation, charge: charge}
  end

  defstep reserve_stock(order_id) do
    IO.puts("reserve_stock: reserving stock for #{order_id}")
    %{order_id: order_id, status: :reserved}
  end

  defstep charge_card(order_id, amount) do
    IO.puts("charge_card: charging #{amount} for #{order_id}...")
    Process.sleep(10_000)
    IO.puts("charge_card: charged #{order_id}")
    %{order_id: order_id, amount: amount, charge_id: "ch_#{order_id}"}
  end
end
```

`defworkflow` requires an explicit `name:` — recovery re-dispatches a crashed workflow by
looking up this name. It has to be a stable string you control, independent of the module or
function. Read `reserve_stock` and `charge_card` as two checkpoints: once either one succeeds,
its result is durably recorded, and a replay of `process_order` after a crash will not run it
again.

## 4. Add the supervisor

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

`Dbos.Supervisor` starts after `MyApp.Repo` — it needs a live connection to verify or create
its schema before anything else in the tree runs.

## 5. Start the workflow, with a name you control

A bare call to `MyApp.Checkout.process_order(order_id, amount)` works, but it always mints a
random workflow id. For this demo, pin the id explicitly with `Dbos.start/3` so you can look
the workflow up again from a fresh `iex` session after the crash:

```elixir
iex> {:ok, handle} = Dbos.start("process_order", ["order-1", 4999], workflow_id: "order-1")
{:ok, %Dbos.WorkflowHandle{engine: Dbos, workflow_id: "order-1"}}
```

Within a couple of seconds you should see:

```
reserve_stock: reserving stock for order-1
charge_card: charging 4999 for order-1...
```

`charge_card` is now ten seconds into `Process.sleep(10_000)` — and nothing has been
checkpointed for it yet.

## 6. Kill it mid-flight

From another terminal, find the BEAM's OS pid and send it `SIGKILL` for an actual crash:

```sh
$ pgrep -f "iex -S mix"
83214
$ kill -9 83214
```

The node is gone. `reserve_stock`'s output is safely committed to `dbos.operation_outputs`;
`charge_card` was killed mid-sleep and never checkpointed anything.

## 7. Restart, and watch it resume

```sh
$ iex -S mix
```

`Dbos.Recovery` scans for every `PENDING` workflow owned by this executor as soon as the
supervision tree comes up, before you type a single command. Watch the shell:

```
reserve_stock: reserving stock for order-1
charge_card: charging 4999 for order-1...
charge_card: charged order-1
```

`reserve_stock` does **not** print again — its checkpointed output from step 0 was replayed
without running the step body a second time. `charge_card` never got that far last time, so
it reruns in full, charges again, and this time finishes and checkpoints.

Confirm it from `iex`:

```elixir
iex> Dbos.status("order-1")
{:ok, %Dbos.WorkflowStatus{status: :success, ...}}

iex> Dbos.result("order-1")
{:ok, %{reservation: %{order_id: "order-1", status: :reserved},
        charge: %{order_id: "order-1", amount: 4999, charge_id: "ch_order-1"}}}
```

That's the whole point of the engine: a crash mid-step costs time. Correctness holds, and
anything already checkpointed costs nothing to redo.

## Where to next

- `guides/why-dbos.md` — what problem this solves and when it's the wrong tool.
- `guides/architecture.md` — how replay, checkpoints, and recovery actually work under the hood.
- `guides/integrating-dbos.md` — wiring this into a real Phoenix or OTP app: migrations,
  the LISTEN connection, executor identity, and tests.
