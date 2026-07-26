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

`Dbos` needs a Postgres connection, and reuses your application's own — a bare `Postgrex`
pool (`Dbos.DB.Postgrex`) or an `Ecto.Repo` (`Dbos.DB.Ecto`). This guide uses `Ecto.Repo`.

## 2. Write a workflow

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

`defworkflow` requires an explicit `name:`. Recovery re-dispatches a crashed workflow by
looking up this name, so it must be a stable string you control, independent of the module
and function.

`reserve_stock` and `charge_card` are checkpoints: once one succeeds, its result is durably
recorded, and a replay of `process_order` after a crash returns that record.

## 3. Add the supervisor

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

`otp_app:` discovers every workflow module your application has compiled, so adding a workflow
needs no change here.

`Dbos` keeps its own tables under their own Postgres schema (`dbos` by default). Install it as a
migration in your own sequence:

```console
$ mix dbos.gen.migration
$ mix ecto.migrate
```

At boot `Dbos.Supervisor` verifies the schema is at the version it expects. It starts after
`MyApp.Repo`, so it has a live connection to check with.

## 4. Start the workflow with an id you control

`MyApp.Checkout.process_order(order_id, amount)` mints a random workflow id. `defworkflow` also
generates a dispatcher one arity higher, taking options — pin the id there so you can look the
workflow up from a fresh `iex` session after the crash:

```elixir
iex> {:ok, handle} = MyApp.Checkout.process_order("order-1", 4999, workflow_id: "order-1")
{:ok, %Dbos.WorkflowHandle{engine: Dbos, workflow_id: "order-1"}}
```

Within a couple of seconds:

```
reserve_stock: reserving stock for order-1
charge_card: charging 4999 for order-1...
```

`charge_card` is ten seconds into `Process.sleep(10_000)`, with nothing checkpointed for it.

## 5. Kill it mid-flight

From another terminal, find the BEAM's OS pid and `SIGKILL` it:

```sh
$ pgrep -f "iex -S mix"
83214
$ kill -9 83214
```

`reserve_stock`'s output is committed to `dbos.operation_outputs`. `charge_card` died
mid-sleep.

## 6. Restart, and watch it resume

```sh
$ iex -S mix
```

`Dbos.Recovery` scans for every `PENDING` workflow owned by this executor as soon as the
supervision tree comes up, before you type a command:

```
reserve_stock: reserving stock for order-1
charge_card: charging 4999 for order-1...
charge_card: charged order-1
```

`reserve_stock` stays silent — its checkpointed output at step 0 was replayed without running
the body. `charge_card` had no checkpoint, so it runs in full, and this time finishes.

```elixir
iex> Dbos.status("order-1")
{:ok, %Dbos.WorkflowStatus{status: :success, ...}}

iex> Dbos.result("order-1")
{:ok, %{reservation: %{order_id: "order-1", status: :reserved},
        charge: %{order_id: "order-1", amount: 4999, charge_id: "ch_order-1"}}}
```

A crash mid-step costs time. Correctness holds, and anything already checkpointed costs
nothing to redo.

## Where to next

- `guides/why-dbos.md` — what this solves, and when it's the wrong tool.
- `guides/architecture.md` — the model replay and recovery are built on.
- `guides/integrating-dbos.md` — wiring into a real Phoenix or OTP app.
