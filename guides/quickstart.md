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

Turn on the determinism compiler in the same file. It walks your whole application and warns
when a workflow reaches a nondeterministic call through a helper function:

```elixir
def project do
  [
    compilers: [:dbos] ++ Mix.compilers(),
    ...
  ]
end
```

See [the determinism contract](../docs/determinism.md) for what it checks and how to silence a finding.

## 2. Write a workflow

```elixir
defmodule MyApp.Checkout do
  use Dbos, repo: MyApp.Repo

  defworkflow process_order(order_id, amount), name: "MyApp.Checkout.process_order" do
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
looking up this name, so it must be unique across your application and stable across deploys.
Qualifying it with the defining module keeps both properties easy to hold.

`reserve_stock` and `charge_card` are checkpoints: once one succeeds, its result is durably
recorded, and a replay of `process_order` after a crash returns that record.

## 3. Install the tables

`Dbos` keeps its own tables under their own Postgres schema (`dbos` by default). Install it as a
migration in your own sequence:

```console
$ mix dbos.gen.migration
$ mix ecto.migrate
```

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
       otp_app: :my_app}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

`otp_app:` discovers every workflow module your application has compiled.

At boot `Dbos.Supervisor` verifies the schema is at the version it expects. It starts after
`MyApp.Repo`, so it has a live connection to check with.

## 5. Start the workflow with an id you control

Pass a `workflow_id:` so you can look the workflow up by id:

```elixir
iex> {:ok, handle} = MyApp.Checkout.process_order("order-1", 4999, workflow_id: "order-1")
{:ok, %Dbos.WorkflowHandle{engine: Dbos, workflow_id: "order-1"}}
```

Within a couple of seconds:

```
reserve_stock: reserving stock for order-1
charge_card: charging 4999 for order-1...
```

`charge_card` is partway through its ten-second sleep, with nothing checkpointed for it.

## 6. Kill it mid-flight

The workflow runs in its own process, registered under the id you pinned. Kill it from the same
`iex` session:

```elixir
iex> {:ok, pid} = Dbos.WorkflowSup.whereis(Dbos, "order-1")
iex> Process.exit(pid, :kill)
true
```

Workflow processes are `restart: :temporary`, so nothing brings this one back on its own.
`reserve_stock`'s output is committed to `dbos.operation_outputs`. `charge_card` died mid-sleep.

## 7. Recover it, and watch it resume

`Dbos.Recovery` redispatches every `PENDING` workflow owned by this executor:

```elixir
iex> Dbos.Recovery.recover_pending(Dbos)
["order-1"]
```

```
charge_card: charging 4999 for order-1...
charge_card: charged order-1
```

`reserve_stock` stays silent — its checkpointed output at step 0 was replayed without running
the body. `charge_card` had no checkpoint, so it runs in full, and this time finishes.

The same pass runs when the supervision tree comes up, so killing the whole node with `kill -9`
and restarting it recovers `order-1` before you can type a command.

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

- [Why Dbos](why-dbos.md) — what this solves, and when it's the wrong tool.
- [Architecture](architecture.md) — the model replay and recovery are built on.
- [Integrating Dbos](integrating-dbos.md) — wiring into a real Phoenix or OTP app.
