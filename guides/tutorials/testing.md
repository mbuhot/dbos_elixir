# Testing

Testing *your* application's workflows and steps. (`docs/testing.md` covers the engine's own
suite.)

## The sandbox setup you already have

`Ecto.Adapters.SQL.Sandbox` wraps each test in a transaction and rolls it back at the end — the
setup most Elixir apps already run for every other table. `Dbos.Supervisor`'s `testing:` option
makes it work for `Dbos` too:

| `testing:` | Behavior |
|---|---|
| `:inline` | `Dbos.start/3` and `Dbos.enqueue/3` run the workflow synchronously, in the calling process, and hand back a handle to an already-finished workflow. |
| `:manual` | `Dbos.start/3` runs the same way; `Dbos.enqueue/3` only inserts the row — nothing runs until the test calls `Dbos.Testing.drain_queue/2` or `Dbos.Testing.drain_all/1`. |
| not set | Production behavior: background processes, real concurrency, real timing. |

Either mode starts none of `Dbos.Notifications` (the dedicated `LISTEN` connection), the wait
parker, the executor lease, the queue runners, `Dbos.Scheduler`, the boot recovery scan, the
cluster processes, or the admin server — every process that would otherwise touch the database on
its own schedule, outside your test's connection. Everything runs on the connection your test
already checked out, so there is no `Sandbox.allow/3` to call and no second connection to
configure.

```elixir
defmodule MyApp.Checkout.WorkflowTest do
  use MyApp.DataCase, async: true

  setup do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Ecto, MyApp.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       otp_app: :my_app,
       queues: [Dbos.Queue.new("reports"), Dbos.Queue.new("orders")],
       migrations: :skip,
       testing: :inline},
      id: name
    )

    {:ok, engine: name}
  end

  # tests go here
end
```

`migrations: :skip` assumes the schema was applied once already, by `test/test_helper.exs`. Declare
every queue the tests drain: `Dbos.Testing.drain_queue/2` raises `ArgumentError` for a queue the
engine doesn't know about.

## Testing a workflow end to end

Start it, await it, assert on the result:

```elixir
test "process_order reserves stock and charges the card", %{engine: engine} do
  {:ok, handle} = Dbos.start("process_order", ["order-1", 4999], engine: engine)

  assert {:ok, %{reservation: %{status: :reserved}, charge: %{charge_id: "ch_order-1"}}} =
           Dbos.await(handle)
end
```

A workflow that raises completes with status `:error`, and `await` hands back
`{:error, exception}`:

```elixir
test "a declined card surfaces as an error result", %{engine: engine} do
  {:ok, handle} = Dbos.start("process_order", ["order-2", 999_999_999], engine: engine)

  assert {:error, %MyApp.Checkout.CardDeclinedError{amount: 999_999_999}} = Dbos.await(handle)
end
```

## Testing a queue with `Dbos.Testing.drain_queue/2`

Reach for `:manual` mode when a test asserts on the enqueued-but-not-yet-run state as well as the
result:

```elixir
test "enqueuing a report job leaves it queued until drained", %{engine: engine} do
  {:ok, handle} =
    Dbos.enqueue("generate_report", [report_id], queue_name: "reports", engine: engine)

  assert {:ok, %Dbos.WorkflowStatus{status: :enqueued}} =
           Dbos.status(handle.workflow_id, engine: engine)

  assert Dbos.Testing.drain_queue("reports", engine: engine) == 1
  assert {:ok, %{status: :generated}} = Dbos.await(handle, engine: engine)
end
```

`drain_queue/2` claims and runs, synchronously in the caller, every currently dispatchable
workflow on that queue, and returns how many ran (`0` when there's nothing to claim).
`Dbos.Testing.drain_all/1` does the same across every declared queue, including the internal one
`Dbos.resume/2` and `Dbos.fork/3` target by default.

## Testing an approval flow: send before drain, then run

Under `:inline`/`:manual`, `recv_message/2` and `get_event/4` consume whatever is already there,
and raise `Dbos.TestingModeWaitError` immediately when nothing is pending. So enqueue, send, then
drain:

```elixir
test "an order waits for manual approval before shipping", %{engine: engine} do
  {:ok, handle} =
    Dbos.enqueue("ship_order", [order_id],
      queue_name: "orders",
      workflow_id: order_id,
      engine: engine
    )

  :ok = Dbos.send_message(order_id, "approval", :approved, engine: engine)

  assert Dbos.Testing.drain_queue("orders", engine: engine) == 1
  assert {:ok, :shipped} = Dbos.await(handle, engine: engine)
end
```

`Dbos.sleep/1` checkpoints the absolute wake time and returns immediately in these modes, so a
test never waits out the real duration. The recorded wake time is there to assert on.

A stream behaves the same way: an open stream with nothing left to read raises
`Dbos.TestingModeWaitError`. Write the stream (and close it, if the test needs a terminated read)
before reading it back. A stream whose producing workflow has reached a terminal status reads back
whatever was written and stops, in every mode.

## Simulating a crash with `Dbos.Testing.recover_pending/1`

Everything has already run synchronously by the time `Dbos.start/3` or `drain_queue/2` returns, so
simulate the crash at the row: flip a finished workflow back to `PENDING`, let
`Dbos.Testing.recover_pending/1` replay it, and prove an already-checkpointed step did not re-run
its body. An ETS counter bumped inside the step body is the usual way to show that:

```elixir
test "recovering a workflow does not re-run its already-checkpointed steps", %{engine: engine} do
  table = :ets.new(:counters, [:public, :set])
  workflow_id = "order-#{System.unique_integer([:positive])}"

  {:ok, handle} =
    Dbos.start("counted_steps_then_sleep", [table, 3, 3_600_000],
      engine: engine,
      workflow_id: workflow_id
    )

  {:ok, :woke} = Dbos.await(handle, engine: engine)
  assert :ets.lookup_element(table, :count, 2) == 3

  config = Dbos.config(engine)

  Dbos.DB.Ecto.query!(
    config.conn,
    "UPDATE dbos.workflow_status SET status = 'PENDING' WHERE workflow_uuid = $1",
    [workflow_id]
  )

  assert Dbos.Testing.recover_pending(engine: engine) == 1
  assert :ets.lookup_element(table, :count, 2) == 3
  assert {:ok, %Dbos.WorkflowStatus{status: :success}} = Dbos.status(workflow_id, engine: engine)
end
```

A workflow that was *enqueued* is recovered back to `ENQUEUED` for its queue to redistribute, so
follow `recover_pending/1` with `Dbos.Testing.drain_queue/2` before asserting on a terminal status.
The example above starts its workflow directly, which is redispatched and run in the same call.

The counter staying at `3` after recovery is the assertion that matters: proof the replayed steps
returned their recorded output without re-executing.

## What these modes cannot cover

`:inline`/`:manual` prove checkpointing, replay, and queue mechanics. The rest needs a real engine
with no testing mode set:

- A real `LISTEN`/`NOTIFY` wake.
- Two executors competing for the same `PENDING` row, or a lease expiring.
- Background timing: a queue runner's polling interval, the scheduler's cron reconciliation, a
  lease's renewal cadence.
- `workflow_timeout_ms` cancelling a workflow on the wall clock. The deadline is still resolved and
  persisted to `workflow_deadline_epoch_ms`, so a test can assert on it, and the background task
  that would call `Dbos.cancel/2` at that moment stays unarmed under these modes. Drive
  cancellation directly with `Dbos.cancel/2`.

## Asserting on checkpoints

`Dbos.Client.steps/2` returns every checkpointed `Dbos.StepInfo`, ordered by `function_id`, with
`output`/`error` already decoded:

```elixir
test "process_order checkpoints exactly two steps, in order", %{engine: engine} do
  {:ok, handle} = Dbos.start("process_order", ["order-1", 4999], engine: engine)
  {:ok, _result} = Dbos.await(handle)

  {:ok, steps} = Dbos.Client.steps(Dbos.config(engine), handle.workflow_id)

  assert Enum.map(steps, & &1.function_name) == ["reserve_stock/1", "charge_card/2"]
  assert Enum.map(steps, & &1.function_id) == [0, 1]
end
```

## Mocking a step's external call

Swap a step's external call with ordinary Elixir dependency injection: push the call behind a
module (an HTTP client, a payment gateway wrapper) and pass that module in as an argument, or read
it from `Application.get_env/2` at the call site inside the step body:

```elixir
defworkflow process_order(order_id, amount), name: "process_order" do
  charge_card(order_id, amount)
end

defstep charge_card(order_id, amount) do
  payment_client().charge(order_id, amount)
end

defp payment_client, do: Application.get_env(:my_app, :payment_client, MyApp.StripeClient)
```

Keep the `payment_client/0` read inside the step body, where mutable configuration is safe to
read: a value that changes between the original run and a replay breaks replay when it is read
from the workflow body. Configure `:my_app, :payment_client, MyApp.MockPaymentClient` in
`config/test.exs`, or override it per test with `Application.put_env/3`.
