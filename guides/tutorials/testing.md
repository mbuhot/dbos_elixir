# Testing

This page is about testing *your* application's workflows and steps — the engine's own test
suite (`docs/testing.md`) is a different document, about testing `Dbos` itself.

## The sandbox setup you already have

`Ecto.Adapters.SQL.Sandbox` wraps each test in a transaction and rolls it back at the end — the
setup most Elixir apps already run for every other table. `Dbos.Supervisor`'s `testing:` option
makes it work for `Dbos` too:

| `testing:` | Behavior |
|---|---|
| `:inline` | `Dbos.start/3` and `Dbos.enqueue/3` run the workflow synchronously, in the calling process, and hand back a handle to an already-finished workflow. |
| `:manual` | `Dbos.start/3` runs the same way; `Dbos.enqueue/3` only inserts the row — nothing runs until the test calls `Dbos.Testing.drain_queue/2` or `Dbos.Testing.drain_all/1`. |
| not set | Production behavior: background processes, real concurrency, real timing. |

Either mode starts none of `Dbos.Notifications` (the dedicated `LISTEN` connection),
`Dbos.Waits`, `Dbos.Lease`, the queue runners, `Dbos.Scheduler`, the boot recovery scan, or
`Dbos.Cluster*` — every process that would otherwise touch the database on its own schedule,
outside your test's connection. With nothing left to race the sandbox, there is no
`Sandbox.allow/3` to call and no separate connection to configure: everything runs on the
connection your test already checked out.

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
       migrations: :skip,
       testing: :inline},
      id: name
    )

    {:ok, engine: name}
  end

  # tests go here
end
```

`migrations: :skip` — the schema's already applied once, by `test/test_helper.exs`, same as
`guides/integrating-dbos.md` describes. There is no `Dbos.Recovery.await_boot_recovery/1` call
here: `:inline`/`:manual` mode never starts the boot recovery scan, so there's nothing to wait
for.

## Testing a workflow end to end

Start it, await it, assert on the unwrapped result — indistinguishable from the non-testing
form, except it returns instantly:

```elixir
test "process_order reserves stock and charges the card", %{engine: engine} do
  {:ok, handle} = Dbos.start("process_order", ["order-1", 4999], engine: engine)

  assert {:ok, %{reservation: %{status: :reserved}, charge: %{charge_id: "ch_order-1"}}} =
           Dbos.await(handle)
end
```

A workflow that raises completes with status `:error`, and `await` hands back
`{:error, exception}` — the exception itself never propagates to the caller:

```elixir
test "a declined card surfaces as an error result", %{engine: engine} do
  {:ok, handle} = Dbos.start("process_order", ["order-2", 999_999_999], engine: engine)

  assert {:error, %MyApp.Checkout.CardDeclinedError{amount: 999_999_999}} = Dbos.await(handle)
end
```

## Testing a queue with `Dbos.Testing.drain_queue/2`

`:manual` mode is the one to reach for when a test needs to assert on the *before*-drain state —
an enqueued-but-not-yet-run row — as well as the result:

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
workflow on that queue, and returns how many ran — `0`, not an error, when there's nothing to
claim. `Dbos.Testing.drain_all/1` does the same across every declared queue, including the
internal one `Dbos.resume/2` and `Dbos.fork/3` target by default.

## Testing an approval flow: send before drain, then run

`recv_message/2` and `get_event/4` only ever look at what's already there under `:inline`/
`:manual`: a pending message is consumed immediately; nothing pending raises
`Dbos.TestingModeWaitError` right away rather than blocking the test process. The send-before-run
order already works, so enqueue-then-send-then-drain is the way to test a workflow waiting on an
external signal:

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

`Dbos.sleep/1` follows the same don't-block rule for a different reason: it checkpoints the
absolute wake time and returns immediately, so a test never waits out the real duration — the
recorded wake time is still there to assert on afterward.

## Simulating a crash and asserting recovery with `Dbos.Testing.recover_pending/1`

There's no live process to `Process.exit/2` under `:inline`/`:manual` — everything already ran,
synchronously, by the time `Dbos.start/3` or `drain_queue/2` returned. Simulate the crash at the
row instead: flip a finished workflow back to `PENDING` and let `Dbos.Testing.recover_pending/1`
replay it, then prove a step that already checkpointed did not run its body again — an ETS
counter bumped only inside the step body, never observable from a replayed, short-circuited
call, is the usual way to prove that:

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

The counter staying at `3` after recovery — never `6` — is the assertion that matters: proof the
replayed steps returned their recorded output without re-executing.

## What these modes cannot cover

`:inline`/`:manual` prove checkpointing, replay, and queue mechanics — not the things that only
exist once real background processes are racing each other:

- A real `LISTEN`/`NOTIFY` wake — `Dbos.Notifications` never starts.
- Two executors competing for the same `PENDING` row, or a lease actually expiring.
- Background timing: a queue runner's polling interval, the scheduler's cron reconciliation, a
  lease's renewal cadence.

Those need a real, running engine against a real database with no testing mode set — which is
exactly what `Dbos`'s own suite (`test/test_helper.exs`, `test/support/case.ex`) uses throughout.

## Asserting on checkpoints

`Dbos.SystemDb.get_workflow_steps/2` returns every checkpointed `Dbos.StepInfo`, ordered by
`function_id`, with `output`/`error` already decoded:

```elixir
test "process_order checkpoints exactly two steps, in order", %{engine: engine} do
  {:ok, handle} = Dbos.start("process_order", ["order-1", 4999], engine: engine)
  {:ok, _result} = Dbos.await(handle)

  config = Dbos.config(engine)
  {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)

  assert Enum.map(steps, & &1.function_name) == ["reserve_stock/1", "charge_card/2"]
  assert Enum.map(steps, & &1.function_id) == [0, 1]
end
```

## Testing determinism violations are caught at compile time

The determinism checker (`Dbos.Determinism`) runs at compile time on every `defworkflow` body.
Test a violation by compiling a small fixture module at test time and asserting on the raised
`CompileError`, the same way `test/dbos/determinism_test.exs` tests the engine's own checker:

```elixir
test "a workflow calling DateTime.utc_now/0 directly fails to compile" do
  source = """
  defmodule MyApp.BadWorkflowFixture do
    use Dbos

    defworkflow run(x), name: "bad_workflow" do
      DateTime.utc_now()
    end
  end
  """

  error =
    assert_raise CompileError, fn ->
      Code.compile_string(source, "test/fixture.ex")
    end

  assert error.description =~ "DateTime.utc_now"
  assert error.description =~ "nondeterministic"
end
```

This is worth one or two smoke tests in your own suite mainly to confirm `use Dbos` is wired up
the way you expect (the right `:repo`, the right `warn_cross_module_calls` setting). `Dbos`'s own
tests already cover the checker itself.

## Mocking a step's external call

A step is checkpointed by its **name and position** alone — there's no built-in way to swap what
a given `defstep` calls at test time other than ordinary Elixir dependency injection. Push the
external call behind a module (an HTTP client, a payment gateway wrapper) and pass that module in
as an argument, or read it from `Application.get_env/2` at the call site inside the step body:

```elixir
defworkflow process_order(order_id, amount), name: "process_order" do
  charge_card(order_id, amount)
end

defstep charge_card(order_id, amount) do
  payment_client().charge(order_id, amount)
end

defp payment_client, do: Application.get_env(:my_app, :payment_client, MyApp.StripeClient)
```

`payment_client/0` is read from inside the step body. A live read of mutable configuration
belongs there — inside `defworkflow` itself, a value that could change between the original run
and a replay is exactly what breaks replay (`docs/determinism.md`). Configure
`:my_app, :payment_client, MyApp.MockPaymentClient` in `config/test.exs`, or override it per test
with `Application.put_env/3`.
