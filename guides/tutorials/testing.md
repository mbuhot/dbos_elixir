# Testing

This page is about testing *your* application's workflows and steps — the engine's own test
suite (`docs/testing.md`) is a different document, about testing `Dbos` itself.

It builds on `guides/integrating-dbos.md`'s "What to do in tests" section: a scratch Postgres
database with the schema applied once, a fresh engine `:name` per test, `migrations: :skip`, and
truncation between tests. Read that section first if you haven't set up a test engine yet. This
page goes further: asserting on checkpoints, simulating a crash, testing determinism violations,
and mocking a step's external call.

## Why not the Ecto sandbox

`Ecto.Adapters.SQL.Sandbox` wraps each test in a transaction and rolls it back at the end — the
usual reason a test suite runs fast and stays isolated without truncating tables by hand. It
does not work for `Dbos`, for a structural reason, not a configuration one:

`Dbos.Notifications` opens a **dedicated `LISTEN` connection**, separate from your Ecto pool
(`guides/integrating-dbos.md`, "The dedicated LISTEN connection"). Postgres only delivers a
`NOTIFY` to a listener once the transaction that fired it **commits**. A sandboxed test's
writes live inside a transaction that's rolled back, never committed — so the checkpoint insert
that should have triggered a wake for a blocked `recv_message`/`get_event`/stream reader never
fires one. The listener isn't sharing the sandbox's connection to begin with, so even the
non-blocking reads (`Dbos.status/2`, `SystemDb.get_workflow_status/2` from your test process, if
that process isn't the one enrolled in the sandboxed connection) can see a different, empty view
of the same rows.

`Dbos`'s own suite (`test/support/case.ex`, `Dbos.Case`) sidesteps this entirely: it runs against
a real Postgrex connection with autocommit, and truncates the `dbos` tables between tests instead
of rolling back a transaction. Do the same in your application's tests — point the test engine at
a real, committing connection (an `Ecto.Repo` in its ordinary, non-sandboxed mode is fine; so is
a bare `Postgrex` pool), and truncate between tests.

If you truly need the sandbox for the rest of your test's assertions (checking your own
application tables), keep `Dbos`'s connection and the dedicated LISTEN connection outside it —
run `Dbos` against a second, non-sandboxed pool or repo pointed at the same test database, and
accept that your `Dbos`-related assertions won't roll back with the rest of the test.

## Setting up a test engine

```elixir
defmodule MyApp.Checkout.WorkflowTest do
  use MyApp.DataCase, async: false

  setup do
    name = Module.concat(__MODULE__, :"Engine#{System.unique_integer([:positive])}")

    start_supervised!(
      {Dbos.Supervisor,
       name: name,
       db: {Dbos.DB.Ecto, MyApp.Repo},
       executor_id: "test-#{System.unique_integer([:positive])}",
       workflows: [MyApp.Checkout],
       migrations: :skip},
      id: name
    )

    Dbos.Recovery.await_boot_recovery(name)
    {:ok, engine: name}
  end

  # tests go here
end
```

`async: false` — workflows run in their own processes against a shared database; two test cases
racing the same engine name or the same workflow id will interfere with each other. Truncate the
`dbos` tables in your case template's `setup`, the same way `Dbos.Case` does (`test/support/case.ex`).

## Testing a workflow end to end

Start it, await it, assert on the unwrapped result:

```elixir
test "process_order reserves stock and charges the card", %{engine: engine} do
  {:ok, handle} = Dbos.start("process_order", ["order-1", 4999], engine: engine)

  assert {:ok, %{reservation: %{status: :reserved}, charge: %{charge_id: "ch_order-1"}}} =
           Dbos.await(handle)
end
```

A workflow that raises completes `:error`, not an exception propagated to the caller — `await`
hands back `{:error, exception}`:

```elixir
test "a declined card surfaces as an error result", %{engine: engine} do
  {:ok, handle} = Dbos.start("process_order", ["order-2", 999_999_999], engine: engine)

  assert {:error, %MyApp.Checkout.CardDeclinedError{amount: 999_999_999}} = Dbos.await(handle)
end
```

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

## Simulating a crash and asserting recovery

Kill the workflow's live process directly (`Dbos.WorkflowSup.whereis/2` finds it), confirm the
row is left `PENDING`, then trigger recovery and confirm the finished result — while proving a
step that already checkpointed did not run its body again. An ETS or `:persistent_term` counter
bumped only inside the step body (never observable from a replayed, short-circuited call) is the
usual way to prove that:

```elixir
test "a crash before the second step does not re-run the first", %{engine: engine} do
  config = Dbos.config(engine)
  {:ok, handle} = Dbos.start("blocking_workflow", [:ignored], engine: engine)

  wait_until(fn ->
    {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
    length(steps) == 1
  end)

  {:ok, pid} = Dbos.WorkflowSup.whereis(engine, handle.workflow_id)
  Process.exit(pid, :kill)

  {:ok, status} = SystemDb.get_workflow_status(config, handle.workflow_id)
  assert status.status == :pending

  Dbos.Recovery.recover_pending(engine)

  {:ok, new_pid} = Dbos.WorkflowSup.whereis(engine, handle.workflow_id)
  send(new_pid, :go)

  assert {:ok, :done} = Dbos.await(handle)

  {:ok, steps} = SystemDb.get_workflow_steps(config, handle.workflow_id)
  assert Enum.map(steps, & &1.function_id) == [0, 1]
  assert :persistent_term.get({:first_step_runs, handle.workflow_id}) == 1
end

defp wait_until(fun, attempts \\ 50)
defp wait_until(_fun, 0), do: flunk("condition never became true")

defp wait_until(fun, attempts) do
  if fun.(), do: :ok, else: (Process.sleep(20); wait_until(fun, attempts - 1))
end
```

The step count staying at exactly `[0, 1]` (rather than `[0, 0, 1]` or similar) after recovery is
the assertion that matters — it's proof the replayed step 0 returned its recorded output instead
of re-executing.

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
the way you expect (the right `:repo`, the right `warn_cross_module_calls` setting) — not to
re-verify the checker itself, which is already covered by `Dbos`'s own tests.

## Mocking a step's external call

A step is checkpointed by its **name and position**, never by which module or function backed
it — there's no built-in way to swap what a given `defstep` calls at test time other than
ordinary Elixir dependency injection. Push the external call behind a module (an HTTP client, a
payment gateway wrapper) and pass that module in as an argument, or read it from
`Application.get_env/2` at the call site inside the step body:

```elixir
defworkflow process_order(order_id, amount), name: "process_order" do
  charge_card(order_id, amount, payment_client())
end

defstep charge_card(order_id, amount, client) do
  client.charge(order_id, amount)
end

defp payment_client, do: Application.get_env(:my_app, :payment_client, MyApp.StripeClient)
```

Note that `payment_client/0` itself must not be called from inside the workflow body directly if
it can change across replay — call it from inside the step (as above), or make it a step's
argument computed from a prior step's output, never a live read of mutable configuration
resolved inside `defworkflow` itself (`docs/determinism.md`). Configure
`:my_app, :payment_client, MyApp.MockPaymentClient` in `config/test.exs`, or override it per test
with `Application.put_env/3` if your test suite runs `async: false` (as it must here, for the
reasons above).
