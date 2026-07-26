# Agent Inbox

A `Dbos` sample: an agent workflow that reaches a decision point, hands it to a human, and
waits — genuinely long-lived, not a polling loop — for their answer.

## What it demonstrates

- `AgentInbox.Approvals.request_approval/5` publishes a pending request (`Dbos.set_event/2`),
  then blocks on `Dbos.recv_message/2` for a human's decision.
- A human lists pending requests and answers one via a `Mix.Task`, from a separate CLI
  invocation — no engine of its own, no shared BEAM with the workflow.
- An unanswered request times out and escalates.
- A response sent **before** the workflow reaches its wait is still delivered.

## Durability property under test

> A wait longer than `park_exit_threshold_ms` (a `Dbos.Supervisor` option, default one minute —
> see `guides/tutorials/workflow-communication.md`) does more than block: the workflow's BEAM
> process exits entirely, leaving behind one row in an ETS table (`Dbos.Waits`) and a timer. The
> human's answer, or the timeout firing, redispatches the workflow — replayed from its
> checkpoints back to the wait site — exactly like crash recovery.

This is why parking thousands of pending approvals, each waiting hours or days, is cheap: no
process, no supervision-tree entry, no memory held per pending request — a timer and a few dozen
bytes, regardless of how many are waiting or how long. A `request_approval` workflow waiting
`:timer.hours(72)` for a response behaves exactly this way in production; the sample's own test
suite uses short timeouts (milliseconds) only so it runs fast, not because the mechanism differs.

## Running it

```sh
mix deps.get
createdb agent_inbox_dev   # or set AGENT_INBOX_DATABASE
mix ecto.migrate
```

```elixir
iex -S mix

iex> children = [
...>   AgentInbox.Repo,
...>   {Dbos.Supervisor,
...>    name: Dbos,
...>    db: {Dbos.DB.Ecto, AgentInbox.Repo},
...>    workflows: [AgentInbox.Approvals],
...>    migrations: :verify}
...> ]
iex> Supervisor.start_link(children, strategy: :one_for_one)

iex> Dbos.start("request_approval",
...>   ["req-1", "refund over $500", "customer disputes a charge", :timer.hours(72), 0],
...>   workflow_id: "req-1"
...> )
```

From another terminal, or another day:

```sh
mix agent_inbox.list
mix agent_inbox.answer req-1 approve "confirmed with customer"
```

## What to kill mid-run to see recovery work

Start a request with a long timeout, confirm it's parked (`Dbos.Waits.count(Dbos)` returns at
least `1` once `park_exit_threshold_ms` has elapsed with no answer — lower it via
`Dbos.Supervisor`'s option for a faster demo), then `kill -9` the BEAM. Restart with
`iex -S mix` and answer it with `mix agent_inbox.answer` from the fresh node: the workflow
redispatches, replays past its two `Dbos.set_event/2` calls without re-running them, and resumes
waiting exactly where it left off.

## Test

```sh
mix test
```

Covers all four documented outcomes:

- **Approve** — a human's `{:approved, note}` is delivered and returned.
- **Reject** — a human's `{:rejected, reason}` is delivered and returned.
- **Timeout** — nobody answers; the workflow returns `:expired` and logs an escalation.
- **Early response** — the answer is sent before the workflow reaches `Dbos.recv_message/2` (a
  `pre_wait_ms` durable sleep holds it back deterministically); the message is still there when
  the workflow gets to its wait, and it returns immediately without a second wait.

## Notes on this port

- `defworkflow` args declared with a default (`\\`) only get that default applied when called
  through the generated Elixir dispatcher function. `Dbos.start("request_approval", args, opts)`
  by registered name — and recovery, which redispatches the same way — invoke the workflow's body
  with exactly the argument list stored for it, bypassing Elixir's own default-argument
  expansion. `pre_wait_ms` is declared with no default here for that reason; pass `0` explicitly
  for ordinary use.
- The Mix tasks (`agent_inbox.list`, `agent_inbox.answer`) don't start a full `Dbos.Supervisor` —
  listing pending workflows and sending one message need only a `Dbos.Config` and a database
  connection, not the engine's registry, workflow supervisor, or recovery pass. See
  `AgentInbox.Cli`.
- The schema is installed by `priv/repo/migrations/20260101000001_add_dbos.exs`, an ordinary Ecto
  migration that calls `Dbos.Migration.up/0`. This sample keeps no tables of its own, so it's the
  only migration in the sequence.
