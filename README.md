[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/mbuhot/dbos_elixir/blob/main/LICENSE)
[![Elixir](https://img.shields.io/badge/elixir-~%3E%201.19-purple.svg)](https://elixir-lang.org)
[![Postgres](https://img.shields.io/badge/postgres-17%2B-blue.svg)](https://www.postgresql.org)

# Dbos for Elixir: Durable Workflow Orchestration on Postgres

#### [Documentation](https://mbuhot.github.io/dbos_elixir/) • [Examples](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps) • [Determinism Contract](https://mbuhot.github.io/dbos_elixir/determinism.html)

---

## Credit

This library is a port of **[DBOS Transact for Go](https://github.com/dbos-inc/dbos-transact-golang)**, created by **[DBOS, Inc.](https://www.dbos.dev)** and released under the MIT License.

The design is theirs. The database schema, the status and step-name protocol, the SQL, and the algorithms for checkpointing, replay, recovery, queue dequeueing, messaging, and transactional steps are all derived from their work, at commit [`2a7705c`](https://github.com/dbos-inc/dbos-transact-golang/commit/2a7705c37c93e5fd1d5c1ce049a4224ec2f1f969). This documentation follows the structure of [docs.dbos.dev](https://docs.dbos.dev), with every example rewritten for Elixir.

If this library is useful to you, the credit belongs upstream. Please [star the original project](https://github.com/dbos-inc/dbos-transact-golang).

DBOS and DBOS Transact are marks of DBOS, Inc. This is an independent port and carries no endorsement. [LICENSE](https://github.com/mbuhot/dbos_elixir/blob/main/LICENSE) reproduces their copyright notice in full.

---

## What is Dbos?

Dbos provides durable workflow orchestration on the Postgres you already run. Annotate your functions and the engine checkpoints their progress. When a process dies, the workflow resumes from its last completed step.

It runs as a library inside your existing supervision tree, using your existing database and connection pool.

## When should I use it?

Reach for Dbos when your application must **reliably survive failure** partway through a multi-step operation — a payment that charges then ships, a pipeline that fetches then embeds then indexes, an agent making a long chain of expensive API calls.

An OTP supervisor restarts a dead process. The half-finished work that process was doing dies with it. Dbos is what recovers the work.

| You get | You pay |
|---|---|
| Steps that never re-run once they complete | A determinism contract on workflow bodies, enforced at compile time |
| Automatic recovery after any crash | One row per workflow and one per step in Postgres |
| Durable queues, notifications, scheduling, and sleep | Values persist as Erlang terms, readable from Elixir |

## Features

### 💾 Durable Workflows

A workflow checkpoints each completed step. After a crash, restarting replays the body, and every step that already finished returns its recorded value.

```elixir
defmodule MyApp.Checkout do
  use Dbos, repo: MyApp.Repo

  defworkflow process_order(order_id, amount), name: "process_order" do
    charge = charge_card(order_id, amount)
    record_receipt(order_id, charge)
    ship(order_id)
  end

  defstep charge_card(order_id, amount) do
    MyApp.Payments.charge(order_id, amount)
  end

  deftransaction record_receipt(order_id, charge) do
    MyApp.Repo.insert!(%MyApp.Receipt{order_id: order_id, charge_id: charge.id})
  end

  defstep ship(order_id) do
    MyApp.Shipping.dispatch(order_id)
  end
end
```

Add the engine to your supervision tree, after your repo. `otp_app:` discovers every module in your application that defines a workflow:

```elixir
children = [
  MyApp.Repo,
  {Dbos.Supervisor, db: {Dbos.DB.Ecto, MyApp.Repo}, otp_app: :my_app}
]
```

Start a workflow and collect its result:

```elixir
{:ok, handle} = MyApp.Checkout.process_order("ord_1", 4999)
{:ok, result} = Dbos.await(handle)
```

Kill the node between `charge_card` and `ship`. On restart the card stays charged exactly once, and shipping proceeds.

A `deftransaction` commits your write and its checkpoint in one Postgres transaction, so the two always agree.

### 📒 Durable Queues

Enqueue a workflow and any process pointed at the same database may claim it. Concurrency limits, rate limits, and priority are enforced in Postgres, shared across every node.

```elixir
{Dbos.Supervisor,
 db: {Dbos.DB.Ecto, MyApp.Repo},
 otp_app: :my_app,
 queues: [
   Dbos.Queue.new("reports", worker_concurrency: 5, rate_limit: %{limit: 100, period_ms: 60_000})
 ]}
```

```elixir
{:ok, handle} = Dbos.enqueue(&MyApp.Reports.generate/1, [report_id], queue_name: "reports")
{:ok, report} = Dbos.await(handle)
```

Queues support worker and global concurrency, rate limiting, priority, partition keys for per-tenant fairness, delayed start, deduplication, and debouncing (`Dbos.debounce/3`). A node that dies mid-task releases its claim, and another node picks the work up.

### 🎫 Exactly-Once Event Processing

Give a workflow an id derived from the event, and a duplicate delivery collapses onto the original run. Acknowledge the webhook immediately; the workflow completes in the background exactly once, whether the sender retries or your node restarts.

```elixir
{:ok, handle} = MyApp.Webhooks.handle_event(payload, workflow_id: "stripe-#{event.id}")
```

### 📅 Durable Scheduling

Declare a cron schedule on the workflow itself. Several nodes may run the same schedule, and exactly one firing happens per tick. The grammar covers six fields including seconds, month and day names, the `@daily` family, and `@every`.

```elixir
defworkflow nightly_report(scheduled_time_ms, _context),
  name: "nightly_report",
  schedule: "0 0 2 * * *" do
  MyApp.Reports.build_for(scheduled_time_ms)
end
```

Durable sleep records its wake time, so a workflow sleeps through restarts:

```elixir
defworkflow trial_reminder(user_id), name: "trial_reminder" do
  Dbos.sleep(:timer.hours(24 * 14))
  send_reminder(user_id)
end
```

### 📫 Durable Notifications

Pause a workflow until a message arrives, or publish events for external readers. Both persist in Postgres with exactly-once delivery, and both survive a restart.

```elixir
defworkflow await_approval(order_id), name: "await_approval" do
  Dbos.set_event("status", :awaiting_approval)

  case Dbos.recv_message("decision", :timer.hours(48)) do
    %{outcome: :approve, approver: who} -> ship(order_id, who)
    %{outcome: :reject} -> refund(order_id)
  end
end
```

```elixir
Dbos.send_message(order_workflow_id, "decision", %{outcome: :approve, approver: "sam"})
```

Values round-trip as Erlang terms, so the atoms in that payload arrive exactly as sent. A message that arrives before the workflow reaches its `recv_message` is still delivered.

## Extensions

Capabilities this engine adds on top of the design it derives from, mostly by taking advantage of the BEAM.

### Automatic recovery after a node dies

Each executor holds a lease in Postgres, renewed on an interval. A workflow left `PENDING` by an executor whose lease has expired is reclaimed by a live executor and resumed from its last checkpoint, with no operator involvement.

The lease lives in the same database the executor needs in order to checkpoint, so an executor that cannot renew its lease also cannot write conflicting checkpoints. A reclaim only takes workflows whose names the claiming executor has registered, so a fleet where different nodes carry different workflow modules recovers correctly.

The guarantee is **exactly-once checkpoints, at-least-once side effects.** A step that performs an external effect and crashes before its checkpoint commits runs that effect again on recovery. A step that must never repeat needs an idempotency key at its own boundary.

### Long waits cost no process

A workflow waiting longer than a minute releases its process and is rebuilt when it wakes, on a deadline or on a message. Measured at 163 bytes per parked wait, so a hundred thousand workflows parked for a fortnight cost about 16MB and no processes.

### Determinism checked at compile time

`defworkflow` holds its body's AST, so nondeterministic constructs are rejected by the compiler with the call, the line, and the fix. Step and transaction bodies are checked for constructs that spawn a process out of the workflow context. The optional `:dbos` Mix compiler extends the same tables across the whole application, following every helper a workflow or step body calls, transitively.

### Evolving a workflow in flight

`Dbos.patch/1` guards new code so executions that predate it keep their recorded step sequence; `Dbos.deprecate_patch/1` retires the guard once those executions have drained.

## Getting Started

```elixir
def deps do
  [{:dbos, github: "mbuhot/dbos_elixir"}]
end
```

The engine keeps its tables under their own Postgres schema. Generate the migration and apply it with the rest of your sequence:

```sh
mix dbos.gen.migration
mix ecto.migrate
```

At boot the engine verifies the schema is at the version it targets and refuses to start otherwise.

For tests, `testing: :inline` or `testing: :manual` on `Dbos.Supervisor` runs workflows on the calling process, which keeps them inside `Ecto.Adapters.SQL.Sandbox`. `Dbos.Testing` drains queues and runs recovery synchronously.

The [quickstart](https://mbuhot.github.io/dbos_elixir/quickstart.html) takes you from an empty project to watching a workflow survive a crash. Read the [determinism contract](https://mbuhot.github.io/dbos_elixir/determinism.html) before writing a workflow body — it is short, and the compiler enforces most of it.

Full documentation: [https://mbuhot.github.io/dbos_elixir/](https://mbuhot.github.io/dbos_elixir/)

## Examples

Runnable applications live in [`sample_apps/`](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps), each a standalone Mix project with its own tests.

| Example | Demonstrates |
|---|---|
| [widget_store](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/widget_store) | Fault-tolerant checkout with inventory and refunds |
| [outbox](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/outbox) | Transactional outbox, at-least-once publishing |
| [queue_worker](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/queue_worker) | Durable background workers surviving worker death |
| [queue_patterns](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/queue_patterns) | Fan-out, per-tenant fairness, priority, debouncing |
| [agent_inbox](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/agent_inbox) | Human-in-the-loop approvals that wait for days |
| [document_pipeline](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/document_pipeline) | Concurrent ingestion that re-embeds nothing |
| [hacker_news_agent](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/hacker_news_agent) | A research agent whose LLM calls survive a crash |
| [customer_service_agent](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/customer_service_agent) | An agent with a refund tool that runs exactly once |
| [s3_mirror](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/s3_mirror) | Resumable bulk copy with live progress |
| [deploy_slackbot](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/deploy_slackbot) | Exactly-once notifications from a deploy feed |
| [live_approvals](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps/live_approvals) | Phoenix LiveView and Dbos bridged in both directions |

## Comparisons

| Tool | What it gives you | Reach for Dbos when |
|---|---|---|
| OTP supervision | A restarted process, beginning from its initial state | You need the in-flight work to finish, resuming at its last completed step. Dbos runs inside your supervision tree and depends on it |
| Oban | A mature Postgres-backed job system with a large ecosystem and an excellent UI | You need a multi-step operation checkpointed step by step, able to enqueue children and await their results mid-flight |
| Temporal | A dedicated orchestration cluster with many language SDKs | You want durable execution inside an existing Elixir application, on your current infrastructure |

## Serialization

Values are encoded with `:erlang.term_to_binary/1` and base64-encoded into a TEXT column, under the format name `"erl_etf"`. Atoms, tuples, structs and dates round-trip exactly.

## License

MIT. [LICENSE](https://github.com/mbuhot/dbos_elixir/blob/main/LICENSE) carries the DBOS, Inc. copyright notice in full, as a derivative work requires.
