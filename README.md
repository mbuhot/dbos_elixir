<div align="center">

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/mbuhot/dbos_elixir/blob/main/LICENSE)
[![Elixir](https://img.shields.io/badge/elixir-~%3E%201.19-purple.svg)](https://elixir-lang.org)
[![Postgres](https://img.shields.io/badge/postgres-13%2B-blue.svg)](https://www.postgresql.org)

# Dbos for Elixir: Durable Workflow Orchestration on Postgres

#### [Documentation](https://mbuhot.github.io/dbos_elixir/) &nbsp;&nbsp;•&nbsp;&nbsp; [Examples](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps) &nbsp;&nbsp;•&nbsp;&nbsp; [Determinism Contract](https://mbuhot.github.io/dbos_elixir/determinism.html)

</div>

---

## Credit

This library is a port of **[DBOS Transact for Go](https://github.com/dbos-inc/dbos-transact-golang)**, created by **[DBOS, Inc.](https://www.dbos.dev)** and released under the MIT License.

The design is theirs. The database schema, the status and step-name protocol, the SQL, and the algorithms for checkpointing, replay, recovery, queue dequeueing, messaging, and transactional steps are all derived from their work, at commit [`2a7705c`](https://github.com/dbos-inc/dbos-transact-golang/commit/2a7705c37c93e5fd1d5c1ce049a4224ec2f1f969). This documentation follows the structure of [docs.dbos.dev](https://docs.dbos.dev), with every example rewritten for Elixir.

If this library is useful to you, the credit belongs upstream. Please [star the original project](https://github.com/dbos-inc/dbos-transact-golang).

DBOS and DBOS Transact are marks of DBOS, Inc. This is an independent port and carries no endorsement. [LICENSE](https://github.com/mbuhot/dbos_elixir/blob/main/LICENSE) reproduces their copyright notice in full.

---

## What is Dbos?

Dbos provides durable workflow orchestration on top of the Postgres you already run. Annotate your functions, and the engine checkpoints their progress. When a process dies, the workflow resumes from its last completed step.

Everything lives in your existing database and your existing supervision tree. No orchestration server, no message broker, no second connection pool.

## When should I use it?

Reach for Dbos when your application must **reliably survive failure** partway through a multi-step operation — a payment that charges then ships, a pipeline that fetches then embeds then indexes, an agent making a long chain of expensive API calls.

An OTP supervisor restarts a dead process. The half-finished work that process was doing dies with it. Dbos is what recovers the work.

| You get | You pay |
|---|---|
| Steps that never re-run once they complete | A determinism contract on workflow bodies, enforced at compile time |
| Automatic recovery after any crash | One row per workflow and one per step in Postgres |
| Durable queues, notifications, scheduling, and sleep | Values persist as Erlang terms, readable from Elixir |

## Features

<details open><summary><strong>💾 Durable Workflows</strong></summary>

A workflow checkpoints each completed step. After a crash, restarting replays the body, and every step that already finished returns its recorded value.

```elixir
defmodule MyApp.Checkout do
  use Dbos

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

Add the engine to your supervision tree, after your repo:

```elixir
children = [
  MyApp.Repo,
  {Dbos.Supervisor,
   db: {Dbos.DB.Ecto, MyApp.Repo},
   workflows: [MyApp.Checkout]}
]
```

Start a workflow and collect its result:

```elixir
{:ok, handle} = MyApp.Checkout.process_order("ord_1", 4999)
{:ok, result} = Dbos.await(handle)
```

Kill the node between `charge_card` and `ship`. On restart the card stays charged exactly once, and shipping proceeds.

A `deftransaction` commits your write and its checkpoint in one Postgres transaction, so the two always agree.

</details>

<details><summary><strong>📒 Durable Queues</strong></summary>

Enqueue a workflow and any process pointed at the same database may claim it. Concurrency limits, rate limits, and priority are enforced in Postgres, shared across every node.

```elixir
{Dbos.Supervisor,
 db: {Dbos.DB.Ecto, MyApp.Repo},
 workflows: [MyApp.Reports],
 queues: [
   Dbos.Queue.new("reports", worker_concurrency: 5, rate_limit: %{limit: 100, period_ms: 60_000})
 ]}
```

```elixir
{:ok, handle} = Dbos.enqueue(&MyApp.Reports.generate/1, [report_id], queue_name: "reports")
{:ok, report} = Dbos.await(handle)
```

Queues support worker and global concurrency, rate limiting, priority, partition keys for per-tenant fairness, delayed start, deduplication, and debouncing. A node that dies mid-task releases its claim, and another node picks the work up.

</details>

<details><summary><strong>🎫 Exactly-Once Event Processing</strong></summary>

Give a workflow an id derived from the event, and a duplicate delivery collapses onto the original run.

```elixir
{:ok, handle} = MyApp.Webhooks.handle_event(payload, workflow_id: "stripe-#{event.id}")
```

Acknowledge the webhook immediately. The workflow completes in the background exactly once, whether the sender retries or your node restarts.

</details>

<details><summary><strong>📅 Durable Scheduling</strong></summary>

Declare a cron schedule on the workflow itself. Several nodes may run the same schedule, and exactly one firing happens per tick.

```elixir
defworkflow nightly_report(scheduled_time_ms, _context),
  name: "nightly_report",
  schedule: "0 0 2 * * *" do
  MyApp.Reports.build_for(scheduled_time_ms)
end
```

The grammar covers six fields including seconds, month and day names, the `@daily` family, and `@every`.

Durable sleep records its wake time, so a workflow sleeps through restarts:

```elixir
defworkflow trial_reminder(user_id), name: "trial_reminder" do
  Dbos.sleep(:timer.hours(24 * 14))
  send_reminder(user_id)
end
```

A wait longer than a minute releases its process entirely and is rebuilt on wake, so parking a hundred thousand workflows for a fortnight costs about 16MB and zero processes.

</details>

<details><summary><strong>📫 Durable Notifications</strong></summary>

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

Note the atoms in that payload. Values round-trip as Erlang terms, so what you send is exactly what the workflow receives.

A message that arrives before the workflow reaches its `recv_message` is still delivered.

</details>

## Getting Started

```elixir
def deps do
  [{:dbos, github: "mbuhot/dbos_elixir"}]
end
```

The [quickstart](https://mbuhot.github.io/dbos_elixir/quickstart.html) takes you from an empty project to watching a workflow survive a crash. The [programming guide](https://mbuhot.github.io/dbos_elixir/programming-guide.html) builds one application end to end.

Read the [determinism contract](https://mbuhot.github.io/dbos_elixir/determinism.html) before writing a workflow body. It is short, and the compiler enforces most of it.

## Documentation

[https://mbuhot.github.io/dbos_elixir/](https://mbuhot.github.io/dbos_elixir/)

## Examples

Ten runnable applications live in [`sample_apps/`](https://github.com/mbuhot/dbos_elixir/tree/main/sample_apps), each a standalone Mix project with its own tests.

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

## Dbos vs. other Elixir tools

<details><summary><strong>Dbos vs. OTP supervision</strong></summary>

####

A supervisor restarts a process that died. A restarted `GenServer` begins from its initial state, and the work in flight is gone.

Dbos records progress in Postgres, so a recovered workflow resumes at its last completed step with every earlier result intact.

**When to use OTP supervision:** you need a process to keep running.

**When to use Dbos:** you need the work to finish.

They compose. Dbos runs inside your supervision tree and depends on it.

</details>

<details><summary><strong>Dbos vs. Oban</strong></summary>

####

Both are Postgres-backed, and both give you durable queues with concurrency limits, priority, and retries. Oban is a mature job system with a large ecosystem and an excellent UI.

Dbos adds durable *workflows*: a function whose intermediate steps are individually checkpointed. A workflow can enqueue a thousand children and await their results, and a crash resumes it mid-flight with each completed step preserved.

**When to use Oban:** you need background jobs with a mature ecosystem and tooling around them.

**When to use Dbos:** you need a multi-step operation to survive a crash partway through, with each step recorded.

</details>

<details><summary><strong>Dbos vs. Temporal</strong></summary>

####

Both provide durable execution. Temporal runs an external orchestration cluster, and your workflows move into Temporal workers. Dbos is a library over the Postgres you already have.

**When to use Temporal:** you want a dedicated orchestration platform, or you need languages this port does not cover.

**When to use Dbos:** you want durable execution inside an existing Elixir application, on your current infrastructure.

</details>

## Scope

This port serves Elixir applications. Persisted values are Erlang terms, which preserves atoms, tuples, structs and dates exactly, and means a service in another language reads them as opaque bytes. The [migration guide](https://mbuhot.github.io/dbos_elixir/interop-migration.html) covers the path to a portable format if that changes.

## License

MIT. [LICENSE](https://github.com/mbuhot/dbos_elixir/blob/main/LICENSE) carries the DBOS, Inc. copyright notice in full, as a derivative work requires.
