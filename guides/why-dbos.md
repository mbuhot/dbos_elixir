# Why Durable Execution

## The problem

A supervisor restarts a crashed process. The crashed process's **work** stays lost.

```mermaid
sequenceDiagram
    participant S as Supervisor
    participant P as GenServer / Task
    participant DB as Postgres
    P->>DB: reserve stock (done)
    P->>P: charge card (in progress)
    P--xP: crash
    S->>P: restart (fresh process)
    Note over P: reservation? charge?<br/>the new process has no idea
```

`reserve_stock` may have committed. `charge_card` may have charged the customer and crashed
before recording it, or crashed before charging at all. The restarted process begins at
`init/1` with none of that history, so its only safe move is to start over — which works
only if every step is idempotent, and somebody checked that by hand.

**Durable execution stores the history.** Every step a workflow takes is written to Postgres
before the next one starts. A crash of any kind — process, node, deploy — leaves a record of
how far the workflow got. Recovery replays from that record, and a completed step "replays"
by returning what it recorded.

## What you get

- A multi-step business process (reserve → charge → ship → notify) that survives a crash at
  any point between steps.
- Checkpoints in the same Postgres database your application already uses: one connection
  pool, one backup story.
- Workflows that wait — for a message, an event, a timer — for arbitrarily long. A long wait
  gives up its process and is re-dispatched when it wakes.
- Child workflows, queues (concurrency limits, rate limits, priority, delay, partitioning),
  and cron scheduling, all built on the same checkpoint mechanism.

## What it costs

| Cost | Detail |
|---|---|
| Latency per step | Every step is a round trip to Postgres. Thousands of trivial sub-millisecond operations pay a checkpoint write each. |
| Determinism contract | Workflow bodies must replay identically: clock reads, `:rand`, bare `receive`, and `Task.async` belong inside steps. |
| Operational surface | A schema to migrate and keep in sync, a supervision tree, and workflow names that stay stable across deploys. |

## When *not* to reach for it

- A single database write that a Postgres transaction already makes atomic.
- Fire-and-forget work where "it didn't run" is a shruggable outcome.
- A process whose state is disposable — a cache warmer, a connection — where a supervisor
  restart already fixes things by starting fresh.
- High-throughput, low-latency hot paths where a round trip per step is the wrong trade-off.

## How this compares

| | Recovers from | Does **not** recover | Where the state lives |
|---|---|---|---|
| **Supervisor restart** | A crashed process, structurally | Work the process was doing — in-progress steps, partial side effects | Nowhere; the new process starts from `init/1` |
| **GenServer state** | Nothing | The state itself, on crash or node death | The process's own heap |
| **Plain retries** | A single failed call, retried in place | Multi-step processes; a crash between steps loses position | Nowhere durable |
| **Oban** | A failed *job*, requeued and retried from the top | Partial progress within one job | The queue's table; the job body re-runs in full |
| **Temporal** | A crashed workflow, resumed from an event history | — | A separate Temporal cluster you operate |
| **`Dbos`** | A crashed workflow, resumed from its last completed step | — | `dbos.workflow_status` / `dbos.operation_outputs`, in your own Postgres |

Oban guarantees a job *runs*, retried as a whole. `Dbos` guarantees a workflow's *steps*
individually survive a crash: the card will not be charged twice because the charge step
already recorded that it happened. The two compose — a `Dbos.Queue` plays a similar role to
an Oban queue for *starting* work, and `defstep`/`deftransaction` add per-operation durability
underneath.

Temporal offers the same programming model with a much larger operational footprint: its own
server cluster, its own storage, and its own deployment lifecycle. `Dbos` runs inside your
BEAM nodes and stores its state in the database you already run.

## The one sentence

A supervisor restart recovers a **process**; `Dbos` recovers the **work** the process was
partway through.
