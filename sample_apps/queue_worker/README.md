# Queue Worker

A port of [DBOS's Queue Worker example](https://docs.dbos.dev/python/examples/queue-worker) to
Elixir: tasks get enqueued from one place and executed reliably somewhere else, surviving a
worker crashing mid-batch.

## What this demonstrates

| Piece | Where |
|---|---|
| A queue with worker concurrency | `Dbos.Queue.new("tasks", worker_concurrency: N)` in `QueueWorker.Application` |
| A workflow that processes one task | `QueueWorker.Tasks.process_task/2` |
| A producer that enqueues a batch | `QueueWorker.Producer.enqueue_batch/2` |
| Progress observation via handles | `QueueWorker.Producer.statuses/1`, `completed_count/1` |
| Surviving a worker death | `Dbos.Recovery` — see below |

`process_task/2` runs two checkpointed steps: `claim_task/2` (instant) then `do_work/2`
(simulated work, `Process.sleep(200)`). Every task is enqueued onto the `"tasks"` queue and
dispatched by whichever worker process is free, up to `worker_concurrency` at a time.

## The durability property

**A worker dying mid-batch loses no work, and every task still runs to `SUCCESS` exactly once.**

- A step whose checkpoint already committed to `dbos.operation_outputs` is never re-run — replay
  returns its recorded output.
- A step that was actually in flight when its worker process died has no checkpoint yet, so
  recovery reruns it from the top. That's expected: only a step that never finished retries: it is
  *at-least-once* for the step body caught mid-flight, but the workflow as a whole still finishes
  exactly once, with `dbos.operation_outputs` ending up with exactly one row per step.
- Recovery is automatic: `Dbos.Recovery` scans for this executor's `PENDING` workflows both at
  boot and on demand (`Dbos.Recovery.recover_pending/1`), and a queued row whose owning process
  died is simply handed back to the queue for any worker to pick up.

## Running it

Requires a local Postgres reachable with `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`
(all optional — they default to `localhost`/`5432`/your OS user/no password), and a database
named `queue_worker_dev` (override with `QUEUE_WORKER_DATABASE`).

```sh
cd sample_apps/queue_worker
mix deps.get
createdb queue_worker_dev
mix ecto.migrate
mix queue_worker.run 20
```

`mix queue_worker.run [count]` (default 20):

1. Enqueues `count` tasks under one batch id.
2. Finds whichever task is currently running and `Process.exit(pid, :kill)`s its worker process —
   simulating that worker crashing mid-task.
3. Calls `Dbos.Recovery.recover_pending/1` — what a restarted worker process does automatically at
   boot, run here explicitly since this demo is one BEAM, not a separate worker to restart.
4. Awaits every task and confirms in `dbos.workflow_status` that all `count` reached `SUCCESS`.

Sample output:

```
Enqueueing 20 tasks under "batch-134"...
Killing worker process for batch-134-1 (#PID<0.257.0>)
Recovering this executor's pending workflows (a restarted worker does this at boot)...
Awaiting completion (checkpointed tasks are not re-run)...
20/20 tasks SUCCESS in dbos.workflow_status
Collected 20 results
Per-task claim_task attempts: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
Per-task do_work attempts: [2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
Exactly-once outcome confirmed: every task reached SUCCESS despite the kill, none lost.
```

Task 1's `do_work` shows `2` attempts — that's the one whose worker got killed mid-sleep, replayed
once after recovery. Every other task shows `1`.

### Killing a whole worker BEAM, not just a process

For a closer analogue to a real deployment, run the producer and worker as separate `iex`
sessions and kill the whole node:

```sh
# terminal 1 — the worker
iex -S mix

# terminal 2 — enqueue a batch, then find and kill terminal 1's OS pid
iex -S mix
iex> QueueWorker.Producer.enqueue_batch("batch-1", 20)
```

```sh
$ pgrep -f "iex -S mix"
$ kill -9 <pid from terminal 1>
```

Restart terminal 1 (`iex -S mix`) and watch `Dbos.Recovery`'s boot pass pick the batch back up —
`QueueWorker.Producer.completed_count/1` against the same handles will climb back to `count`.

## Tests

```sh
mix test
```

`test/queue_worker_test.exs` enqueues a batch, kills one task's in-flight worker process, triggers
recovery, and asserts every task reaches `SUCCESS` with exactly one checkpoint row per step
(`function_id`s `[0, 1]`) — the same crash-and-recover pattern from `guides/tutorials/testing.md`,
applied to a queued batch instead of a single directly-started workflow.
