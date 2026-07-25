# Workflow Management

Every workflow run is a row in `workflow_status`, plus one row per checkpointed step in
`operation_outputs`. This tutorial covers reading that state back — listing workflows, inspecting
a single one's steps — and acting on it: cancel, resume, fork, and garbage collection. It also
covers what happens when a node dies mid-workflow, and the admin HTTP API that exposes all of this
over the wire.

## Listing and inspecting workflows

```elixir
{:ok, workflows} =
  Dbos.Client.list(Dbos.config(),
    status: :error,
    name: "process_order",
    limit: 50,
    sort: :desc
  )
```

`Dbos.Client.list/2` (backed by `Dbos.SystemDb.list_workflows/2`) filters on `:status`, `:name`,
`:queue_name`, `:executor_id`, and `:application_version` — each accepting a single value or a
list — plus `:created_after`/`:created_before` (epoch-ms bounds), `:limit`, `:offset`, and `:sort`
(`:desc`, the default, or `:asc`), ordered by `created_at`.

```elixir
{:ok, status} = Dbos.status(workflow_id)
{:ok, steps} = Dbos.Client.steps(Dbos.config(), workflow_id)
{:ok, output_or_error} = Dbos.result(workflow_id)
```

- `Dbos.status/2` fetches one workflow's full `%Dbos.WorkflowStatus{}` row. Called from inside a
  workflow, it consumes a step id and checkpoints under `"DBOS.getStatus"`, so a replay returns the
  status as recorded then, frozen regardless of whatever it has changed to since.
- `Dbos.Client.steps/2` returns every checkpointed step (`%Dbos.StepInfo{}`), ordered by
  `function_id` — each one's name, timing, and recorded output or error.
- `Dbos.result/2` returns `{:ok, output}`, `{:error, exception}`, or `:pending`.

## Cancel

```elixir
:ok = Dbos.cancel(workflow_id)
```

Durably marks the workflow `CANCELLED` — a no-op if it's already `SUCCESS`/`ERROR`/`CANCELLED`. If
the workflow has a live process on this engine, it's woken immediately, so a blocked
`recv_message`/`get_event`/`sleep` is interrupted right away, skipping the rest of its timeout. A
workflow actively running plain steps instead stops cooperatively at its next step boundary.

## Resume

```elixir
:ok = Dbos.resume(workflow_id)
```

Resumes from the workflow's last checkpoint: clears its queue assignment and deadline, then
re-enqueues it (onto `opts[:queue_name]`, default the reserved internal queue). Every step already
recorded is skipped on replay; execution continues from the first uncheckpointed step. Resuming a
workflow that already finished (`SUCCESS`/`ERROR`) is a silent no-op.

## Fork

```elixir
{:ok, new_handle} = Dbos.fork(workflow_id, 3)
```

`Dbos.fork/3` (`workflow_id`, `start_step`, `opts`) copies a workflow's history up to (not
including) `start_step` into a brand-new workflow id, and enqueues that new workflow to re-run
starting at `start_step` — useful for retrying from partway through after fixing a bug, without
redoing the steps that already succeeded, and without disturbing the original run.

What gets copied, all with `function_id < start_step`:

- `operation_outputs` — every checkpointed step's recorded output/error
- `workflow_events_history` and the derived latest-value `workflow_events` snapshot
- `streams`

The original workflow is marked `was_forked_from = TRUE` (informational only — it keeps running
or stays finished as it was). `opts`: `:new_workflow_id` (default a fresh UUID), `:queue_name`
(default the internal queue), `:application_version` (overrides the copied one — handy when
forking to retry under a fixed version of the code). Raises `Dbos.NonExistentWorkflowError` if
`workflow_id` doesn't exist.

Called from inside a workflow, `fork/3` consumes a step id and checkpoints under
`"DBOS.forkWorkflow"`, so replaying the caller doesn't fork a second time.

## Garbage collection

```elixir
Dbos.SystemDb.garbage_collect_workflows(Dbos.config(),
  cutoff_epoch_timestamp_ms: System.os_time(:millisecond) - :timer.hours(24) * 30,
  rows_threshold: 100_000
)
```

Deletes `workflow_status` rows (cascading to their steps, events, and streams) older than an
effective cutoff. Both options may be given together: `:cutoff_epoch_timestamp_ms` is an absolute
age cutoff, `:rows_threshold` instead keeps at least that many of the newest rows regardless of
age — the effective cutoff used is whichever of the two is more permissive (the max of both).
`PENDING`/`ENQUEUED`/`DELAYED` rows are never deleted, no matter how old. Returns the number of
rows deleted. Reachable over HTTP via `POST /dbos-garbage-collect` on the admin server.

## Recovery model

Each engine instance has an `executor_id` (from `Dbos.Supervisor`'s `:executor_id` option, the
`DBOS__VMID` env var, or the BEAM node name, in that order) and an `application_version` (from
`:application_version`, `DBOS__APPVERSION`, or a hash of every registered workflow module's code).
A workflow row records which executor started it and which application version it's running.

```mermaid
flowchart TD
    A[engine boots] --> B["Dbos.Recovery scans this executor_id's own PENDING rows"]
    B --> C["re-inserted PENDING (recovery_attempts + 1), redispatched via WorkflowSup"]
    C --> D{exceeds max_recovery_attempts?}
    D -->|yes| E[MAX_RECOVERY_ATTEMPTS_EXCEEDED, skipped]
    D -->|no| F[replays from its checkpoints]
```

- **On boot**, every engine synchronously recovers its own `PENDING` workflows —
  `Dbos.Recovery.recover_pending/1` reclaims this executor's own id (a no-op the first time, and
  the mechanism that resumes work after a clean restart of the same node). This happens in a
  `handle_continue` so `start_link` itself returns promptly.
- **Redispatch** re-inserts the row as `PENDING` with `recovery_attempts` incremented (bounded by
  `:max_recovery_attempts`, default `3` — a workflow that keeps failing to even start is marked
  `MAX_RECOVERY_ATTEMPTS_EXCEEDED` and left alone, ending the retry loop), then starts the
  workflow process with `replay: true`, which runs the body from the top but every already-recorded
  step returns its checkpointed result. Re-execution is skipped.
- **A queued `PENDING` workflow** — one still assigned to a queue — is handed back to its queue
  (cleared to `ENQUEUED`) for the queue's own dequeue logic to pick back up.
- **An unregistered workflow name** (this executor doesn't have that workflow's module loaded) is
  logged and skipped; the rest of the recovery batch continues.

### Reclaiming a dead executor's work

`Dbos.Recovery.reclaim/3` (`engine_name`, `dead_executor_ids`, `opts`) reassigns every `PENDING`
row owned by any of `dead_executor_ids` to this engine's own `executor_id` and redispatches it —
the mechanism a *different* node uses to pick up a peer's abandoned work. Every survivor may call
this concurrently for the same dead ids safely: the reassigning `UPDATE` is the serialization
point, so whichever call loses the race simply redispatches nothing. `opts[:batch_size]` bounds how
many non-queued rows one call claims (unbounded by default); callers driven by cluster membership
pass an explicit batch size. Returns every workflow id the call actually acted on.

Nothing here decides *who* is dead — that's `Dbos.Cluster`'s job (see `docs/clustering.md`) or a
manual call, e.g. from an operator hitting the admin server's recovery route.

## The admin HTTP API

Opt in via `Dbos.Supervisor`'s `:admin_server` option (default port `3001`):

```elixir
{Dbos.Supervisor,
 name: Dbos,
 db: {Dbos.DB.Ecto, MyApp.Repo},
 admin_server: [enabled: true, port: 3001]}
```

| Method | Path | Does |
|---|---|---|
| `GET` | `/dbos-healthz` | Liveness check. |
| `POST` | `/dbos-workflow-recovery` | Body is a JSON array of dead executor ids; calls `Dbos.Recovery.reclaim/3` and returns the reclaimed workflow ids. |
| `GET` | `/deactivate` | Stops the scheduler firing new cron ticks (`Dbos.Scheduler.deactivate/1`); already-enqueued work is unaffected. |
| `GET` | `/dbos-workflow-queues-metadata` | Lists every declared queue's configuration, including the internal queue. |
| `POST` | `/dbos-garbage-collect` | Body may set `cutoff_epoch_timestamp_ms`/`rows_threshold`; returns `{"deleted": n}`. |
| `POST` | `/dbos-global-timeout` | Body sets `cutoff_epoch_timestamp_ms`; cancels every workflow created before it. |
| `POST` | `/queues`, `/workflows` | List workflows (`/queues` restricts to queued ones); body filters by `workflow_name`, `queue_name`, `executor_id`, `application_version`, `status`, `limit`, `offset`, `sort_desc`. |
| `GET` | `/workflows/{id}` | One workflow's status. |
| `GET` | `/workflows/{id}/steps` | One workflow's checkpointed steps. |
| `POST` | `/workflows/{id}/cancel` | `Dbos.cancel/2`. |
| `POST` | `/workflows/{id}/resume` | `Dbos.resume/2`. |
| `POST` | `/workflows/{id}/fork` | Body may set `start_step` (default `0`), `new_workflow_id`, `application_version`. |

The server is a bare `:gen_tcp` HTTP/1.0-style listener (one process per connection, no
keep-alive) — deliberately simple, since a dozen JSON routes don't warrant a full HTTP stack.

### Values render through `inspect/1`

`workflow_status.inputs`/`.output`/`.error` and each step's `output`/`.error` are stored as
Erlang-term-encoded binaries. JSON can't represent an arbitrary Elixir term losslessly
(an atom, a tuple, a struct), so the admin API renders them through `inspect/1` as a readable
string, sidestepping a lossy JSON conversion. A workflow that took `%{currency: :usd,
amount: 4200}` as input shows up over the API as `"input": "[%{currency: :usd, amount: 4200}]"` —
a string meant for reading, standing apart from the structured JSON fields elsewhere in the
response. Every other field (status, timestamps, executor id, and so on) is already JSON-safe
and renders as itself.
