# Production Checklist

What to get right before an application built on `Dbos` carries real traffic. Each section links
onward to the document that already covers the details.

## Schema migration and verify-at-launch

`Dbos.Migrator.verify!/1` checks `<schema>.dbos_migrations.version` is exactly the version this
port targets (`42`) and **raises**, refusing to start, on anything else — a wrong-version schema
would checkpoint into tables whose shape the engine doesn't actually match.

- Use `migrations: :verify` (the default) in production. Apply and review schema changes through
  your own deploy pipeline, the same way you'd handle any other migration. Reserve
  `:create_if_absent` for local dev and quick starts.
- Full detail, including what each `:migrations` value does: `guides/integrating-dbos.md`,
  "Migration verification at launch".

## Executor identity and application version

Two identifiers every multi-instance deployment needs to set deliberately:

- `executor_id` — set `DBOS__VMID` to something stable for *this* instance (a pod name, a task
  ARN) if your platform gives node names that are random or ephemeral.
- `application_version` — set `DBOS__APPVERSION` to a git SHA or release tag, so every instance
  of the same deployment agrees on the same value. The computed fallback
  (`Dbos.Version.compute/1`) hashes code per instance.

Both gate which executor recovery and dequeue let touch a given in-flight workflow — get this
wrong and a workflow can sit unclaimed indefinitely. Full detail:
`guides/integrating-dbos.md`, "Executor identity and application version for a release", and
`guides/tutorials/upgrading-workflows.md` for how this plays out across a deploy.

## The LISTEN connection and the polling fallback

`Dbos.Notifications` opens one dedicated connection per engine so blocking waits
(`recv_message`, `get_event`, stream reads) wake promptly on `NOTIFY`. If it can't be
established, `Dbos` logs a warning and falls back to 1-second polling. Startup continues.

- Confirm in your logs, at boot, that the listener actually connected (absence of the fallback
  warning) — a silent fallback to polling is not a startup failure, so it won't show up any
  other way.
- Full detail, including how connection options are derived and how to point them at a
  standalone Postgrex pool: `guides/integrating-dbos.md`, "The dedicated LISTEN connection".

## Connection pool sizing

`Dbos` checks out connections from whichever pool you already run (`Dbos.DB.Ecto` or
`Dbos.DB.Postgrex`) — there is no second pool to size. Account for these when sizing that pool:

- One connection permanently held by `Dbos.Notifications`, outside the pool (a separate
  connection, sitting outside your pool's `pool_size`).
- One connection per `deftransaction` step actually running, held for that transaction's
  duration — including any of your own repo calls made inside the same step body.
- One connection per in-flight `Dbos.SystemDb` query outside a transaction (checkpoint writes,
  status reads, queue dequeue polls) — brief, but concurrent across every workflow process
  running at once.

Size the pool for your expected concurrent workflow count. Web request concurrency is
additional, if workflows and web requests share the same pool.

## Clustering and dead-node recovery

Off by default. If you run more than one node and want a node's death to trigger reclaiming its
`PENDING` workflows automatically, without waiting for that node to come back, enable
`cluster.enabled` (and `cluster.orphan_sweep` for the case `:nodedown` itself never fires — a
whole-cluster restart, a pod that's gone for good).

Full detail, including the exact reclaim query, what it costs, and the partition-risk tradeoff
this design deliberately makes: `docs/clustering.md`.

## Queue concurrency and rate limits

Every `Dbos.Queue` you declare persists its configuration (`register_queue/2`) and is enforced
at dequeue time: `worker_concurrency` bounds this executor's own concurrent claims,
`global_concurrency` bounds the whole fleet's, `rate_limit` bounds claims per period. Get these
values wrong and you either starve a queue of throughput or let it overwhelm whatever it calls
downstream.

- `guides/tutorials/queues.md` covers declaring a queue, priorities, partitions, and
  deduplication/debouncing.
- Every persisted queue's live configuration is readable at runtime through the admin server's
  `GET /dbos-workflow-queues-metadata` route (see below) if you need to confirm what's actually
  deployed versus what you intended.

## The admin server: whether to expose it

`Dbos.AdminServer` is off by default (`admin_server: [enabled: true, port: 3001]` to turn it on).
It exposes workflow introspection (`GET /workflows`, `GET /workflows/{id}`,
`GET /workflows/{id}/steps`) alongside **mutating, unauthenticated** operations:
`POST /workflows/{id}/cancel`, `POST /workflows/{id}/resume`, `POST /workflows/{id}/fork`,
`POST /dbos-workflow-recovery`, `POST /dbos-garbage-collect`, `POST /dbos-global-timeout`
(cancels every non-terminal workflow created at or before a given cutoff — fleet-wide).

There is no authentication or authorization built in. Do not expose this port to the public
internet. Put it behind whatever your platform uses to restrict access to operational tooling —
a private network, an authenticating proxy, a bastion — the same way you'd treat direct database
access, since several of these routes have that much blast radius.

## Telemetry and what to alert on

Four `:telemetry.span/3` spans: workflow, step, queue dequeue, recovery — each with `:start`,
`:stop`, and `:exception` events. No built-in backend; attach your own handler.

Full event/metadata reference: `docs/telemetry.md`. At minimum, alert on:

- `[:dbos, :workflow, :exception]` — a workflow process crashed running its body.
- `[:dbos, :step, :exception]` — a step failed (check `Dbos.SystemDb.contention_error?/1` before
  paging on `[:dbos, :queue, :dequeue, :exception]` specifically — lock-contention races there
  are a routine, expected outcome under concurrent dequeuers).
- `[:dbos, :recovery, :exception]` — a recovery/reclaim pass itself failed, distinct from an
  individual workflow failing inside it.

Beyond telemetry events, a periodic query against `workflow_status` for stuck or failed rows is
worth alerting on directly — `docs/system-database.md` has the queries (workflows stuck
`PENDING` past a threshold, anything `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, queue depth per queue).

## Garbage collection and retention

`Dbos.SystemDb.garbage_collect_workflows/2` (also `POST /dbos-garbage-collect` on the admin
server) deletes `workflow_status` rows — cascading to their `operation_outputs` and the rest —
older than a cutoff, by age (`cutoff_epoch_timestamp_ms`) or by row count
(`rows_threshold`, keeping at least that many of the newest). `PENDING`/`ENQUEUED`/`DELAYED` rows
are never deleted, regardless of cutoff.

Decide a retention policy before volume makes the decision for you: every checkpoint from every
step of every workflow this application has ever run stays in `operation_outputs` until
something calls this. There's no automatic schedule — wire it into your own periodic job (a
scheduled workflow, a cron container, whatever your platform already runs) for ongoing cleanup
beyond a manual one-off.

## Known operational risks

- **Split-brain double execution under a network partition.** Both sides of a partition reclaim
  the same `PENDING` rows independently if clustering is enabled; `owner_xid` detects this after
  the fact but does not prevent it. Detail and the deliberate availability-over-consistency
  tradeoff: `docs/clustering.md`, "Partition risk".
- **No in-place patch for an in-flight workflow.** Changing a workflow's step layout while
  instances of it are still running requires a version bump or a new workflow name — there is no
  lighter-weight patch mechanism yet. See `guides/tutorials/upgrading-workflows.md`.
- **A `Task` inside a step loses checkpointing silently.** The determinism checker bans
  `Task.async`/`await`/`async_stream`/`start` inside a `defworkflow` body, but a step's own body
  is not checked the same way — a `Task` spawned from inside a `defstep` runs without the
  workflow context, so any durable operation called from inside it is not checkpointed at all.
  See `guides/faq.md`.
- **The admin server has no auth.** Covered above — worth repeating in a risk list, since it's an
  easy thing to enable for convenience and forget to lock down.
