# Production Checklist

Work through this before an application built on `Dbos` carries real traffic. Deeper detail on
every item lives in the rest of the guides.

## Schema

- [ ] Generate the migration with `mix dbos.gen.migration` and apply it through `mix ecto.migrate`,
      reviewed like any other migration in your sequence.
- [ ] Leave `migrations: :verify` (the default) in production. `Dbos.Migrator.verify!/1` checks
      `<schema>.dbos_migrations.version` is exactly `42` and raises at boot on anything else.

## Identity

| Setting | Env var | What to set it to |
|---|---|---|
| `executor_id` | `DBOS__VMID` | A stable identifier for this instance (pod name, task ARN) |
| `application_version` | `DBOS__APPVERSION` | A git SHA or release tag, identical across every instance of one deployment |

- [ ] Set both deliberately for any multi-instance deployment. The computed fallback
      (`Dbos.Version.compute/1`) hashes code per instance, so instances can disagree.
- [ ] Confirm they gate recovery and dequeue the way you expect: a workflow whose
      `application_version` no live executor matches sits unclaimed.

## Notifications

- [ ] Check the boot logs of a real deployment for the listener fallback warning.
      `Dbos.Notifications` opens one dedicated connection so `recv_message`, `get_event`, and
      stream reads wake on `NOTIFY`; a failure to establish it degrades to 1-second polling and
      startup continues.
- [ ] On `Dbos.DB.Postgrex`, pass `notifications_conn_opts:` so connection options can be derived.

## Connection pool

`Dbos` checks out connections from the pool you already run. Size it for:

- [ ] One connection per `deftransaction` step running, held for the transaction's duration,
      including your own repo calls inside the same step body.
- [ ] One connection per in-flight system-database query outside a transaction — checkpoint
      writes, status reads, queue dequeue polls — brief, and concurrent across every running
      workflow.
- [ ] Web request concurrency on top, if requests and workflows share the pool.
- [ ] `Dbos.Notifications` holds its own connection outside the pool, so it does not count
      against `pool_size`.

## Leases and dead-executor recovery

On by default (`lease_sweep: [enabled: true]`): any `PENDING` workflow whose executor's lease has
expired or is absent is reclaimed and redispatched.

- [ ] Review `lease.ttl_ms` (default `60_000`) and `lease.renew_interval_ms` (default `10_000`)
      against how long you can tolerate a dead instance's workflows sitting idle.
- [ ] Review `lease_sweep.interval_ms` (default `30_000`), the scan cadence. Worst-case detection
      is the TTL plus this interval; each pass is one indexed query over `PENDING` rows.
- [ ] A `DBOS__VMID` that changes every deploy is handled by the lease alone: the old instance's
      lease expires once it stops renewing, and its `PENDING` rows become reclaimable.

## Queues

- [ ] Set `worker_concurrency` (this executor's concurrent claims), `global_concurrency` (the
      fleet's), and `rate_limit` (claims per period) on every `Dbos.Queue` to values matched to
      what the queue calls downstream.
- [ ] Confirm what is actually deployed through the admin server's
      `GET /dbos-workflow-queues-metadata`, which reads each queue's persisted configuration.

## Admin server

`Dbos.AdminServer` is off by default (`admin_server: [enabled: true, port: 3001]`). Alongside
introspection (`POST /workflows`, `GET /workflows/{id}`, `GET /workflows/{id}/steps`) it exposes
mutating operations: `POST /workflows/{id}/cancel`, `POST /workflows/{id}/resume`,
`POST /workflows/{id}/fork`, `POST /dbos-workflow-recovery`, `POST /dbos-garbage-collect`,
`GET /deactivate`, and `POST /dbos-global-timeout` (cancels every non-terminal workflow created at
or before a cutoff, fleet-wide).

- [ ] **There is no authentication or authorization on any of these routes.** Keep the port off
      the public internet. Put it behind whatever restricts access to your operational tooling —
      a private network, an authenticating proxy, a bastion — and treat it with the same care as
      direct database access.

## Telemetry and alerting

Four `:telemetry.span/3` spans — workflow, step, queue dequeue, recovery — each with `:start`,
`:stop`, and `:exception`. Attach your own handler.

- [ ] Alert on `[:dbos, :workflow, :exception]` — a workflow process crashed running its body.
- [ ] Alert on `[:dbos, :step, :exception]` — a step failed.
- [ ] Alert on `[:dbos, :recovery, :exception]` — a recovery or reclaim pass itself failed.
- [ ] Filter `[:dbos, :queue, :dequeue, :exception]` through
      `Dbos.SystemDb.contention_error?/1` before paging. Lock-contention races under concurrent
      dequeuers are routine.
- [ ] Add a periodic query against `workflow_status` for workflows stuck `PENDING` past a
      threshold, anything at `MAX_RECOVERY_ATTEMPTS_EXCEEDED`, and queue depth per queue.

## Retention

`POST /dbos-garbage-collect` deletes
`workflow_status` rows and everything cascading from them, by age
(`cutoff_epoch_timestamp_ms`) or by row count (`rows_threshold`, keeping that many of the newest).
`PENDING`, `ENQUEUED`, and `DELAYED` rows survive any cutoff.

- [ ] Pick a retention policy. Every checkpoint of every step of every workflow stays in
      `operation_outputs` until something calls this.
- [ ] Wire the call into a periodic job of your own — a scheduled workflow, a cron container.
      The engine runs no schedule for it.

## Risks to accept knowingly

- [ ] **A node that hangs while still renewing its lease.** The sweep protects against a node
      that stops renewing. A node alive enough to renew but too wedged to progress keeps its
      claim; `owner_xid` detects a resulting double execution after the fact.
- [ ] **At-least-once side effects, exactly-once checkpoints.** A crash between a step's real
      side effect and its checkpoint committing re-runs that side effect on replay. Steps that
      must not repeat need an idempotency key at their own boundary.
- [ ] **Changing a workflow's step layout while instances are in flight.** Guard the new code
      with `Dbos.patch/1`, and retire the guard with `Dbos.deprecate_patch/1` once those
      executions have drained. A version bump or a new workflow name covers larger changes.
- [ ] **The admin server has no auth.** Repeated here because it is easy to enable for
      convenience and forget to lock down.
