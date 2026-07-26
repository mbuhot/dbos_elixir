# Per-workflow version

## The problem

`SystemDb.reclaim_pending_workflows/4` filters on `application_version`, which is commonly a git
SHA. Every deploy changes it, so every deploy strands the previous build's `PENDING` workflows —
permanently, and for every workflow, because one fleet-wide identity covers them all.

## The design

A workflow declares its own version. Nothing is derived, inferred, or digested.

```elixir
defworkflow charge(order_id), name: "charge", version: "2" do
  ...
end
```

Reclaim matches on that instead of on `application_version`, so a deploy that leaves a workflow's
definition alone keeps claiming its in-flight instances.

Bumping the version when a change breaks replay is the developer's call, exactly as keeping the
body deterministic is. The engine records what it is told.

## Undeclared workflows

`version:` is optional. A workflow without one is claimable by any executor that registers its
name, at any application version.

The reported bug therefore disappears by default rather than on opt-in. Declaring no version says
"any deploy may resume this", which suits the majority of workflows: most changes leave replay
intact, and the current behaviour strands them all regardless.

## Schema

The next extension migration:

```sql
ALTER TABLE "dbos".workflow_status ADD COLUMN ex_workflow_version TEXT;

CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_workflow_status_reclaim"
  ON "dbos".workflow_status ("executor_id", "name", "ex_workflow_version")
  WHERE "status" = 'PENDING' AND "queue_name" IS NULL;
```

Nullable with no default, so the `ALTER TABLE` is metadata-only. The `ex_` prefix marks it as this
engine's extension and avoids a collision if upstream adds `workflow_version`.

`application_version` keeps its meaning: dequeue, the `application_versions` table, the admin
filter and Conductor all read it as fleet identity.

## Where the value is written

| Site | Behaviour |
|---|---|
| `SystemDb.insert_workflow_status/3` | Stamp from the registry entry for `name`; `NULL` when the name is unregistered here or declares no version |
| `SystemDb.claim_one/3` | Re-stamp alongside `application_version`, which this site already overwrites — a row enqueued by one build and dequeued by another runs the dequeuing build's code |
| `SystemDb.fork_workflow/4` | Copy verbatim |

## How reclaim uses it

The registry supplies parallel arrays of accepted `(name, version)` pairs, joined with `unnest`,
replacing the existing `name = ANY($4)` capability filter:

```sql
JOIN unnest($4::text[], $5::text[]) AS reg(name, version)
  ON reg.name = ws2.name
 AND ws2.ex_workflow_version IS NOT DISTINCT FROM reg.version
```

`IS NOT DISTINCT FROM` matches `NULL` to `NULL`, so an undeclared workflow is claimed by any
executor registering that name, while a declared one must match exactly.

`list_reclaimable_pending_workflow_ids/3` takes the same predicate, so the rescan loop agrees with
the claim.

## Capabilities

`executor_leases.ex_capabilities` already publishes `{name, version}` objects, pairing every
registered name with the executor's `application_version`. It publishes the declared workflow
version instead, so `Dbos.Recovery.orphans/1`, `GET /dbos-orphans` and `mix dbos.orphans` report
against the same identity reclaim uses.

## Open

- **`ENQUEUED` rows** hold no checkpoints, so dequeue's version gate is about routing rather than
  replay. Leaning: leave dequeue on `application_version`.
- **Child workflows** resolve their own name, so a parent at v1 and a child at v2 are independent.
  Worth a test.

## Rejected

Everything that computed the version on the developer's behalf:

- **A replay-skeleton digest of the workflow body.** It cannot see through a helper function, so
  it is unsound in the direction that matters, and it moves on edits that leave replay intact,
  stranding work for no reason.
- **`:off | :observe | :enforce` rollout modes.** Needed only because a computed version could be
  wrong.
- **Compatibility sets and ranges per workflow.** A deploy registers one version per name.
- **`Dbos.Recovery.adopt/3` and replay-prefix verification.** Machinery for recovering from the
  digest's mistakes.
