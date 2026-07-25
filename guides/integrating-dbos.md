# Integrating Dbos into an Existing Application

This guide is for adding `Dbos` to an app that already has a Postgres-backed `Ecto.Repo` (or
a bare `Postgrex` pool) and its own supervision tree — the situation `guides/quickstart.md`
skips past.

## Where it goes in the supervision tree

`Dbos.Supervisor` needs a live database connection the moment it starts, to verify its schema.
Start it **after** your repo:

```elixir
def start(_type, _args) do
  children = [
    MyApp.Repo,
    {Dbos.Supervisor,
     name: Dbos,
     db: {Dbos.DB.Ecto, MyApp.Repo},
     workflows: [MyApp.Checkout, MyApp.Onboarding],
     migrations: :verify}
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
end
```

`workflows:` takes either module names — every `defworkflow` in each module is registered
automatically via its generated `__dbos_workflows__/0` — or explicit `{name, {module,
function, arity}}` tuples if you'd rather list them by hand.

## Sharing the repo and pool — there is no second pool

`Dbos.Config` holds a `{db_module, conn}` pair, and every checkpoint write goes through it:

| `:db` | `conn` | Use when |
|---|---|---|
| `{Dbos.DB.Ecto, MyApp.Repo}` | your `Ecto.Repo` module | your app already has one |
| `{Dbos.DB.Postgrex, pool_name}` | a `Postgrex` process/pool name | no Ecto in the app |

Either way, `Dbos` checks out connections from the pool you already run — there is no second
connection pool to size or monitor. A `deftransaction` step runs inside one
`config.db.transaction/3` call on that same pool, so your own `Repo` writes made inside the
step body and the step's checkpoint commit or roll back together, in one transaction.

## The dedicated LISTEN connection

Blocking waits (`Dbos.recv_message/3`, `Dbos.get_event/4`, stream reads) need to wake up
promptly when the row they're waiting on appears, without polling constantly. That needs a
Postgres `LISTEN`, and `LISTEN` occupies a connection for its entire lifetime — it can't be
borrowed from a pool designed to be checked in and out per query. So `Dbos.Notifications`
opens **one** extra, separate connection per engine for this, distinct from your app's pool.

Where those connection options come from:

- `notifications_conn_opts:` on `Dbos.Supervisor` — pass them explicitly and they're used as-is.
- Otherwise, if `:db` is `{Dbos.DB.Ecto, MyApp.Repo}`, they're derived automatically from
  `MyApp.Repo.config()`.
- Otherwise (a bare `Dbos.DB.Postgrex` pool, with no `notifications_conn_opts` given), there is
  nothing to derive connection options from.

If no connection options can be found, or the connection attempt itself fails, `Dbos` logs a
warning and falls back to polling (`Dbos.Notifications`'s `:poll` mode, fixed at a 1-second
cadence) for every blocking wait on that engine. Startup continues. Waits still work; they
wake up on a timer. A connection that drops later (network blip, Postgres restart) reconnects
with backoff automatically, and every registered waiter is woken to re-check once it's back, so
a `NOTIFY` fired during the outage isn't lost.

If you're on the `Dbos.DB.Postgrex` adapter and want real `LISTEN` notifications, pass
`notifications_conn_opts:` yourself to enable them — there's no repo to derive them from:

```elixir
{Dbos.Supervisor,
 db: {Dbos.DB.Postgrex, MyApp.PostgrexPool},
 notifications_conn_opts: [hostname: "localhost", database: "myapp_prod", username: "myapp"],
 ...}
```

Pass `notifications: :poll` instead if you'd rather skip the dedicated connection entirely —
useful in an environment with a hard connection-count ceiling.

## Migration verification at launch

`Dbos.Migrator.verify!/1` checks `<schema>.dbos_migrations.version` is exactly `42` — the
version this port targets — and **raises**, refusing to start, if it's anything else or the
table doesn't exist at all. This is deliberate: an engine running against the wrong schema
version would checkpoint into tables whose shape it doesn't actually match.

`:migrations` on `Dbos.Supervisor` controls what happens:

| Value | Behavior |
|---|---|
| `:verify` (default) | Only checks. Raises if the schema isn't already at version 42. **Use this in production** — schema changes belong in your own deploy pipeline, run and reviewed like any other migration. |
| `:create_if_absent` | Tries `verify!/1` first; on failure, applies `priv/schema/dbos_schema.sql` verbatim and verifies again. Convenient for local dev and quick starts. In production, migrations should be reviewed and applied deliberately through your own deploy pipeline. |
| `:skip` | Does neither. The test suites use this because the schema is already applied once, up front, by `test/test_helper.exs`, and every test just reuses it. |

## Executor identity and application version for a release

Two identifiers matter once you have more than one running instance.

**`executor_id`** — which running process owns a given `PENDING` workflow, so recovery and
cluster reclaim know whose rows are whose. Resolved as: `opts[:executor_id]`, else the
`DBOS__VMID` environment variable, else `to_string(node())`. In a release running under a
stable, meaningful node name that's fine; in a container-orchestrated deployment where node
names are often random or ephemeral, set `DBOS__VMID` explicitly to something stable for that
instance — a pod name, a task ARN, whatever your platform gives you that survives a restart of
*that* instance but changes on a genuinely new one.

**`application_version`** — stamped onto every workflow a given process starts
(`workflow_status.application_version`), used to gate which executor is allowed to recover a
workflow started by a specific build (see `guides/architecture.md` / `docs/interop-migration.md`
for how this interacts with in-flight upgrades). Resolved as: `opts[:application_version]`,
else the `DBOS__APPVERSION` environment variable, else a hash computed from the compiled BEAM
code of every workflow module (`Dbos.Version.compute/1`). The computed fallback is fine for a
single-node dev loop; for a real release, set `DBOS__APPVERSION` to something that identifies
the actual deployed build — a git SHA or release tag — so every instance of the same
deployment agrees on the same version without needing to recompute a hash from code.

Both are read once, at `Dbos.Supervisor` boot, into the resolved `Dbos.Config` stored in
`:persistent_term` — set the environment variables before the supervisor starts.

## What to do in tests

Point a test repo or pool at a real scratch Postgres database with the schema already
applied once, and reuse it across the run — don't apply migrations per test.

`test/test_helper.exs` in this codebase does exactly that: drops and recreates a scratch
database, applies `priv/schema/dbos_schema.sql` once, and starts a plain `Postgrex`
connection alongside an `Ecto.Repo` so both adapters can be exercised. Model your own
`test_helper.exs` the same way against your app's test database.

Per test, start an engine with a **fresh `:name`** so tests don't collide with each other's
workflows or registries, `migrations: :skip` (the schema's already there), and an explicit
`:executor_id` so recovery scoping is deterministic:

```elixir
name = Module.concat(MyTest, :"Engine#{System.unique_integer([:positive])}")

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
```

`await_boot_recovery/1` blocks until the at-boot recovery pass (kicked off asynchronously via
`handle_continue`, so `start_link` itself doesn't block) has actually finished — without it, a
test that starts a workflow right after `start_supervised!` can race that pass. Truncate the
`dbos` tables between tests (`workflow_status`, `operation_outputs`, and the rest — see
`test/support/case.ex`); the schema itself stays in place across the run.
