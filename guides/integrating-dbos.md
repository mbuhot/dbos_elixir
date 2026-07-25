# Integrating Dbos into an Existing Application

For adding `Dbos` to an app that already has a Postgres-backed `Ecto.Repo` (or a bare `Postgrex`
pool) and its own supervision tree.

## Where it goes in the supervision tree

`Dbos.Supervisor` needs a live database connection the moment it starts, to verify its schema.
Start it after your repo:

```elixir
def start(_type, _args) do
  children = [
    MyApp.Repo,
    {Dbos.Supervisor,
     name: Dbos,
     db: {Dbos.DB.Ecto, MyApp.Repo},
     otp_app: :my_app}
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
end
```

`otp_app: :my_app` discovers every workflow module compiled into that OTP application — anything
exporting `__dbos_workflows__/0`, which every module containing a `defworkflow` does
automatically. A module is registered by being compiled into the app.

`workflows:` is additive on top of discovery, for a workflow module that lives in a dependency:

```elixir
{Dbos.Supervisor,
 name: Dbos,
 db: {Dbos.DB.Ecto, MyApp.Repo},
 otp_app: :my_app,
 workflows: [SomeDependency.Workflows]}
```

It takes module names, or explicit `{name, {module, function, arity}}` tuples. At least one of
`otp_app:` or `workflows:` is required; an engine started with neither raises.

## Sharing the repo and pool

`Dbos.Config` holds a `{db_module, conn}` pair, and every checkpoint write goes through it:

| `:db` | `conn` | Use when |
|---|---|---|
| `{Dbos.DB.Ecto, MyApp.Repo}` | your `Ecto.Repo` module | your app already has one |
| `{Dbos.DB.Postgrex, pool_name}` | a `Postgrex` process/pool name | no Ecto in the app |

`Dbos` checks out connections from the pool you already run. A `deftransaction` step runs inside
one transaction on that same pool, so your own `Repo` writes made in the step body and the
step's checkpoint commit or roll back together.

## The dedicated LISTEN connection

Blocking waits (`Dbos.recv_message/3`, `Dbos.get_event/4`, stream reads) wake up on a Postgres
`NOTIFY`. `LISTEN` occupies a connection for its entire lifetime, so `Dbos.Notifications` opens
one extra connection per engine, separate from your app's pool.

Where those connection options come from:

| Source | Applies when |
|---|---|
| `notifications_conn_opts:` on `Dbos.Supervisor` | always wins, used as-is |
| `MyApp.Repo.config()` | `:db` is `{Dbos.DB.Ecto, MyApp.Repo}` and no explicit opts given |

On the `Dbos.DB.Postgrex` adapter there is no repo to derive from, so pass the options yourself:

```elixir
{Dbos.Supervisor,
 db: {Dbos.DB.Postgrex, MyApp.PostgrexPool},
 notifications_conn_opts: [hostname: "localhost", database: "myapp_prod", username: "myapp"],
 ...}
```

If no options can be found, or the connection attempt fails, `Dbos` logs a warning and falls
back to polling every blocking wait on that engine at a 1-second cadence. Startup continues. A
connection that drops later reconnects with backoff, and every registered waiter is woken to
re-check once it is back, so a `NOTIFY` fired during the outage still gets seen.

Pass `notifications: :poll` to skip the dedicated connection deliberately — useful under a hard
connection-count ceiling.

## Installing the schema

The schema is an explicit step in your own migration sequence, generated once and committed:

```
mix dbos.gen.migration
mix dbos.gen.migration -r MyApp.Repo
```

This writes `priv/repo/migrations/<timestamp>_add_dbos.exs`:

```elixir
defmodule MyApp.Repo.Migrations.AddDbos do
  use Ecto.Migration

  def up, do: Dbos.Migration.up()

  def down, do: Dbos.Migration.down()
end
```

Run it with `mix ecto.migrate`. It appears in `mix ecto.migrations`, and `mix ecto.rollback`
reverts it. `Dbos.Migration.up/1` is safe to run twice: it brings the schema up to the version
this engine targets and does nothing for parts already present. See `Dbos.Migration` for the
`:prefix`/`:version` options.

`opts[:prefix]` (default `"dbos"`) must match the `:schema` passed to `Dbos.Supervisor`.

## Migration verification at launch

`Dbos.Migrator.verify!/1` checks that `<schema>.dbos_migrations.version` and
`<schema>.extension_migrations.version` are exactly the versions this engine targets
(`Dbos.Migrator.expected_version/0` and `Dbos.Migrator.expected_extension_version/0`). It raises
and refuses to start on any mismatch or missing table — an engine running against a schema whose
shape it does not match would checkpoint into the wrong tables.

`:migrations` on `Dbos.Supervisor` controls what runs before that check:

| Value | Behavior |
|---|---|
| `:verify` (default) | Only verifies. Install the schema with `mix dbos.gen.migration` and `mix ecto.migrate`. |
| `:skip` | Neither. For test engines where the schema is applied once up front. |

## Executor identity and application version

Two identifiers matter once more than one instance is running.

**`executor_id`** identifies which running process owns a given `PENDING` workflow, so recovery
and cluster reclaim know whose rows are whose. Resolved as `opts[:executor_id]`, else the
`DBOS__VMID` environment variable, else `to_string(node())`. Under container orchestration,
where node names are often ephemeral, set `DBOS__VMID` to something stable for that instance —
a pod name, a task ARN — that survives a restart of *that* instance and changes for a genuinely
new one.

**`application_version`** is stamped onto every workflow a process starts
(`workflow_status.application_version`) and gates which executor may recover a workflow started
by a specific build. Resolved as `opts[:application_version]`, else `DBOS__APPVERSION`, else a
hash of the compiled BEAM code of every workflow module. For a
release, set `DBOS__APPVERSION` to a git SHA or release tag so every instance of the same
deployment agrees on one value.

Both are read once, at `Dbos.Supervisor` boot, into the resolved `Dbos.Config` in
`:persistent_term`. Set the environment variables before the supervisor starts.

## Tests

Apply the generated migration to your test database (`MIX_ENV=test mix ecto.migrate`), then
start each test engine with `migrations: :skip` and `testing: :inline` (or `:manual`). Those
modes start none of the background processes, so everything runs on the connection
`Ecto.Adapters.SQL.Sandbox` already checked out for the test.

See `guides/tutorials/testing.md` for the full walkthrough.
