# DBOS system database schema (Postgres)

Source: `reference/dbos-transact-golang/dbos/internal/sysdb/migrations/` (47 files),
applied by `BuildMigrations` / `RunMigrations` in
`reference/dbos-transact-golang/dbos/internal/sysdb/system_database.go`.

Fixture: `priv/schema/dbos_schema.sql` (hand-assembled, concrete SQL for schema
`dbos`), verified by applying it to a scratch database and dumping the result
to `priv/schema/dbos_schema_dump.sql` with `pg_dump --schema-only`.

## Ordering / variant rule

Migrations do **not** run in filesystem/lexical order off disk. Each file is
`//go:embed`-ed into a named Go string constant
(`system_database.go:203-342`), and `BuildMigrations(schema, isCockroach)`
(`system_database.go:377-465`) returns a hardcoded `[]MigrationFile{...}`
slice in explicit version order 1..42. The `<n>_name.sql` filename is only a
human label; the array literal is authoritative.

Several version numbers have more than one file on disk; which one is used
depends on `isCockroach` (this fixture is the Postgres path, `isCockroach =
false`):

| Version | Files on disk | Postgres path uses | Rule (source) |
|---|---|---|---|
| 1 | `1_initial_dbos_schema.sql`, `1_initial_dbos_schema_listen_notify.sql` | both, concatenated (base + `\n` + listen/notify tail) | `system_database.go:380-388` — listen/notify tail only appended `if !isCockroach` |
| 10 | `10_add_notifications_pkey.sql`, `10_add_notifications_pkey_cockroach.sql`, `10_check_notifications_pkey_cockroach.sql` | `10_add_notifications_pkey.sql` (the `DO $$ ... $$` block) | `system_database.go:437` builds it unconditionally from `migration10SQL`; the two `*_cockroach.sql` files are only read by `applyCockroachMigration10` (`system_database.go:684-711`), invoked only `case migration.Version == 10 && isCockroach` (`system_database.go:614`) |
| 20 | `20_set_function_search_path.sql` | the file's SQL | `system_database.go:394-398` — populated `if !isCockroach`, else empty string (no-op, version still advances) |
| 28 | `28_drop_dedup_id_constraint.sql`, `28_drop_dedup_id_constraint_cockroach.sql` | `28_drop_dedup_id_constraint.sql` (`ALTER TABLE ... DROP CONSTRAINT`) | `system_database.go:403-407` — `migration28File` defaults to the Postgres SQL, swapped to the cockroach file only `if isCockroach` |
| 38 | `38_update_enqueue_workflow.sql`, `38_set_enqueue_workflow_search_path.sql` | both, concatenated (base + `\n` + search-path tail) | `system_database.go:413-416` — tail appended `if !isCockroach` |
| 39 | `39_create_streams_trigger.sql` | the file's SQL | `system_database.go:421-426` — populated `if !isCockroach`, else empty string (no-op) |

The `CONCURRENTLY` keyword in the 12 "online" index migrations (22-27, 29,
30, 32, 34, 35, 37 — each has `Online: !isCockroach` at
`system_database.go:449-464`) is filled in from `concurrentlyKw(isCockroach)`
(`system_database.go:369-375`), which returns `"CONCURRENTLY"` for Postgres
and `""` for CockroachDB. Fixture uses `CONCURRENTLY` throughout.

The `sqlite/` subdirectory and `sqlite_migrations.go` are a completely
separate migration set for the embedded SQLite driver, never touched by
`BuildMigrations`/`RunMigrations` (the Postgres path) — irrelevant here.

## Migration-version bookkeeping

Table: `<schema>.dbos_migrations` (here `dbos.dbos_migrations`), created by
`RunMigrations` itself, **not** by migration 1:

```sql
CREATE TABLE dbos.dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)
```

(`system_database.go:596-597`, inside the same short transaction that
conditionally does `CREATE SCHEMA` if the schema doesn't exist yet).

Single-row table. `writeMigrationVersion` (`system_database.go:568-583`)
`INSERT`s the row the first time (`lastApplied == 0`, i.e. no prior version
recorded) and `UPDATE`s it (unconditional `SET version = $1`, no `WHERE`) on
every subsequent migration. Confirms the doc's claim of
`dbos.dbos_migrations.version` — correct.

Final version after all 42 migrations: **42**. Verified in the fixture
database:

```
 version
---------
      42
```

## Tables

All 11 tables live in schema `dbos`. Columns listed in final (post-migration)
order.

### `workflow_status`
Primary workflow record. PK `workflow_uuid`.

| Column | Type | Null | Default |
|---|---|---|---|
| workflow_uuid | text | not null | |
| status | text | | |
| name | text | | |
| authenticated_user | text | | |
| assumed_role | text | | |
| authenticated_roles | text | | |
| request | text | | |
| output | text | | |
| error | text | | |
| executor_id | text | | |
| created_at | bigint | not null | `(EXTRACT(epoch FROM now())::numeric * 1000)::bigint` |
| updated_at | bigint | not null | `(EXTRACT(epoch FROM now())::numeric * 1000)::bigint` |
| application_version | text | | |
| application_id | text | | |
| class_name | varchar(255) | | NULL |
| config_name | varchar(255) | | NULL |
| recovery_attempts | bigint | | 0 |
| queue_name | text | | |
| workflow_timeout_ms | bigint | | |
| workflow_deadline_epoch_ms | bigint | | |
| inputs | text | | |
| started_at_epoch_ms | bigint | | |
| deduplication_id | text | | |
| priority | integer | not null | 0 |
| queue_partition_key | text | | (migration 2) |
| forked_from | text | | (migration 4) |
| owner_xid | text | | NULL (migration 7) |
| parent_workflow_id | text | | NULL (migration 8) |
| serialization | text | | NULL (migration 11) |
| delay_until_epoch_ms | bigint | | NULL (migration 16) |
| was_forked_from | boolean | not null | false (migration 18) |
| rate_limited | boolean | not null | false (migration 33) |
| completed_at | bigint | | (migration 36) |
| attributes | jsonb | | (migration 40) |
| schedule_name | text | | (migration 41) |
| debounce_deadline_epoch_ms | bigint | | NULL (migration 42) |
| is_debounced | boolean | not null | false (migration 42) |

Constraints/indexes on `workflow_status` — this table absorbed a long
sequence of index churn (create → drop → recreate-as-partial):

- PK `workflow_status_pkey` (`workflow_uuid`) — migration 1
- `workflow_status_created_at_index` (`created_at`) — migration 1, survives unchanged
- `workflow_status_executor_id_index` (`executor_id`) — migration 1, **dropped** in migration 26
- `workflow_status_status_index` (`status`) — migration 1, **dropped** in migration 31 (superseded by two partial indexes below)
- `uq_workflow_status_queue_name_dedup_id` UNIQUE (`queue_name`,`deduplication_id`) constraint — migration 1, **dropped** in migration 28 (superseded by partial unique index below)
- `idx_workflow_status_queue_status_started` (`queue_name`,`status`,`started_at_epoch_ms`) — migration 3, **dropped** in migration 35
- `idx_workflow_status_forked_from` (`forked_from`) — created migration 4, dropped migration 22, recreated as **partial** (`WHERE forked_from IS NOT NULL`) migration 23
- `idx_workflow_status_parent_workflow_id` (`parent_workflow_id`) — created migration 8, dropped migration 24, recreated as **partial** (`WHERE parent_workflow_id IS NOT NULL`) migration 25
- `uq_workflow_status_dedup_id` UNIQUE partial (`queue_name`,`deduplication_id`) `WHERE deduplication_id IS NOT NULL` — migration 27 (replacement for the dropped full-table unique constraint)
- `idx_workflow_status_pending` (`created_at`) `WHERE status = 'PENDING'` — migration 29
- `idx_workflow_status_failed` (`status`,`created_at`) `WHERE status IN ('ERROR','CANCELLED','MAX_RECOVERY_ATTEMPTS_EXCEEDED')` — migration 30
- `idx_workflow_status_in_flight` (`queue_name`,`status`,`priority`,`created_at`) `WHERE status IN ('ENQUEUED','PENDING')` — migration 32, the main dequeue-query index
- `idx_workflow_status_rate_limited` (`queue_name`,`started_at_epoch_ms`) `WHERE rate_limited = TRUE` — migration 34
- `idx_workflow_status_delayed` (`delay_until_epoch_ms`) `WHERE status = 'DELAYED'` — migration 16
- `idx_workflow_status_started_at` (`started_at_epoch_ms`) `WHERE started_at_epoch_ms IS NOT NULL` — migration 37
- `idx_workflow_status_completed_at` (`completed_at`) `WHERE completed_at IS NOT NULL` — migration 36
- `idx_workflow_status_attributes` GIN (`attributes`) `WHERE attributes IS NOT NULL` — migration 40 (containment `@>` queries)
- `idx_workflow_status_schedule_name` (`schedule_name`) `WHERE schedule_name IS NOT NULL` — migration 41

Final live index set (confirmed via `psql \d`): `workflow_status_pkey`,
`idx_workflow_status_attributes`, `idx_workflow_status_completed_at`,
`idx_workflow_status_delayed`, `idx_workflow_status_failed`,
`idx_workflow_status_forked_from`, `idx_workflow_status_in_flight`,
`idx_workflow_status_parent_workflow_id`, `idx_workflow_status_pending`,
`idx_workflow_status_rate_limited`, `idx_workflow_status_schedule_name`,
`idx_workflow_status_started_at`, `uq_workflow_status_dedup_id`,
`workflow_status_created_at_index` — 14 indexes total, matching the churn
above (the four dropped/superseded ones are gone).

### `operation_outputs`
Step-level results. PK `(workflow_uuid, function_id)`. FK `workflow_uuid` →
`workflow_status(workflow_uuid)` `ON UPDATE CASCADE ON DELETE CASCADE`.

| Column | Type | Null | Default |
|---|---|---|---|
| workflow_uuid | text | not null | |
| function_id | integer | not null | |
| function_name | text | not null | `''` |
| output | text | | |
| error | text | | |
| child_workflow_id | text | | |
| started_at_epoch_ms | bigint | | (migration 5) |
| completed_at_epoch_ms | bigint | | (migration 5) |
| serialization | text | | NULL (migration 11) |

Index: `idx_operation_outputs_completed_at_function_name`
(`completed_at_epoch_ms`,`function_name`) — migration 19.

### `notifications`
Cross-workflow send/recv mailbox. PK `message_uuid` (added in migration 1's
`DEFAULT ... PRIMARY KEY` inline in the column def). FK `destination_uuid` →
`workflow_status`.

| Column | Type | Null | Default |
|---|---|---|---|
| destination_uuid | text | not null | |
| topic | text | | |
| message | text | not null | |
| created_at_epoch_ms | bigint | not null | `(EXTRACT(epoch FROM now())::numeric * 1000)::bigint` |
| message_uuid | text | not null (PK) | `gen_random_uuid()` |
| serialization | text | | NULL (migration 11) |
| consumed | boolean | not null | false (migration 12) |

Indexes: `idx_workflow_topic` (`destination_uuid`,`topic`) — migration 1;
`idx_notifications` (`destination_uuid`,`topic`) — migration 12, a near-exact
duplicate of `idx_workflow_topic` (see Surprises below).

Trigger: `dbos_notifications_trigger` AFTER INSERT → `notifications_function()`.

### `workflow_events`
Durable workflow-to-caller key/value events. PK `(workflow_uuid, key)`. FK →
`workflow_status`.

| Column | Type | Null | Default |
|---|---|---|---|
| workflow_uuid | text | not null | |
| key | text | not null | |
| value | text | not null | |
| serialization | text | | NULL (migration 11) |

Trigger: `dbos_workflow_events_trigger` AFTER INSERT → `workflow_events_function()`.

### `workflow_events_history`
Per-step audit trail of event writes, added migration 6 (so fork can replay
which step wrote which event). PK `(workflow_uuid, function_id, key)`. FK →
`workflow_status`.

| Column | Type | Null | Default |
|---|---|---|---|
| workflow_uuid | text | not null | |
| function_id | integer | not null | |
| key | text | not null | |
| value | text | not null | |
| serialization | text | | NULL (migration 11) |

No indexes beyond the PK.

### `streams`
Append-only per-workflow value stream (offset-ordered). PK `(workflow_uuid,
key, "offset")`. FK → `workflow_status`.

| Column | Type | Null | Default |
|---|---|---|---|
| workflow_uuid | text | not null | |
| key | text | not null | |
| value | text | not null | |
| "offset" | integer | not null | |
| function_id | integer | not null | 0 (migration 6) |
| serialization | text | | NULL (migration 11) |

Trigger: `dbos_streams_trigger` AFTER INSERT → `streams_function()` — installed
migration 39, Postgres-only.

### `event_dispatch_kv`
Generic per-service/workflow-function KV store, unchanged since migration 1.
PK `(service_name, workflow_fn_name, key)`.

| Column | Type | Null | Default |
|---|---|---|---|
| service_name | text | not null | |
| workflow_fn_name | text | not null | |
| key | text | not null | |
| value | text | | |
| update_seq | numeric(38,0) | | |
| update_time | numeric(38,15) | | |

### `application_versions`
Added migration 13. PK `version_id`, unique `version_name`.

| Column | Type | Null | Default |
|---|---|---|---|
| version_id | text | not null (PK) | |
| version_name | text | not null (UNIQUE) | |
| version_timestamp | bigint | not null | `(EXTRACT(epoch FROM now())::numeric * 1000)::bigint` |
| created_at | bigint | not null | `(EXTRACT(epoch FROM now())::numeric * 1000)::bigint` |

### `workflow_schedules`
Added migration 9, extended migrations 15 and 17. PK `schedule_id`, unique
`schedule_name`.

| Column | Type | Null | Default |
|---|---|---|---|
| schedule_id | text | not null (PK) | |
| schedule_name | text | not null (UNIQUE) | |
| workflow_name | text | not null | |
| workflow_class_name | text | | |
| schedule | text | not null | |
| status | text | not null | `'ACTIVE'` |
| context | text | not null | |
| last_fired_at | text | | NULL (migration 15) |
| automatic_backfill | boolean | not null | false (migration 15) |
| cron_timezone | text | | NULL (migration 15) |
| queue_name | text | | NULL (migration 17) |

### `queues`
Added migration 21. PK `queue_id`, unique `name`.

| Column | Type | Null | Default |
|---|---|---|---|
| queue_id | text | not null (PK) | `gen_random_uuid()::text` |
| name | text | not null (UNIQUE) | |
| concurrency | integer | | |
| worker_concurrency | integer | | |
| rate_limit_max | integer | | |
| rate_limit_period_sec | double precision | | |
| priority_enabled | boolean | not null | false |
| partition_queue | boolean | not null | false |
| polling_interval_sec | double precision | not null | 1.0 |
| created_at | bigint | not null | `(EXTRACT(epoch FROM now()) * 1000.0)::bigint` |
| updated_at | bigint | not null | `(EXTRACT(epoch FROM now()) * 1000.0)::bigint` |

### `dbos_migrations`
Bookkeeping table, created by `RunMigrations`, not a migration file. PK
`version`.

| Column | Type | Null | Default |
|---|---|---|---|
| version | bigint | not null (PK) | |

## Functions

| Function | Returns | Purpose | Installed / changed |
|---|---|---|---|
| `notifications_function()` | trigger | Builds `destination_uuid || '::' || topic` and `pg_notify`s it on the notifications channel | migration 1 (listen/notify tail); `search_path` pinned migration 20 |
| `workflow_events_function()` | trigger | Builds `workflow_uuid || '::' || key` and `pg_notify`s it on the workflow-events channel | migration 1 (listen/notify tail); `search_path` pinned migration 20 |
| `streams_function()` | trigger | Builds `workflow_uuid || '::' || key` and `pg_notify`s it on the streams channel | migration 39; `search_path` pinned in the same migration |
| `enqueue_workflow(...)` | text | Client-side SQL entry point to enqueue a workflow directly (no Go runtime needed) — validates params, inserts a row into `workflow_status` with status `ENQUEUED` or `DELAYED`, `ON CONFLICT (workflow_uuid) DO UPDATE SET updated_at`, returns the workflow id | created migration 14 (13-arg signature); `search_path` pinned migration 20; **dropped and replaced** migration 38 with a 16-arg signature adding `authenticated_user`, `authenticated_roles`, `delay_until_epoch_ms`; `search_path` re-pinned in migration 38's Postgres-only tail |
| `send_message(...)` | void | Client-side SQL entry point to insert into `notifications`, `ON CONFLICT (message_uuid) DO NOTHING`, raises `foreign_key_violation` as a custom "DBOS non-existent workflow" error | migration 14; `search_path` pinned migration 20 |

Only the 16-arg `enqueue_workflow` exists in the final schema — confirmed via
`\df dbos.*` on the fixture (one row, not two).

## LISTEN/NOTIFY channels

Three channel name constants, defined in Go at `system_database.go:352-355`
and hardcoded as string literals inside the trigger function bodies:

| Channel | Constant | Payload expression (quoted from the migration file) | Fired by |
|---|---|---|---|
| `dbos_notifications_channel` | `_DBOS_NOTIFICATIONS_CHANNEL` | `NEW.destination_uuid \|\| '::' \|\| NEW.topic` | `dbos_notifications_trigger` AFTER INSERT ON `notifications` |
| `dbos_workflow_events_channel` | `_DBOS_WORKFLOW_EVENTS_CHANNEL` | `NEW.workflow_uuid \|\| '::' \|\| NEW.key` | `dbos_workflow_events_trigger` AFTER INSERT ON `workflow_events` |
| `dbos_streams_channel` | `_DBOS_STREAMS_CHANNEL` | `NEW.workflow_uuid \|\| '::' \|\| NEW.key` | `dbos_streams_trigger` AFTER INSERT ON `streams` |

All three payloads are literally `<id> || '::' || <sub-key>` — a consumer
subscribed to the channel splits on `::` to recover which row changed,
without needing the payload to carry the actual value (callers re-query).

## Surprises / contradictions between migrations

- **`idx_notifications` duplicates `idx_workflow_topic`.** Migration 1
  creates `idx_workflow_topic ON notifications (destination_uuid, topic)`.
  Migration 12's comment says it's adding "consumed column ... and index for
  unconsumed lookups," but the index it actually creates —
  `idx_notifications ON notifications (destination_uuid, topic)` — has the
  *exact same columns* as the migration-1 index, not `(destination_uuid,
  topic) WHERE NOT consumed` or similar. Both indexes exist in the final
  schema; the second is redundant (same key, same access pattern, no partial
  predicate to make it more selective for "unconsumed lookups"). This looks
  like dead weight the DBOS authors never cleaned up.

- **Migration file header comments carry stale version numbers.** The
  comment inside `22_drop_forked_from_index.sql` says "Migration 18", inside
  `23_create_partial_forked_from_index.sql` says "Migration 19", `24_...`
  says "Migration 20", `25_...` says "Migration 21", `26_...` says "Migration
  22", `27_...` says "Migration 23" — each one off by 4 from its actual
  `BuildMigrations` version (22, 23, 24, 25, 26, 27 respectively). The
  authoritative version is the position in the `BuildMigrations` Go slice,
  not the filename or the in-file comment — these comments are leftover from
  a renumbering and are simply wrong.

- **CockroachDB variants are invisible from the Go build's naming
  convention alone.** `10_add_notifications_pkey_cockroach.sql` and
  `10_check_notifications_pkey_cockroach.sql` are never assembled into a
  version-10 migration string via `fmt.Sprintf`/concatenation the way every
  other file is — they're read directly as raw query text by
  `applyCockroachMigration10` and executed with hand-written Go control flow
  (query for existing PK, conditionally `ALTER TABLE ... ADD PRIMARY KEY`
  inside the same tx). A naive "concatenate all files whose name contains the
  version number" approach would have wrongly included both cockroach files
  in the Postgres build.

- **Migration 20 and 39 are conditionally-empty, not conditionally-absent.**
  Both still consume a version number and advance `dbos_migrations.version`
  on CockroachDB even though they execute no SQL there
  (`strings.TrimSpace(migration.SQL) == ""` branch at
  `system_database.go:611-613` is a deliberate no-op, not an error). The
  version sequence has no gaps on either dialect; behavior differs, numbering
  doesn't.

- **The `workflow_status` primary key comes from the column definition
  (`workflow_uuid TEXT PRIMARY KEY`), while `notifications`' primary key
  comes from an inline `DEFAULT gen_random_uuid() PRIMARY KEY` clause** — an
  earlier version of DBOS shipped `notifications` *without* a primary key at
  all (per migration 10's own comment), which is why migration 10 exists: to
  backfill the PK for pre-existing deployments. Fresh deployments (like this
  fixture) get the PK from migration 1 directly, so migration 10's `DO $$`
  block is a no-op here — confirmed no error and no second constraint
  appeared when applying it.

## Verification performed

```
dropdb --if-exists dbos_schema_fixture
createdb dbos_schema_fixture
psql -v ON_ERROR_STOP=1 -d dbos_schema_fixture -f priv/schema/dbos_schema.sql
```

Applied with zero errors (one harmless `NOTICE: trigger ... does not exist,
skipping` from the defensive `DROP TRIGGER IF EXISTS` in migration 39, which
is expected on a fresh database). Final state confirmed:

- `SELECT version FROM dbos.dbos_migrations` → `42`
- `\dt dbos.*` → 11 tables
- `\df dbos.*` → 5 functions, single `enqueue_workflow` signature (16 args)
- Dumped via `pg_dump --schema-only --no-owner --no-privileges -d
  dbos_schema_fixture -n dbos > priv/schema/dbos_schema_dump.sql`
