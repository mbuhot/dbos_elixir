# Decisions

Durable execution engine for Elixir, derived from DBOS Transact.

## Scope

| Decision | Choice |
|---|---|
| Target languages | Elixir only. Cross-language interop is a non-goal. |
| Reference | `dbos-transact-golang` @ `2a7705c37c93e5fd1d5c1ce049a4224ec2f1f969`, MIT (verified). Vendored read-only at `reference/`, gitignored. |
| Serialization | ETF (`:erlang.term_to_binary/1`), format name `"erl_etf"` in the `serialization` column. One-way door — documented in `docs/interop-migration.md`. |
| System DB | The host application's existing Postgres, via its existing repo and pool. No second pool. |
| Schema | The reference `dbos` schema, unmodified, at migration version 42. |

## Reference facts established in Phase 0

Recorded in `notes/`. Corrections to the original handoff document:

| Claim | Reality |
|---|---|
| 47 migrations | 47 migration *files*, 42 *versions*. Order is a hardcoded Go slice (`system_database.go:427-465`), not filename order. Cockroach and SQLite variants are not on the Postgres path. |
| `setEvent` allocates two step IDs | It allocates **one**. `getEvent` allocates two (`workflow.go:3269-3282`, `3325-3326`). |
| `recovery_attempts` increments unless status is ENQUEUED/DELAYED | Also requires the caller's `IncrementAttempts` flag, set only for dequeue and recovery. |
| `owner_xid` | Written on the INSERT branch only; the ON CONFLICT DO UPDATE never touches it. |
| Admin `/dbos-garbage-collect` | Upstream stub — the route does nothing. The underlying SQL exists. |

## Design

| Decision | Choice | Rationale |
|---|---|---|
| Context passing | Process dictionary, as Ecto threads its transaction. | No second language forces an explicit context argument. |
| Bare workflow call | `Checkout.process_order(id)` is durable. Inside a workflow it becomes a child workflow; outside one it starts a workflow and awaits the result; with the engine not started it raises `Dbos.NotStartedError`. | Ergonomics. The failure mode is loud. |
| Step naming | Function name and arity, module excluded (`"charge_card/2"`). `name:` overrides. | A module move leaves in-flight workflows replayable. |
| Workflow naming | Explicit `name:` required. | Recovery dispatches on it. |
| Determinism | Compile-time checker over the `defworkflow` body AST. | Step inputs are not stored, so the runtime `function_name` check cannot catch argument drift. |
| Errors | The raised exception struct, ETF-encoded, re-raised on replay with its type intact. | |
| Reserved step names | Reuse the upstream `DBOS.*` strings verbatim. | The DBOS console renders our workflows for free. |
| Transactional steps | `sameAsSystemDB` single-transaction path first. Separate-database Layer 2 only on demand. | The common Elixir deployment shares one database. |
| Migrations | Verify at launch, refuse to start on mismatch. Opt-in creation for dev. | |
| Workflow processes | `DynamicSupervisor` children, `:temporary`. | An OTP restart would re-invoke the body outside the replay path. |

## Phase 1

| Decision | Choice | Rationale |
|---|---|---|
| Serialization column encoding | `Dbos.Serialization.encode/1` is `:erlang.term_to_binary/1` then `Base.encode64/1`; `decode/1` reverses it. | The `inputs`/`output`/`error` columns are `TEXT`, not `BYTEA`. Raw ETF bytes are not guaranteed valid UTF-8 and cannot be stored in a `TEXT` column safely; base64 makes the column contents plain ASCII. |
| `nil` round-trip | No sentinel marker (unlike upstream's `"__DBOS_NIL"` string for `DBOS_JSON`). `nil` is a native ETF term and round-trips through `encode/1`/`decode/1` like any other value. | ETF, unlike JSON-over-a-string-column, has no ambiguity between "absent" and "the value nil" to work around. |
| Decoding unsafe terms | `decode/1` uses `:erlang.binary_to_term(binary, [:safe])` and additionally walks the result rejecting any pid, port, or reference, raising `ArgumentError`. | `:safe` alone does not reject pids/ports/refs already present in the source binary (verified empirically) — only atom-table growth. Workflow inputs/outputs are not process identities and should never contain one. |

## Phase 2b

| Decision | Choice | Rationale |
|---|---|---|
| Engine namespacing | Every per-engine process (`Dbos.Registry`, `Dbos.WorkflowSup`, its `DynamicSupervisor`, the process-tracking `Registry`, `Dbos.Recovery`) is named via `Module.concat(engine_name, ThatModule)`, and the resolved `Dbos.Config` lives in `:persistent_term` keyed by `{Dbos, :config, engine_name}`. | Lets multiple engines coexist in one BEAM with no global singletons or unnamespaced atoms, per the task's two-node test requirement. |
| `executor_id` default | `System.get_env("DBOS__VMID")`, else `to_string(node())`. | Deviates from upstream's literal `"local"` fallback (`notes/recovery.md` §2) by substituting the BEAM node name, as directed — a more useful default in a clustered Elixir deployment, and still non-empty (upstream's `ListWorkflows{ExecutorIDs: [...]}` gotcha about a real, non-empty value still holds). |
| `application_version` default | `System.get_env("DBOS__APPVERSION")`, else `Dbos.Version.compute/1` over the registered workflow modules' BEAM code chunks (SHA-256 over every chunk except `Docs`, truncated to 16 hex chars). | Upstream hashes the whole compiled executable (`getBinaryHash`, `dbos.go:940-973`), which has no equivalent in a BEAM release (no single "the executable"); hashing the registered workflow modules' own code is the closest equivalent that is still deterministic across runs of the same code and changes when workflow code changes. |
| Migration verification | Runs as a plain function call inside `Dbos.Supervisor.init/1` (raising aborts the supervisor start), not as a supervised child. | Functionally equivalent ordering guarantee to a dedicated child (it still runs after `Dbos.Registry` starts and before `Dbos.WorkflowSup`/`Dbos.Recovery`), without an awkward one-shot child spec. |
| `dbos_schema.sql` statement splitting | `Dbos.Migrator.create!/1` reads the fixture and splits it into individual statements with a small state-machine splitter (tracking `$$...$$` dollar-quoted bodies and `--` line comments) rather than executing the file as one multi-statement string. | Postgrex/Ecto's extended query protocol does not support multiple SQL commands in one prepared statement; the fixture also mixes `CREATE INDEX CONCURRENTLY` (illegal inside a transaction block) with trigger functions containing embedded semicolons, so each statement must run as its own top-level query. |
| Recovery's `max_retries` | `Dbos.Config.max_recovery_attempts`, default `3`, threaded into `insert_workflow_status(..., max_retries: config.max_recovery_attempts)` on every recovery re-dispatch. | Upstream's `MaxRetries` is a per-workflow value not persisted in `workflow_status` at all (only ever a caller-supplied parameter to `InsertWorkflowStatus`); since Phase 2b has no per-workflow options surface yet, recovery needs *some* value to drive the `MAX_RECOVERY_ATTEMPTS_EXCEEDED` transition. An engine-level default stands in until a later phase threads a real per-workflow value through. **Flagged for review** — this is invented, not specified by the reference. |
| Recovery-at-boot vs. a fresh `Dbos.start` | Recovery's initial scan runs in `handle_continue` (per the task), which is not guaranteed to complete before `Dbos.Supervisor.start_link/1` returns to the caller. A workflow started immediately after boot could theoretically race the boot-time recovery scan if (implausibly) a same-ID workflow were already `PENDING`; in practice this only matters for genuinely pre-existing `PENDING` rows, and `Dbos.Recovery.await_boot_recovery/1` (`:sys.get_state/1` on the recovery process) is exposed so tests can serialize against it deterministically. | Accepted trade-off, per the task's explicit "must not block `start_link`" requirement; documented rather than silently working around it. |
| Child workflow start point | `Dbos.start/3`, called from inside a workflow, detects the ambient context via `Dbos.Runtime.in_workflow?/0` and reads its `Dbos.Config`/engine from `Dbos.Runtime.current_config/0` rather than from the caller's `opts[:engine]`. | A workflow body calling `Dbos.start` for a child should always target the same engine it is itself running under; requiring the body to know and repeat its own engine name would be redundant and error-prone. **Invented** — the reference has no concept of a separate "engine" to disambiguate, so there is no equivalent upstream behavior to match against. |
| Workflow process failure handling | `Dbos.WorkflowProcess` classifies a rescued/caught failure by matching `%Dbos.WorkflowCancelledError{}` specifically (kind `:error`) and skips calling `update_workflow_outcome` entirely in that case, rather than calling it and relying on its own cancellation guard. `update_workflow_outcome`'s own `WorkflowCancelledError` (raised if the row was cancelled concurrently, underneath the call) is also rescued and treated as `:ok`. | Belt-and-suspenders: covers both "the body itself observed cancellation and raised" and "cancellation landed in the gap between the body finishing and the outcome write," per `notes/engine-core.md` §5. |

## Deviations and invented behavior needing review

- `max_recovery_attempts` as an engine-level `Dbos.Config` field (see table above) — the reference has no equivalent single knob; a later phase should replace this with whatever per-workflow retry configuration surface it introduces.
- The exact wording and log level (`Logger.warning/1`) of recovery's "unregistered workflow name" and "dead-lettered during recovery" messages are invented; upstream logs similarly but the message text is not part of any documented contract.
- `Dbos.Registry.name_for_mfa/2` (reverse lookup, name for a given `{module, function, arity}`) is new surface not requested verbatim by the task, added because resolving a capture (`&Mod.fun/n`) passed to `Dbos.start/3` requires it.
- `Dbos.WorkflowSup.whereis/2` (workflow id → pid) is new surface, added because the crash-and-resume acceptance test needs a way to locate a running workflow's process to kill it; the task only asked for the queue/partition-key live count.
