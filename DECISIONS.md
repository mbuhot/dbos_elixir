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
