# The engine's own test suites

This page is about testing `Dbos` itself. For testing *your* workflows, see
`guides/tutorials/testing.md`.

Two suites, two different failure modes.

| Suite | Command | Kills | Needs Docker |
|---|---|---|---|
| Unit/acceptance | `mix test` | a workflow *process*, in-BEAM (`Process.exit/2`) | no |
| Integration | `mix test.integration` | a whole *node*, `docker compose kill -s SIGKILL` | yes |

## Unit/acceptance suite

`mix test` runs `test/dbos` against a local Postgres (`dbos_test`). `test_paths` is
`["test/dbos"]`, so this suite's speed and count are unaffected by the integration suite.

Covers everything a single BEAM can: start/await, step checkpointing, recovery after
`Process.exit/2`, error propagation, child workflows, per-engine namespacing.

## Integration suite

`mix test.integration` brings up a `docker compose` stack:

| Service | Role |
|---|---|
| `postgres` | the shared system database, `postgres:17`, healthchecked, published on `localhost:55432` |
| `migrate` | applies the schema once, plus the bookkeeping tables below, before either node starts |
| `node1` / `node2` | identical engines, distinct `DBOS__VMID` and node name, both pointed at `postgres` |
| `test-runner` | runs the suite, driving `docker compose` against its siblings via the mounted Docker socket |

Every test carries `@moduletag :integration`; the alias adds `--include integration`.

### Scenarios

| File | Proves |
|---|---|
| `hard_node_kill_test.exs` | node1 dies (`SIGKILL`) mid-workflow; node2 recovers it; every step body ran exactly once across both nodes |
| `recovery_ownership_test.exs` | node2 recovers only the workflow reassigned to it after node1 dies; a peer row it was never handed stays untouched |
| `concurrent_start_test.exs` | the same workflow id started from both nodes at once leaves one status row and one checkpointed step |
| `queue_competition_test.exs` | 50 workflows on one queue, two nodes racing to dequeue; every workflow executes exactly once with no duplicate claims |

### How execution is counted

A workflow process can die mid-step, so an in-memory counter does not survive the kill. Every step
body writes to plain Postgres tables created by `migrate` outside the `dbos` schema:

| Table | Contents |
|---|---|
| `execution_attempts` | Append-only, one row per actual invocation of a step body, including a duplicate caused by a race. |
| `execution_log` | Unique on `(workflow_id, step_name)` — the durable count of completed, checkpointed executions. |
| `release_signals` | Lets a test unblock a step that is parked waiting to be killed. |

`test/integration/support/harness.ex` wraps `docker compose`: waiting for a node's distributed
Erlang name to answer, sending it a signal, and querying those tables plus `dbos.workflow_status`
and `dbos.operation_outputs` directly.

The harness reassigns a dead executor's rows with a raw `UPDATE` and then calls
`Dbos.Recovery.recover_pending/1`, which scans only for its own `executor_id`. The engine's own
`Dbos.Recovery.reclaim/2,3` does that reassignment, capability-aware, and the orphan sweep drives
it automatically from lease expiry — these scenarios should move onto that path.

## Running things

```sh
mix test                # unit/acceptance, no Docker
mix test.integration    # full Docker stack, requires the Docker daemon running
docker compose config   # validates docker-compose.yml without starting anything
```
