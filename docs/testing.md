# Testing

Two suites, two different failure modes.

| Suite | Command | Kills | Needs Docker |
|---|---|---|---|
| Unit/acceptance | `mix test` | a workflow *process*, in-BEAM (`Process.exit/2`) | no |
| Integration | `mix test.integration` | a whole *node*, `docker compose kill -s SIGKILL` | yes |

## Unit/acceptance suite

`mix test` against a local Postgres (`dbos_test`). Exercises everything a single BEAM can:
start/await, step checkpointing, recovery after `Process.exit/2`, error propagation, child
workflows, per-engine namespacing.

Excludes `test/integration` entirely — that directory isn't in `test_paths`, so this suite's
speed and count don't change as the integration suite grows.

## Integration suite

`mix test.integration` — a `docker compose` stack:

| Service | Role |
|---|---|
| `postgres` | the shared system database, `postgres:17`, healthchecked, published on `localhost:55432` |
| `migrate` | applies the schema fixture once, before either node starts |
| `node1` / `node2` | identical engines, distinct `DBOS__VMID`/node name, both pointed at `postgres` |
| `test-runner` | runs the suite, drives `docker compose` against its siblings via the mounted Docker socket |

Every test tags itself `@moduletag :integration`, so `mix test` alone never runs them; the
alias adds `--include integration`.

### Scenarios

| # | File | Proves |
|---|---|---|
| 1 | `hard_node_kill_test.exs` | node1 dies (`SIGKILL`) mid-workflow; node2 recovers it; every step body ran exactly once across both nodes |
| 2 | `recovery_ownership_test.exs` | node2 recovers only the workflow reassigned to it after node1 dies; a peer row it was never handed stays untouched |
| 3 | `concurrent_start_test.exs` | the same workflow id started from both nodes at once still leaves one status row and one checkpointed step |
| 4 | `queue_competition_test.exs` | 50 workflows enqueued onto one queue, two nodes racing to dequeue and run them; every workflow executes exactly once with no duplicate claims |

### How execution is counted

A workflow process can die mid-step, so counting in-memory (a `persistent_term`, an ETS table)
doesn't survive the kill. Every step body writes to two plain Postgres tables instead, created
by `migrate` outside the `dbos` schema:

- `execution_attempts` — append-only, one row per actual invocation of the step body, including
  a duplicate caused by a race.
- `execution_log` — unique on `(workflow_id, step_name)`, the durable count of *completed,
  checkpointed* executions.

`test/integration/support/harness.ex` wraps `docker compose`, waiting for a node's distributed
Erlang name to answer, sending it a signal, and querying both tables plus `dbos.workflow_status`
and `dbos.operation_outputs` directly.

### A known gap this suite surfaces

`Dbos.Recovery.recover_pending/1` only ever scans for `PENDING` rows already stamped with its
own `config.executor_id`. There is no public API for reassigning a dead executor's rows to a
live one — the integration suite's harness does that reassignment with a raw `UPDATE` as a
stand-in for the ops action a real deployment would take. See the harness's `@moduledoc` for
where this would need to grow a real API.

## Running things

```sh
mix test                # unit/acceptance, unchanged, no Docker
mix test.integration     # full Docker stack, requires the Docker daemon running
docker compose config    # validates docker-compose.yml without starting anything
```
