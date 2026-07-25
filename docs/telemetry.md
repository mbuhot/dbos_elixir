# Telemetry

Four `:telemetry.span/3` spans. No OpenTelemetry dependency — attach your own handler
(`:telemetry.attach/4`, `:telemetry.attach_many/4`) and bridge to whatever backend you use.

Every span emits the standard `:telemetry.span/3` triple: `[..., :start]`, `[..., :stop]`, and
`[..., :exception]` on a raised/thrown error (which is then re-raised — a span never swallows a
failure). `:start` measurements always carry `system_time` (native units); `:stop`/`:exception`
measurements always carry `duration` (native units, convert with
`System.convert_time_unit(duration, :native, :millisecond)`) plus the fields listed below.

## `[:dbos, :workflow, :start | :stop | :exception]`

One span per workflow **process** execution (a fresh run, a replay after recovery, or a resumed/
forked continuation) — wraps the call into the registered workflow function, per
`Dbos.WorkflowProcess`.

| Metadata | Present | Meaning |
|---|---|---|
| `workflow_id` | always | The workflow's id. |
| `name` | always (`nil` if unresolvable) | The registered workflow name. |
| `engine` | always | The engine name. |
| `replay` | always | Whether this run is a recovery replay. |
| `kind`, `reason`, `stacktrace` | `:exception` only | Standard `:telemetry.span/3` exception fields. |

Firing on every checkpoint-skipping replay, not only a workflow's first attempt, is deliberate:
a span per *process run* mirrors what actually consumed CPU/latency, not what the workflow's
lifetime looked like end to end (`workflow_status.created_at`..`completed_at` already answers
that from the database).

## `[:dbos, :step, :start | :stop | :exception]`

One span per step's actual **execution** — `defstep`/`deftransaction`. Does **not** fire when
`Dbos.Runtime.run_step/3`'s replay short-circuit returns an already-recorded output without
calling the step body; only real invocations are spanned.

| Metadata | Present | Meaning |
|---|---|---|
| `function_name` | always | The step's name (`"name/arity"`, or a `name:` override). |
| `workflow_id` | always | The owning workflow's id. |
| `kind`, `reason`, `stacktrace` | `:exception` only | Standard exception fields. |

## `[:dbos, :queue, :dequeue, :start | :stop | :exception]`

One span per `Dbos.Queue.Runner` poll of one queue/partition pair — wraps the
`Dbos.SystemDb.dequeue_workflows/3` call, per `notes/queues.md` §2.

| Metadata | Present | Meaning |
|---|---|---|
| `engine` | always | The engine name. |
| `queue_name` | always | The queue being polled. |
| `partition_key` | always (`nil` for an unpartitioned queue) | Which partition this poll covers. |
| `count` | `:stop` only | How many workflows this poll claimed (`0` on an empty poll). |
| `kind`, `reason`, `stacktrace` | `:exception` only | Fires on **every** lock-contention race (`NOWAIT` losing a claim under `GlobalConcurrency`), not only genuine failures — contention is an expected, frequent outcome under concurrent dequeuers; filter on `reason` if a handler only cares about non-contention errors (`Dbos.SystemDb.contention_error?/1`). |

## `[:dbos, :recovery, :start | :stop | :exception]`

One span per `Dbos.Recovery.reclaim/2,3` call — covers both the boot-time self-recovery pass
(`recover_pending/1`) and any dead-executor reclaim (cluster-driven or the
`POST /dbos-workflow-recovery` admin route).

| Metadata | Present | Meaning |
|---|---|---|
| `engine` | always | The engine name. |
| `executor_ids` | always | The executor ids this pass is reclaiming. |
| `kind`, `reason`, `stacktrace` | `:exception` only | Standard exception fields. |

## Attaching a handler

```elixir
:telemetry.attach_many(
  "my-app-dbos-logger",
  [
    [:dbos, :workflow, :stop],
    [:dbos, :workflow, :exception],
    [:dbos, :step, :exception]
  ],
  &MyApp.DbosTelemetry.handle_event/4,
  nil
)
```

Tests use `:telemetry_test.attach_event_handlers/2` to assert on these events directly — see
`test/dbos/telemetry_test.exs`.
