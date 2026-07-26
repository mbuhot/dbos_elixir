# Telemetry

Five spans. Attach your own handler (`:telemetry.attach/4`, `:telemetry.attach_many/4`) and bridge
to whatever backend you use.

Every span emits the standard triple: `[..., :start]`, `[..., :stop]`, and `[..., :exception]` on a
raised or thrown error, which is then re-raised. `:start` measurements carry `system_time` (native
units); `:stop`/`:exception` measurements carry `duration` (native units, convert with
`System.convert_time_unit(duration, :native, :millisecond)`).

| Event prefix | One span per |
|---|---|
| `[:dbos, :workflow]` | one execution of a workflow body |
| `[:dbos, :step]` | one execution of a step body |
| `[:dbos, :wait]` | one blocking wait: `Dbos.sleep/1`, `Dbos.recv_message/3`, `Dbos.get_event/4` |
| `[:dbos, :queue, :dequeue]` | one dequeue poll of one queue/partition pair |
| `[:dbos, :recovery]` | one `Dbos.Recovery.reclaim/2,3` pass |

`kind`, `reason`, and `stacktrace` are present on every `:exception` event and are omitted from
the tables below.

## `[:dbos, :workflow, :start | :stop | :exception]`

Wraps one *process run* of a workflow body — a fresh run, a replay after recovery, or a resumed
or forked continuation. A workflow that is recovered three times emits three spans. Its end-to-end
lifetime is `workflow_status.created_at`..`completed_at` in the database.

| Metadata | Meaning |
|---|---|
| `workflow_id` | The workflow's id. |
| `name` | The registered workflow name (`nil` if unresolvable). |
| `engine` | The engine name. |
| `replay` | Whether this run is a recovery replay. |

## `[:dbos, :step, :start | :stop | :exception]`

Wraps one execution of a `defstep`/`deftransaction` body. A step whose output is already
checkpointed emits nothing on replay — only real invocations are spanned.

| Metadata | Meaning |
|---|---|
| `function_name` | The step's name (`"name/arity"`, or a `name:` override). |
| `workflow_id` | The owning workflow's id, `nil` for a step called outside a workflow. |

## `[:dbos, :wait, :start | :stop | :exception]`

Wraps one blocking wait inside `Dbos.sleep/1`, `Dbos.recv_message/3` or `Dbos.get_event/4` — the
state a human-in-the-loop UI watches. `Dbos.get_event/4` called outside a workflow is spanned too,
with a `nil` `workflow_id`. A wait already satisfied when it is reached still emits the pair, with
a near-zero duration and `outcome: :resolved`.

| Metadata | Meaning |
|---|---|
| `engine` | The engine name. |
| `workflow_id` | The waiting workflow's id, `nil` for a wait outside a workflow. |
| `kind` | `:sleep`, `:recv`, or `:event`. |
| `key` | The `recv` topic, or the `get_event` key. `nil` for `:sleep`. |
| `target_workflow_id` | The workflow whose event is being watched. `nil` for `:sleep` and `:recv`. |
| `timeout_ms` | The requested timeout, or the requested sleep duration. |
| `outcome` | `:stop` only — `:resolved`, `:timeout`, or `:parked`. |

A workflow cancelled mid-wait raises, so cancellation arrives as `[:dbos, :wait, :exception]`.

### Parking

A wait longer than the engine's `park_exit_threshold_ms` releases the workflow process
entirely, so one `:telemetry.span/3` cannot cover the whole wait. The span closes at the
moment the process is released, with `outcome: :parked` and a duration measuring only the resident
portion.

The resumption is a separate span. When the deadline fires or a message wakes the wait, the
workflow is replayed from its checkpoints, reaches the same call site, and emits a fresh
`[:dbos, :wait, :start]` — from a different process, after a restart possibly on a different node,
minutes or days later. `workflow_id` plus `kind` and `key` are the only link between the two; there
is no shared span id, and no single handler invocation sees both ends.

To measure how long a workflow was parked, pair the `:parked` stop with the next `:start` for the
same `workflow_id`, or read `workflow_status` from the database.

## `[:dbos, :queue, :dequeue, :start | :stop | :exception]`

Wraps one dequeue poll of one queue/partition pair.

| Metadata | Meaning |
|---|---|
| `engine` | The engine name. |
| `queue_name` | The queue being polled. |
| `partition_key` | Which partition this poll covers (`nil` for an unpartitioned queue). |
| `count` | `:stop` only — how many workflows this poll claimed (`0` on an empty poll). |

Lock contention counts as an exception here: a `NOWAIT` claim lost under `GlobalConcurrency`
raises, and contention is an expected, frequent outcome with concurrent dequeuers. Filter on
`reason` if a handler only cares about genuine failures.

## `[:dbos, :recovery, :start | :stop | :exception]`

Wraps one `Dbos.Recovery.reclaim/2,3` pass: the boot-time self-recovery scan, the orphan sweep's
reclaim, and the `POST /dbos-workflow-recovery` admin route all go through it.

| Metadata | Meaning |
|---|---|
| `engine` | The engine name. |
| `executor_ids` | The executor ids this pass is reclaiming. |

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

`:telemetry_test.attach_event_handlers/2` is the simplest way to assert on these events in your
own tests.
