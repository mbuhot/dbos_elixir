# Step retries in dbos-transact-golang

Reference: `reference/dbos-transact-golang/dbos/workflow.go:2116-2394` (`stepOptions`,
`executeStepWithRetry`, `RunAsStep`).

## Options and defaults

`stepOptions` (`workflow.go:2117-2126`), applied by `setDefaults` (`:2129-2149`):

| Option | Default | Setter |
|---|---|---|
| `maxRetries` | `0` (no retries) | `WithStepMaxRetries` (`:2166-2170`) |
| `baseInterval` | `100ms` (`_DEFAULT_STEP_BASE_INTERVAL`, `:563`) | `WithStepBaseInterval` (`:2183-2187`) |
| `backoffFactor` | `2.0` (`_DEFAULT_STEP_BACKOFF_FACTOR`, `:565`) | `WithStepBackoffFactor` (`:2175-2179`) |
| `maxInterval` | `5s` (`_DEFAULT_STEP_MAX_INTERVAL`, `:564`) | `WithStepMaxInterval` (`:2191-2195`) |
| `retryPredicate` | `nil` (retry every error) | `WithStepRetryPredicate` (`:2217-2221`) — not ported in phase 2a |

Delay for retry attempt N (1-indexed): `min(maxInterval, baseInterval * backoffFactor^(N-1))`,
plus a `[0.95, 1.05]` jitter multiplier in Go (`workflow.go:2359-2365`, `sysdb.BackoffSchedule`).
The Elixir port omits jitter for deterministic, fast tests; this is a documented deviation, not an
upstream behavior gap.

## `executeStepWithRetry` (`workflow.go:2354-2394`)

Runs `runOnce` (the step body) in a loop via `sysdb.RetryLoop`. Each attempt increments a `runs`
counter (1-indexed, counting completed runs). `decide(err, runs)`:

- `runs > maxRetries` (the last allowed attempt just failed):
  - `maxRetries <= 0` → stop, return the **raw, unwrapped** error. A step with no retry
    configuration that fails on its only attempt sees its own error, not a wrapper.
  - `maxRetries > 0` → stop, return `models.NewMaxStepRetriesExceededError(workflowID, stepName,
    maxRetries, joinedErrors)`, `joinedErrors` being every attempt's error joined via
    `errors.Join`.
- Otherwise (budget remains): sleep the computed backoff, run again.

So total attempts on exhaustion = `maxRetries + 1`.

## Retries run entirely before any checkpoint write

`RunAsStep` (`workflow.go:2487-2566`): `CheckOperationExecution` (replay-hit check) runs once,
before any retry loop. If it's a genuine miss, `executeStepWithRetry` runs the *entire* retry
loop — every attempt's failure and backoff sleep — synchronously, in-process, with **no**
`operation_outputs` write in between. Only after the loop settles (success, or the final
wrapped/unwrapped error) does `RunAsStep` call `RecordOperationResult` exactly once
(`workflow.go:2547-2563`), writing the settled `stepOutput`/`stepError` (whichever the loop
produced) alongside the `stepStartTime`/`stepCompletedTime` captured immediately before/after the
*entire* retry loop (`:2521`, `:2541`) — not per-attempt. A crash mid-retry-loop leaves **zero**
rows for that `function_id`; recovery re-runs the whole step (all attempts) from scratch. This
also means only one row is ever written per step regardless of how many attempts it took.

## What gets recorded when retries are exhausted

The single `operation_outputs` row's `error` column holds the serialized
`MaxStepRetriesExceededError` (when `maxRetries > 0`) — not the original underlying error, and not
a list of the joined per-attempt errors individually; the joined errors live only inside that
wrapper's message/cause. The workflow body's `RunAsStep` call sees that same wrapped error as its
return value and must handle or propagate it like any other step error.

## Port decisions for `Dbos.Runtime.run_step/3`

- Options: `:max_retries` (default `0`), `:base_interval_ms` (default `100`), `:backoff_factor`
  (default `2.0`), `:max_interval_ms` (default `5000`). `:retry_predicate` is not ported.
- The retry loop catches all three failure kinds (`raise`/`throw`/`exit`) per attempt, matching
  `Dbos.Serialization.encode_failure/3`'s three-kind model; Go only has `error` returns, so this is
  a necessary generalization, not a divergence from a Go behavior that exists to contradict.
- On exhaustion with `max_retries > 0`, the loop raises `Dbos.MaxStepRetriesExceededError`
  (`workflow_id`, `function_name`, `max_retries`, `cause` — the last attempt's failure value) and
  that new exception, not the original failure, is what gets `encode_failure/3`-encoded and
  written to `operation_outputs.error`, and what is re-raised to the workflow body — mirroring
  Go's "wrapped error is both the recorded and the returned outcome."
- On exhaustion with `max_retries <= 0` (the default, i.e. a single attempt), the original
  raw failure (kind, value, stacktrace) is recorded and re-raised unwrapped.
- One `record_operation_result/3` call happens after the entire retry loop settles, using
  `started_at`/`completed_at` timestamps captured immediately before/after the whole loop, not
  per-attempt — matching the "retries run before the checkpoint write" ordering above.

## Summary (three lines)

1. Defaults: `max_retries = 0`, `base_interval_ms = 100`, `backoff_factor = 2.0`,
   `max_interval_ms = 5000`; delay = `min(max_interval, base * factor^(attempt-1))`, no jitter in
   the port.
2. The entire retry loop runs before any checkpoint write — one `operation_outputs` row is written
   per step, after the loop settles, using timestamps spanning the whole loop.
3. `max_retries <= 0` (default) records/reraises the raw failure unwrapped; exhausting a
   configured retry budget records/reraises a new `Dbos.MaxStepRetriesExceededError` wrapping the
   last failure, mirroring upstream's `MaxStepRetriesExceededError`.
