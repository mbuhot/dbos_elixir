# Queue Patterns

A port of [DBOS's Advanced Queue Patterns example](https://docs.dbos.dev/python/examples/queue-patterns)
to Elixir. Each pattern below is its own module, backed by its own declared queue in
`QueuePatterns.Application`.

## Running it

Requires a local Postgres reachable with `PGHOST`/`PGPORT`/`PGUSER`/`PGPASSWORD`
(defaults: `localhost`/`5432`/your OS user/no password), and a database named
`queue_patterns_dev` (override with `QUEUE_PATTERNS_DATABASE`).

```sh
cd sample_apps/queue_patterns
mix deps.get
createdb queue_patterns_dev
mix ecto.migrate
iex -S mix
```

Every snippet below assumes that `iex` session.

## Fan-out / fan-in — `QueuePatterns.FanOut`

**Problem**: a batch of independent items needs to run concurrently, bounded by the queue's
`worker_concurrency`, with the caller getting one combined result back in order.

**Code**: `process_batch/2` enqueues one child `process_item/2` per item onto `"fan_out"`, then
awaits every handle and collects the results — fan-out to spread work, fan-in to reassemble it.

```elixir
iex> {:ok, handle} = QueuePatterns.FanOut.process_batch("batch-1", [1, 2, 3, 4, 5])
iex> Dbos.await(handle)
{:ok, [%{item: 1, result: 1, ...}, %{item: 2, result: 4, ...}, ...]}
```

**Observe**: the result list is the same length and order as the input, each entry the
corresponding child's result — fan-in reassembles work in submission order, not finish order.
Covered by `test/fan_out_test.exs`.

## Rate-limited external API — `QueuePatterns.RateLimitedApi`

**Problem**: a burst of callers all want to hit a third-party API with a hard rate limit (an LLM
provider billed and throttled per minute, say).

**Code**: `"rate_limited_api"` is declared `rate_limit: %{limit: 2, period_ms: 1_000}` — at most 2
workflow starts leave the queue per second, globally, across every enqueuer.

```elixir
iex> handles = for i <- 1..10, do: (
...>   {:ok, h} = QueuePatterns.RateLimitedApi.call_api(i, queue_name: "rate_limited_api")
...>   h
...> )
iex> Enum.map(handles, fn h -> {:ok, r} = Dbos.await(h); r.called_at end)
```

**Observe**: sort the returned `called_at` timestamps — no two-per-second window has more than 2
of them, however many of the 10 requests were enqueued in the same instant.

## Per-tenant fairness with a global cap — `QueuePatterns.TenantFairness`

**Problem**: a partitioned queue gives every distinct partition key (here, `tenant_id`) its own
independent concurrency, which is exactly what stops one noisy tenant from starving another's
slice — but that per-partition isolation also makes a *global* cap awkward, since there's no
single place a queue-wide count is computed.

**Solution — the two-queue composition**: `"tenant_jobs"` is partitioned with
`worker_concurrency: 1` — one job per tenant runs at a time. `route_job/2` runs on that per-tenant
slot; it does no real work itself, only re-enqueuing the actual job onto `"global_jobs"` — a
second, non-partitioned queue with `global_concurrency: 3` capping the combined total across every
tenant — and awaiting it.

```elixir
iex> {:ok, h1} = QueuePatterns.TenantFairness.route_job("tenant-a", "job-1",
...>   queue_name: "tenant_jobs", partition_key: "tenant-a")
iex> {:ok, h2} = QueuePatterns.TenantFairness.route_job("tenant-b", "job-1",
...>   queue_name: "tenant_jobs", partition_key: "tenant-b")
iex> Dbos.await(h1)
iex> Dbos.await(h2)
```

**Observe**: enqueue several jobs per tenant — each tenant's own jobs run one at a time (their
`route_job` holds its `tenant_jobs` slot until `do_job` finishes), but different tenants' jobs run
concurrently with each other, capped in total by `global_jobs`'s `global_concurrency` regardless of
how many tenants are submitting. Covered by `test/tenant_fairness_test.exs`.

## Priority — `QueuePatterns.Priority`

**Problem**: most work should run in submission order, but some requests (a paying customer, an
on-call alert) need to jump ahead of whatever is already waiting.

**Ordering rule**: `"priority_queue"` is declared `priority_enabled: true`. Candidates are claimed
`priority ASC, created_at ASC` — a lower `:priority` number runs first; equal priority runs in
enqueue order. `priority: 0` (the default) sorts before anything positive.

```elixir
iex> {:ok, normal} = QueuePatterns.Priority.handle_request("normal",
...>   queue_name: "priority_queue", priority: 10)
iex> {:ok, urgent} = QueuePatterns.Priority.handle_request("urgent",
...>   queue_name: "priority_queue", priority: 0)
```

**Observe**: on a queue with `worker_concurrency: 1` (one request runs at a time), `urgent`'s
recorded `started_at_epoch_ms` (`Dbos.status/1`) comes before `normal`'s, even though `normal` was
enqueued first. Covered by `test/priority_test.exs`.

## Deduplication — `QueuePatterns.Deduplication`

**Problem**: the same logical request (say, "generate this report") might get submitted more than
once — a retried HTTP request, a double click — and should only run once while the first
submission is still enqueued or in flight.

**Code**: `:deduplication_id` reserves one slot per `(queue_name, deduplication_id)` pair.

```elixir
iex> {:ok, first} = QueuePatterns.Deduplication.generate_report("report-42",
...>   queue_name: "dedup_queue", deduplication_id: "report-42")
iex> QueuePatterns.Deduplication.generate_report("report-42",
...>   queue_name: "dedup_queue", deduplication_id: "report-42")
** (Dbos.QueueDeduplicatedError) ...
```

**Observe**: the second call raises `Dbos.QueueDeduplicatedError` naming the workflow already
holding the slot; only one workflow ever runs for that report. Covered by
`test/deduplication_test.exs`.

## Debouncing — `QueuePatterns.Debouncing`

**Problem**: the "wait until the user stops typing" case — a document gets edited several times
in quick succession, and only the last edit should trigger a reindex, once things settle down.

**Code**: `debounce_reindex/3` calls `Dbos.Debouncer.debounce/4`, which either starts a fresh
delayed workflow keyed by `doc_id`, or "bounces" one still waiting out its delay — replacing its
inputs with the latest call's and pushing its start time out by `:period_ms`.

```elixir
iex> QueuePatterns.Debouncing.debounce_reindex("doc-1", 1, period_ms: 2_000)
iex> QueuePatterns.Debouncing.debounce_reindex("doc-1", 2, period_ms: 2_000)
iex> QueuePatterns.Debouncing.debounce_reindex("doc-1", 3, period_ms: 2_000)
{:ok, workflow_id}
```

**Observe**: all three calls return the same `workflow_id`. Only one `reindex_document/2` workflow
ever runs for `"doc-1"`, starting 2 seconds after the *last* call, with `revision: 3` — the first
two edits are collapsed away entirely, never separately executed.

## Serialized per-key execution — `QueuePatterns.SerializedPerKey`

**Problem**: concurrent updates to the same logical entity (an account balance, a row a
read-modify-write step touches) must run one at a time to stay correct, while unrelated entities
should still run in parallel for throughput.

**Code**: `"serial_per_key"` is a partitioned queue with `worker_concurrency: 1` — each distinct
`:partition_key` gets its own independent concurrency slot capped at one.

```elixir
iex> for i <- 1..5 do
...>   {:ok, h} = QueuePatterns.SerializedPerKey.update_account("acct-1", i,
...>     queue_name: "serial_per_key", partition_key: "acct-1")
...>   h
...> end
```

**Observe**: pull each handle's `Dbos.status/1` — their `started_at_epoch_ms`/`completed_at`
windows never overlap for `"acct-1"`. Enqueue updates for `"acct-2"` at the same time and they run
concurrently with `"acct-1"`'s, since they hold a different partition's slot.

## Tests

```sh
mix test
```

Covers fan-out/fan-in, the tenant-fairness two-queue composition, priority ordering, and
deduplication — the four patterns with outcomes deterministic enough to assert on without timing
flakiness. Rate limiting, debouncing, and serialized-per-key are demonstrated via the `iex`
snippets above; their guarantees hold over a sliding time window or across separate workflow runs,
which is easier to see running interactively than to assert without race-prone sleeps.
