# Workflow Communication

Three primitives let workflows exchange data with each other and with the outside world:
messages, events, and streams. All three are durable — every send/set/write is checkpointed, and
every receive/get/read survives a crash and resumes waiting where it left off. A fourth
primitive, durable sleep, isn't communication on its own but shares the same waiting machinery, so
it's covered here too.

Every value passed through any of these travels as a real Erlang term, encoded for storage and
decoded back on the way out — not JSON. Atoms, tuples, and structs make the round trip intact.

```elixir
Dbos.send_message(other_id, nil, {:approved, %MyApp.Approval{by: "alice", note: "looks good"}})

# ... in the other workflow:
{:approved, %MyApp.Approval{by: by, note: note}} = Dbos.recv_message(nil, 30_000)
```

No JSON-encode/decode step, no re-parsing a tagged map back into a struct — the tuple and the
struct arrive exactly as sent.

## Messages: `send_message` / `recv_message`

A message is a point-to-point, fire-and-forget delivery to a specific workflow id, optionally
scoped to a topic.

```elixir
Dbos.send_message(payment_workflow_id, "payment_confirmed", %{amount: 4200, currency: :usd})
```

```elixir
defworkflow await_payment(order_id), name: "await_payment" do
  try do
    {:ok, Dbos.recv_message("payment_confirmed", 300_000)}
  rescue
    Dbos.RecvTimeoutError -> {:error, :timed_out}
  end
end
```

- `Dbos.send_message/4` (`destination_id`, `topic`, `message`, `opts`) is a durable, checkpointed
  step inside a workflow, and a direct write when called from ordinary code. `topic: nil`
  normalizes to a reserved null-topic sentinel.
- `Dbos.recv_message/3` (`topic`, `timeout_ms`, `opts`) must be called from inside a workflow. It
  blocks until a message on that topic arrives, returning it, or raises
  `Dbos.RecvTimeoutError` once `timeout_ms` elapses with nothing delivered.

**Ordering and delivery**: at most one `recv_message` may be registered per `(workflow_id, topic)`
at a time — a second concurrent `recv_message` call on the same topic raises
`Dbos.RecvConflictError`. A message already sent before a `recv_message` call is picked up
immediately (checked once up front before waiting).

Each `recv_message` call consumes one durable step id for the receive itself, plus one for the
underlying sleep — both checkpointed, so a workflow that crashes mid-wait resumes waiting only
for whatever time is left, not the full timeout again.

## Events: `set_event` / `get_event`

An event is a durable key/value slot owned by one workflow, readable by any number of other
callers — status flags, partial results, anything another party might want to poll for or wait
on.

```elixir
defworkflow process_upload(file_id), name: "process_upload" do
  Dbos.set_event("stage", :validating)
  validate(file_id)
  Dbos.set_event("stage", :transcoding)
  transcode(file_id)
  Dbos.set_event("stage", :done)
  :ok
end
```

```elixir
stage = Dbos.get_event(upload_workflow_id, "stage", 5_000)
```

- `Dbos.set_event/3` (`key`, `value`, `opts`) must be called from inside a workflow. Each call
  both upserts the current value and appends to the key's history, one durable step id.
- `Dbos.get_event/4` (`target_workflow_id`, `key`, `timeout_ms \\ nil`, `opts`) works both inside
  and outside a workflow, and blocks up to `timeout_ms` (or indefinitely if `nil`) for the key to
  be set, returning `nil` if it times out first.

**Ordering and delivery**: unlike `recv_message`, `get_event` registrations are non-exclusive —
any number of callers may wait on the same `(target_workflow_id, key)` concurrently, and all of
them wake when it's set. Reading only ever returns the latest value for the key, not a queue of
every value it was ever set to (use a stream for that). Called from inside a workflow, the wait
and the read are both checkpointed under their own step ids; called from ordinary code, only the
wait happens — there's no workflow run to checkpoint into.

## Streams: `write_stream` / `read_stream` / `close_stream`

A stream is an ordered, append-only sequence a workflow produces incrementally and any number of
readers consume as it goes — progress updates, generated tokens, paginated results.

```elixir
defworkflow generate_report(report_id), name: "generate_report" do
  for chunk <- build_chunks(report_id) do
    Dbos.write_stream("progress", chunk)
  end

  Dbos.close_stream("progress")
  :ok
end
```

```elixir
for chunk <- Dbos.read_stream(report_workflow_id, "progress") do
  IO.puts("got chunk: #{inspect(chunk)}")
end
```

- `Dbos.write_stream/3` (`key`, `value`, `opts`) appends one value; must be called from inside a
  workflow. One durable step id per call.
- `Dbos.close_stream/2` (`key`, `opts`) writes the close sentinel; further writes to a closed
  stream raise `Dbos.StreamClosedError`.
- `Dbos.read_stream/3` (`workflow_id`, `key`, `opts`) returns a plain Elixir `Stream`, lazily
  polling for new values from the beginning and terminating once the close sentinel is read. If
  the producing workflow reaches a terminal status before closing the stream, `read_stream`
  performs one final read and stops — closing the race where the last write(s) landed after the
  reader's last poll but before the workflow finished, without waiting forever for a `close_stream`
  call that will never come.

**Ordering**: values are delivered in write order, from the start, every time — `read_stream`
always begins at offset zero. Multiple readers may consume the same stream independently and
concurrently; each gets its own cursor.

## Durable sleep

```elixir
defworkflow send_followup_email(user_id), name: "send_followup_email" do
  Dbos.sleep(:timer.hours(72))
  MyApp.Mailer.send_followup(user_id)
end
```

`Dbos.sleep/1` checkpoints the absolute wake time under one step, then waits only the *remaining*
interval — a workflow recovered after a crash mid-sleep does not wait out the full duration again,
only whatever was left.

A wait longer than `park_exit_threshold_ms` (a `Dbos.Supervisor` option, default `60_000` — one
minute) does something more than just block: the workflow's BEAM process exits entirely, leaving
behind a single row in an ETS table and a timer. When the timer fires (or a message/event it was
waiting on arrives), the workflow is redispatched exactly like a crash recovery — replayed from
its checkpoints back up to the wait site. This applies to `sleep`, `recv_message`, and
`get_event`, and it's why parking a workflow for hours or days is cheap: no live process, no
per-workflow supervision tree entry, no memory held — just a timer and a few dozen bytes,
regardless of how many workflows are waiting or how long they wait for. A workflow already deep
into a run (more than `park_replay_ceiling` steps completed, default `500`) skips parking even
past the threshold, since a wait that far in would pay for parking with a proportionally expensive
replay on wake.

Cancelling a parked or actively-waiting workflow interrupts the wait immediately rather than
waiting it out — `Dbos.cancel/2` wakes any live process, and a woken/redispatched workflow checks
for cancellation as soon as it resumes.
