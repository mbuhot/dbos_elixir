# Workflow Communication

Three primitives let workflows exchange data with each other and with the outside world:
messages, events, and streams. All three are durable — every send/set/write is checkpointed, and
every receive/get/read survives a crash and resumes waiting where it left off. Durable sleep
shares the same waiting machinery, so it's covered here too.

Every value travels as an Erlang term, so atoms, tuples, and structs arrive exactly as sent.

```elixir
Dbos.send_message(other_id, nil, {:approved, %MyApp.Approval{by: "alice", note: "looks good"}})

# ... in the other workflow:
{:approved, %MyApp.Approval{by: by, note: note}} = Dbos.recv_message(nil, 30_000)
```

## Messages: `send_message` / `recv_message`

A message is a point-to-point, fire-and-forget delivery to a specific workflow id, optionally
scoped to a topic.

```elixir
Dbos.send_message(payment_workflow_id, "payment_confirmed", %{amount: 4200, currency: :usd})
```

```elixir
defworkflow await_payment(order_id), name: "MyApp.Orders.await_payment" do
  try do
    {:ok, Dbos.recv_message("payment_confirmed", 300_000)}
  rescue
    Dbos.RecvTimeoutError -> {:error, :timed_out}
  end
end
```

- `Dbos.send_message/4` (`destination_id`, `topic`, `message`, `opts`) is a durable, checkpointed
  step inside a workflow, and a direct write when called from ordinary code. `topic: nil` is its
  own topic, distinct from any named one.
- `Dbos.recv_message/3` (`topic`, `timeout_ms`, `opts`) must be called from inside a workflow. It
  blocks until a message on that topic arrives, returning it, or raises
  `Dbos.RecvTimeoutError` once `timeout_ms` elapses with nothing delivered.

**Ordering and delivery**: one workflow may wait on a given topic only once at a time — a second
concurrent `recv_message` call on the same topic raises `Dbos.RecvConflictError`. A message already
sent before a `recv_message` call is picked up immediately.

Each `recv_message` call consumes two durable step ids, both checkpointed, so a workflow that
crashes mid-wait resumes waiting only for whatever time is left.

## Events: `set_event` / `get_event`

An event is a durable key/value slot owned by one workflow, readable by any number of other
callers — status flags, partial results, anything another party might want to poll for or wait
on.

```elixir
defworkflow process_upload(file_id), name: "MyApp.Uploads.process_upload" do
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

**Ordering and delivery**: any number of callers may wait on the same workflow's key
concurrently, and all of them wake when it's set. Reading returns the key's latest value; use a
stream to see every value it was ever set to. Called from inside a workflow, the wait and the read
are each checkpointed.

## Streams: `write_stream` / `read_stream` / `close_stream`

A stream is an ordered, append-only sequence a workflow produces incrementally and any number of
readers consume as it goes — progress updates, generated tokens, paginated results.

```elixir
defworkflow generate_report(report_id), name: "MyApp.Reports.generate_report" do
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
- `Dbos.close_stream/2` (`key`, `opts`) closes the stream; further writes to a closed stream raise
  `Dbos.StreamClosedError`.
- `Dbos.read_stream/3` (`workflow_id`, `key`, `opts`) returns a plain Elixir `Stream`, lazily
  polling for new values from the beginning and ending once the stream is closed. A producing
  workflow that reaches a terminal status without closing the stream also ends the read, after one
  final read.

**Ordering**: values are delivered in write order, from offset zero, every time. Multiple readers
may consume the same stream independently and concurrently; each gets its own cursor.

## Durable sleep

```elixir
defworkflow send_followup_email(user_id), name: "MyApp.Emails.send_followup_email" do
  Dbos.sleep(:timer.hours(72))
  MyApp.Mailer.send_followup(user_id)
end
```

`Dbos.sleep/1` checkpoints the absolute wake time under one step, then waits only the *remaining*
interval — a workflow recovered after a crash mid-sleep waits out only whatever was left.

## Parking a long wait

A wait longer than `park_exit_threshold_ms` (a `Dbos.Supervisor` option, default `60_000`) parks:
the workflow's process exits, and the workflow is redispatched — replayed from its checkpoints back
up to the wait site — when the wait resolves. This applies to `sleep`, `recv_message`, and
`get_event`, and it is why waiting for hours or days is cheap: no live process and no memory held,
however many workflows are waiting. A workflow more than `park_replay_ceiling` steps into its run
(default `500`) stays resident, since replaying it on wake would cost more than the process it
saves.

`Dbos.cancel/2` interrupts a wait immediately: it wakes any live process, and a
woken or redispatched workflow checks for cancellation as soon as it resumes.
