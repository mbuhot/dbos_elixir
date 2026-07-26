# Live Approvals — Phoenix, LiveView and Dbos in both directions

An expense-approval app. A claim is submitted, a durable workflow reviews it, large claims park
waiting for a human, an approver answers from a different browser session, and the workflow
resumes and settles the claim.

The domain is scaffolding. The subject is the **two bridges** between the `Dbos` event world and
the `Phoenix.PubSub` event world.

```
                     ┌──────────────────────────────────────────┐
                     │           LiveApprovals.Approvals        │
   OUTBOUND          │  plain Ecto context, publishes on write  │
   workflow ────────▶│  (an Ash resource with a PubSub          │◀──────── INBOUND
   step calls it     │   notifier, without Ash)                 │   approver LiveView
                     └───────────────┬──────────────────────────┘   calls it
                                     │
                        Phoenix.PubSub broadcast
                                     │
         ┌───────────────────────────┼──────────────────────────────┐
         ▼                           ▼                              ▼
   "approvals"               "approvals:<id>"             "approvals:decisions"
   list + queue LVs          one claim's timeline LV      Bridges.Inbound
                                                                    │
                                                       Dbos.send_message/4
                                                                    ▼
                                                      dbos.notifications row
                                                                    │
                                                       Dbos.recv_message/3
                                                                    ▼
                                                       the parked review resumes
```

## The pieces

| Module | Role |
|---|---|
| `LiveApprovals.Approvals` | Ecto context. Every write publishes on PubSub. Knows nothing about `Dbos`. |
| `LiveApprovals.Reviews` | The only module that both writes claims and calls `Dbos`. |
| `LiveApprovals.Reviews.ReviewWorkflow` | The durable review. Announces stages through steps; parks in `Dbos.recv_message/3`. |
| `LiveApprovals.Bridges.Inbound` | PubSub → `Dbos`. Subscribes to `"approvals:decisions"`, calls `Dbos.send_message/4`. |
| `RequestLive.Index` | Submit a claim, watch every claim's stage move. |
| `RequestLive.Show` | One claim's live timeline. |
| `ApproverLive.Index` | The approver's queue. Approve or reject. |

A claim's id **is** its workflow id, so a request id is the only handle anything needs.

---

## Outbound: Dbos → PubSub → LiveView

Each stage transition is a durable step that calls the context, and the context broadcasts:

```elixir
defstep announce(request_id, stage) do
  {:ok, _event} = Approvals.record_stage(request_id, stage)
  :ok
end
```

`record_stage/3` writes the `expense_requests.stage` column and a `request_events` row, then
broadcasts. The LiveView reads the durable rows on mount and thereafter only ever changes on a
broadcast. No timer, no `Process.send_after`, no re-query loop.

### The choice, and why

| Option | What it looks like | Verdict |
|---|---|---|
| Broadcast inside a durable step (**chosen**) | the step body writes the row and publishes | the message is a stage name the domain already understands, emitted at the same instant the durable state changes |
| A bridge process on `Dbos.Notifications` | a `GenServer` calling `subscribe_all/2`, republishing onto PubSub | one registration covers every review, and it carries the workflow id and the key that changed — but a bridge is the wrong place to name a domain stage, and the notification is a wake-up the bridge must then read back |
| A bridge process on telemetry | attach to `[:dbos, :step, :stop]` and `[:dbos, :wait, :stop]`, republish | genuinely global and needs no workflow code, but the payload is a step name rather than a domain stage |

The deciding factor is the **late joiner**. A broadcast only reaches whoever is subscribed at that
instant. Anyone opening a claim page after the fact needs the state to still exist somewhere.
Writing the stage durably and broadcasting it in the same call gives both readers the same source
of truth: `mount/3` reads the rows, `handle_info/2` takes the broadcast.

### What happens on replay

A step body does **not** re-run on replay — the recorded output is returned. So the ordinary
recovery path re-broadcasts nothing:

| Situation | Does `announce/2` run again? |
|---|---|
| Recovery replays a step that already checkpointed | no — the recorded `:ok` is returned |
| The process died *between* the broadcast and the checkpoint commit | yes — the announcement is at-least-once |

Two things make that second row harmless here:

- `request_events` is **unique on `(request_id, stage)`** with `on_conflict: :nothing`, so a
  repeated announcement leaves exactly one timeline row.
- The broadcast carries the full new stage, and `RequestLive.Show` keeps stages in a map keyed by
  stage. Applying the same message twice is the same as applying it once.

A duplicate would matter if the payload were a delta rather than a state (`"+1 approval"`,
`"append this line"`), or if the broadcast triggered an outward-facing effect — an email, a push
notification. Make the message a full state replacement and replay stops being a UI concern.

`test "replaying a settled review announces none of its stages a second time"` pins this down: it
flips a finished review back to `PENDING`, recovers it, and asserts no notification arrives.

---

## Inbound: LiveView → PubSub → Dbos

`ApproverLive.Index` never mentions `Dbos`. Clicking Approve calls one context function:

```elixir
Approvals.record_decision(request_id, :approved, approver)
```

which writes the decision and publishes on `"approvals:decisions"`.
`LiveApprovals.Bridges.Inbound` is subscribed to that topic and does the translation:

```elixir
def handle_info({:decision_recorded, request_id, decision, decided_by}, state) do
  Reviews.deliver_decision(request_id, decision, decided_by, engine: state.engine)
  {:noreply, state}
end
```

`Reviews.deliver_decision/4` is `Dbos.send_message/4`. That call **writes a row** into the engine's
`notifications` table. This is the whole point of the direction:

| Layer | Lifetime |
|---|---|
| `Phoenix.PubSub` notification | in memory, this cluster, right now |
| `dbos.notifications` row | in Postgres, until a review consumes it |

So the approval survives everything downstream of the click:

- **The review process is parked.** A wait longer than the parking threshold releases the process
  entirely (`Dbos.Waits`); there is no pid holding the review open. The row waits in Postgres.
- **The node restarted.** Recovery replays the review from its checkpoints, reaches
  `Dbos.recv_message/3` again, finds the row, and consumes it.
- **The answer arrived first.** `recv_message` checks for an already-pending notification before it
  waits at all, so an approval recorded before the review ever ran is picked up the moment it does.
- **Another node.** `Dbos.Notifications` rides Postgres `LISTEN`/`NOTIFY`, so a review parked on
  node A wakes on a message written by node B.

The workflow side is three lines:

```elixir
announce(request_id, :awaiting_decision)
decision = Dbos.recv_message("decision", :timer.hours(72))
settle(request_id, decision.decision, decision.decided_by)
```

### Why the workflow's own settlement does not loop back

`record_decision/3` (a human answering) publishes on `"approvals:decisions"`.
`record_outcome/3` (the review settling) does not. Only one of the two is an inbound trigger, so
the workflow writing its result never looks like a fresh decision and never feeds itself a message.

---

## Running it

```sh
mix setup            # deps, database, migrations, seeds, assets
mix phx.server
```

Then, to see the cross-session effect:

1. Open <http://localhost:4000/requests> and submit a claim for more than **$100.00**.
2. Follow the claim's link. The timeline fills in on its own: submitted, validating, policy check,
   awaiting decision.
3. Open <http://localhost:4000/approvals> in a **second browser window**, set your name, and
   approve the claim.
4. Watch the first window finish, without a reload.

A claim of $100.00 or less never reaches step 3 — the policy engine settles it inside the workflow.

The `Dbos` schema is installed as an explicit migration in this app's own sequence
(`priv/repo/migrations/20260101000002_add_dbos.exs`), and the engine starts with
`migrations: :verify`.

## Telemetry

`LiveApprovalsWeb.Telemetry` defines summaries over the engine's spans alongside the Phoenix and
repo metrics: `dbos.workflow.stop.duration` (tagged `name`, `replay`),
`dbos.step.stop.duration` (tagged `function_name`), and `dbos.queue.dequeue.stop.duration`.
`[:dbos, :wait, :stop]` reports a review's park on a human, tagged `kind` and `outcome`. No
reporter is attached — add one, or `Telemetry.Metrics.ConsoleReporter`, to see them.

Telemetry is not used to drive the UI. See the outbound table above for the reasoning.

## Tests

```sh
mix test
```

Every test runs under `Ecto.Adapters.SQL.Sandbox` with a per-test engine in `Dbos`'s `:manual`
testing mode, so nothing races the sandbox and reviews run synchronously via
`Dbos.Testing.drain_queue/2`.

| File | Direction covered |
|---|---|
| `test/live_approvals/reviews_test.exs` | both — settlement, an answer stored before the review runs, an answer that arrives while the review is not running, and replay announcing nothing twice |
| `test/live_approvals_web/live/request_live_test.exs` | outbound — a LiveView filling in its timeline because a workflow progressed |
| `test/live_approvals_web/live/approver_live_test.exs` | inbound — one session's approval settling a review and reaching another session's open page |

`:manual` mode never blocks: `recv_message` with nothing pending raises immediately rather than
parking. The tests use that on purpose — draining a review with no answer waiting leaves it stopped
at exactly the point it would have parked, which is then recovered once the answer is delivered.
That is the same shape as a node dying mid-wait.
