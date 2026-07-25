# Reliable Customer Service Agent

An agent that handles a customer request by calling an LLM with tools, executing whichever
tools it picks, and — for refunds over a threshold — escalating to a human approver over a wait
that can span hours. A durable port of DBOS's
[customer service agent](https://docs.dbos.dev/python/examples/customer-service) sample.

## What this proves

Every LLM call and every tool execution is a step or a child workflow. A crash anywhere in the
conversation — mid-tool-call, or hours into an escalated approval wait — resumes from the last
checkpoint with the full message history intact, because that history is nothing more than
recorded step outputs. Every `[step] ...` log line is a real, non-idempotent call; after a crash
and restart, lines that already printed do not print again, and the refund itself never runs
twice.

## Architecture

```
CustomerServiceAgent.Support
├── handle_request/2       defworkflow — the conversation loop: LLM turn, tool calls, repeat
├── process_refund/1       defworkflow — refunds ≤ $1000 immediately; escalates the rest
├── approval_workflow/1    defworkflow — emails an approver, waits up to 6h for their decision
├── call_llm/1             defstep — one LLM turn with tools bound
├── get_purchase_step/1    defstep — order lookup
├── update_purchase_status_step/2   defstep — the actual refund/rejection write
└── send_approval_email_step/1     defstep — notifies the human approver

CustomerServiceAgent.ClaudeLLM    Anthropic Messages API with tool use, over req
CustomerServiceAgent.OrderStore   in-memory order table, read/written only from steps
```

`CustomerServiceAgent.LLM` is a behaviour; which module implements it is read from
`Application.get_env/3` **inside `call_llm`'s step body**, never inside a workflow body — a live
read of mutable config belongs in a step, not in replayable workflow code. Tests swap in a
network-free stub.

### The refund tool is also a workflow

`request_refund` is a tool the LLM can call, but it is backed by `process_refund/1`, a
`defworkflow`. Called from inside `handle_request`'s workflow body, that becomes a **child
workflow**: checkpointed once, awaited for its result, and never started twice on replay —
exactly what "a refund runs exactly once" requires.

### The escalation wait

`process_refund/1` starts `approval_workflow/1` under a **deterministic** workflow id
(`"approval-<order_id>"`, computed from the order id alone — no randomness) rather than the
bare, auto-derived one, using `Dbos.start/3` and `Dbos.await/2` directly instead of a bare
function call. That id is what a human approver's decision targets:

```elixir
Dbos.send_message("approval-202", "approval_decision", "approve")
```

`approval_workflow/1` waits on `Dbos.recv_message("approval_decision", timeout_ms)` for up to 6
hours. A wait that long exceeds the engine's parking threshold: the workflow's process **exits**
rather than sitting resident for hours, leaving only a row and a timer behind. Sending the
decision (or the deadline firing) wakes it, and it rehydrates by replaying from its checkpoints
back to the wait site — cheap, because `send_approval_email_step` already checkpointed and does
not re-run. This is not a special case of recovery; it is the same mechanism.

## Running it

Requires Postgres and an Anthropic API key.

```sh
export ANTHROPIC_API_KEY=sk-ant-...
export PGHOST=localhost PGUSER=postgres PGPASSWORD=postgres   # override as needed
mix deps.get
iex -S mix
```

```elixir
iex> {:ok, handle} = Dbos.start("customer_request", ["cust-1", "Please refund order 101"], workflow_id: "demo-1")
iex> Dbos.await(handle)
```

Order `101` is $29.99 (refunds immediately). Order `202` is $1299.00 (escalates).

```elixir
iex> {:ok, handle} = Dbos.start("customer_request", ["cust-2", "Please refund order 202"], workflow_id: "demo-2")
```

Watch the console for the approval instructions the `send_approval_email_step` log line prints,
then approve or reject from another `iex` session against the same database:

```elixir
iex> Dbos.send_message("approval-202", "approval_decision", "approve")
iex> Dbos.await(%Dbos.WorkflowHandle{workflow_id: "demo-2"})
```

## Killing it mid-run

While `demo-1` or `demo-2` is in flight, kill the BEAM hard and restart:

```sh
$ pgrep -f "iex -S mix"
$ kill -9 <pid>
$ iex -S mix
```

`Dbos.Recovery` re-dispatches every `PENDING` workflow before you type a command. Steps whose
`[step] ...` line already printed do not print again. For `demo-2`, kill it *while the approval
email line is on screen but before you've sent a decision* — on restart the workflow re-parks
itself waiting on the same topic, and sending the approval afterward still completes the refund
exactly once.

## Testing

`test/customer_service_agent/support_test.exs` swaps in `CustomerServiceAgent.StubLLM`
(configured via `Application.put_env/3`, no network) and:

- runs a low-value refund end to end and asserts on the reply and the order's new status;
- starts a conversation, waits for the first step to checkpoint, kills the workflow process,
  recovers it, and asserts the LLM call that already completed was never invoked twice — the
  stub bumps a `:persistent_term` counter keyed by the exact message history it saw, which only
  a genuine re-run of the step body can increment;
- escalates a high-value refund, waits for `approval_workflow` to park (its process exits — this
  is asserted directly), sends the approval decision, and confirms the refund lands exactly
  once.

```sh
mix deps.get
mix test
```
