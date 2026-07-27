# Upgrading Workflows

Some workflows outlive a single deploy: they're still `PENDING` in Postgres when you ship new
code. Recovery replays such a workflow against whatever code is running *now*, so the new code has
to reproduce the same step sequence the old code produced. This page covers what breaks a workflow
already in flight, and how to ship a change safely.

## What counts as a breaking change

A checkpoint records a step's name and output against its position in the call sequence. Replay
walks the workflow body from the top and, at each step call, compares the name recorded at that
position against the one the current code is about to run. A mismatch raises
`Dbos.UnexpectedStepError`.

| Change | Breaks replay? | Why |
|---|---|---|
| Reordering two steps | Yes | The first position now names a different step than what's recorded there. |
| Renaming a step (its `name:`, or the default `"fun/arity"`) | Yes | The recorded name no longer matches the expected one. |
| Adding or removing a step before an existing one | Yes | Shifts every later step's position by one. |
| Changing how many step ids a branch allocates (an `if`/`case`/`cond` whose arms call a different number of durable operations) | Yes | The branch taken during the original run fixed the id layout; a differently-shaped branch on replay misaligns every id after it. `mix dbos.explain` flags this statically. |
| Changing the shape of a struct/map stored as a workflow's input, a step's output, an event value, or a stream item | Yes, on decode | Values are stored as Erlang terms, so a struct's fields and module name are part of the stored value. Anything written before the change decodes back into the shape it was written with. |
| Adding a brand new step at the *end*, after every step an in-flight instance could already have completed | No, for that instance | Nothing before it shifted. Still a landmine for an instance not yet that far along, unless the new step is unconditionally reached on every remaining path. |
| Changing a step's *body*, leaving its name, position, and id count alone | No | Replay never re-runs a completed step; only the checkpointed name and position are matched. A step not yet reached picks up the new behavior on its first execution. |

The compile-time determinism checker catches nondeterministic *constructs*. It has no knowledge of
the previous deploy, so upgrade safety stays your call.

## Application version

Every workflow records the `application_version` it started under. Dequeue uses it to decide which
running executor may claim a queued workflow. It resolves once, at `Dbos.Supervisor` boot:

1. the `:application_version` option, if given;
2. else the `DBOS__APPVERSION` environment variable;
3. else a digest of every registered workflow module's compiled code.

```elixir
{Dbos.Supervisor,
 name: Dbos,
 application_version: System.get_env("RELEASE_VSN") || System.get_env("GIT_SHA"),
 ...}
```

Pin it explicitly for a real release. The computed fallback moves whenever any registered workflow
module's compiled code changes, which makes "which version is this" hard to answer from the outside;
a git SHA or release tag you control answers it directly.

One place gates on it: **dequeue** — an executor claims queued workflows matching its own version,
and an executor running the most recently booted version also claims workflows that carry no
version at all.

Recovery gates on the workflow's own version instead, below.

## Workflow version

A workflow declares its own version, and recovery claims a `PENDING` row only on an executor
registering that name at that version:

```elixir
defworkflow charge(order_id), name: "MyApp.Billing.charge", version: "2" do
  ...
end
```

| The row declares | An executor registering the name at | Recovery |
|---|---|---|
| `"2"` | `"2"` | Claims it |
| `"2"` | `"1"`, or nothing | Leaves it |
| nothing | anything | Claims it |

`version:` is optional, and omitting it is the right default for most workflows: a deploy that
changes anything else in the application still recovers them. Declare one on a workflow whose body
you are about to change in a way the table above calls breaking, so instances mid-flight under the
old body wait for the old build rather than crashing against the new one.

Bumping the version is the developer's call, exactly as keeping the body deterministic is. Nothing
is computed or inferred — the engine records what it is told.

`mix dbos.orphans` reports any group left with no executor able to claim it, so a version bumped
without a build still running the old one is visible rather than silent.

## Four ways to ship a change safely

### 1. Bump the workflow's `version:`

Old in-flight instances of *that one workflow* wait for a build still registering the old version;
every other workflow keeps recovering across the deploy untouched.

```elixir
defworkflow process_order(order_id, amount),
  name: "MyApp.Checkout.process_order",
  version: "2" do
  # the changed body
end
```

Keep the previous build running until its instances drain, then retire it:

```elixir
{:ok, remaining} =
  Dbos.Client.list(Dbos.config(),
    name: "MyApp.Checkout.process_order",
    status: [:pending]
  )
```

Cost is the same two-fleet overlap as bumping `application_version`, narrowed to the workflows that
actually changed.

### 2. Bump `application_version`

This holds back *queued* work: an `ENQUEUED` row is dequeued by an executor at its own version, so
a `v1` fleet keeps draining the `v1` queue while `v2` takes new arrivals.

```mermaid
flowchart LR
    subgraph "old fleet — application_version = v1"
        A["queued workflows, version = v1"]
    end
    subgraph "new fleet — application_version = v2"
        B["new workflows, version = v2"]
    end
    A -->|drains naturally, v1 executors still running| C["v1 fleet decommissioned"]
    B --> D["v2 fleet runs everything from here"]
```

1. Ship the new code as `v2`, running *alongside* the still-running `v1` fleet.
2. New workflows start under `v2`; `v1` executors keep dequeuing their own queued `v1` workflows.
3. Decommission `v1` once it has no unfinished workflows left:

```elixir
{:ok, remaining} =
  Dbos.Client.list(Dbos.config(),
    application_version: "v1",
    status: [:pending, :enqueued, :delayed]
  )
```

A workflow already running — `PENDING`, off its queue — is recovered on the workflow version, so
`v2` picks it up regardless of the fleet version it started under. That is the point: a deploy
should not strand work whose body it can still replay. Combine this with strategy 1 on the
workflows whose bodies actually changed.

Cost: two versions of the application run at once for as long as the oldest queued workflow takes
to start. Start the `v2` fleet deliberately — whichever version boots first becomes "latest" and
starts absorbing `NULL`-version rows.

### 3. A new workflow name

Give the changed workflow a new `name:`, leaving the existing one in place and registered. Old
in-flight instances keep dispatching under the old name to the old body; every new invocation goes
through the new name.

```elixir
defworkflow process_order(order_id, amount), name: "MyApp.Checkout.process_order" do
  # keep this body untouched and registered until every in-flight instance has finished
end

defworkflow process_order_v2(order_id, amount), name: "MyApp.Checkout.process_order_v2" do
  # new behavior
end
```

Both bodies live in one deployed version, so no parallel fleet is needed. The cost is code you keep
around until the old name's last instance drains, plus a naming scheme to track.

### 4. A patch

`Dbos.patch/1` inserts new steps into a workflow body under one version and one name. It returns a
boolean: `true` for code taking the new path, `false` for an existing instance whose replay must
skip it and keep its original step sequence intact.

```elixir
defworkflow process_order(order_id), name: "MyApp.Checkout.process_order" do
  charge = charge_card(order_id)

  if Dbos.patch("fraud-check") do
    fraud_check(order_id)
  end

  ship(order_id)
end
```

| Call site | `Dbos.patch("fraud-check")` returns | Why |
|---|---|---|
| A brand-new workflow | `true` | Nothing recorded at this id yet; writes the marker and takes the new path. |
| An in-flight instance that has not reached this point | `true` | Same: no checkpoint there yet. |
| An in-flight instance that already ran past this point | `false` | Some other step is already recorded at this position. No id is consumed, so every later step keeps its original position. |
| A replay of an instance that already took this patch | `true` | The checkpoint here is the patch's own marker; replay reproduces the decision. |

The `true` path consumes an id; the `false` path consumes none, which is what keeps an old
instance's later steps aligned with what it recorded. `mix dbos.explain` recognizes
`if Dbos.patch(...) do ... end` as a conditional 0-or-1-id allocation.

`Dbos.patch/1` must be called from a workflow body: outside a workflow it raises
`Dbos.NotInWorkflowError`, and from inside a step or a `Dbos.transaction/3` body,
`Dbos.PatchInStepError`.

#### Retiring the patch

Once every pre-patch instance has drained, the `if Dbos.patch(...)` check comes out and the new
step runs unconditionally. Instances that *did* record the patch may still be in flight, each
carrying its marker at an id the new code no longer allocates. `Dbos.deprecate_patch/1` stands in
that spot and absorbs it:

```elixir
defworkflow process_order(order_id), name: "MyApp.Checkout.process_order" do
  charge = charge_card(order_id)

  Dbos.deprecate_patch("fraud-check")
  fraud_check(order_id)

  ship(order_id)
end
```

| Call site | `Dbos.deprecate_patch("fraud-check")` | Why |
|---|---|---|
| A replay of an instance that took the patch | Consumes one id | Swallowing the marker's id keeps every later step aligned with what that instance wrote. |
| A brand-new workflow | Consumes no id, writes nothing | The marker is retired; the next step takes the id it held. |
| An in-flight instance that ran past this point without the marker | Consumes no id | Its recorded sequence is left as it is. |

It obeys the same call-site rule as `Dbos.patch/1`. Once every marker-carrying instance has drained,
delete the line.

## Summary

| Strategy | In-flight workflow's fate | Cost |
|---|---|---|
| Bump the workflow's `version:` | Waits for a build still registering that name at its own version; every other workflow recovers as normal | Run two builds until that workflow drains |
| Bump `application_version` | A queued one waits for an executor still running the old version; a running one is recovered by either | Run two fleets until the old one drains |
| New workflow name | Keeps dispatching to the untouched old body under the old name | Keep the old function and name registered until it drains |
| Patch (`Dbos.patch/1`) | Sees `false` at the patch point and skips the new step, keeping its recorded sequence | Keep the check in the code until every pre-patch instance drains |
| Change the workflow in place | `Dbos.UnexpectedStepError` on its next step, or a decode failure on a changed struct shape | Broken workflow, manual recovery |
