# Function ID Allocation in dbos-transact-golang

Reference: `reference/dbos-transact-golang/dbos/workflow.go` (Go source names this
"step ID"; the persisted column is `operation_outputs.function_id`. Same concept,
two names — this doc uses "step ID" to match the Go identifier and calls out
`function_id` where it hits the database).

All line references are `dbos/workflow.go:N` unless another file is named.

## 1. The counter

The counter lives on the in-memory `workflowState` struct, one instance per
running workflow (or per running step, see below):

```go
// workflow.go:27-44
type workflowState struct {
    workflowID          string
    stepID              int
    isWithinStep        bool
    ...
}

// nextStepID returns the next step ID and increments the counter
func (ws *workflowState) nextStepID() int {
    ws.stepID++
    return ws.stepID
}
```

- Initial value: **`-1`**, set explicitly when a workflow's `workflowState` is
  constructed (`workflow.go:1511-1518`, comment: `stepID: -1, // Steps are 0-indexed`).
- The first call to `nextStepID()` therefore returns `0`. Step numbering is
  **0-indexed**, not 1-indexed.
- There is no separate persisted counter. `wfState.stepID` is purely an
  in-memory value that gets rebuilt from scratch every time the workflow
  function body runs (fresh execution, replay after crash, or recovery) — see
  §4.
- The increment primitive is exactly `ws.stepID++; return ws.stepID` — pre-increment,
  so allocated IDs are `0, 1, 2, ...` in call order.
- A step body itself runs with its own child `workflowState` (`stepState`,
  `workflow.go:2332-2337`) that has `isWithinStep = true` and a **fixed**
  `stepID` (no further `nextStepID()` calls are legal from inside a step —
  nested step/child-workflow calls from within a step return errors, e.g.
  `workflow.go:3017-3019`, `1236-1239`).

## 2. Per-operation ID table

"When" = ordering of ID allocation relative to the operation's own work.
"Early-return-safe" = whether the allocation happens before any error/early-return
path inside that operation (i.e. is it guaranteed to run every time the call is made,
even on paths that produce no database row).

| Operation | IDs consumed | Allocation order | Before/after early-return | `function_name` recorded |
|---|---|---|---|---|
| Plain step (`RunAsStep`) | 1 | Eagerly, via `prepareStepExecution` → `wfState.nextStepID()` (`workflow.go:2330`), before checking whether the step already ran | Before the `CheckOperationExecution` / early replay-hit return (`workflow.go:2504-2518`) | user-supplied step name, defaulting to the Go function name via `runtime.FuncForPC` (`workflow.go:2469`) |
| Transaction step (`RunAsStep`/`RunAsTransaction` inner `runAsTxn`) | 1 | Same as plain step, same `prepareStepExecution` path (`workflow.go:2613-2617`) | Same | user-supplied / function name |
| `Go(...)` (async step) | 1 | "generates a deterministic step ID... before running the step in a routine" (`workflow.go:2749`), same `prepareStepExecution` path | Before the goroutine starts | user-supplied / function name |
| `Sleep` | 1 | Allocated inside `runAsTxn("DBOS.sleep")` (no pre-generated ID) (`workflow.go:3975-3977`) | N/A (no early-return path before allocation) | `"DBOS.sleep"` |
| `Send` (from within workflow) | 1 | Inside `runAsTxn(..., WithStepName("DBOS.send"))` (`workflow.go:3048-3051`) | After the "cannot call within a step" check (`3017-3019`), which returns before any allocation | `"DBOS.send"` |
| `Send` (outside workflow) | 0 | n/a — direct DB write, no step machinery (`workflow.go:3052-3057`) | — | — |
| `Recv` | **2** (recv + internal timeout sleep) | Both allocated **up front**, unconditionally, before the checkpoint-hit check: `stepID := wfState.nextStepID(); sleepStepID := wfState.nextStepID()` (`workflow.go:3093-3094`) | Before the early-return-on-replay path (`3101-3113`) and before the "message already pending" skip that never touches `sleepStepID` (`3122-3137`) | `"DBOS.recv"` (id 1), `"DBOS.sleep"` (id 2, only written if a wait actually occurs — see below) |
| `SetEvent` | **1** (not 2) | Single `runAsTxn(..., WithStepName("DBOS.setEvent"))` call (`workflow.go:3269-3282`); no sleep involved (SetEvent doesn't block) | n/a | `"DBOS.setEvent"` |
| `GetEvent` (from within workflow) | **2** (getEvent + internal timeout sleep) | Both allocated up front: `stepID = wfState.nextStepID(); sleepStepID = wfState.nextStepID()` (`workflow.go:3325-3326`), mirroring `Recv` | Before the checkpoint-hit early return (`3330-3342`) | `"DBOS.getEvent"` (id 1), `"DBOS.sleep"` (id 2, conditional — see below) |
| `GetEvent` (outside workflow) | 0 | n/a, `isInWorkflow == false` skips all step-ID logic (`workflow.go:3317-3395`) | — | — |
| `WriteStream` | 1 | Inside `runAsTxn(..., WithStepName("DBOS.writeStream"))` (`workflow.go:3533-3546`) | n/a | `"DBOS.writeStream"` |
| `CloseStream` | 1 | Inside `runAsTxn(..., WithStepName("DBOS.closeStream"))` (`workflow.go:3931-3944`) | n/a | `"DBOS.closeStream"` |
| `GetResult` (blocking wait, from within a workflow) | 1 | **Allocated only after the wait completes** — first a non-consuming peek at `stepID+1` (`workflow.go:209`, `checkGetResultExecution`), then, only on the actual (non-cached) path, `workflowState.nextStepID()` is called at record time, after `<-h.outcomeChan` resolves (`workflow.go:311`, `425`) | ID consumption happens *after* the operation's real work (the wait), unlike every other entry in this table | `"DBOS.getResult"` |
| `GetStatus()` on a handle, called from within a workflow | 1 | Wrapped in `RunAsStep(..., WithStepName("DBOS.getStatus"))` (`workflow.go:137-152`) — standard plain-step allocation | Standard plain-step semantics | `"DBOS.getStatus"` |
| Enqueueing a child (`Enqueue(...)`, standalone client call, from within a workflow) | 1 | Inside `runAsTxn(..., WithStepName("DBOS.enqueue"))` (`workflow.go:1963-1966`) | n/a | `"DBOS.enqueue"` |
| Enqueueing a child (`Enqueue`, outside a workflow) | 0 | n/a, direct transaction, no step state (`workflow.go:1967-1983`) | — | — |
| Starting/enqueueing a child workflow via `RunWorkflow` (covers both `WithQueue` and direct start) | 1 (parent-side) | `parentWorkflowState.nextStepID()` (`workflow.go:1243`) is called **unconditionally, very early** — right after the "not from within a step" guard (`1236-1239`) and *before* `CheckChildWorkflow` (the replay lookup, `1276-1293`), before serializing input, before starting any DB transaction | Before every downstream error/early-return path in the function (dedup conflicts, insert failures, etc.) | The child's `params.WorkflowName` is written as `function_name` in the `operation_outputs` row created by `RecordChildWorkflow` (`workflow.go:1427-1434`), keyed by `child_workflow_id` |
| `forkWorkflow` (called from within a workflow) | 1 | Inside `runAsTxn(..., WithStepName("DBOS.forkWorkflow"))` (`workflow.go:4754-4758`) | n/a | `"DBOS.forkWorkflow"` |
| `forkWorkflow` (called from outside a workflow, i.e. client-side) | 0 | n/a — direct `sysdb.RetryWithResult`, no workflow state (`workflow.go:4759-4764`) | — | — |
| `Patch` | **0 or 1**, conditional | Peeks at `wfState.stepID + 1` without consuming it (`workflow.go:4041`); only if the patch DB call returns `patched == true` does it call `wfState.nextStepID()` to actually consume that ID (`workflow.go:4046-4049`) | The peek happens before the conditional consumption; on the non-patched path, no ID is ever consumed and the counter is untouched | `"DBOS.patch-" + patchName` (see §3) |
| `DeprecatePatch` | **0 or 1**, conditional (mirrors `Patch`) | Peeks at `wfState.stepID + 1` (`workflow.go:4105`); consumes via `wfState.nextStepID()` only if a patch is found and deprecated (`workflow.go:4119-4121`) | Same shape as `Patch` | `"DBOS.patch-" + patchName` |
| `GetStepID` / `GetWorkflowID` | 0 | Pure reads of `wfState.stepID` / `wfState.workflowID` (`workflow.go:4154-4160`, `4151`) | — | — (no row written) |

### The `Recv`/`GetEvent` internal sleep: allocated but sometimes not recorded

Both `Recv` and `GetEvent` reserve two IDs unconditionally up front so the
layout is stable across replays, but the second ID (the "DBOS.sleep"
checkpoint) is only actually **written** to `operation_outputs` if a wait is
needed:

```go
// workflow.go:3122-3137 (Recv); same shape at 3352-3371 for GetEvent
var timeoutOccurred bool
if !waiter.Pending {
    // Checkpoint the timeout deadline as a "DBOS.sleep" step before waiting.
    deadlineMs, err := runAsTxn(c, func(ctx context.Context, tx Tx) (int64, error) {
        return time.Now().Add(timeout).UnixMilli(), nil
    }, WithStepName("DBOS.sleep"), withNextStepID(sleepStepID))
    ...
}
```

If `waiter.Pending` is true (the message/event was already available when
`Recv`/`GetEvent` registered as a listener), the `runAsTxn` call — and thus the
`operation_outputs` row for `sleepStepID` — never happens. The in-memory
counter has *already* advanced past `sleepStepID` regardless (both
`nextStepID()` calls happened unconditionally at the top of the function), so
there is a genuine gap in `function_id` values for that workflow. This is safe
because replay re-executes the same code path deterministically: on replay the
same "was it pending" branch is retaken (`waiter.Pending` reads current DB
state, but if the recv/getEvent step was already checkpointed, execution
returns early at `workflow.go:3111-3113` / `3340-3342` before ever reaching the
`waiter` logic at all). **Port note:** an Elixir port must reserve both IDs
unconditionally before checking whether a wait is needed, not just when a wait
actually happens.

## 3. Reserved `function_name` strings

Every literal `"DBOS.xxx"` string used as a `function_name`/`StepName` in
`workflow.go`:

```
DBOS.getStatus            workflow.go:152
DBOS.getResult            workflow.go:210,316,430
DBOS.enqueue              workflow.go:1966
DBOS.select               workflow.go:2972
DBOS.send                 workflow.go:3051
DBOS.recv                 workflow.go:3105,3156
DBOS.sleep                workflow.go:3128,3360,3977   (shared name for both the standalone Sleep step and the internal recv/getEvent timeout step)
DBOS.setEvent             workflow.go:3282
DBOS.getEvent             workflow.go:3334,3413
DBOS.writeStream          workflow.go:3546
DBOS.closeStream          workflow.go:3944
DBOS.retrieveWorkflow     workflow.go:4212
DBOS.cancelWorkflow       workflow.go:4287
DBOS.updateWorkflowAttributes  workflow.go:4339
DBOS.cancelWorkflows      workflow.go:4377
DBOS.setWorkflowDelay     workflow.go:4459
DBOS.deleteWorkflows      workflow.go:4499
DBOS.resumeWorkflow       workflow.go:4590
DBOS.forkWorkflow         workflow.go:4758
DBOS.listWorkflows        workflow.go:5135
DBOS.getWorkflowSteps     workflow.go:5301
DBOS.getWorkflowAggregates     workflow.go:5415
DBOS.getStepAggregates    workflow.go:5482
DBOS.createSchedule       workflow.go:5660
DBOS.pauseSchedule        workflow.go:5800
DBOS.resumeSchedule       workflow.go:5840
DBOS.deleteSchedule       workflow.go:5869
DBOS.getSchedule          workflow.go:5905
DBOS.listSchedules        workflow.go:5950
```

Patch prefix (`workflow.go:4014`):

```go
const _DBOS_PATCH_PREFIX = "DBOS.patch-"
```

`Patch`/`DeprecatePatch` form the recorded `function_name` as
`_DBOS_PATCH_PREFIX + patchName`, i.e. literally `"DBOS.patch-" + patchName`
(`workflow.go:4036`, `4100`) — the suffix is the caller-supplied patch name
verbatim, no additional encoding or hashing.

Plain steps, transaction steps, and `Go(...)` do **not** have a reserved name:
their `function_name` is either a caller-supplied `WithStepName(...)` string or
(default) the Go runtime's fully-qualified function name obtained via
`runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()` (`workflow.go:2469`,
`2585`). A port must reproduce *some* deterministic default name per step
identity, but it does not need to match Go's reflection-derived string
verbatim (function_name mismatch only matters for replay consistency within a
single language's workflow history, not cross-language).

Child-workflow steps use the invoked workflow's registered name
(`params.WorkflowName`, i.e. the same string passed to `RunWorkflow`) as their
`function_name`, not a `"DBOS.xxx"` reserved string (`workflow.go:1430`,
`1291`, `2790-2812`).

## 4. Replay path

There is **no persisted counter and no counter-reconstruction step**. Recovery
re-executes the workflow function from the top with a brand-new
`workflowState{stepID: -1, ...}` (`workflow.go:1511-1518` — this code path runs
identically for a fresh start, a resumed/recovered execution, and a dequeue;
`isRecovery`/`isDequeue` only affect status-row bookkeeping, not step-ID
initialization). As the function body re-executes, every `nextStepID()` call
reproduces the exact same sequence of integers as the original run, *provided*
the workflow code is deterministic.

For each step/child-workflow/etc., the allocated `(workflowID, stepID)` pair is
looked up in `operation_outputs` (`SysDB.CheckOperationExecution`,
`system_database.go:2851-2921`; `SysDB.CheckChildWorkflow`,
`system_database.go:2790-2813`):

- **No row found** → this is genuinely new work; execute it for real and then
  write the row (`RecordOperationResult` / `RecordChildWorkflow`).
- **Row found, `function_name` matches** → return the cached
  output/error without re-running any side effect (this is what makes replay
  safe for non-idempotent operations like emails or payments).
- **Row found, `function_name` does NOT match input's step name** → hard
  determinism-violation error, `ErrorCodeUnexpectedStep`
  (`models.NewUnexpectedStepError`, thrown from `system_database.go:2701`,
  `2783`, `2809`, `2908`). This is the *only* safeguard against ID drift: if a
  branch, an added/removed step, or an off-by-one in ID allocation causes step
  N to be a different named operation than what was recorded at that
  `function_id` on the original run, this error fires. **Critically, this
  check only catches a name mismatch — if two operations happen to share the
  same `function_name` (e.g. two calls to the same step function, or forgetting
  to bump the ID before a duplicate call), a misallocated ID silently returns
  the wrong cached result instead of erroring.** This is exactly the
  "off-by-one → silent misreplay" risk the task description warns about: get
  step-ID bookkeeping wrong in a way that lines up two same-named steps, and
  there is no error, just wrong data silently returned from cache.

## 5. Operations that do NOT consume an ID (despite looking like they should)

- `GetStepID(ctx)` / `GetWorkflowID(ctx)` — pure reads of `workflowState`
  fields, no DB interaction (`workflow.go:4154-4160`, `4145-4152`).
- `Send` when called **outside** a workflow — direct fire-and-forget DB write,
  bypasses all step machinery (`workflow.go:3011-3059`, the `else` branch at
  `3052-3057`).
- `GetEvent` when called **outside** a workflow — same reasoning
  (`workflow.go:3308-3395`, `isInWorkflow == false` branch).
- `GetResult` when called **outside** a workflow — `checkGetResultExecution`
  and `processOutcome` both check `isWithinWorkflow` and skip all
  checkpoint/step-ID logic entirely if false (`workflow.go:199-204`,
  `279-333`).
- `Enqueue(...)` (the standalone client call) when called **outside** a
  workflow — direct transaction, no `workflowState` (`workflow.go:1967-1983`).
- `forkWorkflow` when called **outside** a workflow — direct
  `sysdb.RetryWithResult`, no step wrapping (`workflow.go:4759-4764`).
- `Patch` / `DeprecatePatch` on the "not patched"/"already deprecated" path —
  the ID is only consumed conditionally; the common "old code" steady-state
  path for an already-patched-long-ago workflow consumes **zero** IDs per call
  once it has passed the patch point on a prior run (line 4046: `if patched
  && err == nil`).
- `GetStatus()` on a handle, when called **outside** a workflow —
  `isWithinWorkflow` false skips the `RunAsStep` wrapper entirely
  (`workflow.go:130-165`, the `else` branch calls `ListWorkflows` directly).
- Almost every `DBOS.xxx` administrative call in the reserved-name list above
  (`DBOS.retrieveWorkflow`, `DBOS.cancelWorkflow`, `DBOS.listWorkflows`, etc.)
  is wrapped in `RunAsStep`/`runAsTxn`/`WithStepName` **only when invoked from
  inside a workflow**; the exported top-level functions (`RetrieveWorkflow`,
  `CancelWorkflow`, ...) used from ordinary client code consume no ID at all,
  since there is no `workflowState` in context.

## 6. Child workflows

- **`child_workflow_id` write**: recorded in the *parent's* `operation_outputs`
  row via `RecordChildWorkflow` (`system_database.go:2745-2788`), a
  `child_workflow_id` column alongside `function_id`/`function_name`, keyed on
  `(workflow_uuid = parentID, function_id = parentStepID)`. This is the same
  table/row shape used by plain steps; `child_workflow_id` is just an extra
  populated column, not a separate table.
- **Deriving the child's own workflow ID**: deterministic *unless the caller
  supplies an explicit `WorkflowID`*. When no ID is given:
  ```go
  // workflow.go:1258-1268
  if params.WorkflowID == "" {
      if isChildWorkflow {
          stepID := parentWorkflowState.stepID   // already incremented at line 1243
          workflowID = fmt.Sprintf("%s-%d", parentWorkflowState.workflowID, stepID)
      } else {
          workflowID = uuid.New().String()
      }
  }
  ```
  So the default child ID is literally `"<parentWorkflowID>-<parentStepID>"`,
  where `parentStepID` is the ID *just consumed* by the unconditional
  `parentWorkflowState.nextStepID()` at `workflow.go:1243`. This is fully
  deterministic given (parent ID, parent step counter at that point in
  replay), which is exactly why the ID must be consumed *before* any check or
  early return — a replay that consumed it later or conditionally would derive
  a different child ID and break the recorded parent→child mapping.
- **Replay when the child already exists**: before doing anything else for a
  child, `RunWorkflow` calls `CheckChildWorkflow(parentID, parentStepID,
  workflowName)` (`workflow.go:1276-1293`). If a row is already there:
  - matching `function_name` (the workflow name) → return a
    `workflowPollingHandle` pointing at the already-recorded
    `child_workflow_id` immediately; the child is *not* re-started, re-inserted,
    or re-run — the parent just resumes polling/awaiting it
    (`workflow.go:1290-1293`).
  - mismatched `function_name` (a different child workflow at that step) →
    `ErrorCodeUnexpectedStep`, logged as `"non-deterministic child workflow
    invocation"` (`workflow.go:1283-1286`, `system_database.go:2805-2812`).

## 7. Worked examples

Notation: `#N: <function_name>` for each allocated ID, in call order. "(gap)"
marks an ID that was reserved (counter advanced) but never got an
`operation_outputs` row.

### Example A — straight-line workflow, no branches

```go
func LinearWorkflow(ctx dbos.Context, in Input) (Output, error) {
    a, _ := dbos.RunAsStep(ctx, stepA)                 // #0
    dbos.Sleep(ctx, 5*time.Second)                      // #1
    handle, _ := dbos.RunWorkflow(ctx, ChildWF, in)     // #2 (parent-side ID for starting child)
    result, _ := handle.GetResult()                     // #3 (allocated AFTER the wait completes)
    b, _ := dbos.RunAsStep(ctx, stepB)                  // #4
    return Output{a, result, b}, nil
}
```

| ID | function_name | notes |
|---|---|---|
| 0 | (Go func name of `stepA`, or `WithStepName` override) | plain step |
| 1 | `DBOS.sleep` | |
| 2 | `ChildWF`'s registered workflow name | child workflow ID = `"<thisWorkflowID>-2"` |
| 3 | `DBOS.getResult` | allocated only once the child's result is actually available |
| 4 | (Go func name of `stepB`) | plain step |

Nothing to break here — this is the baseline every other example is compared
against.

### Example B — `Recv` with a timeout, message not yet pending

```go
func ReceiverWorkflow(ctx dbos.Context) (string, error) {
    dbos.RunAsStep(ctx, setup)                          // #0
    msg, err := dbos.Recv[string](ctx, "topic", 10*time.Second) // #1 (recv), #2 (sleep)
    dbos.RunAsStep(ctx, handleMsg)                       // #3
    return msg, err
}
```

If the message is *not* already pending when `Recv` starts listening, the
internal sleep at ID 2 is both reserved and written (`function_name =
"DBOS.sleep"`). If the message *is* already pending, ID 2 is still reserved
(counter advances from 1 to 2 unconditionally) but no row is ever written for
it — a real gap in `operation_outputs.function_id` for this workflow. Either
way `handleMsg` gets ID 3, so downstream numbering is stable regardless of
timing. **This is precisely why `Recv` must reserve both IDs up front rather
than lazily allocating the sleep ID only if a wait happens**: if a port instead
did `stepID := next(); ...; if needsWait { sleepID := next() }`, then on an
original run where the message was pending (no sleep, `handleMsg` gets ID 2)
versus a replay where — due to timing differences on a *different* run of the
same code — the message is *not* yet pending (sleep now takes ID 2, pushing
`handleMsg` to ID 3), the two runs would disagree about what's at ID 2. That
specific race can't happen in the real Go implementation (ID 2 is reserved
before the `Pending` check runs at all), but it is exactly the class of bug an
Elixir port must not reintroduce by allocating the sleep ID lazily.

### Example C — branch that allocates IDs unevenly (the dangerous one)

```go
func BranchingWorkflow(ctx dbos.Context, useNewPath bool) (string, error) {
    dbos.RunAsStep(ctx, stepA)                          // #0

    if useNewPath {
        dbos.RunAsStep(ctx, stepB)                       // #1 (only on this branch)
    }

    dbos.RunAsStep(ctx, stepC)                           // #1 or #2 depending on branch
    return "done", nil
}
```

This is a **user bug**, not a framework bug, and the framework's only defense
is the `function_name` mismatch check (§4). Concretely:

1. Original run executes with `useNewPath = true`: `stepA`→0, `stepB`→1,
   `stepC`→2. `operation_outputs` rows: `(0, stepA)`, `(1, stepB)`, `(2,
   stepC)`.
2. Workflow crashes after `stepB` completes but before `stepC` starts. Say the
   *code is redeployed* with a bug/config change that makes `useNewPath` now
   evaluate to `false` for this same workflow ID on recovery (e.g. a bad
   `Patch` migration, or a non-deterministic condition depending on
   wall-clock/env state rather than durable input).
3. Replay executes: `stepA` → ID 0 (cache hit, `stepA` matches, returns cached
   result, no re-execution). Branch skipped this time. `stepC` now asks for ID
   **1**, not 2.
4. `CheckOperationExecution(workflowID, 1, "stepC")` looks up the row at
   `function_id = 1`, finds `function_name = "stepB"`. `"stepC" !=
   "stepB"` → `ErrorCodeUnexpectedStep`, workflow recovery fails loudly.
   **This is the good case** — the names differ, so DBOS catches it.
5. **What silently breaks**: if `stepB` and `stepC` happen to share the exact
   same `function_name` (e.g. both left at their default Go-reflected name
   because of a refactor that renamed a function but kept the same
   `WithStepName`, or both are anonymous closures that resolve to the same
   `runtime.FuncForPC` string in some Go versions, or a developer manually
   passed the same `WithStepName("process")` to two different steps), then at
   step 4 `CheckOperationExecution` returns the **cached output of the
   original `stepB` call** as if it were `stepC`'s result — no error, no
   log noise, just the wrong step's output silently fed into the workflow's
   continued logic. This is the misreplay this document exists to prevent: any
   port must guarantee steps get unique, code-position-derived names by
   default (mirroring Go's `runtime.FuncForPC` behavior) precisely so that two
   different call sites essentially never collide, and must never allow a
   workflow's control flow to depend on anything but durably-recorded, replayed
   inputs.

## Summary of the three claims to verify

| Claim | Verdict | Evidence |
|---|---|---|
| `recv` allocates two (recv + internal timeout sleep), both up front | **CONFIRMED** | `workflow.go:3093-3094`: `stepID := wfState.nextStepID(); sleepStepID := wfState.nextStepID()`, executed unconditionally before the replay-cache check at `3101` |
| `setEvent` allocates two | **WRONG** | `workflow.go:3269-3282`: `SetEvent` makes exactly one `runAsTxn(..., WithStepName("DBOS.setEvent"))` call, no sleep, no second ID. (It is `GetEvent`, not `SetEvent`, that allocates two — `workflow.go:3325-3326` — apparently the source of the doc's confusion.) |
| `Patch` allocates one conditionally, checking against `stepID + 1` | **CONFIRMED** | `workflow.go:4041`: peeks `StepID: wfState.stepID + 1` without consuming; `workflow.go:4046-4049`: `if patched && err == nil { wfState.nextStepID() }` — only consumes the ID when the patch check actually returned `patched == true` |
