# TODO

The work queue. Each entry is something we intend to do. Resolved items are deleted, not ticked.

## Saga compensation

`compensate:` on a step, recorded onto the step's checkpoint row, unwound in reverse by a separate
durable workflow. Design and rationale in `notes/saga-compensation-design.md`.

**Phase 1 is done**: extension migration 4 adds `operation_outputs.ex_compensation`, `compensate:`
on `defstep`/`deftransaction` expands to a closure the runtime calls only on the branch that
checkpoints a success, and the `@before_compile` check rejects a target that is not a step of
matching arity in the same module.

**Phase 2 is done**: `Dbos.Compensation` is registered on every engine under `"DBOS.compensate"`,
with no declared version. It takes a target workflow id, walks that one history newest-first over
the rows carrying a compensation, and runs each undo by calling the step it names — so each undo
checkpoints itself. Nothing is caught; `[:dbos, :compensation, :stuck]` carries the step id to
`Dbos.fork/3` from. `Dbos.unwind/2` starts one at the deterministic `"<id>-compensate"`.

**Phase 3 is done**: `SystemDb.update_workflow_outcome/3` enqueues the unwind onto the internal
queue in the same transaction as an `:error` status, guarded by whether the history holds a
compensation and by the row not being an unwind itself. `Dbos.abort/1` raises `Dbos.AbortError` to
reach that path deliberately, from a workflow body or a step.

**Phase 4 is done**: `:cancelling` is a non-terminal status. `Dbos.cancel/2` puts a running
workflow with compensable history there instead of `CANCELLED`; its next checkpoint check raises
`Dbos.WorkflowCancellingError`, and the process commits `CANCELLED` with the unwind. A durable wait
breaks out of `:cancelling` as it does out of `:cancelled`, so a blocked workflow stops at once
rather than waiting out its timeout. The lease sweep finishes a `CANCELLING` row whose executor's
lease has lapsed.

**Phase 5 is done**: the walk also returns rows naming a descendant
(`getResult`/`enqueue`/`forkWorkflow`), and reverses each by handing that workflow to its own
compensator. A descendant's state is read as a step of its own, so the branch taken for it is
checkpointed and reproduced on replay: still running is cancelled and awaited, `SUCCESS` with
something to reverse gets its own compensator awaited, and anything already unwinding or with
nothing to reverse is skipped. `unwindable?/2` now counts a spawning step too, so a workflow whose
only compensable effects are its children's still gets an unwind.

**Phase 6 is done**: `Dbos.send_message/4`, `Dbos.set_event/3` and `Dbos.write_stream/3` take
`opts[:compensate]`. They are functions, not macros, so the form is `&Module.fun/1` or an explicit
`{module, function, args}` with `:__checkpoint__` marking the slot, normalised and validated by
`Dbos.Compensation.record!/1`.

**Phase 7 is done**: `sample_apps/widget_store` is the saga demo. The checkout body describes only
the happy path; the reservation and the charge declare their undos, an unfilled order raises so its
failed step records none, and a declined payment calls `Dbos.abort/1`. `mix widget_store.demo
decline` shows the unwind. `guides/tutorials/compensation.md` documents the feature.

Saga compensation is complete.
