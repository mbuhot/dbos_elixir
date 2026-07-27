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

Remaining phases:

5. Descendants named by `getResult`/`enqueue`/`forkWorkflow`: each resolved to a workflow id and
   handed its own compensator, awaited in reverse order.
6. `compensate:` on `send_message`/`set_event`/`write_stream`.
7. `widget_store` reworked as the saga demo, covering both the failure and the
   nothing-to-unwind branches.
