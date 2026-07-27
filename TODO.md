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

Remaining phases:

3. Triggers: automatic enqueue in the terminal transaction for the exception path, and
   `Dbos.abort/1`.
4. `CANCELLING` status, the process-side transition, and lease-sweep pickup.
5. Descendants named by `getResult`/`enqueue`/`forkWorkflow`: each resolved to a workflow id and
   handed its own compensator, awaited in reverse order.
6. `compensate:` on `send_message`/`set_event`/`write_stream`.
7. `widget_store` reworked as the saga demo, covering both the failure and the
   nothing-to-unwind branches.
