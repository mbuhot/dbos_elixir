# TODO

The work queue. Each entry is something we intend to do. Resolved items are deleted, not ticked.

## Saga compensation

`compensate:` on a step, recorded onto the step's checkpoint row, unwound in reverse by a separate
durable workflow. Design and rationale in `notes/saga-compensation-design.md`.

Phases:

1. Extension migration 4 (`operation_outputs.ex_compensation`), the `compensate:` option on
   `defstep`/`deftransaction`, the `@before_compile` contract check, and the record written at
   checkpoint time.
2. The compensation workflow: reverse walk, `DBOS.` filtering, undo-per-step, fail-fast, and
   `[:dbos, :compensation, :stuck]`.
3. Triggers: automatic enqueue in the terminal transaction for the exception path, and
   `Dbos.abort/1`.
4. `CANCELLING` status, the process-side transition, and lease-sweep pickup.
5. Recursion into `getResult`/`enqueue`/`forkWorkflow` descendants, awaited.
6. `compensate:` on `send_message`/`set_event`/`write_stream`.
7. `widget_store` reworked as the saga demo, covering both the failure and the
   nothing-to-unwind branches.
