# TODO

The work queue. Each entry is something we intend to do. Resolved items are deleted, not ticked.

## Re-poll a queue that filled its capacity

A runner claims at most its free capacity per pass and a completing workflow notifies nobody, so
throughput per engine is capped at `worker_concurrency / base_polling_interval` — ~32/s at the
defaults, ~230/s at a 50 ms interval. A claim that came back full should poll again straight away
rather than wait for the tick. See `docs/performance.md`.

## Resolve an outcome without reading every column

`Dbos.await/2` reads the whole `workflow_status` row twice per workflow, `inputs` and `output`
included, to learn a status and then an outcome. The first read needs `status` alone.

## Skip the deadline lookup for a workflow with no timeout

`resolve_workflow_deadline/2` reads `workflow_timeout_ms` once per workflow, though
`insert_workflow_status/3` already returned that column.
