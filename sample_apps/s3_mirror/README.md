# S3 Mirror

Mirrors every object under a prefix from one bucket to another — a durable port of
[DBOS's S3 mirror example](https://docs.dbos.dev/python/examples/s3mirror).

## What it demonstrates

- **A workflow per object**, enqueued onto a `Dbos` queue with a concurrency limit
  (`worker_concurrency: 5`) and a rate limit (`20` starts/second).
- **Live progress without polling the source** — the orchestrator workflow writes each
  object's outcome to a `Dbos` stream as it finishes; a watcher reads that stream.
- **Resuming a killed mirror copies only what remains.** Two independent layers make that
  true:
  - `mirror_bucket` is itself a durable workflow. A crash mid-run leaves every already
    finished object's outcome checkpointed; replay resumes only the objects not yet reached.
  - Each `copy_object` workflow has a **deterministic id** derived from the destination and
    key (`S3Mirror.Workflows.workflow_id/3`). Re-enqueueing the same object — from a
    replayed `mirror_bucket`, or a completely fresh mirror run over the same destination —
    collapses onto the one row already there instead of copying twice.
- **Objects skipped as already done** — independent of `Dbos`'s own bookkeeping,
  `copy_object` checks the destination directly and skips a key that's already there, the
  same way a real S3 sync does.

## The object store, and which one runs by default

Object storage sits behind `S3Mirror.ObjectStore` (`list_keys/2`, `read/2`, `write/3`,
`exists?/2`):

| Implementation | Backing | Used when |
|---|---|---|
| `S3Mirror.ObjectStore.Local` | A local directory tree | **Default** — no cloud account needed. Every `iex` example and the test suite use this. |
| `S3Mirror.ObjectStore.S3` | A real S3 bucket, signed with a small hand-rolled SigV4 helper (`S3Mirror.AwsSigv4`) over `Req` | Only when you explicitly build a store with `S3Mirror.ObjectStore.S3.from_env!/1` and the `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` environment variables are set. |

No AWS SDK dependency: three verbs (`GET`, `PUT`, `ListObjectsV2`) over `Req` with a signed
`Authorization` header is a few dozen lines and covers everything this sample needs. A real
S3 client library would still be the right call for multipart upload, presigned URLs, or
bucket policies — none of which this sample touches.

## Running it

Needs a local Postgres reachable as the current OS user, no password:

```sh
createdb s3_mirror_dev
mix deps.get
iex -S mix
```

```elixir
source = %{root: "/tmp/s3_mirror_demo/source"}
dest = %{root: "/tmp/s3_mirror_demo/dest"}
File.mkdir_p!(source.root)
for i <- 1..20, do: File.write!(Path.join(source.root, "object-#{i}.txt"), "payload #{i}")

alias S3Mirror.ObjectStore.Local

{:ok, handle} =
  S3Mirror.Workflows.mirror_bucket(Local, source, Local, dest, "", workflow_id: "demo-mirror")

for chunk <- Dbos.read_stream("demo-mirror", "progress") do
  IO.inspect(chunk, label: "progress")
end

Dbos.await(handle)
```

## What to kill mid-run to see recovery work

Populate the source with enough objects that the copy takes a few seconds (a few hundred is
plenty), start the mirror, and:

- **Kill the whole node** (`kill -9` the `iex -S mix` process) while objects are still
  copying, then `iex -S mix` again. `Dbos.Recovery` redispatches the `PENDING`
  `mirror_bucket` and every `PENDING`/`ENQUEUED` `copy_object` on boot. Re-run the
  `read_stream`/`await` snippet above with the same `workflow_id` — the objects already
  copied are not touched again; only the remainder finishes.
- **From another `iex` node**, find and kill just a `copy_object` workflow's process
  (`Dbos.WorkflowSup.whereis(Dbos, S3Mirror.Workflows.workflow_id(Local, dest, "object-3.txt"))`)
  to see one object recover in isolation without disturbing the rest.

## Checking progress mid-run

```elixir
S3Mirror.Workflows.progress(Local, source, Local, dest, "")
# => [{"object-1.txt", :copied}, {"object-2.txt", :remaining}, ...]
```

This reads `Dbos` workflow status directly (no source or destination polling), so it works
while the mirror is still running.

## Tests

```sh
createdb s3_mirror_test   # once
mix test
```

`test/s3_mirror/mirror_test.exs` mirrors five objects between two temp directories, kills
every live workflow process mid-copy, triggers recovery, and asserts each object was copied
exactly once (one `copy_into_dest/4` checkpoint per object, byte-for-byte identical content).
A second test pre-populates the destination and asserts those objects are reported
`:skipped` rather than re-copied.
