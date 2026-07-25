# Document Pipeline

A `Dbos` sample: ingest a document, chunk it, embed each chunk, store the result — and survive
a crash partway through a large batch without redoing the expensive part.

## What it demonstrates

Embedding is the expensive, rate-limited step in a real ingestion pipeline. This sample makes
that cost visible and proves the engine protects it:

- One `Dbos` workflow per document (`ingest_document/2`).
- One durable step per chunk for embedding (`embed_chunk/3`) and one for storage
  (`store_chunk/4`) — each its own checkpoint.
- Concurrency across a batch comes from a `Dbos.Queue`, not from any in-process pooling.
- Killing a document's workflow mid-run and recovering it re-embeds **nothing** already
  checkpointed — the accompanying test counts real embed calls (via a `:telemetry` event) and
  asserts the count equals the number of chunks, not the number of attempts.

## Durability property under test

> Recovering a workflow after a crash replays it from its last checkpoint. A step whose output
> is already recorded returns that output instead of running again.

Embedding is deliberately the most expensive step here. Proving it runs exactly once per chunk,
even across a crash and recovery, is the entire point of the sample.

## Storage

Chunks live in a plain Postgres table (`chunks`), one row per `(document_id, chunk_index)`, with
`embedding` stored as a plain `{:array, :float}` column. No `pgvector` extension, no vector
column — this sample never runs a similarity search, so an ordinary array is enough. Reach for a
real vector column only when you need nearest-neighbour search over these rows.

## The embedder is swappable

`DocumentPipeline.Embedder` is a small behaviour (`embed/1`). Two implementations:

- `DocumentPipeline.Embedder.Stub` — deterministic, offline, no API key. Hashes the chunk text
  into a fixed-size float vector. Emits a `[:document_pipeline, :embed]` telemetry event per
  call, which is how the test counts real embeds.
- `DocumentPipeline.Embedder.OpenAI` — calls OpenAI's embeddings API via `req`. Requires
  `OPENAI_API_KEY`; raises a clear error if it's absent.

Select one with `config :document_pipeline, embedder: DocumentPipeline.Embedder.OpenAI` (default
is the stub).

## Running it

```sh
mix deps.get
createdb document_pipeline_dev   # or set DOCUMENT_PIPELINE_DATABASE_URL
mix ecto.migrate
```

```elixir
iex -S mix

iex> children = [
...>   DocumentPipeline.Repo,
...>   {Dbos.Supervisor,
...>    name: Dbos,
...>    db: {Dbos.DB.Ecto, DocumentPipeline.Repo},
...>    workflows: [DocumentPipeline.Pipeline],
...>    queues: [Dbos.Queue.new(DocumentPipeline.Pipeline.queue_name(), worker_concurrency: 4)],
...>    migrations: :create_if_absent}
...> ]
iex> Supervisor.start_link(children, strategy: :one_for_one)

iex> DocumentPipeline.Pipeline.ingest_batch([
...>   {"doc-1", {:text, "some long document text..."}},
...>   {"doc-2", {:url, "https://example.com/report.txt"}}
...> ])
```

## What to kill mid-run to see recovery work

Ingest a document with enough text to produce several chunks, watch `mix.exs`'s app log for
`embed_chunk` output, and `kill -9` the BEAM partway through. Restart:

```sh
iex -S mix
```

`Dbos.Recovery` replays the document's workflow from its last checkpoint. Chunks already
embedded and stored are not re-embedded — check `dbos.operation_outputs` for the workflow id
and count `embed_chunk/3` rows, or just re-run the batch and watch how few new
`[:document_pipeline, :embed]` telemetry events fire.

## Test

```sh
mix test
```

The test ingests a document, waits for the first chunk's embed-and-store to checkpoint, kills
the workflow's process, recovers it, and asserts every chunk is embedded exactly once — by
comparing the number of `[:document_pipeline, :embed]` telemetry events fired to the number of
`embed_chunk/3` checkpoints and stored rows.
