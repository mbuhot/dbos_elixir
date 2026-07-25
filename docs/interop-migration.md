# Interop: the one-way door

This engine stores workflow and step values as encoded **Erlang terms**. Only an Elixir
(or Erlang) process can decode them. This document is for anyone who later wants a
non-Elixir service to read this database.

## What's portable, what isn't

| Portable across languages | Not portable |
|---|---|
| The schema — table and column names, types | `workflow_status.inputs` |
| The status values (`PENDING`, `SUCCESS`, `ERROR`, ...) | `workflow_status.output` |
| Step names and workflow names | `workflow_status.error` |
| The replay algorithm — step IDs, checkpoint lookup, retry semantics | `operation_outputs.output` / `.error` |
| | `notifications.message` |
| | `workflow_events.value` |
| | `streams.value` |

The shape of the system is language-neutral. The **payload bytes** inside the columns
above are not — they are produced by `:erlang.term_to_binary/1` and can only be
correctly reversed by `:erlang.binary_to_term/1`.

## The `serialization` column tells you what you're looking at

Every table that stores an encoded payload carries its own `serialization` column
recording the exact format used to write that row:

| Table | Serialized column(s) | `serialization` column |
|---|---|---|
| `workflow_status` | `inputs`, `output` | `workflow_status.serialization` |
| `operation_outputs` | `output` | `operation_outputs.serialization` |
| `notifications` | `message` | `notifications.serialization` |
| `workflow_events` | `value` | `workflow_events.serialization` |
| `workflow_events_history` | `value` | `workflow_events_history.serialization` |
| `streams` | `value` | `streams.serialization` |

This port writes exactly one format today: `"erl_etf"` — `:erlang.term_to_binary/1`,
then base64-encoded so the bytes are safe to store in a `TEXT` column. `error` columns
are stored as plain serialized error text and are not themselves subject to the
`serialization` column's format.

A reader must always check `serialization` before decoding a payload, never
assuming a fixed format — that column exists specifically so more than one format can
coexist in the same table over time.

## Migration path, if a non-Elixir service ever needs to read this database

1. **Add a portable JSON encoder alongside the ETF one.**
   New code path only; nothing about existing rows changes. Cost: moderate — every
   struct that can appear in `inputs`/`output`/`error`/messages/events/stream items needs
   a JSON encoding, and every one of those encodings has to be lossless enough to decode
   back into the same Elixir value (see "what cannot be migrated cheaply" below).

2. **Write new rows in the portable format; read both, dispatching on `serialization`.**
   Flip the default encoder for new writes to the portable format, tagging new rows
   `"portable_json"` (or whatever name is chosen). Every read path — replay, `GetResult`,
   listing, event/stream consumers — must branch on the row's `serialization` value and
   pick the matching decoder. Cost: moderate, and it must ship as a single atomic change,
   because from this point on both formats exist simultaneously in the same tables.

3. **Drain or backfill the in-flight workflows still holding ETF payloads.**
   Any workflow that started before step 2 has ETF-encoded rows in its history and will
   go on producing more ETF rows for any step whose output was already checkpointed
   before the switch (a step's format is fixed at the moment it's first recorded). Either
   let those workflows run to completion under the dual-read path, or write an explicit
   backfill job that decodes each ETF payload in Elixir and re-encodes it as portable
   JSON. **This is the hard step.** It requires an Elixir reader (nothing else can decode
   the old rows), a lossless re-encoding of every value that ever occurred in these
   tables historically, and a decision about workflows that are re-encoded while still
   in flight.

4. **Retire the ETF writer.**
   Once no row's `serialization` column reads `"erl_etf"` and no in-flight workflow can
   still produce one, delete the ETF encoder and the dual-read branch. Cheap — only code
   changes; no data is touched.

The expensive step is always 3. Steps 1, 2, and 4 are ordinary code changes; step 3 is a
data migration across every historical workflow this system has ever run.

## What cannot be migrated cheaply

JSON has no native representation for several things Erlang terms carry every day:

- **Atoms** — `:ok`, `:error`, `Elixir.MyModule` — JSON has no atom type; every atom
  needs an explicit encoding convention (e.g. a tagged string) and a matching decode
  rule, applied consistently everywhere atoms can appear.
- **Tuples** — JSON has arrays, which don't preserve fixed-arity heterogeneous tuples; a
  tuple-to-array encoding loses the "this is exactly 3 elements of these types" guarantee
  unless the decoder re-validates it.
- **Structs** — an Elixir struct is a tagged map (`__struct__` plus fields); a portable
  encoding has to carry that tag explicitly and the reading side needs some way to
  reconstruct (or at least name) the original struct, which a generic JSON reader in
  another language cannot do without a shared schema.
- **Charlists** — indistinguishable from a list of integers once decoded; round-tripping
  through JSON requires knowing in advance that a given list "is really" a charlist.
- **Improper lists** — no JSON equivalent at all.
- **Bitstrings that aren't whole bytes** — JSON strings are byte- or codepoint-oriented;
  an arbitrary bitstring needs its own encoding (e.g. base64 plus a bit-length field).

None of these are exotic edge cases in practice — atoms and structs in particular show
up constantly as step/workflow inputs and outputs in ordinary Elixir code, so a portable
encoder has to make a deliberate, documented choice for each one before it can be
trusted.

Separately: **in-flight workflows carry ETF-encoded step outputs today**. A non-Elixir
service reading this database before step 3 above completes can identify these rows
(via `serialization = "erl_etf"`) but is locked out of their content across the board: the
workflow's `inputs`, any step's `output`, queued messages, event values, stream items.

## When to adopt portable JSON up front

Skip the migration path entirely and design with portable JSON from day one if any of
these are true before you write your first workflow:

- A non-Elixir service is already planned to read or write this database, even if "not
  yet, but soon."
- Workflow inputs/outputs are expected to be simple data (maps, lists, strings, numbers)
  with no idiomatic Elixir structs, atoms, or tuples. The portable encoding
  then costs little.
- The team anticipates needing to inspect step outputs with generic tooling (a SQL client,
  a JSON-aware log pipeline, a support dashboard) that isn't Elixir-aware.
- Long-lived workflows (weeks/months in flight) are expected to be common, which makes
  the step-3 "drain the old format" migration cost much higher whenever it eventually
  happens.

If none of these apply, ETF is the simpler, cheaper choice for a single-language system,
with this document as the plan for later.
