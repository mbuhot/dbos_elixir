# Hacker News Research Agent

An agent that researches a topic by searching Hacker News, reading comment threads, and asking
an LLM to synthesize what it found — a durable port of DBOS's
[Hacker News research agent](https://docs.dbos.dev/python/examples/hacker-news-agent) sample.

## What this proves

A research agent makes many expensive, non-idempotent calls: a Hacker News search, a thread
read, an LLM completion. A crash halfway through a ten-step research run should not repeat the
nine completed calls.

**Every search, every thread read, and every LLM call is a step.** Recovery resumes at the first
uncompleted one — completed calls are never repeated, no matter how many times the process dies
mid-run. Each step logs `[step] <name>(...)` to standard output so a reader can watch, after a
crash and restart, that already-checkpointed lines do not print a second time while incomplete
ones do.

## The determinism lesson: loop count comes from step results, not the clock

`HackerNewsAgent.Research.research/2` loops over up to `max_iterations` rounds. Whether it does
another round is decided by `continue_research/4` — an LLM call, but a **checkpointed** one. On
replay, that step returns its recorded output instead of asking the LLM again, so the workflow
takes the exact same number of iterations it took originally, deterministically, every time.

Nothing about the loop reads `System.os_time`, a random number, or how much wall-clock time has
elapsed. If it did, a replay could compute a different iteration count than the original run,
and every step after the divergence would be checked against the wrong recorded position —
silently returning the wrong step's cached output (see `docs/determinism.md` in the engine).
`Enum.reduce_while/3` here is not just a loop construct; it is the mechanism that keeps the
iteration count itself a recorded, replayable value.

## Architecture

```
HackerNewsAgent.Research         defworkflow research/2 — the iterative research loop
├── search_stories/1             defstep — Algolia HN search
├── read_thread/1                defstep — Firebase Item API, one story's full thread
├── evaluate_findings/3          defstep — LLM: relevance + insights for this round
├── continue_research/4          defstep — LLM: another round, or stop?
├── next_query_step/2            defstep — LLM: this round's follow-up query
└── synthesize_report/2          defstep — LLM: final markdown report

HackerNewsAgent.HnClient.Algolia   Algolia search API + Firebase Item API, over req
HackerNewsAgent.ClaudeAgent        Anthropic Messages API, over req
```

`HackerNewsAgent.HnClient` and `HackerNewsAgent.Agent` are behaviours; which module backs each is
read from `Application.get_env/3` **inside the step body** (never inside the workflow body —
that would be a live read of mutable state from replayable code). Tests override both with
network-free stubs.

## Running it

Requires Postgres and an Anthropic API key.

```sh
export ANTHROPIC_API_KEY=sk-ant-...
export PGHOST=localhost PGUSER=postgres PGPASSWORD=postgres   # override as needed
mix deps.get
iex -S mix
```

```elixir
iex> {:ok, handle} = Dbos.start("hn_research", ["durable execution", 3], workflow_id: "demo-1")
iex> Dbos.await(handle)
```

## Killing it mid-run

While the `research` workflow is running, find the BEAM and kill it hard:

```sh
$ pgrep -f "iex -S mix"
$ kill -9 <pid>
```

Restart with `iex -S mix`. `Dbos.Recovery` re-dispatches the `PENDING` workflow before you type a
command. Watch the `[step] ...` log lines: every step whose line printed before the kill does
**not** print again — its checkpointed output was replayed instead. The step that was in flight
when you killed it reruns in full, and research picks up from there.

```elixir
iex> Dbos.status("demo-1")
iex> Dbos.result("demo-1")
```

## Testing

`test/hacker_news_agent/research_test.exs` swaps in `HackerNewsAgent.StubAgent` and
`HackerNewsAgent.StubHnClient` (both configured via `Application.put_env/3`, no network) and:

- runs the workflow end to end and asserts on the synthesized report;
- starts it, waits for the first step to checkpoint, kills the workflow process, calls
  `Dbos.Recovery.recover_pending/1`, and asserts the completed search was never called twice —
  the stub bumps a `:persistent_term` counter per real invocation, which only a genuine re-run of
  the step body can increment.

```sh
mix deps.get
mix test
```
