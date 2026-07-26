# Whole-application determinism checking

Design note. Research into `boundary`'s Mix compiler, and a design for a tracer-based
determinism checker for `dbos`.

---

## 1. Recommendation

**Build `Mix.Tasks.Compile.Dbos`: a `Mix.Task.Compiler` that installs a compilation tracer,
accumulates a function-level call graph in ETS, persists it to a manifest, and reports
transitive determinism violations as `Mix.Task.Compiler.Diagnostic` warnings.**

The premise holds. A tracer sees resolved `{module, function, arity}` after alias, import and
macro expansion, across every file in the application. That is exactly the information the two
current checkers lack, and it is the information the problem needs.

| Layer | Verdict | Role after this change |
|---|---|---|
| `Dbos.Determinism` AST walk in the macros | **Keep** | Immediate, hard `CompileError` for violations written literally in a workflow/step body. Zero config, catches special forms. |
| `Credo.Check.Warning.DbosDeterminism` | **Delete** | Same-module reachability is a strict subset of whole-app reachability. |
| `Mix.Tasks.Compile.Dbos` (new) | **Add** | Whole-app transitive reachability. Warning by default. |

Three deviations from a pure copy of `boundary`:

1. **Function-level graph, not module-level.** `boundary` records `from_module -> to_module`.
   We need `{mod, fun, arity} -> {mod, fun, arity}`, because a workflow body is one function
   in a module full of ordinary ones.
2. **A denylist, not an allowlist.** `boundary` asks "is this edge permitted?". We ask "does
   any path from an entry point reach a banned node?". Forward BFS from the entry points, over
   a subgraph the cut rules keep small (§3.5).
3. **A second signal for special forms.** `receive` is not a call and emits no trace event.
   The `:on_module` trace event carries the module bytecode; scanning its abstract code finds
   `receive` with a real line number.

The one part that is genuinely hard is manifest staleness across a checker upgrade
(§6.3). Everything else is mechanical.

---

## 2. What `boundary` actually does

Read at `github.com/sasa1977/boundary` @ `master`, v0.10.4, `elixir: "~> 1.10"`.

### 2.1 Compiler hook

`Mix.Tasks.Compile.Boundary.run/1`:

```elixir
@recursive true

def run(argv) do
  CompilerState.start_link(Keyword.take(opts, [:force]))
  Mix.Task.Compiler.after_compiler(:elixir, &after_elixir_compiler/1)
  Mix.Task.Compiler.after_compiler(:app,    &after_app_compiler(&1, opts))

  tracers = Code.get_compiler_option(:tracers)
  Code.put_compiler_option(:tracers, [__MODULE__ | tracers])
  {:ok, []}
end
```

The shape to copy:

| Step | Why |
|---|---|
| Compiler listed **before** `Mix.compilers()` in `mix.exs` | It must install the tracer before `compile.elixir` runs. |
| `after_compiler(:elixir, ...)` | Removes the tracer whether compilation succeeded or failed. |
| `after_compiler(:app, ...)` | Runs the check only once the `.app` file exists, so `Application.spec(app, :modules)` is complete and correct. |
| `@recursive true` | Runs once per umbrella child. |

`after_app_compiler/2` guards on `{status, diagnostics} when status in [:ok, :noop]` — no
checking on a failed compile, to avoid false positives from a half-built app.

### 2.2 Tracer events consumed

```elixir
def trace({remote, meta, to_module, _name, _arity}, env)
    when remote in ~w/remote_function imported_function remote_macro imported_macro/a
def trace({local, _meta, _to_module, _name, _arity}, env)
    when local in ~w/local_function local_macro/a
def trace({:struct_expansion, meta, to_module, _keys}, env)
def trace({:alias_reference, meta, to_module}, env)
def trace({:on_module, _bytecode, _ignore}, env)
def trace(_event, _env), do: :ok
```

Per event it records:

```elixir
%{from_function: env.function,      # {name, arity} | nil
  to: to_module,
  mode: :compile | :runtime,        # nil env.function or a macro => :compile
  type: :call | :struct_expansion | :alias_reference,
  file: Path.relative_to_cwd(env.file),
  line: Keyword.get(meta, :line, env.line)}
```

Notes worth carrying over:

- `env.function` is `nil` inside a module body. `boundary` uses that to classify the reference
  as compile-time.
- `local_function` / `local_macro` are handled **only** to call `initialize_module(env.module)`
  — see §2.4. `boundary` throws the edge away; we will keep it.
- `env.module in [nil, to_module]` and `system_module?/1` are filtered out. `system_module?/1`
  is generated at compile time by unrolling `elixir`, `stdlib`, `kernel` module lists plus
  `:erlang.pre_loaded()` into `defp system_module?(unquote(module)), do: true` clauses.
  Non-`Elixir.`-prefixed modules are also dropped.
- The docs warn: *"Slow tracers will slow down compilation."* Tracers run synchronously in the
  compiler process, one per compiling file, concurrently.

`Code` tracer API: Elixir 1.10+. `{:on_module, bytecode, _ignore}`: Elixir 1.13+. This repo is
`elixir: "~> 1.19"`.

### 2.3 In-run state: three ETS tables owned by a GenServer

`Boundary.Mix.CompilerState` is a `GenServer` whose only job is to own named ETS tables. All
writes go straight to ETS from the compiler processes.

| Table | Type | Contents |
|---|---|---|
| `...References` | `duplicate_bag` | `{from_module, entry_map}` |
| `...Modules` | `duplicate_bag` | `{module, {meta_key, value}}` — `:protocol?`, `:boundary_def` |
| `...Seen` | `set` | `{module}` — modules touched in *this* run |

Table and GenServer names are suffixed with the app name so umbrellas do not collide.
`start_link/1` tolerates `{:error, {:already_started, pid}}`, because ElixirLS keeps the
process alive between compiles.

### 2.4 The invalidation trick

This is the core of the whole design:

```elixir
def initialize_module(module) do
  if :ets.insert_new(seen_table(), {module}) do
    :ets.delete(references_table(), module)
    :ets.delete(modules_table(), module)
  end
  :ok
end
```

The **first** trace event for a module in a given run atomically wipes that module's rows
loaded from the manifest. Subsequent events append. So after the run, each module's rows are
either freshly traced (it recompiled) or untouched from the manifest (it did not).

`initialize_module/1` is called even for events that are not recorded — that is the entire
reason `local_function` is handled at all. A module whose only change is a local call must
still clear its stale rows.

### 2.5 Persistence

`CompilerState.flush(app_modules)`, called from `after_app_compiler`:

1. Delete rows for any module not in `Application.spec(app, :modules)` — handles deleted
   source files.
2. For each module in `Seen` (i.e. recompiled), rewrite its rows through `compress_entries/1`:
   `drop_leading_aliases/1` (an `alias_reference` immediately followed by a call/struct
   expansion at the same file/line/target is redundant), then `dedup_entries/1` (one row per
   `{file, line, to, mode}`, calls beat alias references).
3. Clear `Seen`.
4. `if deleted_any? or recompiled_modules != []`, write both tables with `:ets.tab2file/2`.
   Nothing changed means no disk write at all.

Manifest paths come from `Boundary.Mix.manifest_path/1`:

```elixir
def manifest_path(name), do: Path.join(Mix.Project.manifest_path(Mix.Project.config()), "compile.#{name}")
```

So `_build/<env>/lib/<app>/.mix/compile.boundary_references`, `compile.boundary_modules`, and
`compile.boundary_view_v2`. Loading is `:ets.file2tab/1` in `init/1`, skipped when `--force` is
passed, falling back to a fresh table on any error.

The third manifest (the *view*: boundaries, exports, classification) uses
`:erlang.term_to_binary/1` and **is** versioned — `Boundary.Mix.View.refresh/2` matches
`%{version: unquote(Mix.Project.config()[:version])}` against the stored view and rebuilds from
scratch on mismatch. The version is the `boundary` package version, frozen into the code at its
own compile time.

**The references manifest is not versioned.** Only `--force` clears it. This is a real gap and
we should not copy it (§6.3).

### 2.6 Dependencies and external modules

Deps are compiled in their own Mix project run, so `boundary`'s tracer never sees inside them.
It gets boundary definitions out of a compiled dep from the BEAM:

```elixir
# Boundary.Definition.__before_compile__
Module.register_attribute(__MODULE__, Boundary, persist: true, accumulate: false)
Module.put_attribute(__MODULE__, Boundary, data)
Boundary.Mix.CompilerState.add_module_meta(__MODULE__, :boundary_def, data)
```

- **Persisted module attribute** → readable from any compiled dep via
  `Keyword.get(boundary.__info__(:attributes), Boundary)`, guarded by
  `:code.get_object_code(boundary) != :error`.
- **ETS meta** → the fast path for the app currently being compiled.

`Boundary.Mix.View.refresh/2` caches the classification of non-user apps (library deps),
because "we want to avoid loading modules of those apps on every compilation, since that's very
slow". User apps (the main app, in-umbrella deps, path deps) are dropped from the stored view
and reclassified each run.

Calls *into* deps are checked as edges (`:invalid_external_dep_call`), but the dep's own
internals are opaque.

### 2.7 Reporting

`Mix.Task.Compiler.Diagnostic` with `compiler_name: "boundary"`, `severity: :warning`,
`file: Path.relative_to_cwd(...)`, `position: line`. Returned as the second element of the
compiler result tuple so editors (ElixirLS) can place them, **and** printed manually via
`Mix.shell().info/1` with ANSI colour.

Severity is `:warning` by design: *"The compiler doesn't force you to immediately fix these
violations, which is a deliberate decision made to avoid disrupting the development flow."*
CI uses `--warnings-as-errors`, which `boundary` implements itself in `status/2`.

### 2.8 Cycle detection

`Boundary.Checker.cycles/1`, using stdlib `:digraph`:

```elixir
graph = :digraph.new([:cyclic])
Enum.each(all, &:digraph.add_vertex(graph, &1.name))
for boundary <- all, {dep, _type} <- boundary.deps, do: :digraph.add_edge(graph, boundary.name, dep)

:digraph.vertices(graph)
|> Stream.map(&:digraph.get_short_cycle(graph, &1))
|> Stream.reject(&(&1 == false))
|> Stream.uniq_by(&MapSet.new/1)
|> Enum.map(&{:cycle, &1})
```

`Boundary.Graph` is unrelated — it only renders Graphviz dot for `mix boundary.visualize`.

`Boundary.Checker.errors/2` chunks the reference list across `System.schedulers_online()` and
runs every check under `Task.async_stream/2`, "since there may be hundreds of thousands of
them".

### 2.9 Limitations and escape hatches

| Escape hatch | Scope |
|---|---|
| `dirty_xrefs: [Mod, ...]` | Calls from this boundary to those modules are unchecked. Unused entries are themselves reported (`:unused_dirty_xref`). |
| `check: [in: false, out: false]` | Whole boundary ignored. Top-level boundaries only. |
| `classify_to:` | Reassigns a mix task or protocol impl to another boundary. |

Known limits: no Erlang module support; not exercised on very large or umbrella projects
(README); dynamic dispatch and `apply/3` are simply invisible — there is no trace event for
them and no mitigation.

### 2.10 Is any of it reusable?

**No.** `Boundary.Mix`, `Boundary.Mix.CompilerState` and `Boundary.Mix.View` are all
`@moduledoc false` internals of the boundary application, and `Boundary.Mix.CompilerState` is
keyed and named around boundary's own tables. There is no general tracing or manifest API on
hex. The pattern is ~250 lines and we reimplement it.

---

## 3. Design: `Mix.Tasks.Compile.Dbos`

### 3.1 Pipeline

```
mix compile
  │
  ├─ Mix.Tasks.Compile.Dbos.run/1
  │    ├─ Dbos.Compiler.State.start_link/1        (GenServer owning ETS, loads manifests)
  │    ├─ after_compiler(:elixir, &untrace/1)
  │    ├─ after_compiler(:app,    &check/2)
  │    └─ Code.put_compiler_option(:tracers, [__MODULE__ | existing])
  │
  ├─ compile.elixir  ──── trace/2 per call site ───► ETS  calls / entries / receives / seen
  │
  ├─ compile.app     ──── .app file written
  │
  └─ check/2
       ├─ State.flush(app_modules)                (invalidate, compress, tab2file)
       ├─ Analysis.build/1                        (graph + entry points + violation sites)
       ├─ Analysis.reachable_from_entries/1     (forward BFS, shortest witness chains)
       └─ Diagnostic list ─► printed + returned
```

### 3.2 Entry points

**Mechanism: the existing reflection functions, extended with source position; mirrored into
ETS at `@before_compile` time.**

`Dbos.Macros.__before_compile__/1` already emits:

```elixir
def __dbos_workflows__, do: [{name, {module, body_fun, arity}}]
def __dbos_steps__,     do: [{step_name, {fun, arity}}]
```

`{module, body_fun, arity}` is *already* the exact call-graph node for a workflow body — the
generated `def __dbos_workflow_body_...__` that the tracer will attribute calls to. Nothing new
is needed to identify entry points; only the declaration's `file`/`line` must be added, for
reporting.

Changes to `lib/dbos/macros.ex`:

| Change | Reason |
|---|---|
| `__dbos_workflows__/0` entries gain `%{file:, line:}` | Report "workflow X, declared here". |
| `__dbos_steps__/0` entries gain `%{file:, line:, kind: :step \| :transaction}` | Steps are entry points too, under the weaker step ban list. |
| `@before_compile` calls `Dbos.Compiler.State.register_entries(module, entries)` | Fast path; the ETS row is what `flush/1` persists. |
| `Module.register_attribute(__MODULE__, Dbos.Entries, persist: true)` | Fallback for reading entries out of a *compiled dependency's* BEAM, exactly as `Boundary.Definition` does. |

The ETS write is guarded by `GenServer.whereis/1` returning a pid, so `use Dbos` still compiles
when the Mix compiler is not installed.

Why not a tracer event or a generated marker function: we already own the macro and already
generate reflection. Reusing it costs one field per entry, keeps a single source of truth, and
survives the compiler not being installed.

### 3.3 Call graph construction

Node: `{module, function, arity}`. Edge: caller node → callee node, with `file` + `line` of the
call site.

| Trace event | Edge recorded |
|---|---|
| `{:remote_function, meta, mod, fun, arity}` | `{env.module, env.function} -> {mod, fun, arity}` |
| `{:imported_function, meta, mod, fun, arity}` | same — this is how `Kernel.send/2`, `spawn/1`, `make_ref/0` resolve |
| `{:local_function, meta, fun, arity}` | `{env.module, env.function} -> {env.module, fun, arity}` |
| `{:remote_macro, ...}` / `{:imported_macro, ...}` | recorded, flagged `mode: :compile` |
| `{:on_module, bytecode, _}` | dispatched off-process; abstract-code scan for special forms (§3.4) |
| everything else | `initialize_module(env.module)` only |

Line attribution comes from `meta[:line]` alone. Inside a re-injected workflow body `env.line`
is the `defmodule` line, so `boundary`'s `Keyword.get(meta, :line, env.line)` fallback would
place every such diagnostic at line 1 (§7, assumption 2). An event with no `:line` in `meta` is
recorded with `line: nil` and reported at the entry point's declared line.

Captures matter and come for free: `&helper/1` emits `local_function`, `&Mod.f/1` emits
`remote_function`. A function value passed as an argument is therefore an edge. This is a
deliberate over-approximation — we assume a captured function is called.

`env.function == nil` (module body) is bucketed under a pseudo-node
`{module, :__module_body__, 0}`, never reachable from an entry point.

**What we record, which `boundary` discards:**

- Local calls (we need intra-module edges).
- Erlang / stdlib targets — `:rand.uniform/1` is the single most important banned MFA and is
  neither `Elixir.`-prefixed nor absent from `system_module?/1`.

**What we discard, to keep the table small:** an edge is stored only if the callee module is in
the app under compilation **or** the callee MFA is in the rule table. Everything else — `Enum`,
`Map`, `Logger`, third-party deps that are not repos — is dropped at trace time.

#### Which bans move to the tracer

| Banned construct | Tracer sees it? | Resolved as |
|---|---|---|
| `:rand.*` | yes | `remote_function` to `:rand` |
| `DateTime.utc_now`, `NaiveDateTime.utc_now`, `Date.utc_today` | yes | `remote_function` |
| `System.system_time` / `os_time` / `monotonic_time` / `unique_integer` | yes | `remote_function` |
| `Process.sleep` | yes | `remote_function` |
| `Task.async` / `await` / `async_stream` / `start` / `start_link` | yes | `remote_function` |
| `spawn` / `spawn_link` / `spawn_monitor` | yes | `imported_function` from `Kernel`, or `remote_function` to `:erlang` |
| `send/2` | yes | `imported_function` `Kernel.send/2` |
| `make_ref/0` | yes | `Kernel.make_ref/0` |
| direct repo call | yes, **and now cross-module** | `remote_function` to the configured repo |
| `self()`, `node()` | yes (currently unenforced) | new capability, ship as `:hint` severity |
| ETS reads, `Application.get_env` | yes (currently unenforced) | new capability, opt-in |
| **`receive`** | **no — special form** | §3.4 |
| `after`/`try` timing constructs | no | §3.4 |

The tracer strictly dominates the AST walk on everything that is a call: it resolves aliases
(`alias Elixir.Task, as: T; T.async(...)` is invisible to the current AST matcher), resolves
imports, and sees through user macros that expand to banned calls.

Both `Kernel.spawn/1` and `:erlang.spawn/1` go in the rule table — the tracer fires before the
compiler inlines `Kernel` into `:erlang`, but recording both costs nothing.

The rule table itself moves to `Dbos.Determinism.Rules`, exposing two views over one list:
`match_ast/2` for the in-macro walk, `match_mfa/3` for the tracer. This preserves the current
invariant that "the two checkers always agree".

### 3.4 Special forms: the `:on_module` scan

`receive` compiles to no call and emits no trace event. The `:on_module` event carries the
module's bytecode, which contains the debug-info chunk:

```elixir
def trace({:on_module, bytecode, _}, env) do
  Dbos.Compiler.State.scan_async(env.module, bytecode)
  :ok
end
```

`scan_async/2` hands the binary to a pooled task (the docs are explicit that slow tracers slow
compilation) which runs `:beam_lib.chunks(bytecode, [:abstract_code])` and walks the Erlang
Abstract Format for `{:receive, line, _clauses}` and `{:receive, line, _clauses, _to, _after}`
nodes, attributing each to the enclosing `{:function, _, name, arity, _}`. Result rows:
`{module, {name, arity}, :receive, line}`.

Trade-offs:

- Needs `debug_info`, which is Elixir's default. If the chunk is missing, skip the module and
  emit one `:hint` diagnostic naming it. Fail open.
- Line numbers are real source lines.
- Post-expansion, so a `receive` produced by a user macro is caught. The reported line will be
  the macro's, which is the same behaviour `boundary` has for macro-generated references.

This is phase 3. Phases 1 and 2 ship without it and the in-macro AST walk continues to catch
literal `receive` in a workflow body, which is the overwhelmingly common case.

### 3.5 Reachability

**Entry points**

| Entry kind | Rule set | Descent stops at |
|---|---|---|
| workflow body | full workflow ban list | any other entry point (step, transaction, workflow body), `Dbos.*`, any module outside the app |
| step / transaction body | step ban list (process-handoff only) | same |

A workflow calling a step is correct and must not inherit the step's nondeterminism — the step
is where nondeterminism belongs. So every entry-point node is a **cut vertex**: reachability
never descends through one. This is the same cutoff the Credo check implements with
`entry_signatures`, lifted to the whole app.

`Dbos.*` runtime modules are cut vertices too — the generated wrapper calls
`Dbos.Runtime.run_step/3` and `Dbos.Telemetry.span_step/2`, which legitimately do everything on
the ban list.

**Algorithm: forward BFS from entry points.**

The cut rules below make the workflow-reachable subgraph a small slice of a large application:
descent stops at every other entry point, at `Dbos.*`, and at every dependency, so what remains
is the user's own helpers that workflows actually call.

1. Build a forward adjacency index `caller -> [callee]` from the ETS rows.
2. Seed a queue with every entry-point node, each carrying its own provenance.
3. BFS forwards, recording each node's predecessor-on-the-witness-path, never expanding through
   a cut vertex.
4. Evaluate the rule table only on nodes actually reached. Each match yields one diagnostic; the
   witness path is read off the recorded predecessors, giving the shortest chain.

Shortest chain is the right choice for the message: it is the one a human will verify fastest.

Searching backwards from banned call sites costs more on a real application. The seeds would be
every match app-wide — `DateTime.utc_now/0` in contexts and schemas, `Application.get_env/2`
across most modules, `send/2` and `Task.async/1` throughout ordinary GenServers — and the
callers-of relation fans out widest at shared utilities, so the walk covers most of the graph
before establishing that none of it reaches a workflow. Forward search evaluates the rules only
where they can apply.

Cost is `O(reachable subgraph)` for one multi-source pass. Per-entry-point witness chains re-walk
that same small region.

Mutual recursion makes the graph cyclic; the BFS visited set handles it. No `:digraph` needed —
unlike `boundary`, cycles are not themselves an error for us.

**Where descent stops**

| Callee | Treatment | Rationale |
|---|---|---|
| Module in the app being compiled | descend | We traced it. |
| Another entry point | cut | Steps are allowed to be nondeterministic. |
| `Dbos.*` | cut | Engine internals. |
| A dependency module | **opaque leaf** | Its own compile run had a different compiler list; we have no graph for it. Checked against the rule table, not descended into. |
| `Kernel`, `Enum`, stdlib | opaque leaf | Same. |

Deps are opaque. The alternative — offline abstract-code analysis of every dep BEAM — is
expensive, noisy, and would flag `Ecto.Adapters.SQL` for calling `System.monotonic_time`.
Explicitly out of scope.

**What stays undecidable**

| Construct | Behaviour | Direction of failure |
|---|---|---|
| `apply(mod, fun, args)` with a computed module | invisible | **unsound** — miss |
| A fun received as an argument and called | invisible at the callee | partially covered: the *capture* at the call site is an edge, so `Enum.map(xs, &bad/1)` is caught |
| Behaviour callbacks / `@impl` dispatch through a variable module | invisible | **unsound** — miss |
| Protocol dispatch | invisible | **unsound** — miss |
| A branch never taken at runtime | reported anyway | **unsafe-loud** — false positive |
| `Module.function_name()` where `Module` is an alias | caught | tracer resolves it |

The design is **not sound and not complete**. It is a large, cheap improvement in recall over
"no cross-module checking at all". Say so in the docs; do not claim a guarantee.

The dynamic-dispatch hole gets one mitigation: a `:hint` diagnostic when a workflow-reachable
function calls `apply/2,3` or `Kernel.apply/3` with a non-literal module — "this checker cannot
see through this call".

### 3.6 Incrementality

The requirement: editing a leaf helper must re-report a violation in a workflow whose own file
did not change.

This falls out of `boundary`'s structure, because the *analysis* is separate from the
*collection*:

```
compile run N+1, only helpers.ex changed
  │
  ├─ manifest loaded into ETS: rows for ALL modules from run N
  ├─ first trace event for MyApp.Helpers
  │     └─ initialize_module/1: :ets.delete(calls, MyApp.Helpers)   ← only this module's rows
  ├─ fresh rows for MyApp.Helpers appended
  ├─ MyApp.Workflows never recompiled → its run-N rows survive untouched
  │
  └─ check/2 runs the FULL analysis over the FULL merged table
        → the workflow→helper edge (from run N) still exists
        → the helper→:rand edge (from run N+1) is new
        → diagnostic emitted at helpers.ex, naming MyApp.Workflows
```

The analysis is re-run in full on every compile, over the union of persisted and fresh rows.
There is no incremental *analysis*, only incremental *collection*. For an app with a few
thousand modules the forward BFS is milliseconds, since it visits only the workflow-reachable subgraph.

Invalidation cases:

| Event | Handled by |
|---|---|
| Module recompiled | `initialize_module/1` on the first trace event of the run. |
| Module's source deleted | `flush/1` drops rows for modules absent from `Application.spec(app, :modules)`. |
| Module recompiled with no calls at all | `initialize_module/1` fires from *any* event, including `local_function` and `alias_reference`. This is why those events must be handled even when their edges are discarded. |
| Nothing changed | `flush/1` skips the `tab2file` write entirely. |
| Entry point removed from a module | Entry rows are keyed by module and cleared by the same `initialize_module/1`. |
| `mix compile --force` | `--force` reaches `State.start_link/1`, which starts with empty tables. |
| Checker rules changed (dbos upgraded) | §6.3 — the hard one. |

Table layout, all `duplicate_bag`, all keyed by **module** so one `:ets.delete/2` invalidates
everything about it:

| Table | Row |
|---|---|
| `Dbos.Calls.<app>` | `{from_module, %{from_fun:, to:, file:, line:, mode:}}` |
| `Dbos.Entries.<app>` | `{module, %{kind:, name:, mfa:, file:, line:}}` |
| `Dbos.SpecialForms.<app>` | `{module, %{fun_arity:, form: :receive, line:}}` |
| `Dbos.Seen.<app>` | `{module}` — in-memory only, cleared each run |

Compression at flush, following `dedup_entries/1`: one row per `{from_fun, to, file, line}`.

### 3.7 Reporting

Primary diagnostic is placed at the **violation site** — the helper's real file and line — so
the editor sends you where the fix is. The chain goes in the message.

```
warning: nondeterministic call reachable from a workflow body

  MyApp.Orders.process/1        (workflow "process_order")
    → MyApp.Helpers.fan_out/1   lib/my_app/helpers.ex:12
    → MyApp.Helpers.pick/1      lib/my_app/helpers.ex:31
    → Task.async_stream/3       lib/my_app/helpers.ex:34

  A Task does not inherit the workflow context, so steps called inside it silently
  skip checkpointing. Use a step, or a child workflow, for concurrency.

  lib/my_app/helpers.ex:34
```

```elixir
%Mix.Task.Compiler.Diagnostic{
  compiler_name: "dbos",
  file: "lib/my_app/helpers.ex",
  position: 34,
  severity: :warning,
  message: chain_message,
  details: %{entry: {MyApp.Orders, :process, 1}, chain: [...], rule: :task_async_stream}
}
```

- `details` carries the structured chain so `mix dbos.determinism --format json` and future
  tooling do not have to parse prose.
- Diagnostics are both returned from the compiler (for ElixirLS) and printed with
  `Mix.shell().info/1` (for the terminal), as `boundary` does.
- Deduplicate on `{entry, violation_site}` — one workflow reaching one bad call through two
  different paths is one diagnostic.
- Sort by `{file, position}`.

### 3.8 Escape hatches

```elixir
@dbos_deterministic "reads a compile-time-frozen config map"
def lookup_region(code), do: :persistent_term.get({:regions, code})
```

| Property | Choice |
|---|---|
| Granularity | **One function clause head**, `@doc`-style. |
| Mechanism | `@on_definition` hook installed by `use Dbos`, reading and then deleting the attribute. |
| Effect | The function becomes a cut vertex: not descended into, its own body not reported. |
| Value | A required string. A bare `true` invites thoughtless suppression. The string shows up in `mix dbos.determinism --show-suppressions`. |
| Availability | Requires `use Dbos` in that module. |

For a module the user does not own — a dep, or a helper module without `use Dbos` — a project
level list, mirroring `dirty_xrefs`:

```elixir
# mix.exs
dbos: [trusted: [MyApp.PureHelpers, {Some.Dep, :fetch_config, 1}]]
```

Two guardrails borrowed from `boundary`:

- Report unused entries (`:unused_trusted`), so the list does not rot. `boundary` does this for
  `dirty_xrefs`.
- Report a `@dbos_deterministic` on a function that is not workflow-reachable, for the same
  reason.

Whole-module and whole-app disables (`use Dbos, determinism_check: false`) exist as a last
resort but are not documented in the tutorial path.

### 3.9 False positives

Failure modes, most to least likely:

| Mode | Cause | Mitigation |
|---|---|---|
| Path-insensitivity | `if in_workflow?(), do: safe(), else: Task.async(...)` in a helper shared between workflow and non-workflow callers | `@dbos_deterministic`, or split the helper. This is the dominant FP. |
| Capture over-approximation | `&bad/1` captured but never invoked | `@dbos_deterministic` on `bad/1`. |
| Over-eager new rules | `self()`, `node()`, `Application.get_env`, ETS reads | Ship at `:hint` severity, or opt-in. Do not turn a documented-but-unenforced ban into a build-breaking one in the same release that introduces the compiler. |
| Macro-attributed lines | Violation inside a user macro reports the macro's line | Same limitation `boundary` has. Message includes the expanding module. |
| Bad chain | Shortest path is not the path the user thinks about | `--verbose` prints all chains for a violation. |

**Severity policy:**

| Violation | Severity | Why |
|---|---|---|
| Literal, in a workflow/step body | `CompileError` (unchanged) | Zero ambiguity, immediate, already shipped. Do not regress it to a warning. |
| Transitive, one or more hops away | `:warning` | Inference can be wrong. Do not break a user's build on an inference. |
| `apply/3` with a computed module in reachable code | `:hint` | Informational. |
| New rules (`self`, `node`, ETS, config) | `:hint` initially | Promote to `:warning` a release later if the noise is low. |

CI gets `mix compile --warnings-as-errors`, exactly as `boundary` recommends. The compiler
implements the flag itself.

A checker that cries wolf gets disabled. Default to warning; let projects opt into strictness.

---

## 4. Migration

### 4.1 In this repo

| Path | Action |
|---|---|
| `lib/credo/check/warning/dbos_determinism.ex` | delete |
| `.credo.exs` | remove the check; the file may be deletable entirely |
| `lib/dbos/determinism.ex` | split the rule table into `Dbos.Determinism.Rules` with `match_ast/2` and `match_mfa/3`; keep `check!/2` and `check_step!/2` on top of it |
| `lib/dbos/determinism.ex` — `maybe_warn_cross_module_calls/2` | **delete**. The `warn_cross_module_calls` heuristic ("public function in another module that is not a step") exists only because the macro could not see across modules. With a real call graph it is strictly worse than what the compiler can say. Keep the `use Dbos, warn_cross_module_calls:` option accepted-and-ignored for one release. |
| `lib/dbos/macros.ex` | add `file`/`line`/`kind` to the reflection entries; register the ETS mirror and the persisted attribute; install `@on_definition` for `@dbos_deterministic` |
| `lib/mix/tasks/compile/dbos.ex` | new |
| `lib/dbos/compiler/state.ex` | new |
| `lib/dbos/compiler/analysis.ex` | new |
| `docs/determinism.md` | rewrite the "Enforced at compile time vs. guidance only" section as a three-layer table |
| `mix.exs` | `compilers: [:dbos] ++ Mix.compilers()` in `:dev`/`:test` only, per `boundary`'s library advice |

### 4.2 The ten `sample_apps/`

Each is an independent Mix project with its own `mix.exs` and `deps`, so each opts in
separately:

```elixir
def project do
  [compilers: [:dbos] ++ Mix.compilers(), ...]
end
```

Sequence:

1. Ship the compiler with an empty rule set behind `dbos: [determinism_check: :off]`, add it to
   one sample app (`widget_store`), confirm the manifest round-trips and compile time is
   acceptable.
2. Enable the rules. Fix or annotate whatever it finds — the findings are the acceptance test
   for the whole design.
3. Roll to the other nine, one commit each.
4. Only then delete the Credo check.

Sample apps double as the integration test suite. A fixture project under `test/` that
compiles, edits one helper, recompiles, and asserts the diagnostic still fires is the single
most valuable test — it is the incrementality claim, and it is the claim most likely to be
quietly wrong. `boundary` has exactly this in `test/support/compiler_tester.ex`.

---

## 5. Phasing

| Phase | Scope | Output |
|---|---|---|
| **0. Spike** | Tracer + `IO.inspect`. Answer: does `env.function` correctly attribute calls inside a `defworkflow` body that was captured with `Macro.escape/1` and re-injected at `@before_compile`? Are line numbers preserved through that round trip? | Go / no-go. This is a ~2 hour question and it invalidates the design if the answer is no. |
| **1. Collection** | Compiler task, ETS state, manifests, `flush/1`, `initialize_module/1`. No checking. `mix dbos.graph` dumps the graph. | The infrastructure, testable on its own. |
| **2. Analysis** | Entry points from reflection, rule table split, forward BFS, diagnostics with chains. Warning severity. | The feature. |
| **3. Escape hatches** | `@dbos_deterministic`, project `trusted:` list, unused-suppression reporting. | Usable by real projects. |
| **4. Special forms** | `:on_module` abstract-code scan for `receive`. | Closes the last gap in the current ban list. |
| **5. Retire** | Delete the Credo check and the cross-module warning. Roll out to `sample_apps/`. | |

Phases 1–2 are the bulk. Estimate ~600 lines of new code plus tests, against ~250 lines
deleted.

---

### 5.1 Assumptions to verify in phase 0

Each is cheap to test and expensive to be wrong about.

1. `env.function` inside the re-injected `defworkflow` body is the generated body function.
2. `Macro.escape/1` preserves `:line` metadata through the `@before_compile` round trip.
3. `&local_fun/1` emits `{:local_function, ...}`; `&Mod.fun/1` emits `{:remote_function, ...}`.
4. `send/2`, `spawn/1`, `make_ref/0` emit `{:imported_function, meta, Kernel, ...}` at
   expansion time, before the compiler inlines them to `:erlang`.
5. `:rand.uniform/1` emits `{:remote_function, meta, :rand, :uniform, 1}` — Erlang targets are
   traced, not filtered by the compiler.
6. `:beam_lib.chunks(bytecode, [:abstract_code])` works on the binary handed to `:on_module`,
   with usable line numbers.

---

## 6. Risks

### 6.1 Compile-time cost — medium likelihood, medium impact

The tracer runs synchronously in every compiler process. Recording a local call is an
`:ets.insert/2` into a `write_concurrency` bag: cheap. The `:on_module` abstract-code scan is
not, which is why it goes off-process.

The real cost is `flush/1` plus the analysis on every single compile, including a no-op one.
`boundary` mitigates by skipping the disk write when nothing changed. We should additionally
skip the analysis when neither the graph nor the entry set changed this run.

Measure on the largest sample app before rolling out. Budget: under 200ms added to a warm
`mix compile`.

### 6.2 Path-insensitive false positives — high likelihood, high impact

The one that kills adoption. A single spurious warning in a user's build and the compiler comes
out of `mix.exs`.

Mitigations, in order of importance: warning severity by default; a good escape hatch that is
one line; a message that shows the chain so the user can immediately see *why* the checker
thinks this; conservative initial rules. Resist enabling `self()`/`node()`/ETS detection in the
first release.

### 6.3 Stale manifest after a dbos upgrade — medium likelihood, high impact

`boundary` never versions its references manifest; only `--force` clears it. Copying that means
a user who upgrades `dbos` keeps a graph recorded under the previous edge format and gets
either a crash or silence.

The bind: a Mix compiler cannot make `compile.elixir` recompile the world. If we discard our
manifest, Elixir will not re-trace unchanged files, and the graph is empty for everything that
did not change.

Options:

| Option | Assessment |
|---|---|
| Version the manifest, discard on mismatch | Silently under-reports until the next full recompile. Unacceptable. |
| Version, and on mismatch delete Elixir's own compile manifest to force a full recompile | Works, and is the pragmatic industry answer. Reaches into `compile.elixir` internals. |
| Version, and on mismatch emit one loud diagnostic: "dbos upgraded, run `mix compile --force`" | Honest, no reaching in, but relies on the user acting. |
| Make the on-disk format append-only and forward-compatible | Best long-term. Store rows as maps; unknown keys ignored; never change the meaning of a key. |

**Recommendation: format stability (option 4) as the primary strategy, plus option 3 as the
backstop** for the rare genuinely-breaking change. Revisit option 2 if it becomes routine.

### 6.4 Entry points in a compiled dependency — low likelihood, low impact

A library that ships workflows and is consumed by an app. Its entry points are readable from
the persisted attribute, but its call graph is not — it was compiled in its own project run.
We would report entry points we cannot check. Correct behaviour: check only entry points whose
module belongs to the app under compilation. Note it in the docs.

### 6.5 ElixirLS interaction — low likelihood, medium annoyance

`boundary` carries two explicit workarounds: `start_link/1` tolerating `:already_started`
because the process survives between compiles, and `Application.unload/1` + `load/1` before
reading `Application.spec/2` to defeat stale state. Both are load-bearing and both must be
copied. Getting this wrong produces phantom warnings for code the user already fixed — the most
corrosive possible failure mode.

### 6.6 Not the tool for the job? — considered and rejected

The one credible alternative is skipping the tracer and analysing BEAM abstract code for the
whole app after compilation: `:beam_lib.chunks/2` over every `.beam` in `_build`, extracting
both the call graph and the special forms in one pass. It needs no tracer, no ETS, no manifest
merge, and it finds `receive` for free.

It loses on three counts:

1. **Incrementality is worse, not better.** You would rebuild the graph from every BEAM on
   every compile, or hand-roll mtime tracking that duplicates what the tracer gets for free
   from `initialize_module/1`.
2. **Post-expansion only.** Everything is seen after macro expansion, so every diagnostic in a
   Phoenix or Ecto-flavoured module risks pointing into generated code.
3. **No compile-time integration.** Diagnostics arrive detached from the compile, so ElixirLS
   does not place them and `--warnings-as-errors` does not apply.

The tracer wins. The abstract-code technique survives in the design, scoped to the one job it
is uniquely good at: `receive` detection, driven off `:on_module` so it stays incremental
(§3.4).

---

## 7. Phase 0 results

**The gate passes. All six assumptions hold.** Verdict summary:

| # | Assumption | Verdict |
|---|---|---|
| 1 | `env.function` in a re-injected workflow body is the generated body function | **yes** |
| 2 | `Macro.escape/1` preserves `:line` through the `@before_compile` round trip | **yes** — and `:column` too |
| 3 | `&local/1` → `local_function`; `&Mod.f/1` → `remote_function` | **yes** |
| 4 | `send/2`, `spawn/1`, `make_ref/0` → `{:imported_function, meta, Kernel, ...}` | **yes** |
| 5 | `:rand.uniform/1` → `{:remote_function, meta, :rand, :uniform, 1}` | **yes** |
| 6 | `:beam_lib.chunks(bytecode, [:abstract_code])` on the `:on_module` binary | **yes**, real source lines |

### 7.1 Method

Throwaway two-project spike, Elixir 1.19.3 / OTP 28. `tracer` holds `SpikeTracer` and a
`Mix.Task.Compiler` that pushes it onto `Code.put_compiler_option(:tracers, ...)`; `spike_app`
depends on it plus a path dep on this repo, sets `compilers: [:spike] ++ Mix.compilers()`, and
contains two fixture modules. The tracer prints every event whose `env.module` starts with
`Fixture`, and runs `:beam_lib.chunks/2` on every `:on_module` bytecode.

`lib/fixture_workflows.ex`:

```elixir
1  defmodule Fixture.Workflows do
2    @moduledoc false
3    use Dbos, warn_cross_module_calls: false
4
5    defworkflow order(id), name: "order" do
6      a = local_helper(id)
7      b = Fixture.Helpers.pure(id)
8      c = Enum.map([1, 2, 3], &local_helper/1)
9      d = Enum.map([1, 2, 3], &Fixture.Helpers.pure/1)
10     e = step_one(id)
11     {a, b, c, d, e}
12   end
13
14   defstep step_one(id) do
15     Fixture.Helpers.nondeterministic(id)
16   end
17
18   def ordinary(id) do
19     local_helper(id)
20   end
21
22   defp local_helper(x), do: x + 1
23 end
```

`lib/fixture_helpers.ex` holds `pure/1` (line 4), `nondeterministic/1` (line 6, with
`:rand.uniform/1` on 7, `DateTime.utc_now/0` on 8, `make_ref/0` on 9, `send/2` on 10,
`spawn/1` on 11), `waits/0` (line 15, `receive`/`after` on 16), `bare_receive/0` (line 23,
`receive` on 24) and `captures/0` (line 29, three captures on 30–32).

### 7.2 Assumption 1 — the gate

Every call written inside the `defworkflow` body is attributed to
`{:order__dbos_workflow_body__, 1}`, which is exactly the node
`__dbos_workflows__/0` already publishes. Observed events, verbatim:

```
env.module=Fixture.Workflows env.function={:order__dbos_workflow_body__, 1} env.line=1
  event={:local_function, [line: 6, column: 9], :local_helper, 1}
env.module=Fixture.Workflows env.function={:order__dbos_workflow_body__, 1} env.line=1
  event={:remote_function, [line: 7, column: 25], Fixture.Helpers, :pure, 1}
env.module=Fixture.Workflows env.function={:order__dbos_workflow_body__, 1} env.line=1
  event={:remote_function, [line: 8, column: 14], Enum, :map, 2}
env.module=Fixture.Workflows env.function={:order__dbos_workflow_body__, 1} env.line=1
  event={:local_function, [line: 8, column: 30], :local_helper, 1}
env.module=Fixture.Workflows env.function={:order__dbos_workflow_body__, 1} env.line=1
  event={:remote_function, [no_parens: true, line: 9, column: 46], Fixture.Helpers, :pure, 1}
env.module=Fixture.Workflows env.function={:order__dbos_workflow_body__, 1} env.line=1
  event={:local_function, [line: 10, column: 9], :step_one, 1}
```

And `Fixture.Workflows.__dbos_workflows__/0` returns
`{"order", {Fixture.Workflows, :order__dbos_workflow_body__, 1}}`. The entry-point mechanism
of §3.2 is confirmed: the reflection entry and the tracer node are the same tuple.

Adjacent attributions, all correct:

- `defstep` body → `env.function={:step_one, 1}`, with
  `{:remote_function, [line: 15, column: 21], Fixture.Helpers, :nondeterministic, 1}`.
  The generated wrapper's own edges (`Dbos.Runtime.run_step/3`, `Dbos.Telemetry.span_step/2`,
  `Dbos.Runtime.in_workflow?/0`) land on the same node with `line: 14`, the `defstep` line.
- Generated dispatchers → `env.function={:order, 1}` and `{:order, 2}`, each with
  `{:remote_function, [line: 1], Dbos.Macros, :dispatch_workflow, 2}` / `..., 3}`.
- Module body → `env.function=nil`, as §3.3 assumes.
- Ordinary function → `env.function={:ordinary, 1}` with
  `{:local_function, [line: 19, column: 5], :local_helper, 1}`.

### 7.3 Assumption 2 — line metadata

`meta[:line]` matches the user's source exactly, and `:column` survives as well: `line: 6` for
the `local_helper(id)` on source line 6, `line: 7` for the `Fixture.Helpers.pure(id)` on line 7,
through to `line: 10` for `step_one(id)` on line 10. `Macro.escape/1` on the raw `do` block
preserves positions through storage in `@dbos_workflow_defs` and re-injection at
`@before_compile`.

**One correction to the design.** `env.line` for every workflow-body event is `1` — the
`defmodule` line, because the body is injected at `@before_compile`. Copying `boundary`'s
`line: Keyword.get(meta, :line, env.line)` would silently place a diagnostic at line 1 whenever
`meta` carries no `:line`. §3.3 now states that the line comes from `meta[:line]` alone.

Generated code that the user never wrote carries `line: 1` in `meta` too (the dispatcher edges
above). Nodes reached only through generated wrappers therefore need their reporting position
taken from the entry point's declared line, which §3.2 already adds to the reflection entries.

### 7.4 Assumption 3 — captures

Both forms fire, inside and outside a workflow body:

```
env.function={:captures, 0} event={:local_function, [line: 30, column: 14], :pure, 1}
env.function={:captures, 0}
  event={:remote_function, [no_parens: true, line: 31, column: 31], Fixture.Helpers, :pure, 1}
env.function={:captures, 0}
  event={:remote_function, [no_parens: true, line: 32, column: 21], :rand, :uniform, 1}
```

`no_parens: true` in `meta` distinguishes a capture from an ordinary call, should a future
refinement want to weaken the "a captured function is called" over-approximation of §3.3.

### 7.5 Assumption 4 — `Kernel` imports

All four resolve as `imported_function` from `Kernel` at expansion time:

```
env.function={:nondeterministic, 1} event={:imported_function, [line: 9,  column: 11], Kernel, :make_ref, 0}
env.function={:nondeterministic, 1} event={:imported_function, [line: 10, column: 5],  Kernel, :send, 2}
env.function={:nondeterministic, 1} event={:imported_function, [line: 10, column: 10], Kernel, :self, 0}
env.function={:nondeterministic, 1} event={:imported_function, [line: 11, column: 11], Kernel, :spawn, 1}
```

The rule table keys on `Kernel`. Keeping the `:erlang` aliases costs nothing and covers an
explicit `:erlang.spawn/1`.

### 7.6 Assumption 5 — Erlang targets

```
env.function={:nondeterministic, 1} event={:remote_function, [line: 7, column: 15], :rand, :uniform, 1}
env.function={:nondeterministic, 1} event={:remote_function, [line: 8, column: 18], DateTime, :utc_now, 0}
```

Erlang modules reach the tracer unfiltered. Compiler-internal Erlang traffic arrives on the same
channel — `:erlang.orelse/2`, `:erlang."=:="/2`, `:elixir_def.store_definition/3`,
`:elixir_utils.noop/0`, `Module.compile_definition_attributes/6` — so the §3.3 storage filter
("callee module is in the app, or the callee MFA is in the rule table") is load-bearing for
table size, not merely an optimisation.

### 7.7 Assumption 6 — `:on_module` abstract code

`:beam_lib.chunks(bytecode, [:abstract_code])` returns
`{:ok, {Module, [abstract_code: {:raw_abstract_v1, forms}]}}` on the binary handed to the trace
callback. Walking the forms for `{:receive, anno, clauses}` and
`{:receive, anno, clauses, timeout, after_body}`, attributed to the enclosing
`{:function, _, name, arity, _}`:

```
ON_MODULE module=Fixture.Helpers bytes=4056
BEAMLIB ok module=Fixture.Helpers forms=11
RECEIVE module=Fixture.Helpers fun=bare_receive/0 line=24
RECEIVE module=Fixture.Helpers fun=waits/0 line=16
```

Source line 24 is `bare_receive/0`'s `receive`; line 16 is `waits/0`'s `receive`/`after`. Both
shapes are found and both lines are real. `env.function` is `nil` on an `:on_module` event, and
`env.module` carries the module — the `{module, {name, arity}, :receive, line}` row of §3.4 is
constructible from the scan alone.

### 7.8 Bearing on §3.5

The spike is silent on forward-versus-reverse BFS; it collects edges and does no reachability.
It does confirm the two structural facts the forward walk depends on: the entry-point seed is a
real traced node (§7.2), and step bodies are traced under their own public `{name, arity}`,
which `__dbos_steps__/0` publishes — so a step is identifiable as a cut vertex from reflection
alone. §3.5 stands as written.

### 7.9 Everything else the spike touched

The design is otherwise accurate as written. `local_function` arrives as
`{:local_function, meta, fun, arity}`, `alias_reference` as `{:alias_reference, meta, module}`,
macros as `{:imported_macro, meta, Kernel, :def, 2}` and
`{:remote_macro, meta, Dbos.Macros, :defworkflow, 3}`, matching §3.3. Compilation of the fixture
app under the tracer succeeded with no interference from the existing `Dbos.Determinism` AST
walk.
