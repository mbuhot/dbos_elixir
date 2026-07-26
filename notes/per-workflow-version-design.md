# Per-workflow version

Design note. Replaces the whole-application `application_version` gate on reclaim with a version
derived from each workflow's own replay contract.

## 1. Recommendation

**Stamp each `workflow_status` row with `ex_workflow_version`: a digest of that workflow's *replay
skeleton* — the step-id/step-name sequence `Dbos.Explain` already derives from the stored
`defworkflow` AST — with unresolvable calls contributing a code digest of the callee's module
closure. Match reclaim on `(name, version)` pairs the claiming executor advertises. Ship the
observability half first, on its own.**

Four decisions, most important first:

| # | Decision | Summary |
|---|---|---|
| 1 | Version the **replay contract** | A helper refactor that leaves the step sequence alone keeps its workflows claimable. This is what makes per-workflow versioning worth building. |
| 2 | Version equality is a **cheap approximation**; the sound relation is a **per-instance prefix check** | Compare the row's recorded `operation_outputs` names against the current code's skeleton prefix. Decidable, and it is what makes an escape hatch safe. |
| 3 | `Dbos.patch/1` regions **contribute nothing** to the digest | Patch's contract is "zero ids consumed for pre-patch instances", so eliding the guarded region leaves the digest of the unpatched code intact. The workflows patch exists to protect stay claimable, by construction. |
| 4 | **Observability ships first and alone** | A silently orphaned workflow is the bug. A visible orphaned workflow is a deploy decision. Phase 0 needs no schema change and no version at all. |

Compatibility *ranges* cannot be made sound (§7). Compatibility *sets* — a developer allowlist of
predecessor digests — can, at the cost of one array element in the reclaim predicate.

The determinism tracer (`notes/determinism-tracer-design.md`) and this feature should **share the
index with separate consumers** (§9). The design below completes without the tracer.

## 2. The bug, verified

`lib/dbos/system_db.ex:500-503` appends `AND application_version = $n` to both the reclaim
`UPDATE` and its companion scan whenever `config.application_version` is set.

```
executor-A (v1)  starts workflow W   → row: executor_id=A, PENDING, application_version=v1
deploy           executor-A dies, executor-B (v2) boots
LeaseSweep(B)    list_expired_lease_pending_executor_ids → [A]
Recovery.reclaim reclaim_pending_workflows(..., AND application_version = 'v2')  → 0 rows
                 list_reclaimable_pending_workflow_ids   → same filter → []
                 rescan? → false → pass ends, span_recovery emits an empty result
```

Why it is silent:

- A zero-row `UPDATE` is indistinguishable from "nothing to do".
- `list_reclaimable_pending_workflow_ids/3` carries the *same* filter, so the rescan loop cannot
  see the row either.
- `Recovery.recover_one/3` warns for an unregistered *name*. A mismatched version never reaches
  Elixir.

Blast radius: `Dbos.Version.compute/1` folds every registered workflow module's `module_info(:md5)`
into one digest, so a change to any workflow module moves the version for all of them.
`guides/tutorials/upgrading-workflows.md:65-95` offers "run two fleets until the old one drains" —
correct, expensive, and currently the only answer.

## 3. What actually breaks replay

Replay compares, at each durable operation, the recorded `function_name` at `function_id` against
the name the current code is about to run (`guides/tutorials/upgrading-workflows.md:9-27`).

| Change to workflow-reachable code | Breaks replay | Whole-app digest strands | Skeleton digest strands |
|---|---|---|---|
| Reorder two steps | yes | yes | yes |
| Rename a step | yes | yes | yes |
| Insert a step before an existing one | yes | yes | yes |
| Change a branch's id count | yes | yes | yes |
| Change a step's *body* only | no | yes | no |
| Refactor a helper, same calls | no | yes | no |
| Rename a private function | no | yes | no |
| Edit a *different* workflow | no | yes | no |
| Upgrade the `dbos` library | no | depends | no |
| Change a struct stored as input/output | yes, on decode | yes | **no — gap, §12** |

The two right-hand columns are the whole argument. A raw code digest is safe and over-strict — it
strands on every row. A skeleton digest tracks the failure mode itself.

## 4. Computing a workflow's version

### 4.1 Tiers

Resolved per workflow at engine boot, highest available tier wins.

| Tier | Source | Precision | Needs | Format |
|---|---|---|---|---|
| **A** | `defworkflow ..., version: "2024-11-orders"` | operator-declared | nothing | `d1:<string>` |
| **B** | Hybrid skeleton digest (§4.2) | semantic where the AST is determinate; module-level code identity elsewhere | stored AST + BEAM `imports` chunk | `w1:<16 hex>` |
| **C** | Graph-scoped skeleton digest | semantic, function-granular | the determinism tracer's call graph | `w2:<16 hex>` |
| **L** | `NULL` | legacy row, stamped before this feature | — | `NULL` |

Tier B is the recommended default and the one to build. Tier C is a drop-in upgrade (§9).

### 4.2 The hybrid skeleton digest

Walk the workflow body AST stored in `__dbos_workflows__/0` with the classifier
`Dbos.Explain.classify/4` already implements, emitting a token per node:

| AST node | Token |
|---|---|
| Resolved durable op (`defstep`, `deftransaction`, local `defworkflow`, `Dbos.*` primitive) | `step:<function_name>` — the exact string replay compares |
| `if` / `case` / `cond` | `branch:<kind>:[<arm skeleton>, ...]`, arms sorted for stability |
| `if Dbos.patch("x") do ... end` | **nothing** (§8) |
| `Dbos.deprecate_patch("x")` | **nothing** (§8) |
| A call the classifier cannot resolve | `opaque:<module>:<closure digest of that module>` |
| Anything else with no id effect | nothing |

Digest = `sha256(token stream)`, first 16 hex chars.

**Module closure digest** (the `opaque:` payload), computed once per module at boot, memoised in
`:persistent_term`. `:beam_lib.chunks(:code.which(M), [:imports])` returns the full static
remote-MFA list — verified working in this repo against `Dbos.Explain`:

```
closure(M) = {M} ∪ ⋃ { closure(N) | {N,_,_} ∈ imports(M), N ∈ app_modules, N ∉ excluded }
digest(M)  = sha256(sorted [module_info(:md5) | m ∈ closure(M)])
```

Excluded from the closure: `Dbos.*` (a library upgrade would otherwise strand the whole fleet),
modules outside the engine's own OTP apps, and stdlib/preloaded modules.

Configurable: `apps:` widens the closure, `trusted: [Mod]` narrows it. `trusted:` is the same list
the tracer design proposes (§3.8 there), so the two features share one configuration surface.

### 4.3 Honest limits of tier B

- A helper that calls a `defstep` lands in an `opaque:` token, so the version moves whenever *any*
  function in that helper's module closure changes. Tier C fixes it precisely; tier A overrides it.
- `module_info(:md5)` is build-artifact identity. Two independent builds of identical source may
  digest differently. Fleets normally ship one artifact; tier A covers those that do not.
- The digest reflects the post-`Macro.escape` AST — the same AST `mix dbos.explain` reports on.

### 4.4 Interaction with `application_version`

`application_version` keeps every current use: dequeue gating (`system_db.ex:1626-1660`), the
`application_versions` table, fleet identity, admin filters, Conductor parity. It names the
*deployment*; `ex_workflow_version` names the *workflow's replay contract*. Two questions, two
columns.

## 5. Where the version lives

| Option | Verdict |
|---|---|
| **New column, extension migration 2** | **Chosen.** |
| Reuse `application_version` with per-workflow contents | Rejected. Dequeue, `application_versions`, the admin `application_version` filter and Conductor all read it as fleet identity. Overloading it breaks four consumers to save one column. |
| Pack into `attributes` (JSONB, already present) | Rejected. No equality index, and the reclaim predicate runs inside `FOR UPDATE SKIP LOCKED` where a GIN lookup is the wrong shape. It is engine state. |

### 5.1 Owning the divergence

The schema already owns and versions its divergence — `priv/schema/dbos_schema.sql:677-693`
defines `extension_migrations`, whose comment states the intent: extension tables are tracked
under their own marker "so dbos_migrations.version — the base schema's own version — stays pinned
at 42". This change is extension migration 2:

```sql
-- extension migration 2: per-workflow version
ALTER TABLE "dbos".workflow_status ADD COLUMN ex_workflow_version TEXT;
ALTER TABLE "dbos".executor_leases  ADD COLUMN ex_capabilities JSONB;

CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_workflow_status_reclaim"
  ON "dbos".workflow_status ("executor_id", "name", "ex_workflow_version")
  WHERE "status" = 'PENDING' AND "queue_name" IS NULL;
```

The honest costs:

| Cost | Assessment |
|---|---|
| An `ALTER TABLE ... ADD COLUMN` on a large `workflow_status` | Nullable with no default: metadata-only in Postgres 11+, no rewrite. |
| A name collision if upstream migration 43 adds `workflow_version` | Real. The `ex_` prefix is the mitigation; it also reads as "this engine's extension" at a glance in `psql`. |
| Upstream tooling reading our rows | Unaffected. Extra columns are invisible to `SELECT`s naming columns, which is every query in both codebases. |
| A future upstream per-workflow version | Two columns for one concept for one release, then drop ours. Acceptable. |

`ex_capabilities` sits on `executor_leases`, already an extension table (`dbos_schema.sql:685`).
It carries the `(name, version)` pairs the executor accepts, which makes fleet-wide orphan
detection exact (§10).

### 5.2 Where the value is written

| Site | Behaviour |
|---|---|
| `SystemDb.insert_workflow_status/3` (`:~67`) | Stamp from the registry entry for `name`; `NULL` when the name is unregistered here. |
| `SystemDb.claim_one/3` (`:1687`) | Re-stamp alongside `application_version`, which this site already overwrites. A row enqueued by v1 and dequeued by v2 runs v2's code and must carry v2's version. |
| `SystemDb.fork_workflow/4` (`:~869`) | Copy verbatim; `opts[:workflow_version]` overrides, mirroring `:application_version`. |
| `Recovery.redispatch/4` | Unchanged — the row matched on version, so it already agrees. |

## 6. How reclaim uses it

Two arrays, one `unnest` join, subsuming the existing `name = ANY($4)` capability filter.

```sql
UPDATE dbos.workflow_status AS ws
   SET executor_id = $1
 WHERE workflow_uuid IN (
   SELECT ws2.workflow_uuid
     FROM dbos.workflow_status ws2
     JOIN unnest($4::text[], $5::text[]) AS reg(name, version)
       ON reg.name = ws2.name
      AND (ws2.ex_workflow_version = reg.version
           OR (ws2.ex_workflow_version IS NULL AND <legacy application_version clause>))
    WHERE ws2.executor_id = ANY($2) AND ws2.status = $3 AND ws2.queue_name IS NULL
    ORDER BY ws2.created_at ASC
    LIMIT $6
    FOR UPDATE SKIP LOCKED
 )
RETURNING ...
```

- `$4`/`$5` are parallel arrays of accepted `(name, version)` pairs. A name may appear several
  times — that is the compatibility set (§7).
- `NULL` marks a pre-feature row: fall back to today's exact `application_version` match, leaving
  existing rows' behaviour unchanged on upgrade.
- `list_reclaimable_pending_workflow_ids/3` takes the same predicate, so the rescan loop agrees
  with the claim. A deliberately **unfiltered** variant feeds §10.

### 6.1 Rollout switch — `workflow_versioning: :off | :observe | :enforce`

| Mode | Stamps the column | Reclaim predicate | Reports orphans |
|---|---|---|---|
| `:off` | no | `application_version` (today) | yes |
| `:observe` (default for one release) | yes | `application_version` (today) | yes, **and** reports rows the per-workflow predicate would have decided differently |
| `:enforce` | yes | per-workflow | yes |

`:observe` is how a deployment learns whether the digest is stable across its own builds before it
can strand anything. Cheap, and it de-risks §14's top entry.

## 7. Ranges: no. Sets: yes.

**A compatibility range cannot be made sound.** Structurally:

- The property required is "the current code's step-id/step-name sequence agrees with what this
  *instance* recorded, and keeps agreeing from its resume point onward".
- That is an **equivalence on skeletons** and carries no order. `v3` may be replay-compatible with
  `v1` while `v2` sits between them and is incompatible.
- A range `>= v1` asserts a property of code that did not exist when the range was written.

"Accept a newer version" reduces to "trust that the step sequence is unchanged", and only an
examination of the sequence discharges that trust.

**Three things that *are* sound**, in ascending cost:

| Relation | What discharges it | Where it runs |
|---|---|---|
| **Equality of skeleton digest** | The digest is computed from the sequence itself | Reclaim predicate, hot path |
| **Declared set** — `defworkflow ..., compatible_with: ["w1:abc123", "w1:def456"]` | A developer's attestation, reviewable in the diff, and checkable in CI against a stored digest history | Extra rows in `$4`/`$5` |
| **Per-instance prefix verification** | Read the row's `operation_outputs` names in `function_id` order; confirm they are a prefix of the current skeleton | On demand, per workflow (§11) |

The declared set is the honest form of "a range": explicit, finite, attributed to a human, visible
in code review. `mix dbos.explain` prints the digest so the value is copy-pasteable.

Prefix verification is strictly stronger and strictly slower. It belongs in the escape hatch (§11).

## 8. `Dbos.patch/1` — the sharpest tension, resolved

The tension: patching exists so a workflow's code can change while instances are in flight. A code
digest moves when a patch is added, so a naive digest strands exactly the instances patching
protects.

The resolution falls out of `patch/1`'s guarantee (`lib/dbos.ex:302-328`): a row recorded under a
different `function_name` returns `false` **without consuming an id**, so a pre-patch instance's
step-id sequence stays intact. Patched code therefore produces, for every pre-patch instance, the
same sequence the unpatched code produced — so the skeleton digest must be identical. Achieved by
eliding:

| Construct | Token contribution |
|---|---|
| `if Dbos.patch("x") do <body> end` | nothing — neither the marker nor `<body>` |
| `if Dbos.patch("x") do <a> else <b> end` | `<b>` only — the else-arm is what a pre-patch instance runs |
| `Dbos.deprecate_patch("x")` | nothing |

Consequences, stated plainly:

- **Adding a patch never moves the digest.** In-flight instances stay claimable across the deploy
  that introduces the patch. This is the behaviour patching was designed for, now extended to
  reclaim.
- **Post-patch instances are claimable by pre-patch executors too**, since both digest alike. A
  pre-patch executor replaying a post-patch instance meets the patch marker at an id where its own
  code has no `patch/1` call and raises `UnexpectedStepError` — loud, located, existing behaviour.
  Silence is the thing being fixed.
- **Removing a patch moves the digest**, because the elided region's `else`-arm changes shape.
  Retiring a patch is exactly the change that breaks marker-carrying instances, which is why
  `deprecate_patch/1` exists. Add the pre-retirement digest to `compatible_with:` only alongside a
  `deprecate_patch/1` call.
- **A pure code digest cannot do this**: the patched module's bytes move. A deployment that digests
  code and patches together must pin tier A across the patch. One more reason tier B is the default.

Upstream reaches the same conclusion by a blunter route (§12): `EnablePatching` pins
`applicationVersion` to the literal `"PATCHING_ENABLED"`, opting the whole application out of
version gating. The per-workflow analogue — `defworkflow ..., version: :unversioned`, stamping
`NULL` and accepting any row — is worth offering per workflow.

## 9. Sharing with the determinism tracer

**Verdict: share the index, separate the consumers. Build tier B first; tier C swaps one function.**

`notes/determinism-tracer-design.md` builds a function-level `{mod,fun,arity} → {mod,fun,arity}`
call graph in a `_build` manifest, with entry points already identified as the generated
`__dbos_workflow_body__` functions (§3.2, §3.3). That is the index tier C needs: it turns every
`opaque:<module>` token into a resolved sub-skeleton walk, function-granular.

| Aspect | Determinism checker | Per-workflow version |
|---|---|---|
| When | Compile time | Boot time (reads a compile-time artifact) |
| Output | `Mix.Task.Compiler.Diagnostic` warnings | A string per workflow |
| Failure mode | False positive → a spurious warning | False positive → a stranded workflow |
| Tolerance for unsoundness | High (warning severity) | Low in one direction, high in the other |
| Needs the graph | Yes, essentially | As an upgrade |

The differing failure tolerance is why they stay separate consumers. The checker is happy to
over-report; versioning must **over-strand** where it is uncertain and must never under-strand.

Mechanics of the shared index:

- `Mix.Tasks.Compile.Dbos` emits, alongside its diagnostics, a
  `priv/dbos_workflow_versions.terms` artifact: `{format, %{workflow_name => "w2:<digest>"}}`.
- `Dbos.WorkflowVersion.resolve/1` runs at `Dbos.Supervisor.init/1` and takes tier A (declared),
  else tier C (that artifact, if present and current), else tier B (in-process).

The artifact is a *file* because the digest depends on modules compiled after the workflow's own
module. A missing artifact degrades to tier B, which is the property that keeps this design alive
if the tracer is never built.

Shared-path risk, inherited from the tracer design's §6.3: a stale manifest yields a stale digest,
which strands or mis-claims. Mitigation: the artifact records the digest-format version and the
manifest's own hash; a mismatch drops to tier B with one loud log line.

## 10. Observability — ship this first

The current failure is silent. Everything below is independent of the version column, is worth
shipping on its own, and is Phase 0.

### 10.1 Local signal — zero schema change

`Recovery.reclaim/3` already calls `list_reclaimable_pending_workflow_ids/3`, which carries the
version filter. Add an unfiltered sibling and diff:

```
declined = unfiltered -- claimable
group by {name, version, reason} → one Logger.warning per group, with a count and one example id
```

Reasons are a closed set: `:name_not_registered | :version_mismatch | :locked_elsewhere`.

### 10.2 Telemetry

| Event | Measurements | Metadata |
|---|---|---|
| `[:dbos, :recovery, :declined]` | `%{count: n}` | `%{engine:, name:, row_version:, executor_version:, reason:}` |
| `[:dbos, :recovery, :orphaned]` | `%{count: n}` | `%{engine:, name:, row_version:}` — no live executor at all |

Emitted once per sweep per group, so a persistently orphaned population produces a steady gauge.
Rate-limit the log line (§10.1) to one per group per sweep; the telemetry
event carries the count.

### 10.3 Fleet-wide truth — needs `ex_capabilities`

One executor's `:version_mismatch` says nothing about the fleet. "Can *any* live executor claim
this row?" is one query, once each lease advertises its capabilities:

```sql
SELECT ws.name, ws.ex_workflow_version, count(*), min(ws.created_at)
  FROM dbos.workflow_status ws
 WHERE ws.status = 'PENDING' AND ws.queue_name IS NULL
   AND NOT EXISTS (
     SELECT 1 FROM dbos.executor_leases el,
                   jsonb_to_recordset(el.ex_capabilities) AS cap(name text, version text)
      WHERE el.lease_expires_epoch_ms > $1
        AND cap.name = ws.name
        AND (cap.version = ws.ex_workflow_version OR ws.ex_workflow_version IS NULL)
   )
 GROUP BY 1, 2;
```

`renew_lease/2` (`system_db.ex:~563`) already upserts a row per executor and gains one column
written from the registry. Surfaces:

Surfaces: `Dbos.Recovery.orphans/1`; `GET /dbos-orphans`, alongside `dbos-workflow-queues-metadata`
in `Dbos.AdminServer.Router`; `mix dbos.orphans`, printing the table plus the
`Dbos.Recovery.adopt/3` line that fixes each group; and the `:orphaned` telemetry gauge.

### 10.4 Decision: no new status

An orphaned row stays `PENDING`. An `ORPHANED` status would diverge from upstream's status enum,
break `ListWorkflows` filters both ways, and write a global fact — nobody can claim this — into a
row that a single deploy falsifies. The condition is a query, and is served as one.

## 11. The escape hatch and the migration path

Today: `Dbos.fork/3` with an `:application_version` override. Insufficient for the scenario that
matters.

| Property | `fork/3` | Needed |
|---|---|---|
| Preserves the workflow id | no — a fresh UUID | yes; ids are referenced by parents, callers, external systems |
| Preserves parent/child links | no | yes |
| Batchable over 200 rows | one call each | one call |
| Tells the operator whether replay is safe | no | yes |
| Marks the original `was_forked_from` | yes | undesirable for a pure re-stamp |

### 11.1 `Dbos.Recovery.adopt/2,3`

```elixir
Dbos.Recovery.adopt(Dbos, name: "process_order", from_version: "w1:9c3f...")
#=> {:ok, %{verified: 187, unsafe: 13, adopted: 187}}
```

One `UPDATE` re-stamping `ex_workflow_version` and `executor_id` to this engine's, then a normal
redispatch. Row selection by `:name`/`:from_version`/`:workflow_ids`; `:verify` (default `true`)
adopts only rows passing prefix verification (§11.2); `:force` adopts the rest with one warning
each; `:dry_run` reports verdicts and changes nothing.

### 11.2 Prefix verification — the sound check

For one workflow row:

```
recorded  = SELECT function_id, function_name FROM operation_outputs
              WHERE workflow_uuid = $1 ORDER BY function_id
current   = skeleton tokens of the registered code for ws.name
verdict   = :replayable       when recorded is a prefix of current, ids aligned
          | :diverged_at(id)  when a name disagrees
          | :indeterminate    when the skeleton hits an `opaque:` or a branch before the
                              recorded prefix ends
```

It proves **the past aligns**: every completed step sits where the current code expects it, under
the same name. Gaps:

| Gap | Consequence |
|---|---|
| Divergence *after* the resume point | `Dbos.UnexpectedStepError` at the first mismatch — loud, located, existing behaviour |
| Value-shape changes (a struct field added to a stored input/output/event) | Decode failure at the step that reads it — loud |
| Branch arms whose id counts differ downstream | Same as the first row |

That asymmetry is the point: verification converts "silently orphaned forever" into "runs, and
fails loudly at the exact step if the code really did change".

### 11.3 The 200-stranded-workflows runbook

```
1. mix dbos.orphans                       # what, how many, since when, which version
2. mix dbos.explain MyApp.Orders.process/1   # what the current code's sequence is
3. mix dbos.orphans --verify              # per-row verdicts: replayable / diverged / indeterminate
4a. all replayable  → mix dbos.adopt --name process_order --from-version w1:9c3f
4b. some diverged   → adopt the replayable set; fork the rest from their last good step
4c. indeterminate   → redeploy the old build, drain, then deploy forward (today's answer)
```

Step 3 exists in no form today, and it is what turns a production incident into a decision.

## 12. Upstream parity — what was found

Read in `reference/dbos-transact-golang/` and `notes/recovery.md`.
| Question | Finding |
|---|---|
| Does upstream version anything below the application? | **No.** `application_version` is one string per process (`dbos.go:126-142`), one column, one filter. |
| How is it computed by default? | SHA-256 of the whole executable file (`dbos.go:940-983`, `notes/recovery.md:57`) — coarser than our per-module digest. |
| Is a version mismatch on recovery observable? | **No.** `recoverPendingWorkflows` filters in `ListWorkflows` (`recovery.go:11-23`), so a filtered-out row is simply absent. Unknown *registry names* log at `Error` (`notes/recovery.md:45`); versions are silent. **We inherited this bug.** |
| Does Conductor work at a finer granularity? | **No.** Every Conductor path is whole-application: `WithFilterAppVersion` on list/recovery (`conductor.go:733,917,1190`), a per-app version list message (`conductor_protocol.go:861-887`), `GroupByApplicationVersion` on a metrics request (`:792`). |
| Does Conductor surface the stranded population? | Partly. `existPendingWorkflows` (`conductor.go:1180-1210`) answers "are there PENDING workflows for this `(executor_id, application_version)`" with `LIMIT 1` — a deploy gate confirming the intended flow is "keep the old fleet alive until this returns false". |
| How does upstream reconcile patching with version gating? | **By opting out.** `dbos.go:127-128`: `if dbosConfig.EnablePatching && ApplicationVersion == "" { ApplicationVersion = "PATCHING_ENABLED" }` — a literal constant, identical across builds, so the version filter degenerates to a tautology for the whole application. |
| Does our port have `EnablePatching`? | **No.** `Dbos.patch/1` is unconditionally available and `resolve_application_version/2` (`supervisor.ex:214`) has no patching branch, so we run patching *and* version gating together with none of upstream's escape. §8 closes that gap properly. |

For the record: per-workflow versioning is a **deliberate divergence from upstream**. Nothing
upstream works at this granularity. The observability half (§10) is a straight bug fix that
upstream needs too.

## 13. Phasing

| Phase | Scope | Schema | Ships value alone |
|---|---|---|---|
| **0** | Declined-reclaim logging + `[:dbos, :recovery, :declined]` + docs | none | **Yes — the highest ratio in the whole change** |
| **1** | Extension migration 2; `ex_capabilities` on leases; `Dbos.Recovery.orphans/1`; `GET /dbos-orphans`; `mix dbos.orphans` | ext 2 | Yes |
| **2** | `Dbos.WorkflowVersion` tiers A + B; stamping at insert/claim/fork; `workflow_versioning: :observe` | — | Diagnostic only |
| **3** | `:enforce` mode: the `unnest` reclaim predicate, `compatible_with:` sets | — | The feature |
| **4** | Prefix verification; `Dbos.Recovery.adopt/3`; `mix dbos.adopt` | — | Yes |
| **5** | Tier C from the tracer manifest, when it exists | — | Precision upgrade |

Phases 0 and 1 stand whatever happens to 2–5. Phase 4 stands even with 3 never enabled — prefix
verification applies to today's `application_version` strandings as they are.

Rough sizes: phase 0 ~60 lines; phase 1 ~200; phase 2 ~250; phase 3 ~80; phase 4 ~200.

## 14. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **Digest instability across builds** — two CI runs of identical source produce different `module_info(:md5)`, so a rolling fleet strands its own rows | medium | high | `:observe` mode measures it before it can bite; tier A is the answer for fleets that see it; the skeleton half of the digest is source-derived and immune |
| **A stranded population created by the fix itself** — `:enforce` with a wrong digest is the same bug at finer granularity | medium | high | `:observe` default for a full release; `mix dbos.adopt` exists before `:enforce` does (phase 4 before 3 is defensible) |
| **Patch elision is wrong in a case not enumerated** — e.g. `unless Dbos.patch(...)`, a patch inside a `case` arm, a patch behind a variable | medium | high | Enumerate the recognised forms; anything else raises at compile time via the same macro-level check `Dbos.Explain` uses. A `patch/1` call the digest cannot classify must **fail the build** |
| **Reclaim query regression** — the `unnest` join changes the plan under `FOR UPDATE SKIP LOCKED` | low | medium | The partial index in §5.1; `EXPLAIN` against a seeded table in the integration suite |
| **Tracer manifest staleness feeding tier C** | medium | medium | Format version + manifest hash in the artifact; degrade to tier B loudly |
| **Scope creep into the determinism checker** | high | medium | The shared surface is one file read at boot. Tier B must land and work with the tracer absent |

## 15. Open questions

- **`ENQUEUED` rows.** An `ENQUEUED` row holds no checkpoints, so dequeue's version gate is about
  *routing*. Leaning: leave dequeue on `application_version`, and say why in the guide.
- **Child workflows.** A child's own name resolves its own version. Needs a test that a parent at
  v1 and a child at v2 both stay claimable.
- **`compatible_with:` hygiene.** A CI check that fails when a digest moves with no
  `compatible_with:` entry needs a committed digest history file. Defer to phase 3.
- **Value-shape changes** (§3, last row) are undetected by every tier here. A digest over the
  structs a workflow serialises is a separate design.
