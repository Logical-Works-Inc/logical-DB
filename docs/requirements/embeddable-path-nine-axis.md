> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Primary URLs:
> `../../evidence/SOURCES.md`.

# Embeddable path — enterprise scale requirements (nine-axis)

> Authority: `GOVERNANCE.md` (claims), `docs/adr/0003-sovereign-hyperscale-data-plane.md`
> (Cloud nine-axis contract), `docs/plans/2026-09-04-logical-db-first-class-finish.md`
> (R1–R14, AE1–AE5), `docs/API_BOUNDARY.md` (app-visible surface).
> This file encodes those axes as binding enterprise requirements for the
> **embeddable path** only. It does not create a second nine-axis contract and
> does not relax ADR-0003 or `~/.claude/agent-requirements.md` A01–A09.

> Status of every row is **pass**, **fail**, or **unknown**. A green compile is
> not a pass. An unrun measurement is unknown, never pass.

## 0. Displacement thesis (why switch)

The embeddable path is judged against the enterprise it must move, not against
other libraries. The incumbent is the **Gary class**: legacy OLTP (MySQL and
peers) installed by someone who left, whose schema invariants, triggers, and
replication topology no one can explain — less interpretability than an AI
reading its own logs, and unsafe to grant write authority to the agents already
doing ops.

Switching is justified only where logical-db is the answer to all three:

| # | Incumbent failure | logical-db obligation | Proof row |
|---|---|---|---|
| D1 | Cannot explain why a row is what it is | Every mutation carries `OperationId`, request digest, receipt, and causal frontier | EP-2, AE6 |
| D2 | Unsafe to grant an agent write authority | Scoped grants, exact-request confirmation, immutable receipts that re-enter evidence | EP-X4, ER-T02 |
| D3 | Cannot leave without a big-bang prayer | Dual-write + content digests + versioned export/restore, fail-closed on corrupt input | EP-1, AE3, AE5 |

**Comparators.** Raw table engines (`native_db`, `redb`, `sqlite`) own local
storage and lose D1–D3. CRDTs (`Automerge`) merge without authority or durable
sync. Cloud sync (`CloudKit`, `Realm`, `ElectricSQL`) takes custody. A
component that cannot own provable operational truth is not a rival for this
slot. **The answer is never “we are a nicer embedded DB.”**

**Buyers (Director, 2026-09-22 — all of them).** Gary-class displacement (B1),
agent-ops platforms (B2), sovereign/regulated shops (B3), local-first app ISVs
(B4), and the enterprise that cannot set up PowerBI and will not staff a DBA,
alongside the real developers who still have to ship (B5).

**Tesler's law is binding on every decision.** Essential complexity of
distributed data — retries, recovery, residency, provenance, grants — is
absorbed by `logical-db`, never left to the app, the operator, or tribal
knowledge. A choice that moves complexity onto B5 is the wrong choice even when
it is the smaller diff. Concretely: one dependency (`logical-db`, with
`logical_db::cache`), typed refusals instead of clamps, receipts instead of
promises, defaults that fail closed with a recovery transition.

**Scale and impact, not code.** The hard work is displacement at A02 scale and
A01 class evidence. Thin local benches cannot pass the thesis.

### Beat-eval rule (where we compete)

Adopt the `native_db` criterion harness class (insert / get / range / delete,
secondary-key counts, batched vs single transaction, `criterion-table`
reporting) as the **local store** eval so numbers are comparable. Beat evals
**only where our semantics are the game**:

| We must beat | We match or disclose |
|---|---|
| Exact retry under replay (duplicate cost = receipt, no second write) | Raw insert / get / delete without sync or receipts |
| Kill / crash / reopen integrity | Single-threaded point read latency |
| Export / restore + corrupt-restore refusal | Secondary-key scan throughput vs `native_db` |
| Multi-replica convergence + checkpoint-expiry recovery | |
| Agent-grant refusal and scoped isolation negatives | |

Do not claim a receipt is free. Disclose overhead of digest + durable receipt on
the raw path as part of A09.

## 1. Scope of the embeddable path

**Definition.** The embeddable path is what an application can consume when it
depends on `logical-db` (and the documented companion `logical-db-kv`) from a
separate package, with no daemon, no network, and no FoundationDB.

**In scope**

| ID | Surface |
|---|---|
| EP-1 | `logical_db::Database` local CRUD, schema, indexes, delete, export, restore |
| EP-2 | Exact retry identity (`OperationId`, request digest) and crash durability |
| EP-3 | `Database::synchronize` push/pull over an app-supplied `Transport` |
| EP-4 | Sync orchestration (`SyncOrchestrator`, opaque `OrchestratorState`) reachable from `logical_db` |
| EP-5 | Cache facade `logical_db_kv::KvCache` (R10–R13: TTL, batch, namespace checkpoints, tiered values) |
| EP-6 | Multi-scope isolation on one embedded binary (account + space) |

**Out of scope** (owned elsewhere, still required by ADR-0003)

| ID | Owner |
|---|---|
| EP-X1 | Partition routing, authorized views, archive (`logical-db-cloud-contract`) |
| EP-X2 | Multi-node FDB failover and point-in-time restore |
| EP-X3 | >1M concurrent-client scale ladder (Cloud Plane, issue #72) |
| EP-X4 | Host-authority grant integration (`LocalScopeAuthority` is the seam) |
| EP-X5 | External-repository consumer tape (PRD WP2) |

Out-of-scope rows stay visible. They cannot be scored pass by an embeddable test.

## 2. Completeness rule

The embeddable path is **complete** when every EP-1…EP-6 requirement below is
**pass** under §4 verification, every AE1–AE5 acceptance example has a black-box
test, and no app-visible obligation is left to the host to reimplement from
crate internals. A host that must import `logical-db-sync`, `logical-db-store`,
or `logical-db-protocol` to finish the documented journey is a failed EP-4.

## 3. Enterprise requirements

Each requirement is binding. **Target** is the acceptance bar. **MoS** is the
margin of safety that must hold before a claim is eligible. Industry bars are
named, not invented.

### A01 — Frontier (ER-EP-01)

**Requirement.** The embeddable path must beat credible current local-first
alternatives on the declared job (scoped structured state, exact retry, bounded
sync, bounded export/restore) without weakening correctness.

- **Target.** Matched capability/cost traces against the architecture classes
  ADR-0003 already names: managed-sync, database-log-sync, CRDT-sync,
  object/compute-stack. Versions and configurations disclosed.
- **MoS.** No axis may regress to fit a comparison. Correctness rows (A03, A06,
  A08) stay green in every comparison run.
- **Evidence.** Dated primary research + matched measurements. Familiarity or a
  smaller diff is not evidence.
- **Current.** **unknown** — three `evidence/matched-trace/embedded-*.json` files
  exist; no class comparison corpus is committed.
- **Verifier.** ER-T09 (bench harness; not in this TDD slice).

### A02 — Hyperscale (ER-EP-02)

**Requirement.** The embeddable replica must remain correct as one participant
under a Cloud Plane that already exceeds 1,000,000 active concurrent clients.

- **Target.** ADR-0003 bar: >1,000,000 active concurrent clients stay correct
  under network diversity, tenant skew, faults, and recovery.
- **Embeddable obligation.** Bounded per-replica work, bounded pages, bounded
  export, no host-visible unbounded memory. A local replica never becomes a
  scale bottleneck or an authority source under load.
- **MoS.** Sustained, burst, saturation, and recovery all measured. A small
  local test cannot pass this row.
- **Current.** **unknown** (EP-X3) with embeddable-side bounds **partially
  read** from `SyncLimits`, `MAX_EXPORT_BYTES`, `MAX_SNAPSHOT_BYTES`.
- **Verifier.** ER-T10 (scale ladder; Cloud harness, issue #72).

### A03 — Idiomatic Rust (ER-EP-03)

**Requirement.** Ownership, errors, states, and lifetimes on the embeddable path
stay explicit and sound. R01–R10 hold for every EP crate.

- **Target.** Zero production `.unwrap()`/`.expect()`, zero defensive clone
  loops, zero `Mutex` across `.await`, zero unbounded channels/caches, zero
  untracked spawns, typed domain errors with source chains, private
  invariant-bearing fields.
- **MoS.** Lint ceiling stated: 25 of 75 catalogued anti-patterns are
  lint-expressible. The other 50 stay on Miri / perf / human review. Do not
  claim full anti-pattern coverage.
- **Evidence.** Compiler + lint contract, exercised failure paths, `unsafe`
  inventory (logical-db is `forbid(unsafe_code)`).
- **Current.** **pass at the lint-expressible ceiling** for EP crates as of
  `c334534`. Lint silence is not sufficient for the non-expressible set.
- **Verifier.** ER-T01 (compile/lint), ER-T02 (failure-path tests).

### A04 — Generalized (ER-EP-04)

**Requirement.** Embeddable semantics hold across the declared input and
deployment family, not one fixture.

- **Declared family.** Unicode and empty keys/values; values at, under, and over
  the change bound; TTL absent / future / expired / `i64::MAX`; batch sizes
  `1…MAX`; schema migrations across versions; export/restore at bounds; account
  and space identities chosen independently of implementation constants.
- **Target.** Boundary and property tests over the family, independently chosen
  cases, no fixture-specific shortcuts.
- **MoS.** A case selected after seeing the implementation is not independent.
  Keep a seed corpus and a property suite.
- **Current.** **partial** — mutation campaigns and wire round-trips exist;
  AE1–AE5 are not all under black-box e2e.
- **Verifier.** ER-T03 (boundary/property), ER-T04 (AE1–AE5).

### A05 — Decoupled (ER-EP-05)

**Requirement.** Storage, CRDT, transport, and cache replace independently
through published contracts. The app imports `logical-db`, never a storage or
CRDT crate.

- **Target.** Contract compatibility across version skew; a replacement of any
  lower layer preserves embeddable journeys.
- **MoS.** `scripts/verify-rewrite.py` + domain contract gate stay green. No
  second database engine and no second sync protocol (plan scope boundary).
- **Evidence.** Dependency receipt, replacement fixture, version-skew test.
- **Current.** **pass internal / partial external** — `domain_contract` green;
  external tape (EP-X5) unbuilt.
- **Verifier.** ER-T05 (dependency + import boundary), ER-T08 (external tape).

### A06 — Ephemeral (ER-EP-06)

**Requirement.** Process loss never corrupts embeddable durable state. Recovery
is owned, bounded, and fail-closed.

- **Target.** Kill during write, kill during restore, corrupt export bytes,
  interrupted restore, reopen, and exact retry after restart all preserve
  integrity. ≥10% of applicable e2e tests induce a real process kill or panic
  and verify recovery. Report the counts.
- **MoS.** Input rejection is not a crash test. A mock restart is not a crash
  test. Subprocess kill with no destructor execution is the bar already used in
  `crash_recovery` / `kill_recovery`.
- **Current.** **pass at local scope** for kill-during-write and
  kill-during-restore; AE3 corrupt-restore needs a black-box assertion in the
  embeddable suite.
- **Verifier.** ER-T06 (crash suite), ER-T04 AE3.

### A07 — Portable (ER-EP-07)

**Requirement.** The same required embeddable semantics hold on every declared
OS and architecture target.

- **Declared targets.** macOS arm64 (developer and CI), Windows-native data
  placement, Linux. Wine/emulator lanes are supporting evidence, not a substitute
  for native results.
- **Target.** Reproducible build plus actual e2e results per target.
- **MoS.** Cross-compilation alone cannot pass this row. Untested targets stay
  visible as **unknown**.
- **Current.** **partial** — `logical-db-platform` covers three OS placements;
  no per-target e2e receipt matrix in `evidence/`.
- **Verifier.** ER-T07 (per-target matrix).

### A08 — Multi-tenant (ER-EP-08)

**Requirement.** Identity, data, caches, receipts, quotas, and recovery stay
isolated under concurrent use.

- **Target.** Account+space scoping is first-class and enforced on every EP-1…EP-5
  call. Cross-scope negatives refuse reads, edits, sync, export, restore, and
  cache hits. Noisy-neighbor and failure-isolation measured on the embeddable
  path (two scopes, one process).
- **MoS.** Client-side filtering is not isolation (ADR-0003 view rule applies
  by analogy: scope checks live in the facade, not in app code).
- **Current.** **partial** — AccountId/SpaceId are first-class; cross-scope
  negatives for cache and orchestrator state need explicit tests.
- **Verifier.** ER-T03 (cross-scope negatives), ER-T04.

### A09 — Fastest correct Rust (ER-EP-09)

**Requirement.** Performance claims cover the complete embeddable execution path
and retain every required semantic.

- **Target.** Matched p50/p95/p99, CPU, allocation, memory, I/O, and contention
  against the A01 classes. Use the named latency bar for the class (tail
  percentiles p50/p95/p99). Do not invent a tighter percentile. Five nines
  (99.999%) is the availability class bar when an availability claim is made;
  embeddable local open/edit does not make that claim.
- **MoS.** No unmeasured superiority claim. No benchmark that drops exact retry,
  bounds, or crash semantics to win.
- **Current.** **thin** — three matched-trace files; no criterion benches; no
  class comparison.
- **Verifier.** ER-T09 (criterion + matched traces).

## 4. Verification map (true TDD tests)

Tests are written against the contract first. Red is the point until the
behavior is real. Each ID must resolve to a named test.

| Test ID | Covers | Kind | Journey |
|---|---|---|---|
| ER-T01 | ER-EP-03 | unit | compile/lint receipt (existing gate) |
| ER-T02 | ER-EP-03, ER-EP-06 | e2e | typed errors on corrupt restore, bad scope, full batch |
| ER-T03 | ER-EP-04, ER-EP-08 | e2e | boundary/property + cross-scope negatives |
| ER-T04 | AE1–AE5 | e2e | see §5 |
| ER-T05 | ER-EP-05 | contract | app imports `logical-db` only |
| ER-T06 | ER-EP-06 | e2e | real process kill + reopen + exact retry |
| ER-T07 | ER-EP-07 | e2e | per-target receipt matrix |
| ER-T08 | ER-EP-05 | external | consumer tape (EP-X5) |
| ER-T09 | ER-EP-01, ER-EP-09 | bench | matched traces + criterion |
| ER-T10 | ER-EP-02 | load | Cloud scale ladder (EP-X3) |

This TDD slice owns **ER-T02 … ER-T06** for the embeddable path. ER-T01 is the
existing local gate. ER-T07…ER-T10 remain open with named owners above.

## 5. Acceptance examples (AE1–AE5) as black-box tests

From the finish plan. Each becomes at least one e2e test through the public
facade.

| ID | Given | When | Then |
|---|---|---|---|
| AE1 | Two replicas hold a document | One deletes it; both sync | Both absent; a late third replica converges to absent, not resurrect |
| AE2 | Replica checkpoint predates retention floor | Pull | Explicit checkpoint-expired recovery, never a partial prefix |
| AE3 | Export bytes corrupted after write | Restore | Refuse without touching live state; report the refusal |
| AE4 | Cache entry with 60s TTL | Read before and after expiry | Value, then absent, with no manual eviction call |
| AE5 | Value larger than the change bound | Cache, sync, evict, read on second replica | Identical bytes; eviction removes tiered bytes and the reference |

Plus the orchestrator journey that makes EP-4 true:

| ID | Given | When | Then |
|---|---|---|---|
| AE6 | App persists `OrchestratorState` across process restarts | `SyncOrchestrator::run` after restore | Same backoff/drain schedule; no reimplementation of push/pull in app code |
| AE7 | Retryable transport failure with a server retry hint | `SyncOrchestrator::run` | Deferred report, `not_ready_until_ms` honors the hint, work is not lost |

## 6. Traceability (fill as tests land)

| Requirement | Test IDs | Status | Evidence |
|---|---|---|---|
| ER-EP-01 | ER-T02, ER-T04 | unknown | pending suite |
| ER-EP-02 | ER-T06 | unknown | pending suite |
| ER-EP-03 | ER-T02 | unknown | pending suite |
| ER-EP-04 | ER-T03, AE6, AE7 | unknown | pending suite |
| ER-EP-05 | ER-T04 AE4–AE5, ER-T05 | unknown | pending suite |
| ER-EP-06 | ER-T02, ER-T06, AE3 | unknown | pending suite |
| ER-EP-08 | ER-T03 | unknown | pending suite |
| ER-EP-01 frontier | ER-T09 | unknown | out of TDD slice |
| ER-EP-02 hyperscale | ER-T10 | unknown | out of TDD slice |
| ER-EP-07 portable | ER-T07 | unknown | out of TDD slice |
| ER-EP-09 performance | ER-T09 | unknown | out of TDD slice |

## 7. Open questions (Director)

1. **Cache layering.** Keep `logical-db-kv` as the documented companion crate
   (current dependency direction: kv sits above the facade), or fold a
   `logical_db::cache` re-export into `logical-db` so one dependency is enough?
2. **External tape owner.** EP-X5 wants LEM or a dedicated fixture repo. Is a
   second repository still the bar, or is a `tests/external-consumer` package
   outside the workspace acceptable for the first proof?
3. **ER-T07 targets.** Which OS/arch pairs are **declared** for A07 on the
   embeddable path (macOS arm64, Windows native, Linux x64, …)? Untested pairs
   will remain visible as unknown.
4. **A01 comparator set.** Confirm the four ADR-0003 classes as the only
   required Frontier comparators for the embeddable path, or name a narrower
   first slice (for example CRDT-sync only) for the first measurement.
