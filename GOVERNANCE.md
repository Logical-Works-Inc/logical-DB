# Logical-DB Governance

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

The charter, laws, gates, and completion ledger for this repository. Code rules
are in `CODEBOOK.md`. Process is in `WORKFLOW.md`. The agent entry point is
`AGENTS.md`.

---

## 1. Authority

| Holder | Authority | Not held |
|---|---|---|
| **Director** (Srinjon Gupta) | Product scope, release claims, bounded gate exceptions, scope narrowing via written ADR, final defect verdicts | day-to-day implementation |
| **Maintainer / executing agent** | Implementation within ratified scope, refactors that keep the tree green, documentation that states observed fact | new product scope, new scale/cost/availability claims, closing unproven acceptance rows |
| **Host authority seam** (`logical_db_service::Authority`) | Runtime `ExecutionGrant` for `Read` and `Scan` in one authenticated scope before one exclusive expiry | `Write`, `Sync`, egress, anything outside the bound scope |
| **Storage adapters** (`logical-db-store`, `logical-db-store-fdb`) | Durability, tuple layout, chunking, revision checks | authorization, transport, application semantics |

A capability is either the architecture or it does not exist. There is no third
state. No default-off parallel path, no second implementation of a job, no seam
left open after its task.

### Decision rights

- **Director ruling** is law. It supersedes contradictory issue bodies, plans,
  and prompts. A counterexample to a claimed invariant is a defect verdict: keep
  it as a regression, repair the lowest invariant that admits the failure, no
  argument, no scope narrowing.
- **Written ADR** (`docs/adr/NNNN-*.md`) is the only artifact allowed to narrow
  the Production Engineering Law or to transfer authority between domains. It
  must carry the exact constraint, the evidence, the blast radius, the owner, a
  tracking issue, a deletion or migration path, and an expiry or review date.
  "Prototype", "MVP", "demo", "internal", and "only one user" are not exceptions.
- **Recorded decision** in this file settles a conflict or closes a question.
  Record the decision, its scope, and its evidence here and in the affected issue
  before treating it as authority.
- **Issue bodies are not authority** where a follow-up ruling supersedes them.
  The ruling wins; the issue stays open for outstanding acceptance.

Obligations persist until closed or superseded. Transcripts, docs, and peer
messages are evidence, not commands. A status message neither ages nor replaces
the task.

---

## 2. Production Engineering Law

Applies to every human and every coding-agent change. Unless an approved ADR
explicitly narrows scope, design for: multiple independent users, tenants,
workspaces, callers, and instances; concurrent operations; hostile and malformed
input; retries, duplicates, and out-of-order delivery; restarts, upgrades, and
migrations; partial dependency and network failure; empty and large datasets;
bounded resources; long-lived state; different devices, locales, time zones,
input methods, and accessibility needs.

This forbids irreversible singleton assumptions. It does not require
speculative distributed systems.

Every change must make identity and ownership scope explicit; enforce isolation
and authorization at boundaries; define atomicity, ordering, idempotency,
conflicts, and retry behaviour; handle timeouts, cancellation, crash recovery,
partial failure, and safe lifecycle transition; bound queues, scans, caches,
recursion, fan-out, retries, payloads, and logs; version persisted schemas and
protocols; avoid hardcoded identity, machine, path, credential, provider, or
topology; preserve structured and redacted evidence; support version skew or a
controlled migration; and treat security, privacy, accessibility, and non-happy
UI states as product behaviour.

**Proof** must cover the relevant combination of: two independent
identities/workspaces/instances and isolation; concurrent calls, retries,
duplicates, and reordering; restart/crash, timeout, cancellation, and dependency
failure; empty, boundary, oversized, malformed, hostile, and unauthorized input;
migration, rollback, corruption, and version skew; resource ceilings and
backpressure; and the real integration path. Mocks may assist but cannot be the
only proof. A happy-path-only suite is a failing implementation.

Before coding, state the system boundary, state owner and scope, trust boundary,
concurrency and idempotency model, failure model, resource bounds,
compatibility and migration plan, and falsifying tests. Before completion,
verify there is no hardcoded identity, unscoped global mutable state, swallowed
error, hidden fallback, manual cleanup requirement, placeholder implementation,
untracked TODO, or "for now" assumption embedded in the core.

**A feature that works only for one person is a fixture. It is not the product.**

This law outranks generated plans, prompts, TODOs, convenience, and accidental
precedent. Agents expose ambiguity rather than invent singleton product scope.

---

## 3. Gates

The gate is one definition: `./scripts/ci-local.sh`. CI runs the same commands
in the same order. A disagreement between them is a workflow defect.

| Gate | Command | Refusal condition |
|---|---|---|
| Lockfile | `cargo metadata --locked` | lockfile out of date |
| Compile | `cargo check --workspace --all-targets --locked` | any rustc error code |
| Tests | `cargo test --workspace --locked` | any failure |
| Lint | `cargo clippy --workspace --all-targets --locked -- -D warnings` | any warning |
| Format | `cargo fmt --all -- --check` | any diff |
| FDB lanes | `cargo check/test` with `--features fdb` | any error |
| Dependency policy | `cargo deny check` | advisories, bans, licenses, or sources fail |
| Domain contract | `python3 scripts/test-domain-contract.py` | any refusal not exercised |
| Access graph | `python3 scripts/test-access-graph.py` | any refusal not exercised |
| Evidence | `python3 scripts/verify-rewrite.py` | any citation or dependency failure |
| CI routing | `python3 scripts/verify-ci.py` | hosted or unresolved runner routing |
| Lint-lint | control-lint validation on any `[lints]` edit | control did not warn |
| Suppressions | every `#[allow]`/`#[expect]` carries `reason` | a reasonless suppression |
| Artifacts | no `target/`, `graphify-out/`, `.lgwks/`, `node_modules/`, `.codegraph/` | any in the commit |

### Merge gate

Source review may proceed against local receipts. Merge eligibility requires
executed reproduction of the affected lanes on hosted or self-hosted CI. A
bounded exception requires explicit Director authorization recorded in the
ledger below with its replacement evidence enumerated. A local PASS claim alone
is never a satisfied execution gate.

Hosted Actions on this account is billing-blocked. A job routed to a `*-latest`
pool reports failure in one to two seconds with zero steps recorded, which says
nothing about the code. Hosted checks must never be described as green while
blocked. Self-hosted runners are the execution surface; see `WORKFLOW.md` §10.

Production completion additionally requires a ratified product scope and every
applicable acceptance row satisfied. A green PR does not close unfinished
roadmap issues. Do not copy historical checkmarks into a current release claim.

---

## 4. Change control for governance

`GOVERNANCE.md`, `CODEBOOK.md`, `WORKFLOW.md`, and `AGENTS.md` are the governance
surface. Changing them is a governance change and follows these rules:

1. A governance change ships in its own commit, separate from the semantic change
   it describes, unless the two are inseparable.
2. It records *why* in the ledger below, with the date and the decision it
   implements or supersedes.
3. It never weakens a gate to clear a refusal. Widening a lint level, editing a
   pinned version, or adding a consumer to a register to pass a check is a
   suppressed gate, and it is a defect.
4. It never turns a `forbid` into a `deny` except through the four documented
   exceptions in `CODEBOOK.md` §2.1, each of which keeps a reasoned `#[expect]`
   as the only lowering.
5. Retiring a document moves it to `docs/archive/` and registers the
   supersession here. There is never a second live copy.

---

## 5. Completion and decision ledger

Newest first. Each entry names what changed, the receipts, and what it does
**not** claim.

### 2026-09-21 — Governance suite, local CI, estate alignment

This entry records the governance suite (`CODEBOOK.md`, `WORKFLOW.md`, this
charter), the local CI gate (`scripts/ci-local.sh`), and the estate-alignment
pass carried in [PR #86](https://github.com/srinji-kaggss/logical-DB/pull/86).

**Estate and toolchain alignment** (carried in PR #86):

| Edge | Was | Now | Receipt |
|---|---|---|---|
| Rust toolchain | `1.98.0` | `1.98.1` | `rustup show active-toolchain` |
| `lgwks_std` | `0.6.1` git rev | `0.6.6` registry | `Cargo.lock` source `registry+...` |
| `lgwks_bot` | `0.3.0` git rev | `0.4.2` registry | same; `contract/APPROVED.toml` `source = "registry"` |
| `lgwks_deps` | `0.1.5` git rev | `0.1.12` registry | same |

A git pin cannot witness that it still matches a published estate release, and
this one was five `lgwks_std` releases behind crates.io. `deny.toml` declares
`allow-git = []`.

**Braid removal.** `logical-db-braid` and its eight `braid-*` git edges are
deleted (directory moved to Trash). Host authority is
`logical_db_service::Authority` with the one local implementation
`LocalScopeAuthority` / `LocalGrant`, which refuses `Write` and `Sync` and every
request outside one authenticated scope and one exclusive expiry. The dual
`lgwks_std` residual is closed: the lockfile carries exactly one estate
`lgwks_std` (0.6.6).

**Lint contract.** `.cargo/config.toml` runs `clippy-driver` as the workspace
rustc wrapper so the `[workspace.lints]` forbid-list binds every `cargo check`,
and `clippy.toml` holds test code to the same contract as production. The tree
was red under that contract at `0c4a0b4`: 620 clippy errors, 259 prints in
library code, 12 reasonless suppressions, and `E0639`/`E0004` fallout from
half-applied `#[non_exhaustive]`. All cleared. FDB lanes compile at zero errors.

`unsafe_code` is `deny` in the workspace table, matching that table's own
documented design and the `print_stderr`/`print_stdout`/`exit` trio: the
FoundationDB network-boot leaf must carry a reasoned `#[expect]` and `E0453`
forbids lowering `forbid`. Every safe crate still states
`#![forbid(unsafe_code)]` at its root.

**Coverage ceiling, stated once.** 25 of 75 catalogued anti-patterns name an
enforceable lint. The other 50 need Miri, hardware counters, allocation
profiling, or human review and are not lint-expressible. This pass claims only
the lint-expressible subset plus the specific structural fixes recorded in
PR #86.

**Nine-axis posture — no axis is claimed:**

| Axis | Posture | Observed gap |
|---|---|---|
| Frontier | Not established | #33–#35 open until a measurement environment exists. No corpus, baseline, or workload committed. |
| Hyperscale | Not established | Program is #66/#67/#70/#72. No >1M concurrent evidence. |
| Idiomatic | Sound at the lint-expressible ceiling | Compiler/lint contract is the receipt. 50/75 not lint-expressible, not claimed. |
| Generalized | Partial | Wire round-trips and mutation campaigns exist. Tenant/region/topology families are Cloud work (#67–#69). |
| Decoupled | Strong internally | `domain_contract` green. Braid removed. Dual `lgwks_std` closed. Frozen boundaries stay registered. |
| Ephemeral | Strong at local/FDB scope | `crash_recovery`, `kill_recovery`, `fdb_process_kill_restore`, `fdb_recovery_composition`. #28 open. Multi-region DR (#71) unbuilt. |
| Portable | Partial | `logical-db-platform` covers three OS placements. No per-target e2e receipt matrix in `evidence/`. |
| Multi-tenant | Partial at embedded scope | Account+space scoping first-class. Cross-tenant negatives for Cloud views (#68) unbuilt. |
| Performance | Thin | Three matched-trace files carry p50/p99/throughput. No criterion benches, no matched architecture-class comparison, no allocation or contention profiling. |

No present-tense scale, cost, availability, or superiority claim becomes
eligible from this pass.

**Does not claim:** production completion; hosted CI evidence; issue #28
closure; any nine-axis green; live FDB cluster reproduction beyond the local
lane.

### 2026-09-20 — Sovereign program state (issue #66)

Issue [#66](https://github.com/srinji-kaggss/logical-DB/issues/66) is the
nine-axis acceptance program for the proposed sovereign hyperscale Cloud Plane.
It preserves #25 and #28 as correctness gates and does not turn the current
scope-frontier FDB relay or local receipts into a scale, availability, cost, or
sovereignty claim. Dependency order: #73 structure first; then
[#67](https://github.com/srinji-kaggss/logical-DB/issues/67) partition/routing,
[#68](https://github.com/srinji-kaggss/logical-DB/issues/68) authorized views,
[#69](https://github.com/srinji-kaggss/logical-DB/issues/69) residency/key/exit;
then #70 delivery, #71 disaster recovery, #72 matched-scale evidence. Cairn
#74, LEM #75, Forge #76 are separate linked migrations. No domain promotion is
implied.

### 2026-09-19 — Domain boundary enforcement (issue #73)

`contract/domains/*.toml` declare owned crates, public contracts, allowed
dependencies, and forbidden domains. `scripts/domain_contract.py` refuses an
undeclared owner, a double-owned crate, a disallowed cross-domain edge, a product
domain importing another domain's implementation crate, and a predecessor twin.
`scripts/test-domain-contract.py` is the falsifier: one forbidden edge per domain
plus undeclared, double-owned, and twin crates, 8/8. **Scope limit:** a structure
gate over the declared import graph. Not proof that target Cairn/LEM/Forge crates
exist, not the external-consumer fixture (PRD WP2), not issue #25's runtime
transition graph.

### 2026-09-16 — Delivered state and Director decisions

- **PR #57 merged** (`f19867b`): HTTP transport bounds credential refresh and
  every exchange under one 30-second operation deadline. Self-hosted quality and
  FDB lanes passed on head `c69631a` (run 35124718030). Issue #54 stays open for
  independent reproduction and measured-tolerance instrumentation.
- **PRD ratified** — PR #58 (`b804838`): `docs/PRD-UNIFIED-SUBSTRATE.md` is the
  product definition of record (embedded v1, FDB as cloud relay only,
  Cairn+LEM unification, swarm work packages WP0–WP8).
- **SOT transfer authorized** (2026-09-16): `docs/adr/0002-cairn-lem-authority-transfer.md`
  records the transfer. The ADR-0001 migration proof is the open obligation
  (PRD WP2). Until it lands, documentation says "migration in progress", never
  "canonical".
- **Issue consolidation**: #6, #8, #30, #32, #33, #34, #35 closed with
  consolidation comments pointing at the PRD. #25, #28, #54 remain open as real
  gates. Open issues are exactly the acceptance list.
- **CloudKit review recorded** — `docs/review-cloudkit.md`: relay-first
  confirmed; CloudKit is a design reference plus at most a bounded optional
  transport adapter for Apple-only consumer apps.

### 2026-09-14 — Local verification and the hosted-CI exception

Local verification at `0a22f9a7` plus the retention-store test-lane repair
(macOS arm64): clean-room verifier passes; workspace suite 308/0; the complete
FDB composition lane executed locally for the first time against the checksummed
FoundationDB 7.3.77 arm64 client and server — daemon fdb 10/10, store-fdb fdb
11/11, `fdb-ambiguity-journey`, `fdb_recovery_composition`, and
`fdb_process_kill_restore` all pass against a live single-memory cluster.

**Hosted CI plan, Director ruling.** GitHub Actions on this account stays
billing-blocked. The FDB cluster lane runs on a self-hosted runner. `.github/workflows/ci.yml`
declares literal self-hosted labels; `scripts/verify-ci.py` refuses anything
else. Hosted-lane evidence remains absent and must not be claimed.

**Bounded exception, PR #55 only.** The Director authorized merge after
hardening on 2026-09-14. Replacement evidence is the local receipts pinned in
`evidence/2026-09-14-local-fdb-receipt.md` plus the Director's own review of the
exact head. This exception covers PR #55 and nothing else.

**Estate ownership, issue #30 resolved.** The estate is one development monorepo
per `docs/MONOREPO.md`. Authority boundaries are in
`docs/adr/0001-semantic-plane-boundary.md`: the LEM SOT remains authoritative for
the CAIRN/ORIGIN/FORGE plane table, `logical-db-cairn` and `logical-db-episode`
are bounded non-authoritative ports, and the sibling repositories are frozen
read-only references. Any future plane-authority transfer requires the ADR's
Director-approved SOT diff and an explicit migration proof.

### 2026-09-12 and earlier — Recorded product decisions

**Backend split (issue #8 follow-up).** redb serves local device state;
FoundationDB serves distributed transaction requirements. Upper key, namespace,
and event contracts must be compatible, but backend guarantees are explicit
through `EmbeddedLocal` and `DistributedTransactional` capability profiles. Do
not fork or strip FDB core roles. The same 1M-operation trace must compare CPU,
memory, bytes written, latency, startup, recovery, and sleep/wake for common
local semantics separately from distributed guarantees.

**Core scope (issue #6 ruling).** The five application-shaped classes are
rejected as the mandatory core. The substrate owns ordered transactional KV,
versions, range/conflict semantics, durability, and a distributed-compatible
seam. Higher-layer semantics remain optional consumers and adapters. These
rulings supersede the contradictory parts of the issue bodies; the issues remain
open for outstanding acceptance.

**PR #36 merged** as `2a17d6b9`. Reviewed head `4d0b810e`; all seven protected
checks passed in run 34170132966. Merge commit run 34170937860 also completed
all seven. The PR #36 Rust scan reported zero blocking findings across 25 files.
Numerical MI remains advisory: 21 files below 76, minimum 49.4. Three Python
verifier findings were documented against argument-list subprocess calls without
a shell. No threshold or detector was changed to hide them.

**PRs #48–#53** delivered the estate-substrate adoption, paged baseline recovery
into one atomic client install, and the process-kill restore journey. Recovery
status, stated once and referenced everywhere else: **implementation present** —
the v2 artifact, the paged HTTP baseline protocol, and one atomic client install
of recovered state and checkpoint exist on `main`. **Local execution reported** —
the 2026-09-14 local live-cluster receipts exercise `fdb_recovery_composition`
and the process-kill restore journey. **Not yet established** — issue #28 stays
open until each acceptance item, including the position-1 prune/fresh-replica
counterexample, is independently reproduced on a runnable hosted or self-hosted
lane.

Canonical action encoding is owned by `logical-db-protocol`. PRs #61 and #62
moved repository digests and MACs to `lgwks_std::hash` BLAKE3; this ledger must
not retain its former SHA-256 description. `OperationDigest` remains caller
intent; it is included in, but never substituted for, `ActionDigest`.

---

## 6. Open correctness and acceptance work

| Priority | Work | Observed gap and completion evidence |
|---|---|---|
| Closed | [#27: action authorization](https://github.com/srinji-kaggss/logical-DB/issues/27) | Validates native CRDT bytes before authorization, computes a versioned canonical `ActionDigest`, FDB recomputes it. Unit HTTP tests and the FDB journey hold a grant fixed across field mutations and preserve exact retry. Independent runtime host-authority confirmation remains a separate release requirement |
| P0 | [#28: recovery before pruning](https://github.com/srinji-kaggss/logical-DB/issues/28) | PRs #49–#53 delivered the state-bearing snapshot artifact, paged HTTP baseline, one atomic client install. Stays open until the acceptance checklist is verified item-by-item against a runnable lane, in particular expired-checkpoint recovery through the public transport and the position-1 prune/fresh-replica counterexample |
| Delivered | [#29: projection semantics](https://github.com/srinji-kaggss/logical-DB/issues/29) | Every `Projection` requires an application-supplied `ProjectionSemantics` hashed with its `IndexId` into the v2 durable schema digest. Same identity and version with changed semantics returns migration-required; `Database::migrate_schema` rebuilds in bounded revision-checked pages |
| P1 | [#25: checked access graph](https://github.com/srinji-kaggss/logical-DB/issues/25) | Audit migration, sync, and service transition coverage against the issue's graph contract after action and recovery boundaries settle |
| Delivered | [#73: domain boundaries](https://github.com/srinji-kaggss/logical-DB/issues/73) | Declared import-graph gate with an 8/8 falsifier. Structure only |
| Proposed | [#67–#69](https://github.com/srinji-kaggss/logical-DB/issues/66) | Partition routing, authorized views, region/key/exit. Cloud contracts, after #73. No Cloud implementation and no nine-axis performance claim |
| Proposed | #70–#72 | Fan-out, regional disaster recovery, matched-scale evidence. The 1M-operation comparison remains unmeasured |
| Proposed | #74–#76 | Cairn ontology/evidence, LEM/ORIGIN LocalWorld, Forge capability/receipt. No domain promoted by documentation |
| Release | External LEM consumption and deployed-service behaviour | The earlier facade/FDB consumer receipt is retired for an invalid confirmation model. Require current external consumption, verified confirmation, and the deployment journeys in `REWRITE_MANIFEST.md` |
| Frontier | #33, #34, #35 | Director ruled 2026-09-14: leave open until a measurement environment exists. No corpus, workload, or measured-superiority claim meanwhile |

---

## 7. Decisions requiring the Director

| Decision | Conflicting authority or missing choice | Rule until decided |
|---|---|---|
| Comparative acceptance | #33, #34, #35 require a corpus, baseline, workload, and measurement environment | Leave open. No corpus or workload is committed and no measured-superiority claim is made |
| Merge while hosted execution is unavailable | Hosted Actions billing-blocked; branch protection on `main` disabled | Local receipts support source review only. Merge eligibility needs executed CI reproduction unless the Director records a bounded exception naming its replacement evidence |

A required decision pauses dependent work. Audits, reproducible counterexamples,
and factual documentation may continue.

---

## 8. Notes and residual working state

Unrelated working changes remain outside a delivery patch. The original
checkout's unstaged KV edit was preserved during PR #36; its disposition is
untracked and its disposition is untracked; any future appearance of that edit
requires explicit review, not silent import.

Branch protection on `main` is disabled, so merges are currently ungated
mechanically. The merge gate in §3 is the operative gate in that state.
