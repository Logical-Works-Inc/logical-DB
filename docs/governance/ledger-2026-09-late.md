# Governance §5 — ledger, 2026-09-19 to 2026-09-21

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

Entries of the completion and decision ledger in [GOVERNANCE.md](../../GOVERNANCE.md),
moved verbatim under the 120-line document law. Newest first.

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
