---
type: Verification
title: Expired checkpoint recovery acceptance probe
description: Issue 28 live-composition regression and explicit verification limits.
generated: {by: agent:codex, at: 2026-09-19}
verified: {by: agent:codex, method: real HTTP and FDB recovery, process-kill companion, negative mutation and restored-source rerun}
status: local-recovery-verified-release-unverified
stale_after: 2026-10-19
sources:
  - https://github.com/srinji-kaggss/logical-DB/issues/28
  - ../crates/logical-db-e2e/tests/fdb_recovery_composition.rs
  - ../REWRITE_MANIFEST.md
---

# Expired checkpoint recovery probe

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39. Source URLs are in `SOURCES.md`.

## Problem and bounded change

Read baseline: `ae5025692937edec114a865b6114e643f279b0a9`. The existing
`fdb_recovery_composition` journey restores a fresh replica after pruning,
but does not hold an expired checkpoint. Its page inventory also discards
the result of `HashSet::insert`, allowing duplicate observations to disappear.

The same journey now obtains a real checkpoint through public synchronization,
advances the existing server Clock port beyond the unchanged 60,000 ms TTL,
and requires `BaselineRequired` over HTTP after pruning. The expired replica
must restore all 300 documents, preserve the tombstone, persist a replacement
checkpoint through reopen, and use that replacement for a successful public
pull. Page enumeration rejects duplicate IDs immediately.

The canonical owners remain `logical-db-service` (checkpoint validity),
`logical-db-sync` (recovery), and the existing FDB adapter (durability).
Only the composition test changes Rust behavior; no runtime, wire format,
dependency, lockfile, or workflow changes. A wall-clock sleep was rejected
because the existing Clock port permits deterministic expiry without waiting.
A mock-only test cannot establish the requested HTTP/FDB composition.

## Acceptance and evidence

Target: every assertion above passes against a live isolated FoundationDB
7.3.77 cluster, the companion process-kill restore test passes, and a temporary
mutation of the expired-checkpoint-with-floor response makes the new test fail.
The mutation must be reverted and the final source revalidated. Failure of any
predicate rejects this bounded change's runtime acceptance.

Ran on macOS arm64:

| Command | Result |
|---|---|
| `cargo check -p logical-db-e2e --features fdb --tests --locked` | exit 0 |
| `cargo fmt --all -- --check` | exit 0 |
| `git diff --check` | exit 0 |
| `python3 scripts/verify-rewrite.py` | exit 0; zero citation and dependency failures including this receipt |
| `cargo check --workspace --all-targets --locked` | exit 0 |
| `cargo test -p logical-db-e2e --locked --test dependency_contract` | exit 0; 10 passed |
| `cargo test --workspace --locked` | exit 0; no failed or ignored cases |
| `cargo clippy --workspace --all-targets --locked -- -D warnings` | exit 0 |
| `cargo deny check` | exit 0; advisories, bans, licenses and sources accepted; seven duplicate-version warnings remain |
| `cargo test -p logical-db-e2e --features fdb --locked --test fdb_recovery_composition -- --nocapture` | exit 0; 300 documents, expired and fresh recovery, replacement checkpoint usable after reopen |
| `cargo test -p logical-db-e2e --features fdb --locked --test fdb_process_kill_restore -- --nocapture` | exit 0; killed before commit and after commit; recovered 300 documents |
| expired-with-floor response mutated to `CheckpointExpired` | expected exit 101 at the new `BaselineRequired` assertion; runtime source restored byte-for-byte, both journeys passed again |
| `cargo clippy -p logical-db-e2e --features fdb --all-targets --locked -- -D warnings` | exit 0 on restored final source |

The declared bounded inventory contains nine predicates: real issued token,
deterministic expiry, HTTP recovery-required response, matching complete state,
reopened replacement checkpoint, successful replacement-token pull, no repeated
page IDs, passing crash companion, and rejection of the broken response.
All nine passed their specified checks. This is not whole-product behavioral
or structural coverage. Two live journey cases ran; one contains actual isolated
process kills (1/2, 50%). Final source SHA-256 for the changed Rust test:
`80c8b6b2fe0b00b5a6ebb0d81eea78885c96308fa8714961fe797ff486fda2d5`.

The local verification attempt extracted the official arm64 package without
installing a system service. Package SHA-256:
`aadeb7e4e530fdbefcf10255c45f4022165325ac43c4535a05559ed6d9ea1a37`.
The isolated single-memory server used loopback port 4679 with a 1 GiB memory
ceiling. Build logs and process samples are retained in session evidence
`74b95d7f850c1218327a7d3e6ad4550526aafe9c1b03cd483c7a056f31f96a93`.
The first clang sample observed `_dyld_start`; the second observed clang
waiting for its linker child. These samples do not establish the OS cause.
The build resumed without an environment change and the live journeys ran.
`fdb-green.log`, `fdb-mutation.log`, and `fdb-final.log` retain the original
pass, expected rejection, and restored-source pass. All three attempts reaped
the owned FDB server and released port 4679. The negative run's two residual
fixture databases were moved to OS Trash and their original paths verified
absent. No unrelated process or file was removed. Repeated memory/disk growth
has not been instrumented; cleanup observation is a narrower claim.

## Nine-axis acceptance retained

| Axis | Required invariant | Evidence required |
|---|---|---|
| Frontier | The selected design beats credible current alternatives on the actual problem without sacrificing correctness. | Dated primary research, named alternatives and matched capability/cost measurements. Familiarity or small size cannot establish superiority. |
| Hyperscale | More than 1,000,000 active concurrent users remain correct under heterogeneous compute, network conditions, tenant skew and failures. | Sustained load, burst, saturation and recovery results with workload definitions, resource bounds and raw telemetry. |
| Idiomatic Rust | Ownership, state transitions, errors and resource lifetimes are explicit and sound. | File-specific design review, compiler/lint results, applicable soundness checks and exercised failure paths. The existing Python hook does not satisfy this requirement. |
| Generalized | Behavior works across the declared input and deployment family without special-casing test examples, identities or topology. | Independently chosen cases, boundary/property tests and configuration variation; no hardcoded acceptance shortcuts. |
| Decoupled | Components have explicit owners and contracts and can be replaced independently without changing required behavior. | Replacement tests, dependency analysis, contract compatibility and version-skew results. |
| Ephemeral | Correctness survives process loss; durable state has an explicit owner outside disposable workers. | Crash/restart/replay tests proving state integrity, idempotency, cancellation and recovery. |
| Portable | The same required semantics hold on every declared OS, architecture and runtime target. | Reproducible builds and actual end-to-end results per target, with untested targets visible. |
| Multi-tenanted by default | Identity, data, caches, receipts, quotas and recovery remain isolated under concurrent use. | Cross-tenant negative tests, authenticated scope propagation and noisy-neighbor/failure-isolation measurements. |
| Fastest possible correct Rust | Performance comparisons preserve every required semantic and account for the complete execution path. | Matched CPU, allocation, memory, I/O, contention, throughput and p50/p95/p99 results against credible alternatives; justified lower bounds where applicable. |

File-specific disposition: the Rust test compiles and uses existing public
ports plus a standard-library atomic clock. The bounded Generalized and
Ephemeral checks above passed on macOS arm64. Concurrent tenant/noisy-neighbor
behavior remains unverified by this change. Frontier, hyperscale, portable execution,
matched performance, physical Ryzen 3 budgets, and cumulative lifecycle
measurements remain unverified. This receipt records evidence, not new policy.

Human outcome target: a reconnecting client needs zero manual checkpoint
resets. ESTIMATE: zero to one manual recovery intervention avoided per incident
if the acceptance probe passes. No human study was run; participant count is
zero, and task latency/comprehension outcomes are unknown.

## Release boundary

Issue #28 stays open. This change does not close the complete item-by-item
recovery audit, external LEM consumption, Braid confirmation, multi-node
failover/PIT restore, public TLS/rotation, device lifecycle, or PRD work
packages. PR #64 remains separate: its two self-hosted lanes passed on
`ee507a74aa80db677b803364b7770eebdd5d7ea5`, while the hosted run's annotation
says billing prevented jobs from starting. Neither that older result nor this
test's compilation establishes production readiness.
