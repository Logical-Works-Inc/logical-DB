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
The composition test and Windows preflight are test-only Rust; there are no
product runtime, wire format, Rust dependency or lockfile changes. CI routing
is corrected separately below. A wall-clock sleep was rejected
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

## Self-hosted CI correction

The first PR run at `bbc28a6` passed both existing self-hosted lanes (macOS
quality and live arm64 FDB), but 22 hosted jobs did not execute. Treating their
billing annotation as an acceptable external blocker was the wrong decision:
the Director requires self-hosted execution for every check, with emulation
for the remaining platforms. This correction preserves that requirement.

The workflow routes all jobs explicitly to `[self-hosted, macOS, ARM64]`.
Native quality retains rewrite evidence, dependency-contract tests, complete
workspace check/test/clippy, formatting and
cargo-deny. Native arm64 FDB checks retain the same semantics; their environment
exports are grouped to satisfy shell lint. Linux x86-64 runs workspace
quality, all 15 original Linux isolated package builds, and the retained Linux
FDB composition in the existing local Docker
emulator. Windows-GNU runs compile, dependency-contract, workspace tests and
clippy through Wine, not a GitHub-hosted Windows machine.

`scripts/verify-ci.py` uses pinned PyYAML 6.0.3, not a text search, to reject
hosted, unresolved, duplicate-key and opaque reusable-workflow routing. Its
six regression methods pass locally. PyYAML is CI-only tooling; the Rust
dependency register and lockfile are unchanged. Python's standard library
does not parse YAML; a hand-written partial parser was rejected because it
could miss scheduler-significant syntax. The existing dependency-contract
gate remains mandatory; it does not claim to admit Python packages.

`ci-emulated.sh` verifies the resident base-image digest, builds the pinned
Rust 1.98.0 toolchain, and runs each guest with 4 CPUs, 6 GiB memory, 512 PIDs
and a 50-minute execution deadline. The initial cross-rs 0.2.5 image failed
a small Windows Rust runtime probe at link time: its import library lacked
Rust 1.98's `GetHostNameW`. That image is rejected, not worked around in Rust.
The replacement cross-rs image is pinned to
`sha256:7dbfe2c497a798ee3798fecf4b262c0ea12d1d02a6331bdc05ce76c9ad90fd70`:
GCC 13-posix and Wine 10.0. The Windows Rust smoke probe now passes random
hash seeding, a loopback socket and thread join before the full suite builds;
compiling it for the host instead correctly fails. Wine's initial headless
setup diagnostics remain in the build log; no GUI capability is claimed.
The first Windows all-target check passed in 10m19s, but its next shell command
failed with exit 127 (`ets: command not found`): the agent had edited a script
while Bash was reading its live bind mount. The Linux attempt was deliberately
stopped (exit 143) to repair the shared harness. These are retained failures,
not passing runtime evidence. Read-only guest mounts do not freeze host edits.

The repaired driver archives the Git index into a recorded tree snapshot
before execution. Its host body is parsed as one function before starting
children. Guests mount that snapshot read-only, without `.git`, host credentials,
or the Docker socket. The previous snapshot moves to OS Trash on replacement;
no source history is irreversibly removed. Workers stop after each run and retain owned build/Wine caches for
reuse; logs rotate at 2 x 10 MiB. Lane-specific target, registry and Git cache
volumes allow the Actions checkout to reuse the local emulator's compilation
without sharing host credentials or Linux/Windows build outputs. The Linux
default and optional FDB lanes use the same fixed linker path, avoiding a
second dependency build solely because RUSTFLAGS changed. An 8 GiB preflight cache check supplies
backpressure, not a strict peak-storage guarantee. Changed worker images or
mounts are refused pending recoverable maintenance rather than overwritten.
Repeated cancellation, crash, disk-growth and image-upgrade lifecycle results
remain unverified. These limits must not be reported as production acceptance.

Measurable targets: zero jobs schedulable on a hosted runner; all original
check families retained; actual guest test exits zero; expired checkpoint
and process-kill FDB receipts remain present; no owned worker left running.
At this receipt revision the routing regression passed; guest execution and
new exact-head remote verification are pending. Wine is Windows-GNU
compatibility evidence, not proof of native Windows kernel semantics.

The frozen Linux run completed with exit 0: default workspace tests and
clippy, optional FDB compilation/lint/tests, ambiguity journey, HTTP expired
checkpoint recovery (10.18s) and process-kill restore (12.75s). Its snapshot
predated the restored Linux package-isolation loop; that loop still requires
exact-head verification. The Windows Wine 10 run exposed a separate harness
prerequisite: eight dependency-contract tests failed because Windows
`Command::new("cargo")` could not find a Windows executable. The gate was
not skipped or mocked. The image now installs the matching Windows Rust
toolchain and exposes its binaries through the Wine runner. Its fail-fast
probe successfully spawned real Windows Cargo and rustc 1.98.0; the full
suite is being rerun. Its dependency-contract rerun passed all ten tests in
1.39s. Both failed images/workers and their logs are retained. Disabling the
self-hosted membership guard in an in-memory copy of `verify-ci.py` caused
the `[Linux, X64]` adversarial case to fail (`ValueError not raised`); the
unmodified six-method suite passes. No source mutation was written to disk.

Windows then reached the real E2E suite and Rosetta aborted with `invalid gdt
selector index 5`. A one-thread rerun reproduced it. QEMU user-mode was not a
passing substitute: Wine 10 re-executes its loader, and suppressing that
re-exec instead produced a target segmentation fault. Native ARM QEMU also
failed its smoke probe. These attempts are rejected, not enabled CI fallbacks.
The isolated x86-64 QEMU system VM (4 CPUs, 4 GiB RAM, 12 GiB virtual disk,
no host mounts) passed the same E2E executable: ten tests, zero failed or
ignored, exit 0 in 42.61s. No shared Rosetta setting changed. This resolves
the bounded emulator failure, not native Windows equivalence. The complete
Cargo-discovered inventory remains under verification.
The loader decision was checked against Wine 10's primary source:
https://github.com/wine-mirror/wine/blob/wine-10.0/dlls/ntdll/unix/loader.c.

The final Linux snapshot, including all fifteen restored isolation commands,
completed with exit 0: 51 test-result groups, 354 passed test invocations
(including deliberately repeated dependency/FDB checks), zero failed and
zero ignored. HTTP expiry recovery took 24.00s; process-kill restore took
17.60s. Its retained worker was verified stopped with exit 0. These are
bounded correctness receipts, not matched performance measurements.

The Windows compiler exports Cargo's JSON-discovered test inventory and
package working directories, the matching Windows SDK binaries, runtime DLLs,
and SHA-256 transfer checksums. Five regression methods reject incomplete or
duplicate inventories, escaped paths and corrupted transfers. Disabling the
checksum comparison in memory makes the tamper test fail. The runtime repeats
the separate dependency-contract gate before all workspace executables; doc
tests remain a mandatory compiler-container command. The QEMU guest has no
host mounts or forwarded credentials and is stopped on either result. The
first integrated export failed with exit 127 because Python was absent from
the compiler image; Python is now an explicit image prerequisite.

The first complete runtime attempt exposed another exporter omission:
`crash_recovery` could not spawn `CARGO_BIN_EXE_crash_writer` (Windows error
2, file not found). Cargo marks support binaries as non-test artifacts. The
exporter now transfers every executable artifact while running only test
harnesses; its new regression includes a non-test crash writer. No test was
skipped or changed. The ten-test HTTP/recovery suite passed in that same run
(38.63s). The run ended with three failed executables: crash and kill recovery
both lacked support binaries, and the platform executable reported a Wine
server connection reset before printing test results. The public driver
returned exit 1 and the VM was verified stopped. Wine 10's primary manual
documents a default three-second server persistence delay. The harness now
requests 60 seconds between binaries, with VM shutdown still terminating the
owned session; this addresses the inferred teardown race without retries or
suppressed failures. Context7 had no matching Wine persistence documentation;
the primary manual was read through BrowserOS:
https://github.com/wine-mirror/wine/blob/wine-10.0/server/wineserver.man.in.
The corrected-inventory result is recorded below; complete remote-driver
verification remains pending at this receipt revision.

The corrected compiler run at `296e5f4` exported 28 test executables and four
support binaries (3,649,821,329 bytes). Runtime setup then failed with
`FileNotFoundError: wineserver`: the package does not put that name on PATH.
`dpkg-query` identified `/usr/lib/x86_64-linux-gnu/wine/wineserver`; its own
version/help output confirmed Wine 10.0 and `--persistent[=n]`. The harness
now uses that installed path and checks it before transfer. Its corrected
Python runner passed against the same verified bundle without recompiling
unchanged Rust: 29 harness executions, 329 passed invocations (including the
separate repeated dependency gate), zero failed and zero ignored, exit 0.
Both process-exit and forced-termination recovery assertions passed by name,
as did the platform executable. Runner SHA-256:
`47efc5910e5b7dd6f3b511800c76b9d8efdb52d7215a791c33bea9c02e96ad6b`.
The guest source and support binary were readable/executable but not writable
by the test user. The cleanup wrapper stopped the VM successfully. The final
remote run must still execute the complete public driver, not substitute
this bounded diagnostic invocation. Its exact-head result belongs to PR #65.

Measured guest artifact storage was 2.7 GiB before adding support binaries,
with 3.0 GiB free on the initial 12 GiB virtual disk. The owned VM declaration
is increased to 20 GiB so retained failure evidence and the next complete
bundle can coexist. Incoming artifact sizes are checked before transfer
against a 12 GiB retained-artifact budget and 1 GiB remaining-space reserve.
The budget admits the retained failed bundle, corrected local run and
exact-head CI bundle; the first measured support binary alone is 185,061,254
bytes. An 8 GiB budget would not safely cover those three complete receipts.
This is bounded backpressure, not automated historical-artifact cleanup.
The first corrected retry was refused by the compiler worker's original
8 GiB preflight: Docker measured its Windows target volume at 4.843 GB and
writable layer at 2,889,793,536 bytes, before other mounted caches and Wine
state. Windows now has a 12 GiB preflight budget for both target and exported
runtime bundle; Linux retains 8 GiB. The public log reports actual KiB usage
and the limit. This sizes retained compilation evidence, not a test threshold.

At published head `e5cd052`, native quality passed in 3m37s and native FDB
passed in 27m32s. Windows failed at the recorded Rosetta error. The overall
run was cancelled while Linux was queued; it is not a passing CI receipt.
During that run the FDB log reported `TPSLimit=0` and `WorstFreeSpaceTLog=0`
with approximately 26 GiB host space available. Disk-reserve throttling is
the inference, not a measured threshold. Available space later rose above
59 GiB and FDB completed. The Director subsequently confirmed completing
the requested human-only Trash cleanup; a fresh `df -h` measured 60 GiB
available. No agent emptied Trash or uploaded artifacts.

## Nine-axis acceptance retained

Delivery-state correction: the later live BrowserOS settings inspection found
classic rule `82656536` applying to `main`, with seven required status names,
strict freshness, conversation resolution, linear history and administrator
enforcement enabled. This differs from the earlier unprotected observation;
the cause of that difference is unknown. No protection setting was changed.
The workflow retains the seven legacy status names as real self-hosted jobs:
evidence, three OS check names, formatting, dependency policy and FDB. The
`*-latest` strings are check identifiers only, never runner selectors. A new
regression validates the exact required-context set from the parsed workflow.
The delivery endpoint is now protected-branch merge and synchronization after
required checks pass, not merely an open PR. The nine axes remain unchanged.

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
