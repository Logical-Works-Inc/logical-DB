# ADR-002: FoundationDB Is the Canonical Storage Substrate

**Status:** ACCEPTED (Director, 2026-08-26 — "Continue, Approved"; issue
#8 is the directive of record)
**Date:** 2026-08-26
**Owner:** Director · **Tracking:** issues #9 (F0), #10 (F1), #11 (F2),
#12 (F3), #13 (F4) under epic #8
**Supersedes (in part):** the v0.1 direction in which RedbBackend +
framed journal are the canonical production engine; DB_PATHWAY sections
naming a custom engine as end state are re-pointed here.

## Context

Issue #8 rules that logical-DB stops rebuilding storage machinery
FoundationDB already solved. FDB owns ordered transactional KV, MVCC and
conflict detection, durability, distribution, fault tolerance,
watches/versionstamps, and simulation-tested behavior. This repo owns
only the estate contracts mapped onto that substrate: namespace/subspace
discipline, canonical key codec, tenancy prefixes, transaction policy,
typed errors, and the admitted-envelope binding to Braid's three-axis
contract. The seam was already reserved — `kernel/src/port.rs:2-4`
names "a FoundationDB adapter … behind the same trait", and
`port.rs:49-51` names "later a versionstamp oracle" for `ClockPort`.

Current tree evidence: engines are `memkv` (reference oracle),
`redb 4.2`, and a framed journal (`kernel/src/redb_backend.rs`,
`kernel/src/journal.rs`). No fjall exists in `Cargo.lock`; issue #8's
"current Fjall path" wording is an erratum corrected on the issue and in
#13.

## Decision

1. The post-parity canonical production surface is the native FDB
   wrapper (#10), namespace/tuple key layer (#11), and admitted
   transaction envelope (#12). INV-FDB-SINGLE-SURFACE: after retirement,
   no second transaction abstraction or weaker-semantics path exists.
2. Dependency policy — add-pin-vet (Feldman spectrum position 3): the
   `foundationdb` Rust binding is pinned; it wraps the official C
   client; system `libfdb_c`/`fdbserver` artifacts are hash-recorded;
   bumps happen only on a named upstream commit (ABI match with the
   pinned server), never auto-update. Transitive surface audited at F0.
3. `ClockPort` gains an FDB read-version implementation holding
   INV-TS-MONOTONIC.
4. INV-PARITY-BEFORE-DELETE: nothing is deleted until the FactStore
   consumer demonstrates the seven-point parity journey (#13). Then
   `RedbBackend`, the framed journal, and the custom commit/version
   mechanism leave the canonical path. MemKv stays as deterministic
   test oracle.
5. Invariants introduced: INV-FDB-SINGLE-SURFACE, INV-TENANT-PREFIX,
   INV-UNKNOWN-NEVER-WRITES, INV-JUSTIFICATION-FRESH,
   INV-PARITY-BEFORE-DELETE. Existing INV-TS-MONOTONIC and
   INV-NO-STRING-QUERY carry over unchanged.

## Exact constraint

Apple FoundationDB stores and transacts the bytes. Logical-DB provides
namespace, key, admission, state-binding, and Rust integration
contracts — no engine code. Non-goals from #8 hold: no fork, no Flow/C++
changes, no custom consensus/WAL/MVCC, no semantic graph/vector/search
in the substrate, no Record Layer port without measured need.

## Evidence basis

- Issue #8 §Evidence: upstream developer-guide boundary definition;
  Apache-2.0 licence; 7.3 supported line; maintained Rust binding over
  the C client.
- Local-first falsifier (#9): cold start, idle RSS, install/update
  complexity, crash/restart, sleep/wake, macOS Apple Silicon support,
  local → multi-node migration with unchanged keys. Measured BEFORE any
  wrapper code lands. NO-GO(local) selects the pre-declared fallback —
  narrow adapter, explicitly weaker guarantees — never a rebuilt engine.

## Blast radius

Kernel public API (new feature-gated surface, then retirement of the
redb/journal path); five class traits keep compiling during parity via
the transitional `DurableKv` shim and have their destination decided at
F4 (optional layers vs move to LEM/Cairn — Director call). Consumer repos
bind through ports only (INV-ONE-MODEL-EVERYWHERE), so no consumer edits
are forced by this ADR.

## Migration / deletion path

#9 prove dependency → #10 native surface (feature-gated, zero
regression without the flag) → #11 keys → #12 envelope → #13 parity
receipt, then deletion PR, docs re-point, post-merge main observed.

## Review date

Review at F4 completion or 2026-11-26, whichever comes first — re-read
the falsifier numbers against the then-current upstream release policy.
