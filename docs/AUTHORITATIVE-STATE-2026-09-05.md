# Logical-DB authoritative state — 2026-09-05

Status: authoritative for Logical-DB implementation state and next-step ordering until superseded by a later dated state document.

This document exists to prevent four different things from being collapsed into one claim:

1. architecture intent;
2. code present in a branch;
3. behavior exercised on an exact commit;
4. behavior safe to rely on in a consuming system.

A green CI run does not erase an architectural defect. An open PR does not define `main`. A sibling repository's semantic ownership does not implicitly transfer storage ownership here.

## 1. Purpose

Logical-DB is the **storage and replication mechanics layer for local-first structured application state**.

It owns mechanisms such as:

- embedded durable transactions;
- structured CRDT persistence;
- operation idempotency receipts;
- local change/outbox ordering;
- durable change-feed cursors;
- schema-bound materialized indexes and migrations;
- bounded synchronization protocol and checkpoints;
- HTTP transport/service boundaries;
- FoundationDB service storage adapter;
- eventually, safe retention/recovery and portable export/restore.

Logical-DB does **not** own domain semantics for every consuming repository. In particular it must not independently define LEM's episode ontology, contradiction law, clocks/world-lines, LocalWorld semantics, or Cairn's ontology/observation model.

## 2. Current main state

`main` contains a substantive embedded database substrate:

- Automerge-backed structured documents;
- redb-backed immediate durable local storage;
- exact operation retry identity;
- atomic snapshot/change/outbox/index/event transitions;
- schema migration with durable cursor and bounded batches;
- bounded push/pull synchronization machinery;
- HTTP/CBOR transport and service layers;
- Braid authority adapter;
- FoundationDB-backed service log adapter;
- desktop CI across Linux, macOS, and Windows;
- optional live FoundationDB composition/fault-injection CI.

The standalone daemon remains deliberately fail-closed for write and synchronization authority. Valid bearer authentication alone does not make the supplied daemon a usable multi-device sync service.

## 3. PR state

### PR #23 — dependency bump

Merged 2026-09-05 after exact-head CI passed all required jobs.

### PR #26 — lifecycle contracts

Do **not** merge as currently written.

It adds valuable first-class tombstones and logical export/restore work, but its retention design introduces a release-blocking recovery defect: the persisted `SnapshotBaseline` identifies a log position without carrying the recoverable document state represented by that position. Pruning can therefore remove the only history needed by a fresh replica while the pull recovery path resumes after the floor without first installing baseline state.

Issue #28 is the merge gate for that defect. PR #26 may be merged only after either:

- retention/pruning is repaired to be state-bearing and recovery-safe; or
- the unsafe retention/pruning portion is removed while independently complete lifecycle work remains separately reviewable.

## 4. Known blocking findings

### P0 — authorization identity must bind actual action bytes

Tracked by #27.

The service currently accepts a caller-supplied `OperationDigest` as the identity used by the authority path. CRDT validation separately proves change bytes are internally valid, but the trusted service does not derive the authorization identity from the actual action payload. Before remote write authority is enabled, introduce a server-computed canonical action identity distinct from application retry/intent identity.

### P0 — retention recovery must be state-bearing

Tracked by #28.

No pruning is safe until the recovery artifact contains or unambiguously references complete recoverable state and a fresh replica proves it can reconstruct all state that no longer exists in the retained suffix.

### P1 — projection semantic identity

Tracked by #29.

The current schema digest binds schema version plus projection IDs, not projection meaning. Applications can change projection logic while accidentally retaining the same schema identity. Add an explicit application-controlled projection semantic version/digest and include it in the durable schema binding.

### P1 — cross-repo database ownership

Tracked by #30, LEM #8, Cairn #13.

Logical-DB, LEM, and Cairn currently contain overlapping language around database/state ownership. This must be resolved before LEM builds another persistence substrate or Cairn grows canonical storage behavior.

## 5. Authoritative proposed cross-repo boundary

This section is a **proposal pending the explicit LEM SOT amendment required by LEM #8**. It is authoritative only for what Logical-DB should avoid duplicating while that decision is pending.

| Layer | Repository | Owns | Must not own |
|---|---|---|---|
| Storage / replication mechanics | `logical-DB` | atomic persistence, replication, checkpoints, idempotency, materialized indexes, retention/recovery, backend adapters | episode ontology, source ontology, adjudication semantics |
| Canonical episode semantics | `LOGICAL-EVENT-MEMORY` | episode schema, append-only correction law, temporal/world-line semantics, contradiction, LocalWorld reconstruction, acceptance rules | duplicate sync/retention/backup engines if Logical-DB is adopted |
| Observation / ontology | `cairn` | ingestion, source fidelity, type registry, relations/entities, candidate episodes | canonical episode storage, retention, replication authority |
| Action | Forge repos | planning, execution, receipts | silent mutation of canonical memory |

The unresolved choice is whether LEM's current hard `FjallFactStore` binding remains the substrate or becomes an implementation detail behind a storage port implemented by Logical-DB. That decision belongs in LEM's canonical SOT, not in this repository alone.

## 6. Required integration contract if LEM adopts Logical-DB

LEM must consume Logical-DB through the stable public facade or an explicit narrow storage port. It must not import Logical-DB internal crates.

Minimum cross-layer flow:

```text
Cairn source observation
  -> typed candidate episode + provenance
  -> LEM validates/adjudicates episode semantics
  -> Logical-DB commits opaque/domain-encoded episode state atomically
  -> LEM reconstructs LocalWorld from canonical episodes
  -> Forge acts
  -> receipt re-enters Cairn as a candidate
```

Logical-DB should know that a value is durable structured state. It should not need to understand why an episode contradicts another episode or how LEM interprets seven clocks.

## 7. Execution order

Do not reorder these by convenience.

1. **#28 — repair or remove unsafe retention from PR #26.**
2. **Re-review PR #26 by exact head.** Merge only the independently complete paths with exact-head CI and no unresolved P0 finding.
3. **#27 — canonical action binding.** Complete before enabling remote write/sync authority in a production host.
4. **LEM #8 / Logical-DB #30 — decide the storage/semantic ownership split.** This is an explicit SOT decision.
5. **#29 — projection semantic identity.** Close before calling materialized schema evolution production-safe.
6. **Cairn #13 — candidate episode/type boundary.** Cairn produces typed observations; it does not become the store of record.
7. **Cross-repo tape.** Cairn candidate -> LEM acceptance -> Logical-DB durable commit -> LocalWorld read, using public boundaries only.
8. **Production hardening only after the above:** multi-node FDB recovery/restore, public TLS/issuer lifecycle, sustained load/backpressure, disk exhaustion, sleep/wake/mobile lifecycle.

## 8. Merge policy

A Logical-DB PR is complete only when all of the following are separately true:

- the intended behavior is described;
- the implementation is present on the exact PR head;
- the exact path is exercised on that head;
- CI/check evidence for that head is green where applicable;
- no known higher-severity architectural counterexample invalidates the capability claim;
- documentation says exactly what remains unproven.

Self-authored tests and PR prose are first-party evidence. They are useful but do not override a concrete counterexample.

## 9. Licensing state

The Rust dependency policy currently allows permissive licenses only. No compulsory GPL/AGPL dependency was identified in the reviewed graph. `r-efi` is dual/multi-licensed and permits MIT or Apache-2.0 in addition to LGPL. FoundationDB is Apache-2.0 with third-party acknowledgements.

Before a public release, add a root `LICENSE` file matching the workspace's Apache-2.0 declaration and generate an exact distributed-artifact third-party notice/SBOM rather than relying only on Cargo metadata.

## 10. Definition of done for Logical-DB v1 substrate

Logical-DB may be called a reliable substrate for consuming repos only when:

- embedded local CRUD/state is durable and restart-safe;
- deletion/tombstones converge without resurrection;
- export/restore is bounded, resumable, and corruption-safe;
- authorization is cryptographically/structurally bound to actual requested action bytes;
- retention cannot destroy recoverability;
- projection schema identity captures declared semantic change;
- at least one external repo consumes only the stable public boundary;
- the LEM/Cairn ownership boundary is explicit and non-duplicative;
- remaining production claims are supported by exact-path evidence rather than documentation inference.
