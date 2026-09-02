# Logical-DB v3 primary-source ledger

This ledger is normative input to the clean-room rewrite. Source-file headers
cite these identifiers. An identifier is evidence, not an instruction to copy
an implementation. Product decisions remain explicit in `ARCHITECTURE.md`.

## Local-first state and convergence

- **S01 — CKSyncEngine:** local pending changes, bounded batches, automatic
  retry, pull events, account transitions, and application-owned conflict
  handling. <https://developer.apple.com/documentation/cloudkit/cksyncengine-4b4w9>
- **S02 — CloudKit server change tokens:** opaque, scope-specific, cached
  progress positions whose contents must not be interpreted.
  <https://developer.apple.com/documentation/cloudkit/ckserverchangetoken>
- **S03 — CloudKit zone changes:** modifications, deletions, pagination, and
  `moreComing` behavior.
  <https://developer.apple.com/documentation/cloudkit/ckdatabase/recordzonechange>
- **S04 — CloudKit conflict records:** client, server, and ancestor versions
  are surfaced for explicit resolution.
  <https://developer.apple.com/documentation/cloudkit/ckerror/serverrecordchanged>
- **S05 — CloudKit record change tags:** optimistic record-version checks.
  <https://developer.apple.com/documentation/cloudkit/ckrecord/recordchangetag>
- **S06 — Google Drive changes:** durable start tokens and paginated change
  retrieval. <https://developers.google.com/workspace/drive/api/guides/manage-changes>
- **S07 — Google Drive notifications:** notifications are change hints, not
  state transfer, and channels expire.
  <https://developers.google.com/workspace/drive/api/guides/push>
- **S08 — Google Drive resumable uploads:** durable resumable sessions,
  progress queries, and explicit expiration.
  <https://developers.google.com/workspace/drive/api/guides/manage-uploads>
- **S09 — CouchDB replication protocol:** push/pull replication, changes,
  checkpoints, recovery, and HTTP interoperability.
  <https://docs.couchdb.org/en/stable/replication/protocol.html>
- **S10 — CouchDB conflict model:** deterministic visibility without silently
  discarding concurrent revisions.
  <https://docs.couchdb.org/en/stable/replication/conflicts.html>
- **S11 — Automerge core:** mature CRDT document model, causal changes, binary
  persistence, merge, and per-peer sync state.
  <https://github.com/automerge/automerge>
- **S12 — Local-first software paper:** local ownership, offline operation,
  multi-device synchronization, and CRDT/transport separation.
  <https://www.inkandswitch.com/essay/local-first/>
- **S13 — Algebraic replicated data types:** authenticated local-first
  replication and coordination-free convergence.
  <https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ECOOP.2023.14>

## Storage, transaction, and keyspace behavior

- **S14 — FoundationDB developer guide:** strict-serializable transactions,
  MVCC, conflicts, retries, futures, and short transaction constraints.
  <https://apple.github.io/foundationdb/developer-guide.html>
- **S15 — FoundationDB data modeling:** ordered tuples, prefix scans, and
  type-preserving tuple encoding.
  <https://apple.github.io/foundationdb/data-modeling.html>
- **S16 — FoundationDB directory layer:** application subspaces require
  indirection rather than hard-coded global prefixes.
  <https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/directory/DirectoryLayer.html>
- **S17 — FoundationDB limitations:** key, value, transaction, trust, and
  operational ceilings.
  <https://apple.github.io/foundationdb/known-limitations.html>
- **S18 — FoundationDB transaction API:** unknown commit outcomes and retry
  implications.
  <https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/Transaction.html>
- **S19 — SQLite atomic commit:** durable all-or-nothing state transition under
  interruption. <https://www.sqlite.org/atomiccommit.html>
- **S20 — SQLite WAL:** checkpointing, persistent auxiliary state, reader and
  writer concurrency, and recovery.
  <https://www.sqlite.org/wal.html>
- **S21 — SQLite locking:** cross-process exclusion belongs in the storage
  engine/VFS boundary, not ad-hoc application mutexes.
  <https://www.sqlite.org/lockingv3.html>
- **S22 — SQLite corruption guidance:** fault injection, filesystem behavior,
  hot-journal pairing, and safe backup constraints.
  <https://www.sqlite.org/howtocorrupt.html>

## Protocol, identity, errors, and platforms

- **S23 — HTTP semantics:** conditional requests, safe/idempotent operations,
  retries, and status semantics. <https://datatracker.ietf.org/doc/html/rfc9110>
- **S24 — HTTP problem details:** stable machine-readable service errors.
  <https://datatracker.ietf.org/doc/html/rfc9457>
- **S25 — JSON canonicalization:** deterministic bytes for hashing and signing.
  <https://datatracker.ietf.org/doc/html/rfc8785>
- **S26 — UUIDs:** sortable UUIDv7 identifiers and collision requirements.
  <https://datatracker.ietf.org/doc/html/rfc9562>
- **S27 — Windows app data:** packaged and unpackaged per-user storage and the
  distinction between app data and user data.
  <https://learn.microsoft.com/en-us/windows/apps/develop/data/store-and-retrieve-app-data>
- **S28 — Windows file writes:** completion, cancellation debris, oplocks, and
  serialization of conflicting I/O.
  <https://learn.microsoft.com/en-us/windows/apps/develop/files/best-practices-writing-files>

## Evidence rules

1. Every rewritten implementation, test, manifest, workflow, script, and
   product-contract file cites at least twenty `Snn` identifiers.
2. A verifier rejects missing, unknown, or fewer-than-twenty source IDs.
3. A citation does not prove behavior. Black-box tests and fault campaigns do.
4. Current source versions are pinned in dependency manifests and lockfiles.
5. Generated artifacts identify their generator and inherit its source set.

## Current implementation surfaces

- **S29 — RustCrypto HMAC:** constant-time HMAC construction and verification
  over a compatible digest. <https://docs.rs/hmac/0.13.0/hmac/>
- **S30 — RustCrypto SHA-2:** portable SHA-256 implementation and digest API.
  <https://docs.rs/sha2/0.11.0/sha2/>
- **S31 — FoundationDB Rust client:** current 0.11 client, API initialization,
  transactions, tuples, and feature surfaces.
  <https://docs.rs/foundationdb/0.11.0/foundationdb/>
- **S32 — FoundationDB large-value guidance:** chunking strategies and the
  consequences of exceeding the native value limit.
  <https://apple.github.io/foundationdb/largeval.html>
- **S33 — FoundationDB error codes:** unknown commits, timeouts, cancellation,
  version skew, and size failures.
  <https://apple.github.io/foundationdb/api-error-codes.html>
- **S34 — Braid capability authority:** canonical string capability tokens and
  comparison semantics at the pinned integration revision.
  <https://github.com/srinji-kaggss/Braid/blob/4b1b3e93a6b930bac9533722a212a126cd8e3632/crates/braid-capability/src/lib.rs>
- **S35 — Braid admission proof:** independent canonical admission returning an
  opaque proof bound to capsule, registry, and ambient authority identities.
  <https://github.com/srinji-kaggss/Braid/blob/4b1b3e93a6b930bac9533722a212a126cd8e3632/crates/braid-verify/src/lib.rs>
- **S36 — Braid capsule IR:** canonical capsule bytes, exact registry identity,
  requested grants, budget, confirmation, and evidence contract.
  <https://github.com/srinji-kaggss/Braid/blob/4b1b3e93a6b930bac9533722a212a126cd8e3632/crates/braid-ir/src/capsule.rs>
- **S37 — Reqwest async client:** pooled async HTTP, TLS, redirects, proxies,
  streaming bodies, and cross-platform client behavior.
  <https://docs.rs/reqwest/0.13.4/reqwest/>
- **S38 — Axum service boundary:** async routing, extractors, typed responses,
  Tower middleware, timeouts, and shared state.
  <https://docs.rs/axum/0.8.9/axum/>
- **S39 — Ciborium CBOR:** compact Serde-compatible binary representation and
  bounded reader/writer APIs. <https://docs.rs/ciborium/0.2.2/ciborium/>
- **S40 — Braid runnable invocation:** verifier-owned executable value bound to
  capsule, registry, ambient authority, snapshot, and justification identities.
  <https://github.com/srinji-kaggss/Braid/blob/4b1b3e93a6b930bac9533722a212a126cd8e3632/crates/braid-run/src/lib.rs>
- **S41 — Braid flow snapshot:** immutable content-addressed fact maps and
  three-valued predicate proof semantics.
  <https://github.com/srinji-kaggss/Braid/blob/4b1b3e93a6b930bac9533722a212a126cd8e3632/crates/braid-flow-plan/src/snapshot.rs>
- **S42 — FoundationDB automatic idempotency:** why unknown commits require a
  unique durable transaction identity and exact retry, including limitations.
  <https://apple.github.io/foundationdb/automatic-idempotency.html>
- **S43 — FoundationDB client testing:** real-cluster API testing and
  deterministic failure-injection guidance.
  <https://apple.github.io/foundationdb/client-testing.html>
