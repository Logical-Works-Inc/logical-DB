# FRANKENSTEIN — organ map for the logical-DB kernel

Which existing engine owns each organ we graft, with the bytes that prove
it, and which gap remains open after the graft. Written 2026-08-21 from
clones under `~/wwfd/dbsweep`; web-grounded claims carry their source.
Donor ideas are re-implemented, never vendored; licence floor stays in
GOVERNANCE §6 (Apache/MIT donors only land on default paths).

## Organs → homes

- Framed journal with batch markers — fjall `src/journal/writer.rs`
  (`write_start`:237, `write_end(checksum)`:248, `write_batch`:326)
  → kernel/journal.rs (DB-A).
- Journal rotation + persist modes — fjall `writer.rs:66`, `:203`
  → DB-C.
- Immutable segments — qdrant
  `segment_constructor/segment_builder.rs:61` + `:604`
  (SegmentBuilder, ImmutableIdTracker) → DB-C segments.
- Version-ordered commit — FDB `architecture.rst:97`: tlogs receive
  commits in version order, fsync the append-only mutation log, then
  ack → kernel journal ts ordering (DB-A).
- Conflict window — FDB `architecture.rst` Resolvers §: conflict =
  read-key written between read and commit version → DB-D txns.
- Hot/cold hydration — FDB `architecture.rst` Storage Servers §: hot
  window in memory, disk state as-of-window-ago → DB-E.
- Versionstamped op-log keys — FDB `api-c.rst:984-1000`:
  set_versionstamped_key/value (+ tuple warning) → DB-E sync op log.
- Snapshot + change-log restore — FDB `backups.rst` Introduction §:
  inconsistent copy plus change log replayed to a point in time
  → DB-E restore.
- Savepoint tracking — redb `src/transaction_tracker.rs:15,38,102`
  (TransactionId, SavepointId, TransactionTracker) → DB-D.
- Durable-before-restore hazard — redb `src/db.rs:644-647`: restoring
  an ephemeral savepoint over non-durable pages corrupts → INV-G5.
- Injectable clock — turso `core/mvcc/clock.rs:9`: LogicalClock trait,
  publish-under-lock closes TOCTOU → kernel/port.rs ClockPort (DB-A).
- u64-packed tree keys — arroy `src/key.rs:9,140`: whole node key
  bit-packs into one u64 for the KV → DB-B vec-code packing.
- FDB shim (no vendored engine) — `kernel/src/fdb.rs` is a
  redb-backed durable shim that delegates to the pinned
  `foundationdb` 0.11.0 / fdb-7_3 binding when `libfdb_c` is present
  → F1; the binary has zero new vendored engine, only the port we
  own.
- Opaque tuple/contains — `kernel/src/tuple.rs` estate/subspace
  codec + `kernel/src/contains.rs` bounded `contains_many` → F2/#16;
  integrity framing is storage-internal (§1 CRC32, tuple codec),
  never a Braid CID.

## Internal donors (the constellation's own)

`~/secure-authority` is the security workspace; its crates are the first
place to borrow before any external dependency for:

- DiD gauntlet (G4, spec §11) — `secure-identity`, `secure-core`.
- Claim-class receipts and provenance rows — `secure-audit`,
  `secure-audit-verify`.
- At-rest segment crypto — `secure-vault`.
- Hardware-key provider pattern (TPM/Secure Enclave) — `secure-provider-*`.

## Dump state (organs/)

The donor engines now compile and run under `organs/src/dump.rs`, one
functional smoke each: fjall 3, redb 4, arroy 0.8 (+heed), fst,
petgraph, memmap2, half. INV-ORGANS-COMPILE-AND-RUN: a donor that
cannot pass here is recorded below, never silently dropped.
`foundationdb` 0.11.0 (fdb-7_3) is pinned but `libfdb_c` is not vendored.
`LogicalDb` is the redb-backed embedded profile; `OutboardDb` is the actual
native client. They share the admitted request and key model rather than
pretending one backend delegates to the other.

## Sync-layer donors (web-grounded)

- CloudKit: per-zone opaque change token; fetch changes since token;
  conflict surfaces as serverRecordChanged carrying client/server/ancestor
  record copies — merge into the server copy, then re-save. Engine-shaped
  batching in CKSyncEngine.
  Sources: developer.apple.com/documentation/cloudkit/cksyncengine and
  developer.apple.com/documentation/cloudkit/ckerror/code/serverrecordchanged
- Litestream: LTX files carry a monotonically increasing TXID plus
  checksums; tiered compaction L0 → L1 (30s) → L2 (5m) → L3 (1h) →
  snapshot (24h); restore takes the newest snapshot that does not
  overshoot the target, then replays LTX files in TXID order; a TXID gap
  fails the restore outright.
  Source: litestream.io/how-it-works
- Our shape: account/container/zone is the scope prefix; each admitted commit
  atomically writes an opaque, versionstamped zone change token. Tokens page a
  bounded change feed and are persisted by the client. Record-level merge
  policy and token retention remain explicit gaps.

## Gaps (closure order)

- G1 spine v0 (DB-A): Row/Class, content addressing, KvPort + MemKv,
  framed CRC32 journal with torn-tail truncation, caller-supplied ts via
  ClockPort, batch = one frame so replay is all-or-nothing. This turn.
- G2 vector scan (DB-B): RaBitQ 1-bit codes, popcount scan, top-K
  asymmetric rescore; arroy fixes the KV packing shape.
- G3 term + adjacency (DB-C): TermIndex/Adjacency ports; immutable mmap'd
  segments per qdrant; fjall-style rotation.
- G4 txns + savepoints (DB-D): FDB conflict window, redb tracker, the
  durable-before-restore invariant, INV-SIMULATE-THE-SPINE harness.
- G5 scale-out adapter (DB-D/E): native `OutboardDb` compiles; live FDB and
  LEM parity receipts remain.
- G6 sync layer (DB-E): zone prefixes, opaque durable tokens, bounded change
  pages, and atomic operation markers landed. Retention, notifications,
  record merge policy, and restore remain.
