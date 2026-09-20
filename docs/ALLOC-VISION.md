# ALLOC Vision: the application ledger of record

> Evidence: S02 S03 S04 S05 cover opaque progress, ordered changes, and
> explicit conflict provenance; S09 S10 S11 S12 S13 cover resumable logs,
> conflict visibility, CRDT causality, local-first ownership, and convergence;
> S14 S15 S16 S17 S18 cover transactional ordering, keyspaces, indirection,
> bounds, and unknown commit outcomes; S19 S20 S21 S22 cover atomicity, WAL,
> locking, and corruption refusal; S23 S24 S25 S26 cover idempotent APIs,
> typed errors, canonical bytes, and identifiers. Source URLs are in
> `evidence/SOURCES.md`.

ALLOC (Application Ledger of record with Ontology, Clocks, and Checkpoints):
data and its meaning share one content-addressed ledger, so every read cites
the immutable changes that produced it.

This is the Logical-DB substrate vision, not a claim that the current semantic
crates are already canonical LEM ORIGIN. ADR-0002 authorizes plane transfer into
this workspace, while the current crates remain bounded migration models until
the v0 BLAKE3 ↔ v1 SHA-256 adapter, source-preserving migration, durable ORIGIN,
and independent handoff journey are proven. The four-domain product map is in
`docs/PRODUCT-MAP.md`: Cloud supplies durable infrastructure, Cairn produces
evidence candidates, LEM decides canonical episodes, and Forge produces receipts
through Cairn rather than silently mutating memory.

## What is data here

- A change is immutable and content-addressed. Episode identity is
  SHA-256 over a v1 framed schema, sequence, kind, scope, all seven clocks,
  evidence, parents, and payload (`crates/logical-db-episode`); CRDT edits
  carry an `OperationDigest` over the change bytes (`README.md` facade example).
  Identity is recomputed on read; mismatch is refused.
- Projection is deterministic. `Projection` maps document state to `IndexValue`
  rows; a database file binds one `ReplicaId` and one schema/projection digest,
  and `migrate_schema` rebuilds indexes in bounded revision-checked pages.
- Bit/byte packing: columnar index rows over redb, 64 KiB episode payload cap
  (`MAX_PAYLOAD_BYTES`), digest-framed content identity (`digest_frame`), and
  the versioned CBOR push/pull wire with hard ceilings on bodies, pages, and
  page counts.
- Von Neumann dodge: applications read and write the embedded replica with no
  daemon, network, FDB, or Braid. Sync moves digests and bounded change pages
  between replicas; full scans stay local or do not happen.

## Three answers

### 1. Federated Cloud Layer

Each app owns an embedded, immediately durable replica (Automerge + redb).
Sync is a bounded, resumable, idempotent push/pull state machine driven by a
durable service checkpoint; one local scope binds one upstream, and a second
upstream fails instead of suppressing writes. The service stores in
FoundationDB behind HMAC checkpoints that advance atomically with the changes
they cover. Audit reads the ledger from any replica; no center holds the only
copy. Offline convergence, cursors, duplicates, reordering, and restart are
proven by the `logical-db-e2e` black-box harness, not by unit tests alone.

### 2. Memory for AI

The episode log records experience; cosine similarity does not. Each episode
carries seven clocks (observed, occurred, valid-from, valid-to, recorded,
derived, sequence), an epistemic tag (explicit, inferred, predicted,
synthesized), and evidence links. Contradictions are first-class tokens:
`contradictions` and `trajectories` in a `LocalWorldRecord` must be covered by
its `evidence` set (`UnprovenReference` otherwise). A LocalWorld answer is an
evidence-subset record with a bounded rendering, so every claim cites the
episode IDs behind it, with validity intervals and provenance intact.

### 3. Ontology contract

The registry is closed and versioned: 12 `TokenKind`s, 9 `LinkRelation`s, 10
modalities, 6 actionability levels (`crates/logical-db-cairn`), all bounds fail
closed, unknown names rejected. Ledger-stored rules (schema/projection digest
binding, deny-first steering, trust-graded memory claims) travel with the data,
so any replica enforces the same contract. No SDK generates the ontology at
read time; porting the registry is copying types plus the ledger, not
re-implementing a proprietary runtime.

## Why bespoke beats rivals: five falsifiable bets

1. Resume costs zero re-send. Kill a push mid-page and restart: bytes
   re-transmitted after the durable checkpoint equal 0. Rival: Spark shuffle
   stage retry and full-partition rescan.
2. Local CRUD needs no network. With daemon, network, FDB, and Braid disabled,
   edit-then-read and index query succeed with p99 under 50 ms on the
   three-platform harness. Rival: Foundry Functions, which require the service.
3. Conflicts survive instead of collapsing. Two causally concurrent writes to
   one key both remain readable via `conflicts`/`conflicts_at`; no
   last-writer-wins getter. Rival: vector RAG top-k merge, which keeps one
   answer and drops provenance.
4. Time travel reproduces exactly. Reading at checkpoint N twice yields
   byte-identical projection rows; retention pruning through N is refused
   unless a state-bearing baseline covering N exists. Rival: Iceberg/Delta
   snapshot reads.
5. Authorization binds payload bytes. Holding the grant fixed while mutating
   any authorized payload field is rejected; exact-byte retry still succeeds
   idempotently. Rival: Palantir Actions gate and any caller-supplied digest
   admission.

## End-state competir check

| Rival | Steal | Refuse |
|---|---|---|
| Palantir Foundry | Object/link-type ontology with a closed registry; Actions-style write gate; branchable views (checkpoints plus baselines give named, resumable history) | OSDK lock-in: ontology enforcement that only runs inside the vendor runtime; center-held audit copy |
| Apache (Arrow/Parquet + Iceberg/Delta + Spark) | Columnar layout with zero-serde reads; Parquet-style chunking with digest framing; snapshot time travel with pruned-history recovery | Shuffle-heavy compute that moves scans to data instead of caching local replicas; von Neumann tax of re-reading full partitions per query |

## Roadmap pointers

- LEM-2: wire episode operators to the service path; INGEST-1: deterministic
  projection from episodes into indexed state; ownership boundary per #30.
- Bind authorization to server-computed canonical action bytes; separate intent
  digest from wire-action digest per #27.
- Make retention recovery state-bearing: seal a versioned baseline before any
  prune advances the floor; fresh replicas install baseline then resume per #28.
- Datalog projections over the episode log as a read path beside current
  `Projection` index rows.
- FDB multi-node recovery and sustained-load proof before any
  production-complete claim.
