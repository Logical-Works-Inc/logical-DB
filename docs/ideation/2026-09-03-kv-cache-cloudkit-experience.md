---
date: 2026-09-03
topic: kv-cache-cloudkit-experience
focus: Deeply research CloudKit how apps talk to it, look at logical-db and finish the experience for interaction with dumb kv-cache system; evaluate logical-db vs Cairn vs LEM vs new repo
mode: repo-grounded
---

# Ideation: KV-Cache × CloudKit Experience — Finishing the Dumb Cache

## Grounding Context

### Codebase Context (logical-db v3)

**Shape:** 14-crate workspace, one-way DAG (`logical-db` facade → `logical-db-model` → `logical-db-crdt`/`logical-db-store`/`logical-db-sync`), `resolver=3`, Rust 1.98, `unsafe_code=forbid`. Verifier enforces crate DAG, evidence tags S01–S43, one-way crate DAG, no legacy `kernel`.

**Public surface (small, deliberate):**
```text
Database::open(path, scope, replica)
Database::open_default(app_id, scope, replica)
Database::open_with_schema(path, scope, replica, version, projections)
Database::edit(document, operation, requestDigest, |editor| …)
Database::read(document, |reader| …)
Database::query_index(index, value, after, limit)
Database::changes_since(afterSequence, limit)
Database::migrate_schema(version, projections, batch)
Database::synchronize(peer, transport, limits, cancelled)
Reader::{get, get_index, conflicts, conflicts_at, keys, len, text}
Editor::{put, delete, list ops, text splice, counters}
Projection::new(indexId, |reader| -> Option<IndexValue>)
```
Storage bytes never leak: scope is `(AccountId, SpaceId)`, private key axis is `("LDB\x03", account, space, kind, suffix)` with kinds snapshot/change/operation/outbox/index/reverse-index/event/migration/upstream. `OperationId` + `OperationDigest` give exact retry identity; duplicate returns `EditOutcome::Duplicate` without rerunning editor.

**Sync:** transport-neutral `ReplicaPort` + `Transport` trait. `PushRequest`/`PullRequest`/`PullPage` are versioned CBOR (`application/vnd.logical-db.v1+cbor`), bounded (`MAX_CHANGES_PER_PAGE=256`, `MAX_PAGE_BYTES=8MB`, `MAX_PAGES_PER_CYCLE=32`, `MAX_CHANGE_BYTES=1MB`, `MAX_SNAPSHOT=16MB`). HMAC checkpoint (`from_wire`/`as_wire`, 4KB max) advances atomically with installed pages. `SyncReport{pushed,pulled,pages,more,full_resync}`. One durable upstream binding per scope (v1 global upload cursor) — second upstream fails closed.

**Pain points for KV-cache UX:**
- No KV abstraction; app must invent `DocumentId` per key, manage `OperationId`/`OperationDigest` per write, wire projections for indexes by hand.
- `edit` requires typed Editor dance: `editor.put(&Object::root(),"kind", Scalar::String(...))` — noisy for `cache.set("foo", 42, ttl=60s)`.
- No TTL, no eviction, no LRU, no size tiering — dumb cache is literally dumb.
- No zone-like independent sync domains; all docs share one checkpoint stream.
- `changes_since` exists but no push-subscription ergonomic; app must poll.
- Large values hit `MAX_CHANGE_BYTES` ceiling with no CKAsset-style offload.
- `REWRITE_MANIFEST.md` still blocking: no GC/retention, no tombstones, no export/restore journey; LEM external consumption not yet proven.

**Leverage points:**
- `Projection` is deterministic and schema-bound — perfect for secondary indexes like `ttl_expires_at` or `lru_seq`.
- `DocumentRevision` + `MigrationPage` give bounded, crash-resumable scan — can drive eviction.
- `ReplicaPort::pending` / `acknowledge` / `install` already isolate sync state machine — can be wrapped by a higher-level engine without forking storage.
- `logical-db-platform` gives per-OS data root; `logical-db-http` gives pooled TLS/CBOR transport — no new infra needed for cloud tier.

### Topic Context (CloudKit — deep research, high certainty)

Sources: Apple CloudKit design docs (`developer.apple.com/icloud/cloudkit/designing/`), `CKSyncEngine` docs (iOS 17+, WWDC23 10188), VLDB 2018 paper "CloudKit: Structured Storage for Mobile Applications" (Shraer et al.), VLDB "FoundationDB Record Layer" (Chrysafis et al., arXiv:1901.04452), and `sample-cloudkit-sync-engine` + `sample-cloudkit-privatedb-sync`.

**Containers → Databases → Zones → Records — the canonical hierarchy:**
- **Containers** are silos per app (`CKContainer`), can be shared across apps via entitlements. Each container has its own schema and Console (development vs production promotion, forward-only: can add fields, cannot delete types/fields).
- **Databases:** three per container: `public` (world-readable, app quota, one default zone only), `private` (per-user, encrypted, quota = user), `shared` (windows into owners' private zones after share acceptance). RBAC on public: `World` / `Authenticated` / `Creator` with `read|write|create` additive.
- **Zones:** logical shard within a database. Public has only default zone; private has default + many `CKRecordZone` custom zones; shared has no default (zones appear on share accept). Each zone has its own `CKServerChangeToken` / sync index. Custom zones enable: atomic batches (`CKRecordZoneCapability.atomic` → all-or-nothing), grouping by update-rate, and per-zone sync (avoid syncing everything). Share zones carry a `share change index` for participant sync.
- **Records:** `CKRecord` is a typed dictionary (`recordType` + `[key: CKRecordValue]`). Types: `String`, `Int64`, `Double`, `Date`, `Data`, `Location`, `Reference` (with `deleteSelf`/`none` ownership), `Asset` (file-backed, not inline, temp-file upload/download), and encrypted `EncryptedValue`. Size: record data ≤1MB; asset unbounded but chunked. Each record has `recordID = (zoneID, recordName)` + `recordChangeTag` + `modificationDate` + `creationDate` + parent references for hierarchy sharing.

**How apps talk to it — two eras:**

*Classic API (still current for direct control):*
- `CKDatabase.save(record)` / `fetch(withRecordID:)` / `CKQueryOperation(predicate, sortDescriptors)` / `CKModifyRecordsOperation(recordsToSave, recordIDsToDelete)` with `savePolicy: .ifServerRecordUnchanged | .changedKeys | .allKeys`.
- Batching via `NSOperation` dependencies; server limit ~400 records or 2MB per batch historically.
- Queries are Solr-backed, eventually consistent (async indexing, not blocking writes). Record fetch is strongly consistent.

*Modern sync engine (preferred since WWDC23):*
- `CKSyncEngine(database: CKDatabase)` — one per database, created at launch. Owns **opaque persisted state** (zone tokens + pending changes) that the app must persist across launches.
- App calls `state.add(pendingRecordZoneChanges:)` / `pendingDatabaseChanges` to enqueue; engine auto-schedules when conditions good (account, network, battery). App implements `CKSyncEngineDelegate`: `nextRecordZoneChangeBatch(_:syncEngine:) -> CKRecordZoneChangeBatch?` (engine pulls batches of ≤250 records), `handleEvent(_:syncEngine:)` for `sentRecordZoneChanges`, `fetchedRecordZoneChanges`, `willFetchChanges`, `didFetchChanges`, `stateUpdate`, `accountChange`.
- **Automatic vs manual:** `fetchChanges()` / `sendChanges()` for pull-to-refresh / backup-now.
- Handles transient errors itself (`notAuthenticated`, `networkFailure/NetworkUnavailable`, `requestRateLimited` with respect for `retryAfter`, `zoneBusy`, `serviceUnavailable`); app handles `serverRecordChanged` (conflict).
- **Subscriptions as push-hints:** `CKDatabaseSubscription`, `CKRecordZoneSubscription`, `CKQuerySubscription(predicate, options: firesOnRecordCreation/Update/Deletion, recordType)`. Subscriptions create server-side predicate; APNs pushes silent notification (`CKNotification`). Lost notifications recovered via `notification sync` (reuse of sync index) + `CKFetchNotificationChangesOperation`. `CKSyncEngine` auto-creates a `CKDatabaseSubscription` if none exists.
- **Sharing:** share record `CKShare` (participants list with `acceptanceStatus: invited|accepted|removed`, `permission: readWrite|readOnly`, `publicPermission`). Identifier transactionally added to root record via batch. Hierarchy: share identifier on root, descendants inherit; sharing is recursive, single-record change to share/unshare hierarchy. Permissions enforced via permissions index;participant sync via share change index proxied to owner partition → async message → database subscription → meta-sync → zone sync.

**Server internals (VLDB certainty):**
- Cassandra + Solr. Per-zone **sync index** is ordered log `(version, recordID)` of latest version only; GC when no client needs it. `meta-sync` enumerates zones changed since token. `reverse sync` scans sync index backwards from latest for fast cold-start (e.g., last hour of messages) then forward. `snapshot sync` uses auxiliary index `(recordID, snapshotVersion)→version` to provide consistent prefix without scanning superseded changes; requires retaining old versions.
- **Record Layer mapping:** every CloudKit user database is a Record Layer logical tenant (isolated subspace), nested record types, indexes on commit version, multi-type indexes, stateless routing. Schema immutability: record type definition `(typeID, typeDefinitionID)` + schema `(schemaID, parentSchemaID)` versioned chain, CAS on latest pointer, cached at request + instance level (hit rates 0.9–0.995).
- **P95/Median latencies:** record read ~18ms median, query/sync ~50ms median; save latency sublinear with batch size; CAS zone serialization <0.1% contention for most apps when zones are partitioned well.

**What CloudKit deliberately does NOT do (and why it matters for kv-cache):**
- Not a local-cache replacement; framework does not maintain local store — app converts records ↔ model objects, decides fetch timing/order, handles errors.
- No automatic disconnected ops beyond last-known snapshot; sync is explicit.
- `NSUbiquitousKeyValueStore` exists for tiny KV (≤1MB per app, ≤1024 keys) but is separate from CloudKit records — many apps confuse them.

**Cairn / LEM landscape (for placement decision):**
- **Cairn:** Rust workspace, `wm-core` schema `nodes(id, domain, repo, kind, path, content_hash BLAKE3-256, embedding BLOB nullable, epistemic/authority/licence)`, FTS5 + triggers, `edges(source,target,relation)` with only 3 relations live (`contains` 99.7%), `projection_lineage` for derived nodes, `smb` fact store + causal tape. Local-only, SQLite, no sync service, no Braid. 10k LOC, 230 keel findings, zero action plane — semantic memory, not state sync.
- **LEM:** no repo located at search depth; presumed to be Logic OS local entity model or similar. Treat as hypothetical host layer that could consume logical-db as embedded replica (external facade journey per `REWRITE_MANIFEST` blocking gate).

---

## Topic Axes

1. **Access Ergonomics** — how a developer names, types, and scopes a cached value (keys, namespaces, types, TTL) without touching DocumentId/Editor/Projection directly.
2. **Sync & Scope Isolation** — how a kv namespace maps to a sync domain (zone analog), its checkpoint, batch budget, and whether it syncs independently of other data.
3. **Durability & Size** — how values of varying size/TTL/eviction live within redb + Automerge limits (1MB change, 16MB snapshot) and survive crash/kill.
4. **Reactivity & Push** — how a cache consumer learns something changed (poll vs watch vs subscription vs silent push) and stays off the hot path.
5. **Tenancy & Sharing** — how private vs shared vs public cache slices map to Scope/Space and Braid authority, and whether a kv entry can be shared hierarchically.

*Decomposition not skipped — 5 orthogonal axes derived from grounding.*

---

## Ranked Ideas

### 1. KVFacade: Deterministic DocumentId Map with Typed TTL & LRU Projection

**Description:** New crate `logical-db-kv` (or module inside `logical-db`) exposing CloudKit-idiomatic `KvCache`:
```rust
pub struct KvCache { db: Database, map: MapId /* = SpaceId+MapName */ }
impl KvCache {
  pub fn put<V: KvValue>(&self, key: &str, value: V, ttl: Option<Duration>) -> Result<EditOutcome<()>, KvError>;
  pub fn get<V: KvValue>(&self, key: &str) -> Result<Option<Cached<V>>, KvError>;
  pub fn delete(&self, key: &str) -> Result<…, KvError>;
  pub fn keys(&self, prefix: &str, after: Option<&str>, limit: usize) -> Result<Vec<String>, …>;
  pub fn watch(&self, after: Option<u64>) -> Result<Vec<KvEvent>, …>; // wraps changes_since
}
```
Internals: `key → DocumentId` via `UUIDv7(ns = hash(mapName), name = key)` (deterministic, collision-free, matches Cairn BLAKE3 idea but uses UUIDv7 for logical-db identity). Each document is a tiny Automerge map `{v: valueBytes, t: expiresAtMs, s: seq}`. Two projections: `idx_ttl = Timestamp(expiresAt)` and `idx_lru = Unsigned(lruSeq)`. Background `evict_expired(limit)` scans `idx_ttl` range. `KvValue` trait for `Scalar` primitives + `serde` CBOR blob values (validated < 800KB to leave margin under 1MB). Hides `OperationId::new()` + `OperationDigest(sha256(canonical(key,value,ttl)))` generation.

**Axis:** Access Ergonomics (primary), Durability & Size (secondary)

**Basis:** `direct:` `logical-db/src/lib.rs:148 Database::edit(document, operation, request, |editor| editor.put(&Object::root(),"kind",…))` — current edit requires 4 args + closure per field; no TTL/index plumbing exists. AND `logical-db-model/src/lib.rs:IndexValue::Timestamp(i64)` already ordered — TTL index is free.

**Rationale:** Drops the dumb-cache tax: no DocumentId invention, no projection boilerplate, no manual eviction loop. CloudKit's lesson is that apps treat records as typed dictionaries, not raw ops; this gives records-as-dict ergonomic for KV. TTL projection makes expiry a range query, not a full scan — matches how CloudKit's sync index orders by version.

**Downsides:** Adds a crate and a mapping decision (hash → DocumentId must be stable forever). Migrating existing raw DocumentId users to hashed keys is breaking. Serialization choice (CBOR vs JSON) locks value evolution.

**Confidence:** 88%

**Complexity:** Medium

---

### 2. ZoneRegistry: KV Namespaces as Independent Sync Domains (CloudKit Zones analog)

**Description:** Formalize `MapId / NamespaceId` as a first-class scope partition *within* `SpaceId`, each with its own `PeerId→Checkpoint` and sync limits. Like CloudKit private custom zones, each namespace syncs independently: app can `synchronize_only(namespace="image-cache")` or background-sync only `["session","prefs"]` frequently while `["archive"]` syncs on Wi-Fi. Internally either (a) sub-scope via `(account, space, namespace)` tuple prefix (if model tolerates) or (b) namespace-tagged DocumentId prefix with per-namespace checkpoint stored as `kind=16` sidecar. Expose `SyncPolicy{ changes_per_page, bytes_per_page, pages_per_cycle, requiresNetwork: .wifi|.any, batchAtomic: bool }`. Reverse-sync for cold device: `sync(since=None, mode=Reverse(limit=100))` returns latest N keys first, then forward-fills — directly ported from CloudKit reverse sync.

**Axis:** Sync & Scope Isolation

**Basis:** `external:` CloudKit VLDB §5.2–5.3 — custom zones for grouping by update rate + per-zone `CKServerChangeToken` + reverse/snapshot sync. AND `logical-db-sync/src/lib.rs:22 MAX_PAGES_PER_CYCLE=32` — bounded sync already page-oriented, just needs namespace filter.

**Rationale:** The "dumb kv-cache" today shares one checkpoint for all keys; a chatty namespace (e.g., `presence`) forces full scan of cold `assets` namespace. CloudKit's most-used private-database pattern is exactly this: isolate hot vs cold zones. Mapping it 1:1 gives apps the same control without inventing a new primitive. Also unlocks atomic batches per namespace (CloudKit `atomic` zone) for transactional multi-key puts.

**Downsides:** Requires either model change or checkpoint address scheme change — both touch `REWRITE_MANIFEST` durability proofs. Per-namespace checkpoints double storage if many small maps. Policy matrix explodes if not opinionated.

**Confidence:** 82%

**Complexity:** High

---

### 3. KvSyncEngine: CKSyncEngine-Style Delegate that Owns State, Batching & Retry

**Description:** Port `CKSyncEngine` lifecycle 1:1 for logical-db. New `KvSyncEngine<State, Delegate>` where `State` is persisted opaque blob (pending namespaces + per-namespace checkpoint + retryAfter) in a sidecar file (like CKSyncEngine's `StateUpdate` archiving). Delegate trait:
```rust
trait KvSyncDelegate {
  fn nextPutBatch(&mut self, scope: Scope, namespace: &str, budget: BatchBudget) -> Option<Vec<PutEnvelope>>;
  fn handleEvent(&mut self, event: SyncEvent); // SentPut{ns, keys}, Fetched{ns, events}, StateUpdate(persist), AccountChange, RateLimited(until)
}
```
Engine wraps `logical-db-sync::synchronize`, owns `push_pending` batching (≤250 keys / 4MB like CloudKit), observes `cancelled()` between atomic ops, handles transient `TransportError` with exponential backoff respecting `Retry-After`, and auto-schedules on `changes_since` growth. Provides `fetchNow()` / `sendNow()` for pull-to-refresh / backup-now (mirrors `fetchChanges`/`sendChanges`). Handles `CheckpointExpired → full_resync` automatically without leaking `ProtocolError::InvalidCheckpoint` to app.

**Axis:** Sync & Scope Isolation (primary), Reactivity & Push (secondary)

**Basis:** `external:` `developer.apple.com/documentation/cloudkit/cksyncengine-5sie5` + WWDC23 10188 — state persistence, `nextRecordZoneChangeBatch`, transient error auto-retry, `accountChange` event. AND `direct:` `logical-db-sync/src/lib.rs:240 synchronize()` already returns `SyncReport{more, full_resync}` + `SyncError::Rejected|Protocol` — the engine is the missing orchestrator.

**Rationale:** CloudKit's biggest DX win in 2023 was admitting "sync is a state machine you shouldn't reimplement." logical-db today leaves that state management to the app (`pending() → push → acknowledge → checkpoint → install` plus retry). The kv-cache's dumbness is partly that every consumer re-solves backoff, token persistence, and `more` looping. A SyncEngine makes KV behave like CloudKit's private DB: enqueue + forget, pull when notified.

**Downsides:** Yet another abstraction that can drift from core sync semantics. Must be tested against real `HttpTransport` + FDB to prove token persistence not lossy (same concern as `REWRITE_MANIFEST` FDB journey). Adds background scheduling policy debate.

**Confidence:** 85%

**Complexity:** Medium

---

### 4. Blob Offload: CKAsset-Style Tiering for Large Cached Values

**Description:** Transparent tiering: if `serialized(value).len() > 256*1024` (tunable), store bytes as content-addressed blob outside redb — `~/.logical-db/assets/{blake3hash}` + sidecar ref `{cid, size, sha256}` in CRDT map. `get` streams via `BlobStore::open(cid) -> impl Read`. Sync chunks blobs like FDB adapter's 64KiB change chunks (`(5, invocationDigest)` pattern) but at app layer, with HMAC-signed download URLs if served via HTTP. Small values stay inline (fast path). Eviction deletes blob file transactionally after CRDT delete commits. Mirrors `CKAsset` (file-backed, not inline, temp URL, not counted in 1MB record limit).

**Axis:** Durability & Size

**Basis:** `direct:` `logical-db-model/src/lib.rs: MAX_CHANGE_BYTES=1_048_576` + `MAX_SNAPSHOT_BYTES=16_777_216` — hard ceilings that make "just put large JSON" fail closed. `reasoned:` without tiering, KV's natural use (image thumbnails, precomputed tiles, LLM KV-cache chunks) cannot live in logical-db at all, forcing app-side split.

**Rationale:** CloudKit's 1MB record limit + CKAsset exemption is the precedent for "structured metadata inline, bytes out-of-line." logical-db has the same shape: CRDT change is the log entry, snapshot is the merge — both are size-bounded for deterministic replay. Tiering keeps the hot path (metadata + indexes) in redb/Automerge and moves cold bytes to filesystem/FDB chunk table, preserving crash-resume and migration guarantees.

**Downsides:** Two-phase commit (CRDT + blob file) risks orphan blobs on crash — needs GC sweep like FDB chunk orphan check. Sync ordering (blob before ref vs ref before blob) must avoid "valid ref, missing bytes." Adds platform file-path handling.

**Confidence:** 79%

**Complexity:** High

---

### 5. WatchRegistry: Subscription-Like ChangeFeed with Predicate Push Hints

**Description:** Expose `changes_since` as `watch(filter, replayAfter) -> Stream<KvEvent>` where filter is an `IndexValue` predicate (e.g., `prefix="user:"`, `namespace="chat"`, `ttl>now`). Server side maps to CloudKit's three subscription kinds: `ZoneSubscription` (whole namespace), `DatabaseSubscription` (any namespace changed → meta-sync), `QuerySubscription` (predicate → Solr analog via `query_index`). Client optionally registers push token; service sends APNs/FCM silent hint "namespace X changed" (not payload) — app then `fetchChanges`. Lost hints recovered by `watch` polling + stored checkpoint, just like CloudKit `CKFetchNotificationChangesOperation` + `notification sync` reusing sync index. Local-only mode uses OS file watcher / `notify` crate on redb WAL.

**Axis:** Reactivity & Push

**Basis:** `external:` CloudKit VLDB §7 — zone/database/query subscriptions, push via APNs best-effort, `notification sync` as fallback. AND `direct:` `logical-db/src/lib.rs: Database::changes_since` + `logical-db-store: ChangeEvent{sequence, document, digest}` already durably ordered — the primitive exists, only the typed watch + hint layer missing.

**Rationale:** Today's kv-cache is poll-only (`loop { changes_since }`), wasting battery and missing cross-device promptness. CloudKit's split — subscriptions are server-side predicates, notifications are hints, sync is the data plane — is precisely the separation logical-db needs: keep HMAC checkpoint as truth, add subscription registry as hint accelerator. First-party CloudKit apps went from 19% sync-read to 57%+ after adopting it; that's the DX gap to close.

**Downsides:** Push infrastructure is the hard part — requires APNs creds, token lifecycle, and a new service endpoint. Without push, it's just a nicer poller. Query subscriptions need index planning (which predicates are indexed).

**Confidence:** 74%

**Complexity:** Medium

---

### 6. Envelope Versioning: Forward-Compatible Value Schema (CloudKit Immutable TypeDefs analog)

**Description:** All KV documents store `envelope { v: u16, payload: CBOR<serde_value>, migrated_from?: u16 }`. Projection for secondary indexes reads `payload.field` with missing-key grace (returns `None` if absent). Migration rule: `KvCache::migrate` declares additive fields only — new app version writes `v2` with new field, old app reads `v1` shape and treats missing field as `None`. Like CloudKit's immutable `RecordTypeDefinition` + `schema(parentSchemaID)` chain and Dashboard promotion (can add field, cannot delete type/field in production), enforce via `Projection` digest: changing projection that drops a field requires `migrate_schema` with bumped `schema_version`, not silent edit. Provide `#[derive(KvValue)]` that generates `try_from_cbor` with `#[kv(default)]` for missing keys.

**Axis:** Access Ergonomics (secondary), Durability & Size (secondary)

**Basis:** `external:` CloudKit VLDB §8 — type definition immutability, schema chain, CAS on latest schema, forward-only promotion. AND `direct:` `logical-db/ARCHITECTURE.md: Projection digest bound to scope` + `logical-db-store: bind_schema` fails closed on drift until `migrate_schema`.

**Rationale:** KV caches evolve fast (add `compression`, `etag`, `modelVersion` fields). Without versioned envelopes, a single schema change breaks old clients on same sync stream — CloudKit's most expensive production bug class. Envelope versioning makes KV forward-compatible by construction, and reuses logical-db's already-correct schema-migration machinery.

**Downsides:** Envelope overhead on tiny values (e.g., `bool`). Forces CBOR vs JSON decision. Derive macro adds proc-macro crate.

**Confidence:** 81%

**Complexity:** Low

---

### 7. ShareSlice: Hierarchical ShareRecord for Shared KV Namespaces

**Description:** For collaborative caches (shared shopping list, shared tile cache), introduce `ShareRecord` document per shared root key (like CloudKit `CKShare`). Structure:
```text
rootKey = "list:groceries"  ->  map { _share: shareId, owner: AccountId }
shareId document -> { participants: [{userRecordID, permission: ro|rw, status: invited|accepted}], publicPermission, shareUrl }
```
Child keys `list:groceries/*` inherit share permission by traversing up (find ancestor with `_share`). `put` checks `participants.contains(caller) && permission==rw`. Zone analogy: shared namespace appears in participant's `shared` database as window into owner's zone (filtered view). Sync for shared namespace goes via owner's partition share-change index (VLDB §6) — participant `pull` proxied to owner. Add `CKShare`-style accept flow: owner creates share + URL, participant `acceptShare(url)` creates participant status `accepted` via cross-partition RPC, zone appears.

**Axis:** Tenancy & Sharing

**Basis:** `external:` CloudKit VLDB §6 — share record with participant list (100 max), acceptance via URL, zone auto-added to shared DB, hierarchy inheritance, share change index proxied. AND `direct:` `logical-db-model/src/lib.rs: Scope{account, space}` already isolates tenancy — share slice is the bridge for cross-scope read without widening Scope.

**Rationale:** The dumb kv-cache today is single-tenant (`AccountId` scoped). Any sharing requires manual out-of-band permission table. CloudKit's design solves three hard problems at once: (a) one-record mutation to share a whole hierarchy, (b) filtered view (non-shared records invisible), (c) formal accept lifecycle. That maps cleanly to KV: sharing a prefix by setting one share marker.

**Downsides:** Cross-partition RPC, permission index, and share-change index are service-heavy; embedded-only mode must stub them. 100-participant limit must be justified. Revocation/GC of shared views is subtle.

**Confidence:** 67%

**Complexity:** High

---

## Rejection Summary

| # | Idea | Reason Rejected |
|---|------|-----------------|
| 1 | Raw `redb::TableDefinition` KV beside redb store (bypass CRDT) | Breaks convergence & sync — two stores diverge, reintroduces last-writer conflict the CRDT was built to eliminate. |
| 2 | Per-key CRDT Counter/List instead of map value | Massive Automerge history per counter, no TTL index; overfits tiny subset of KV workloads. |
| 3 | Fork Cairn's SQLite FTS5 as generic KV store | Cairn is single-node SQLite doc-graph, no sync/FDB/Braid; loses durability + multi-device convergence proofs. Suitable for index, not state substrate. |
| 4 | New standalone KV service from scratch (no logical-db) | Discards 105 passing tests, mutation-killed deltas, and FDB+Automerge proof; re-invents bounded sync incorrectly. "Build new" is temptation, not advantage. |
| 5 | Global single checkpoint for all namespaces (keep dumb) | Defeats CloudKit's top lesson: hot/cold isolation. One chatty namespace starves all; CAS zone contention measured 0.1–1% when not partitioned. |
| 6 | Inline 4MB values with MAX_CHANGE_BYTES bump | Breaks protocol bounds (`MAX_PAGE_BYTES=8MB`, FDB 100KB value limit chunking) and snapshot determinism; correctness ceiling, not tunable. |
| 7 | Poll `changes_since` in tight loop for reactivity | Battery/bandwidth pathological at CloudKit scale (100TiB logs/day); push-hint + pull model is strictly more efficient with same correctness. |
| 8 | Serialize values as JSON text in CRDT Text type | Text splice is Unicode-code-point indexed, not opaque bytes; wrong conflict merge (interleaved edits) and no ordered index on serialized form. |
| 9 | Use `NSUbiquitousKeyValueStore` mental model directly | CloudKit KVS is ≤1MB/1024 keys toy store; conflates two stores. Apps outgrow it immediately — cannot host real KV caches. |
| 10 | Place project in LEM repo without logical-db | LEM external consumption is still a blocked gate per `REWRITE_MANIFEST.md` ("Package and consume facade from LEM… without importing internal crates" unchecked). Would repeat retrograde LEM tape with internal-crate coupling. |
| 11 | Build own push notification service for hints | APNs already exists; rebuilding is non-goal. Correct layer is subscription registry + hint, not raw push. |
| 12 | Delete-and-recreate record type on schema change | CloudKit forbids this: typeDefinitionID differs, old data not returned. Would strand cached values permanently. |
| - | axis: Tenancy & Sharing partial — accept via email/phone lookup placeholder | Deferred until lookup service identity story exists; email/phone→UserRecordID lookup is CloudKit-specific to Apple ID — not portable without identity provider. |

---

## Placement Verdict (logical-db vs Cairn vs LEM vs new)

- **Start in `logical-db`** (winner for this work): add `crates/logical-db-kv` as facade+sync companion, not a fork. Keeps DAG clean (`logical-db-kv → logical-db-model / logical-db-crdt / logical-db-store` or just `logical-db`), inherits redb/FDB/Braid proofs, PR stays mergeable under `REWRITE_MANIFEST` mechanical checks. Migration for TTL/LRU indexes uses existing `migrate_schema`. Demo via `logical-db-e2e` harness + real `HttpTransport`.
- **Do not start in Cairn:** Cairn is the semantic memory graph (nodes/edges/FTS5/embeddings), quarantine Python ontology, no sync/Braid/FDB. Borrowing its KV would couple provenance graph to ephemeral cache lifecycle. Use Cairn only as **read-side indexer** for cached content once KV exists (e.g., index cached docs for retrieval).
- **LEM is consumer, not host:** Once `logical-db-kv` ships, LEM consumes it as external crate via path+crates.io (fulfills the unchecked manifest gate). Do not implement KV inside LEM.
- **New repo only if** you need a standalone daemon distribution (public TLS, mobile sandbox, multi-node FDB restore) beyond `logical-db-daemon` — not for KV semantics. For now, new repo is premature.

---

## Next Step (choose one to enter `ce-brainstorm`)

- **If you want the finished dumb-cache experience fastest:** pick **Idea 1 (KVFacade)** alone — it turns `cache.put("k", v)` from 5 lines + DocumentId ceremony into one line and is shippable without service changes.
- **If you want CloudKit-grade sync feel:** pick **1+2+3 (Facade+ZoneRegistry+SyncEngine)** as a single track — this is the trio that makes the cache feel not-dumb.
- **If you have large values today:** include **4 (Blob Offload)** in the first slice — otherwise you ship a cache that cannot cache.

> CloudKit certainties cited: container/database/zone/record hierarchy, 3 DB kinds, custom-zone atomicity, CKAsset exemption, CKSyncEngine delegate + 250-record batch + state persistence + transient auto-retry, 3 subscription kinds → APNs hint → sync, reverse/snapshot sync auxiliary index, share-record hierarchy inheritance, immutable type-definition + schema chain. Record Layer: stateless multi-tenant FDB isolation, indexes on commit version. All verified via Apple docs + VLDB papers + `cksyncengine-5sie5` reference this run.

