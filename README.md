# Logical-DB

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `evidence/SOURCES.md`.

Logical-DB is a local-first structured state substrate for one person’s apps.
It is not a FoundationDB client disguised as an application database. Each app
owns an embedded, immediately durable replica; an optional thin-client layer
exchanges immutable causal changes with a service whose production storage
adapter uses FoundationDB.

## What applications embed

The `logical-db` facade exposes typed account/space/document identities,
transactional CRDT edits, read projections, schema-bound secondary indexes, a
durable change feed, and bounded synchronization. It needs neither Braid nor a
network for local reads and writes.

The read boundary exposes map discovery, list access, collaborative text, and
all causally concurrent values; the editor exposes map/list/object operations,
text splice, deletion, and counters. Applications therefore do not have to
discard CRDT conflicts behind a last-writer-looking getter.

```rust
use logical_db::{
    AccountId, Database, DocumentId, IndexId, IndexValue, Item, Object,
    OperationDigest, OperationId, Projection, ReplicaId, Scalar, Scope, SpaceId,
};

let scope = Scope::new(AccountId::new(), SpaceId::new());
let kind = IndexId::new(); // production apps persist a stable schema UUID
let projection = Projection::new(kind, |reader| {
    Ok(match reader.get(&Object::root(), "kind")? {
        Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
        _ => None,
    })
});
let database = Database::open_with_schema(
    "app-state.redb",
    scope,
    ReplicaId::new(),
    1,
    vec![projection],
)?;
let document = DocumentId::new();
database.edit(
    document,
    OperationId::new(),
    OperationDigest::new([7; 32]),
    |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
)?;
let notes = database.query_index(kind, &IndexValue::Text("note".into()), None, 128)?;
# Ok::<(), logical_db::DatabaseError>(())
```

Use `Database::open` when the host supplies an explicit path.
`Database::open_default` selects the native per-user data root (`LOCALAPPDATA`
on Windows) and creates only the app-private parent directories. A database file
is durably bound to one `ReplicaId` and one exact schema/projection digest.
Schema changes use `Database::migrate_schema`: it persists the target identity
and document cursor, rebuilds indexes in bounded revision-checked pages, resumes
after interruption, and swaps the schema binding only after the final page.

Enable the facade's `http` feature to import `logical_db::http::{HttpTransport,
CredentialProvider, StaticBearer}` without coupling an app to internal crates.
Custom transports implement the facade-exported `logical_db::transport::Transport`
using its versioned push/pull types.

Hosts that model semantic observations may enable the facade's `semantics`
feature to reuse the bounded, non-authoritative `logical-db-cairn` vocabulary
and `logical-db-episode` local model through the same `Database` API. This is
not the canonical CAIRN ingestion path or the durable ORIGIN episode database,
and its v1 SHA-256 snapshot is not the locked LEM v0 BLAKE3 wire. A reviewed
versioned adapter is required for external LEM consumption. The feature stays
off by default so embedded replicas keep their minimal footprint, and
`logical-db-e2e` carries a network-free journey proving an episode snapshot
round-trips through a document with its evidence-subset guard intact. See
`docs/adr/0001-semantic-plane-boundary.md`.

## Deployment shapes

- **Embedded only:** app → `logical-db` → Automerge + redb. No daemon, network,
  FDB, or Braid.
- **Thin client:** the same embedded API plus `logical-db-http`; sync is bounded,
  resumable, idempotent, and driven by a durable service checkpoint. A local
  scope is durably bound to one upstream because v1 has one upload cursor;
  attempting a second upstream fails instead of suppressing undelivered writes.
- **Standalone service:** the `logical-db-daemon` binary composes a loopback-only
  Axum listener, bounded request admission, HMAC checkpoints, a Directory Layer
  subspace, and the FDB adapter. Its local Braid authority deliberately refuses
  writes and sync because pinned Braid has no independently verifiable runtime
  confirmation evidence. It is not a usable sync service until an ecosystem
  host supplies externally prepared grants; it also refuses a non-loopback
  plaintext bind.
- **Ecosystem host:** Logic OS or another host nests the same router and uses
  `logical-db-braid` to admit exact account/space/action capabilities computed
  after native validation. Application request digests remain retry identities.
  The host
  does not own embedded CRUD.

The wire boundary is two versioned CBOR operations:
`POST /v1/changes:push` and `POST /v1/changes:pull`, media type
`application/vnd.logical-db.v1+cbor`. Bodies, pages, changes, checkpoints,
redirects, credentials, timeouts, and page counts have hard ceilings.

## Status

The clean-room rewrite verifier requires every surviving text file to name at
least twenty ledger sources and proves all baseline blobs changed, greater than
85% line churn, a one-way crate DAG, and no legacy `kernel` surface. The rewrite
also has deterministic state-machine tests, crash-resumable schema migration,
kill/restart journeys, mutation campaigns, and official FDB unknown-commit
injection. The former facade-to-daemon journey is explicitly retired because
its confirmation model was unsound. `REWRITE_MANIFEST.md` remains
authoritative. Embedded tombstones and export/restore are implemented and have
retained tests. Canonical action-byte authorization is implemented and exercised
through the HTTP/service/FDB seam. The v2 state-bearing server baseline,
retention floor, and transactional prune gate are established, but production
completion still requires paged HTTP baseline recovery and atomic client
installation, independent runtime Braid confirmation, projection semantic
identity, multi-node FDB recovery, public TLS/issuer integration, sustained
load, external LEM consumption, and direct process-kill restore evidence.

See [GOVERNANCE.md](GOVERNANCE.md) for delivered evidence, open acceptance work,
and the Director decisions needed to reconcile historical scope contracts.
