# Logical-DB

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `evidence/SOURCES.md`.

Logical-DB is one product with four independently developed domains: the
sovereign Cloud Plane, Cairn ontology/evidence, LEM/ORIGIN agent memory, and Forge
operations. They share versioned contracts rather than ambient mutable
authority. Read the [product and architecture direction](docs/PRODUCT-DIRECTION.md)
before choosing a workstream: it is the concise statement of what Logical-DB
is, what it deliberately is not, and how the local-first, provenance-first
substrate absorbs old SQL/Postgres systems without making the team own
distributed state.

The current public facade is an embedded structured-state API: each
app owns an immediately durable replica and may exchange immutable causal
 changes through an optional FoundationDB-backed relay.

The desired Cloud Plane is a sovereign hyperscale data substrate; that is a

measured target, not a present-tense scale claim. Local execution reduces
retained remote data, transfer, and cloud compute, but does not replace the
self-hostable distributed service contract. Read the [product map](docs/PRODUCT-MAP.md),
[domain boundaries](docs/architecture/DOMAIN-BOUNDARIES.md), and
[Cloud acceptance ADR](docs/adr/0003-sovereign-hyperscale-data-plane.md) before
choosing a workstream.

## Install the supported embedded surface

The current supported distribution is a revision-pinned Git dependency. Pinning
the reviewed revision makes the source and its internal crate family
reproducible; do not replace `rev` with a moving branch name in a durable
application build.

```toml
[dependencies]
logical-db = { git = "https://github.com/srinji-kaggss/logical-DB", rev = "ae5025692937edec114a865b6114e643f279b0a9", package = "logical-db" }
```

This is the embedded local-replica surface exercised by
[`experience/journeys/embedded-git-consumer-v1.yaml`](experience/journeys/embedded-git-consumer-v1.yaml).
It needs no daemon, network connection, or FoundationDB at runtime.
Crates.io installation is not supported yet: the facade depends on unpublished
workspace crates, and an ordered release is a separate production gate rather
than a fallback the installer can guess. The quickstart below starts with the
same public facade.

## What applications embed

The `logical-db` facade exposes typed account/space/document identities,
transactional CRDT edits, read projections, schema-bound secondary indexes, a
durable change feed, and bounded synchronization. It needs neither a
network nor any host authority for local reads and writes.

The read boundary exposes map discovery, list access, collaborative text, and
all causally concurrent values; the editor exposes map/list/object operations,
text splice, deletion, and counters. Applications therefore do not have to
discard CRDT conflicts behind a last-writer-looking getter.

```rust
use logical_db::{
    AccountId, Database, DocumentId, IndexId, IndexValue, Item, Object,
    OperationDigest, OperationId, Projection, ProjectionSemantics, ReplicaId, Scalar, Scope,
    SpaceId,
};

let scope = Scope::new(AccountId::new(), SpaceId::new());
let kind = IndexId::new(); // production apps persist a stable schema UUID
let projection = Projection::new(kind, ProjectionSemantics::V1, |reader| {
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
Every `Projection` carries an explicit application-declared
`ProjectionSemantics` (a nonzero version or an opaque digest) that is bound into
that digest: authors must bump it whenever a projection function's output for
any document can change, and a changed semantics under the same `IndexId` and
schema version fails closed with migration-required instead of serving stale
rows. The digest domain is versioned as v2; a database bound by an earlier build
that recorded no projection semantics fails closed and needs one
`Database::migrate_schema` to the v2 binding. Schema changes use
`Database::migrate_schema`: it persists the target identity and document cursor,
rebuilds indexes in bounded revision-checked pages, resumes after interruption,
and swaps the schema binding only after the final page.

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
  or FDB.
- **Thin client:** the same embedded API plus `logical-db-http`; sync is bounded,
  resumable, idempotent, and driven by a durable service checkpoint. A local
  scope is durably bound to one upstream because v1 has one upload cursor;
  attempting a second upstream fails instead of suppressing undelivered writes.
- **Standalone service:** the `logical-db-daemon` binary composes a loopback-only
  Axum listener, bounded request admission, HMAC checkpoints, a Directory Layer
  subspace, and the FDB adapter. Its `LocalScopeAuthority` deliberately refuses
  writes and sync: a bearer-authenticated local grant admits only Read and Scan
  inside one scope and expiry. It is not a usable sync service until an
  ecosystem host supplies an `Authority` that admits those effects; it also
  refuses a non-loopback plaintext bind.
- **Ecosystem host:** Logic OS or another host nests the same router and supplies
  its own `logical_db_service::Authority` implementation admitting exact
  account/space/action facts computed after native validation. Application
  request digests remain retry identities. The host does not own embedded CRUD.

The wire boundary is two versioned CBOR operations:
`POST /v1/changes:push` and `POST /v1/changes:pull`, media type
`application/vnd.logical-db.v1+cbor`. Bodies, pages, changes, checkpoints,
redirects, credentials, timeouts, and page counts have hard ceilings.

## Status

The product definition of record is
[docs/PRD-UNIFIED-SUBSTRATE.md](docs/PRD-UNIFIED-SUBSTRATE.md): one
substrate unifying Logical-DB (durable state), Cairn (observations), and
LEM (episodic memory) — an embeddable, local-first ontology substrate
with dedupe-from-first-write, full reversibility, and citation-bearing
agent memory. The 2026-09-16 companion review of Apple CloudKit lives in
[docs/review-cloudkit.md](docs/review-cloudkit.md).

Delivered and evidenced: the clean-room rewrite gates (verifier, one-way
crate DAG, mutation campaigns, FDB unknown-commit injection), the
recovery stack through pruning and process-kill restore (PRs #48–#53,
live-cluster receipts in `evidence/`), the bounded HTTP transport
deadline (PR #57), and self-hosted quality/FDB CI lanes (PR #56).
Retired: the facade-to-daemon journey, whose confirmation model was
unsound. Still gating production release: the recovery-acceptance
checklist (#28), the checked access graph (#25), the Cairn+LEM migration
proof (PRD WP2, ADR-0002), and the deployment/load journeys in
`REWRITE_MANIFEST.md`, which remains the mechanical authority.

See [GOVERNANCE.md](GOVERNANCE.md) for delivered evidence, open acceptance work,
and the Director decisions needed to reconcile historical scope contracts.
