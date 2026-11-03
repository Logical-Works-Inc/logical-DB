# Logical-DB

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `evidence/SOURCES.md`.

Logical-DB is one product with four independently developed domains: the
sovereign Cloud Plane, Cairn ontology/evidence, LEM/ORIGIN agent memory, and
Forge operations. They share versioned contracts rather than ambient mutable
authority. The current public facade is an embedded structured-state API: each
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
logical-db = { git = "https://github.com/Logical-Works-Inc/logical-DB", rev = "ae5025692937edec114a865b6114e643f279b0a9", package = "logical-db" }
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

Opening, schema migration, the `http` and `semantics` features, deployment
shapes, the wire boundary, and delivery status are in
[docs/EMBEDDING.md](docs/EMBEDDING.md).

## Continuous integration

Every pull request and every push to `main` runs on GitHub-hosted runners:
native Linux x86-64, macOS ARM64, and Windows x86-64, plus the official
FoundationDB 7.3.77 client and server on Linux and macOS. The same lanes run
locally with `cargo run -p logical-db-e2e --bin ci_local`. See
[WORKFLOW.md](WORKFLOW.md).

## Contributing

This repository is public to read, use, and audit. External contributions are
not accepted at this stage: issues, pull requests, and comments are limited to
the maintainers. See [CONTRIBUTING.md](CONTRIBUTING.md). Report a vulnerability
privately as described in [SECURITY.md](SECURITY.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
