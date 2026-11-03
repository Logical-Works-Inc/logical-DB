# ADR-0005: Identities stay on `uuid` until the estate id serializes

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Primary URLs:
> `../../evidence/SOURCES.md`.

> Status: accepted 2026-11-03. Gap: Logical-Works-Inc/logicalworks-crates#851.

## Decision

The estate is used first. `logical-db-kv` carries no `uuid` edge: it builds
index and document identities from bytes through
`logical_db::decode_identity_id`, and it names staging files with
`lgwks_std::id::Uuid::new_v7`. The model, store, sync, service and FDB
layers keep the pinned `uuid` crate. Every manifest line that admits it
cites #851.

## Why the estate failed here

We tried migrating `logical-db-model` identities to `lgwks_std::id::Uuid`
(3.2.2, feature `random`) and reverted it. It is missing three things:

1. **No serde.** The model derives `Serialize`/`Deserialize` on every
   identity newtype. JSON carries hyphenated text, and CBOR carries 16 bytes
   through the `is_human_readable` switch. Without serde, each newtype needs a
   hand-written codec. That would be a second implementation of a capability
   the dependency already provides correctly.
2. **Fallible v7.** `new_v7() -> Result<_, V7Error>` makes every `XId::new()`
   fallible. The migration cascaded into hundreds of call sites across the
   sync, service, protocol and FDB crates, and turned infallible constructors
   into `Result` plumbing with no recovery path.
3. **No FDB tuple packing.** Service keys pack identities as the tuple-layer
   UUID type (code 0x30) through `foundationdb-tuple`'s `uuid` feature. The
   estate type has no equivalent, so keys would change bytes, and stored data
   would need a migration for no product gain.

## Exit

When #851 ships these three capabilities, the model newtypes switch to
`lgwks_std::id::Uuid` in one change. Existing JSON, CBOR and FDB key bytes
must stay identical: the store's frozen fixtures assert this. The `uuid` and
`foundationdb-tuple` `uuid` feature edges are deleted in the same change.

## Enforcement

- rust-guard GAP refuses a third-party edge without an open `gap` issue cited
  on its manifest line.
- `crates/logical-db-kv` has no `uuid` dependency; `cargo clippy --locked
  --all-targets` and `cargo nextest run --locked -p logical-db-kv` prove it
  builds and passes without one.
