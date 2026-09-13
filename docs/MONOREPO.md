# Dev monorepo strategy

> Evidence: S11 S12 S13 cover local-first ownership, causal records, and
> convergence; S14 S15 S16 S17 S18 cover transactions, keyspaces, directory
> indirection, limits, and retry outcomes; S19 S20 S21 S22 cover atomicity,
> WAL, locking, and corruption refusal; S23 S24 S25 S26 cover HTTP semantics,
> typed errors, canonical bytes, and identifiers; S27 S28 cover platform file
> data and write boundaries; S31 S37 pin client dependency surfaces. Source
> URLs are in
> `evidence/SOURCES.md`; crate headers in
> `crates/logical-db-cairn/src/lib.rs` and
> `crates/logical-db-episode/src/lib.rs`.

## Why monorepo during dev

- One Cargo workspace: single `resolver = "3"`, shared `edition = "2024"`,
  `rust-version = "1.98"`, and shared `[workspace.lints]` (`unsafe_code =
  "forbid"`, clippy `pedantic = "warn"`) per root `Cargo.toml`.
- One toolchain and one lint set across storage, CRDT, service, and semantic
  crates, so refactors move in a single commit with workspace-wide checks.
- Faster iteration: semantic-layer changes compile against local path
  dependencies without cross-repo version bumps or publish cycles.

## Layout: old repos to new crates

| Old repo source | New crate | What moved |
|---|---|---|
| `cairn/quarantine/cairn_python/world_token.py` | `logical-db-cairn` | Typed world-token vocabulary: 12 token kinds, 10 modalities, 9 link relations, 6 actionability levels |
| `cairn/crates/wm-core` | `logical-db-cairn` | Authority-weight and content-hash concepts |
| Cairn steering/memory broker (smb) | `logical-db-cairn` | Deny-first steering, trust-graded memory claims |
| `LOGICAL-EVENT-MEMORY` `src/lib.rs`, operators | `logical-db-episode` | Bounded v1 local model derived from the operators; not the locked v0 wire |
| `LOGICAL-EVENT-MEMORY` `LocalWorld` | `logical-db-episode` | Evidence-backed local-world projection over an in-memory vector |

Both crates provide semantic port types and bounded validation, with rejection
diagnostics on stderr and no storage, network, ingestion, or canonicity
authority. Episode and log snapshots carry explicit v1 schema versions;
legacy unversioned snapshots are rejected. Log snapshots serialize episodes
only and rebuild the lookup index after validation. IDs reject zero values on
decode; conversions between episode and observation IDs are fallible local
representation conversions, not LEM wire compatibility. Bounds reject
oversized input before retaining it.

## Authority boundary

The LEM SOT remains authoritative for the CAIRN/ORIGIN/FORGE plane table:
CAIRN emits candidates, ORIGIN owns the canonical append-only episode database,
and FORGE owns actions and receipts. The local `OriginLog` is an in-memory
model used by the bounded semantic journey; storing its snapshot through
`Database` does not make that document canonical ORIGIN history. The locked
LEM v0 wire uses BLAKE3 identifiers and payload references, while this
workspace’s v1 local model uses SHA-256 framing over its scoped fields. A
versioned adapter and migration proof are required before external LEM
consumption; no silent identity conversion is allowed. See
`docs/adr/0001-semantic-plane-boundary.md`.

## Dependency DAG position

Per `ARCHITECTURE.md` (crate DAG section) and `AGENTS.md`:

- Both crates are non-authoritative semantic layers **above**
  `logical-db-model` and **below** applications / LEM consumers.
- `logical-db-episode` depends on `logical-db-cairn` (`TokenKind`); neither
  crate sits inside the storage/CRDT/protocol core.
- Lower crates (`logical-db-store`, `logical-db-crdt`, and their peers) must
  not import these semantic crates, per the `AGENTS.md` rule that lower
  storage/CRDT crates never depend on application/service-level types.

## Deliberately NOT ported

Remain in the old repos as read-only reference; not re-implemented here:

- `rusqlite` `world.db` persistence from cairn.
- `fjall` tape-store from LOGICAL-EVENT-MEMORY.
- Embedders (no vector inference in this workspace).
- Frontier eval harness.
- Lean specs.

## Deprecation pointer

- `cairn` and `LOGICAL-EVENT-MEMORY` are frozen for feature work according to
  their local pointers. That freeze does not transfer canonical plane
  authority; the LEM SOT remains the higher-order ownership contract.
- New bounded local semantic-port work goes in `crates/logical-db-cairn` and
  `crates/logical-db-episode` in this workspace. External CAIRN/ORIGIN
  integration requires a separately reviewed adapter.

## Next steps

- [x] FACADE-1: facade `semantics` feature re-export plus the `logical-db-e2e`
  episode-to-document journey.
- [ ] LEM-2: episode operators wired to the service path.
- [ ] INGEST-1: deterministic projection from episodes into indexed state.
- [ ] ADAPTER-1: versioned, source-preserving LEM v0 translation and durable
  ORIGIN handoff.
