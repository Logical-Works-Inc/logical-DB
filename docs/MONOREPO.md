# Dev monorepo strategy

> Evidence: S01-S28 per `evidence/SOURCES.md`; crate headers in
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
| `LOGICAL-EVENT-MEMORY` `src/lib.rs`, wire v0 | `logical-db-episode` | Episode wire: identity `sha256(schema\|seq\|kind\|payload)` |
| `LOGICAL-EVENT-MEMORY` operators, `LocalWorld` | `logical-db-episode` | Operators and local-world record over an in-memory vector |

Both crates are pure types plus bounded validation: no I/O, no storage, no
network. All bounds fail closed.

## Dependency DAG position

Per `ARCHITECTURE.md` (crate DAG section) and `AGENTS.md`:

- Both crates are semantic layers **above** `logical-db-model` and
  **below** applications / LEM consumers.
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

- `cairn` and `LOGICAL-EVENT-MEMORY` are frozen. No new work goes there.
- New semantic-layer work goes in `crates/logical-db-cairn` and
  `crates/logical-db-episode` in this workspace.

## Next steps

- [x] FACADE-1: facade `semantics` feature re-export plus the `logical-db-e2e`
  episode-to-document journey.
- [ ] LEM-2: episode operators wired to the service path.
- [ ] INGEST-1: deterministic projection from episodes into indexed state.
