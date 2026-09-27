> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15 S16
> S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
> `evidence/SOURCES.md`.

# Logical-DB invariants

## Embedded durable document state

- **INV-EMBED-001**: Each document revision reconstructs from one complete Automerge checkpoint and at most 31 later immutable changes; checkpoint/deltas, revision, operation receipt, outbox, indexes, and materialized event advance atomically. Why: rewriting complete history for every edit makes embedded commit work grow with retained history (2026-09-23 matched-trace investigation). Enforced by `crates/logical-db/src/lib.rs` checkpoint/reopen tests and `crates/logical-db-store/src/lib.rs` atomicity tests.
- **INV-EMBED-002**: Checkpoint and incremental-change keys include account, space, and document identity; replay never reads rows from another scope. Why: tenant isolation is part of durable identity. Enforced by scoped key construction and same-document cross-scope tests in `crates/logical-db-store/src/lib.rs`.
- **INV-EMBED-003**: The per-handle CRDT cache is revision-checked, scope-owned, capped at 256 documents, 1 MiB encoded size per document, and 16 MiB aggregate encoded size; misses and oversized states use durable storage. Why: the measured 256-document write trace churned a 64-entry cache. Enforced by `state_cache_enforces_entry_byte_and_revision_bounds` in `crates/logical-db/src/lib.rs`.
- **INV-CRDT-001**: Replaying the bounded suffix over a verified checkpoint preserves user-visible values and causal heads. Why: checkpointing may change physical representation, never CRDT meaning. Enforced by `crates/logical-db/src/lib.rs` roundtrip and restart tests.
