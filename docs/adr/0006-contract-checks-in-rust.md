# ADR-0006: Repository contract checks run in Rust, TOML through a gap edge

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Primary URLs:
> `../../evidence/SOURCES.md`.

> Status: accepted 2026-11-03. Gap: Logical-Works-Inc/logicalworks-crates#852.

## Decision

Python is banned from product repositories. The contract checks that lived in
`scripts/*.py` now run as `logical-db-e2e` integration tests, and the gate
runs them with `cargo nextest run --locked -p logical-db-e2e`. They read
`contract/*.toml` through the pinned `toml` crate, a dev-only edge owned by
`logical-db-e2e`.

## Why the estate failed here

The estate was searched first, in ladder order:

1. **`lgwks_std`** ships `json` and `ron` serde facades and no TOML reader.
2. **`lgwks_deps::contract`** parses `APPROVED.toml` with a subset parser
   bound to the register's own shape. It cannot read arrays of tables with
   arbitrary fields (`[[system]]`) or nested `[layers.<name>]` tables.
3. **Rewriting the contracts as RON or JSON** was rejected. `GRAPH.toml` is
   read by the estate's own guard, and these documents carry their rationale
   as comments, which JSON cannot hold. A format split would leave two
   readers for one contract family.

## Exit

When #852 ships `lgwks_std::toml`, the tests switch to it in one change and
the `toml` approval and manifest lines are deleted.

## Enforcement

- `crates/logical-db-e2e/tests/access_graph.rs` refuses an absent anchor, an
  inventory mismatch, an unknown resource or predecessor, a cycle, an
  unbounded limit, a missing field, a duplicate key and a wrong version. Each
  refusal has a falsifier that mutates the committed registry.
- `tests/dependency_contract.rs` refuses the `toml` edge if its approval is
  withdrawn; rust-guard GAP refuses it without the #852 citation.
