# ADR-0006: Repository contract checks run in Rust

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Primary URLs:
> `../../evidence/SOURCES.md`.

## Status

Accepted 2026-11-03; amended 2026-11-24 (#135).

## Context

The repository contract checks were Python scripts, and product repositories
are pure Rust and C++ (Director, 2026-10-13).

## Decision

Python is banned from product repositories. The contract checks that lived in
`scripts/*.py` run as `logical-db-e2e` integration tests, and the gate runs
them with `cargo nextest run --locked -p logical-db-e2e`.

## Amendment 2026-11-24

#135 deleted the repository-policing gates (`repository_contract`,
`access_graph`, `contract/domains/*.toml`, `contract/access-graph.toml`). The
only `toml` reader went with them, so the `toml` dev edge, its approval and
its manifest line are deleted. Rejected: keep `toml` for a future reader. It
would be a registered edge with no consumer. Reverse when a retained test
must read a TOML file that `cargo metadata` or `lgwks_deps::contract` cannot
read; then use `lgwks_std::toml` if #852 has shipped it.

## Consequences

The checks run in the gate's one nextest invocation. They are enforced by:

- `tests/crate_graph.rs` reads the workspace graph through `cargo metadata`
  and refuses an undeclared crate or an upward edge.
- `tests/dependency_contract.rs` runs the estate `check_dependencies` verdict
  against `contract/APPROVED.toml`.
- `tests/ci_routing.rs` refuses a self-hosted runner.
