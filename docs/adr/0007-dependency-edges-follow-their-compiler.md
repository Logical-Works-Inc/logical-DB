# ADR-0007: Dependency edges follow what compiles them

## Status

Accepted 2026-11-24 (#136, #112 landing).

## Context

Rust 1.100 cargo reports `unused dependency` for a normal edge that only
tests or only a feature compile. The e2e, daemon and store-fdb manifests
carried six such edges. The seeded sync simulation (#112) ran 96.3 s on the
8-core box, CPU bound in unoptimized automerge, over the 90 s suite budget.

## Decision

- An edge only tests compile is a `[dev-dependencies]` edge. An edge only a
  feature compiles is `optional` and named in that feature; when tests also
  compile it, it is a dev edge too.
- automerge, redb, sha2 and blake3 build at `opt-level = 2` in the dev
  profile; workspace crates keep full debug.
- Rejected: `[profile.dev.package."*"] opt-level = 2`. Sim 12.2 s against
  14.8 s, but the uncached build took 9 min. Reverse when a cached cold build
  with every dependency optimized measures under 60 s.

## Consequences

- `cargo check`, `clippy` and `nextest`, with and without `fdb`, build with
  zero warnings. The register allows the `thiserror` dev edge.
- Full suite on the box: 123.3 s with the sim unoptimized, 37.6 s now.
