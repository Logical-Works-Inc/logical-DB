# Plan: Reconcile semantic-plane governance

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
> `evidence/SOURCES.md`.

**Issue**: governance follow-up
**Phase**: implement
**Started**: 2026-09-12

## Goal

Make the Logical-DB semantic ports explicit and non-authoritative relative to
the locked LEM CAIRN/ORIGIN/FORGE plane contract, while preserving the
one-way Cargo boundary and the existing KV work.

## Approach

Use the LEM SOT, systems design, wire schema, invariants, and anti-slop tests
as the higher-order semantic sources. Keep `logical-db-cairn` and
`logical-db-episode` bounded, in-memory semantic types; keep durable ORIGIN
storage and v0 wire compatibility behind a future reviewed adapter. Do not
copy sibling-repository implementations or introduce a reverse dependency.

## Implementation Steps

### Phase 1: Baseline and boundary
- [x] 1.1: Fast-forward to the reviewed semantic hardening commits without losing the local KV tiering changes.
- [x] 1.2: Record the canonical-owner, persistence, identity, and dependency discrepancies.
- [x] 1.3: Encode the boundary in a repository ADR and align the monorepo documentation.

### Phase 2: Verification
- [x] 2.1: Run formatting, default/all-feature Cargo checks, tests, clippy, deny, and the evidence verifier.
- [x] 2.2: Run the semantic public-facade journey and inspect its external state.
- [x] 2.3: Run the available Keel gate; if unavailable, retain the literal blocker and apply the inferred lens without claiming a Keel verdict.

## Current Step

The explicit authority and compatibility boundary is encoded. No service
integration or identity-algorithm change is authorized by this work.

## Blockers

The local Keel binary is unavailable. LEM’s locked v0 BLAKE3 wire identity and
the monorepo’s v1 SHA-256 framed identity are not interchangeable; an adapter
and migration decision are future work, not an implicit conversion.

## Validation

- [x] `python3 scripts/verify-rewrite.py`
- [x] `cargo fmt --all -- --check`
- [x] `cargo check --workspace --all-targets --locked`
- [x] `cargo check --workspace --all-targets --all-features --locked`
- [x] `cargo test --workspace --locked`
- [x] `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`
- [x] `git diff --check`
