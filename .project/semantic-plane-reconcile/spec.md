# Specification: Semantic-plane governance reconciliation

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
> `evidence/SOURCES.md`.

**Issue**: governance follow-up
**Status**: review

## Overview

The monorepo contains bounded semantic ports derived from the frozen Cairn and
LEM repositories. Its documentation must distinguish those ports from the
canonical CAIRN ontology/observation plane and the canonical ORIGIN durable
episode database. The change must prevent an application from mistaking a
local v1 SHA-256 model or an in-memory log for the locked LEM v0 BLAKE3 wire
and Fjall-backed authority.

## Requirements

### Must Have
- [ ] State the authority order and canonical owner for CAIRN, ORIGIN, and FORGE.
- [ ] State that `logical-db-cairn` and `logical-db-episode` have no storage or network authority.
- [ ] State that the monorepo v1 identity is not LEM v0 wire compatibility.
- [ ] Preserve the one-way Cargo dependency graph and public-facade opt-in.
- [ ] Keep existing KV tiering behavior and user changes intact.
- [ ] Do not modify frozen sibling repositories or vendor duplicate implementations.

### Should Have
- [ ] Link the boundary to an explicit ADR and the evidence ledger.
- [ ] Keep unresolved adapter, migration, and service-integration work visible.
- [ ] Retain a public semantic journey covering malformed and cross-scope state.

### Won't Have
- A new ORIGIN persistence adapter or wire migration in this change.
- A change to the LEM SOT, Cairn deprecation record, or frozen sibling code.
- A claim of production completeness, universal correctness, or Keel acceptance without evidence.

## Acceptance Criteria

- [ ] Every changed or added text file passes `python3 scripts/verify-rewrite.py`.
- [ ] Cargo dependency metadata contains no semantic edge below the application/facade layer.
- [ ] The semantic journey proves valid round-trip and explicit rejection of zero IDs, unknown versions, and uncovered evidence.
- [ ] Documentation names the SHA-256/BLAKE3 and in-memory/Fjall differences as intentional compatibility limits.
- [ ] `cargo fmt`, workspace checks, tests, clippy, and `git diff --check` pass with literal receipts.
- [ ] Keel availability and any unproven numerical gate are reported without substitution.

## User Stories

As an application author, I want the public facade to expose semantic types
without importing storage internals so that my dependency boundary remains
stable.

As an LEM maintainer, I want ports clearly marked as non-authoritative so that
no local snapshot is mistaken for canonical ORIGIN history.

As an operator, I want unsupported wire versions and identity schemes to fail
closed so that compatibility is never inferred from equal byte widths.

## Edge Cases

| Case | Handling |
|------|----------|
| LEM v0 BLAKE3 ID supplied to the local v1 model | Reject at an explicit adapter boundary; no implicit identity conversion. |
| Local semantic snapshot reopened without a durable ORIGIN store | Treat it as a bounded embedded projection/test artifact, not canonical history. |
| Lower storage crate imports a semantic crate | Evidence verifier rejects the undeclared edge and the change is not accepted. |
| Frozen sibling documentation conflicts with the LEM SOT | Follow the SOT and record the discrepancy; do not edit the frozen repo. |
| Keel binary unavailable | Report exit 127 and retain Keel numerical acceptance as unresolved. |
