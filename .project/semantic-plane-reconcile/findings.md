# Findings

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
> `evidence/SOURCES.md`.

**Issue**: governance follow-up
**Updated**: 2026-09-12

## Research

- `/Users/srinji/LOGICAL-EVENT-MEMORY/SOT.okf.md` is explicitly canonical and
  assigns CAIRN ingestion/ontology, ORIGIN’s Fjall-backed append-only log, and
  FORGE action/receipts to separate planes.
- LEM’s `spec/EPISODE-WIRE-V0.okf.md` locks v0 identifiers as
  `blake3:<hex64>` and stores payloads as blob references. The monorepo episode
  crate implements `logical-db-episode.v1`, a framed SHA-256 identity over
  local fields and an in-memory bounded log.
- `/Users/srinji/cairn/DEPRECATED.md` and the frozen LEM README point active
  development at this monorepo, but neither supersedes the LEM SOT’s authority
  table. This is a documentation migration ambiguity, not permission to
  redefine the planes.
- The Cargo graph is one-way: `logical-db-episode` depends on
  `logical-db-cairn`; `logical-db` optionally re-exports both; the E2E harness
  uses them; storage, CRDT, protocol, and service layers do not import them.
- The semantic journey is public-facade based and currently validates snapshot
  round-trip, zero-ID refusal, unknown-version refusal, and evidence coverage.
- The local Keel binary is unavailable at
  `~/keel/keel-core/target/release/keel-scan`; the observed exit code was 127.

## Technical Decisions

| Decision | Rationale | Date |
|----------|-----------|------|
| Treat LEM SOT as the authority for plane ownership | The SOT explicitly outranks sibling documents and preserves CAIRN evidence versus ORIGIN authority | 2026-09-12 |
| Treat monorepo semantic crates as bounded ports, not ORIGIN storage | Their source has no storage/network path and `OriginLog` is an in-memory test/model surface | 2026-09-12 |
| Keep v1 SHA-256 local identity distinct from LEM v0 BLAKE3 wire identity | Equal `[u8; 32]` width does not establish algorithm, canonicalization, or wire compatibility | 2026-09-12 |
| Keep sibling repositories unchanged | Cairn is frozen for feature work and the SOT requires an authority diff before amendment | 2026-09-12 |
| Preserve the existing KV tiering change in a separate worktree change | It was pre-existing user work and must not be folded into semantic governance edits | 2026-09-12 |

## Key Insights

The import boundary is structurally sound after fast-forwarding the reviewed
semantic commits. The remaining risk is semantic naming: “ORIGIN” and
“content-hashed log” can be read as durable canonical authority even though the
crate is only an in-memory bounded model. The documentation must state the
negative capability as clearly as the positive API.

The next legitimate integration is an explicit adapter with a versioned
translation contract, source coordinates, hash-scheme handling, persistence,
and independent end-to-end evidence. Adding more fields, a service path, or a
silent BLAKE3/SHA-256 conversion before that contract is defined would violate
the source-of-truth and no-vendored-twins invariants.
