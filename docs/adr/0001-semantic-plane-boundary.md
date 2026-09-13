# ADR-0001: Keep semantic ports below canonical LEM authority

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Source URLs are in
> `evidence/SOURCES.md`.

- **Status:** accepted for the Logical-DB development workspace
- **Date:** 2026-09-12
- **Scope:** `logical-db-cairn`, `logical-db-episode`, the `logical-db`
  `semantics` feature, the E2E harness, and their documentation
- **Authority:** this ADR clarifies the workspace boundary; it does not amend
  the LEM SOT, locked LEM wire schema, or frozen sibling repositories

## Decision

The semantic crates in this workspace are bounded, non-authoritative ports and
local models. They are not the canonical CAIRN ingestion plane, the canonical
ORIGIN database, or the FORGE action plane.

| Plane | Canonical owner | Workspace relationship |
|---|---|---|
| CAIRN observation / ontology | `~/cairn` | `logical-db-cairn` re-expresses vocabulary and validation only; it has no I/O, storage, or promotion authority |
| ORIGIN canonical episode DB | `~/LOGICAL-EVENT-MEMORY` and its `FjallFactStore` contract | `logical-db-episode` models a bounded local episode/log surface; its `OriginLog` and snapshots are not durable ORIGIN history |
| FORGE action and receipts | `~/forge-sdk` + `~/forge-harness` | No FORGE implementation or authority is added here |

The LEM SOT is the source of truth for this ownership table. A sibling
deprecation pointer saying that active development moved to this monorepo does
not, by itself, transfer canonical authority. Such a transfer requires a
Director-approved SOT diff and an explicit migration proof.

## Identity and wire boundary

`logical-db-episode` v1 uses the workspace’s SHA-256 length-framed identity over
its scoped immutable fields. LEM’s locked v0 wire uses `blake3:<hex64>` IDs and
payload references. Equal byte width is not compatibility. The local
`EpisodeId`/`EpisodeRef` conversions are representation conversions inside the
workspace only; they must not be used as an external LEM wire adapter or as a
claim that the two identity schemes are equivalent.

Until a separately reviewed adapter exists, external LEM v0 input must be
rejected or handled by an explicit versioned translation that records source
coordinates, hash scheme, canonicalization, payload location, and migration
status. No silent SHA-256/BLAKE3 substitution is permitted.

## Dependency and import boundary

The allowed dependency direction is:

```text
application / E2E
  -> logical-db (optional semantics feature)
       -> logical-db-episode -> logical-db-cairn
```

The storage, CRDT, protocol, sync, HTTP, and service layers must not import
either semantic crate. The facade’s `semantics` feature is opt-in and only
re-exports the ports; it does not make them part of the minimal embedded
surface. The E2E crate may import both semantic crates directly because it is a
non-published black-box harness, not a production lower layer.

## Persistence and lifecycle boundary

The semantic ports may be serialized into an application document for a
bounded local journey. That document is application state or a test artifact,
not an ORIGIN append-only tape. A future ORIGIN adapter must own durable
append, sequence assignment, canonical identity validation, replay, recovery,
and v0/v1 migration. A future CAIRN adapter must emit candidates and source
coordinates without declaring them canonical.

## Consequences

- Documentation uses “semantic port”, “local model”, and “bounded snapshot” for
  these crates; it reserves “canonical ORIGIN” for the LEM authority.
- LEM-2 and INGEST-1 remain open. Their service/storage integration cannot be
  marked complete by the current embedded semantic journey.
- No code is copied from the frozen Cairn or LEM repositories, and no reverse
  path dependency is introduced.
- Identity, persistence, provenance, and version-skew tests remain required for
  any future adapter. A successful local snapshot round-trip does not satisfy
  external LEM consumption.

## Rejected alternatives

1. **Treat the monorepo crate as canonical ORIGIN now.** Rejected because the
   LEM SOT assigns ORIGIN authority to the LEM/Fjall plane and the local log is
   in memory.
2. **Accept any 32-byte ID as an LEM ID.** Rejected because hash algorithm,
   framing, schema version, payload addressing, and canonicalization differ.
3. **Add a direct path dependency on LEM or Cairn.** Rejected because it would
   create a reverse/frozen-repo coupling and violate the no-vendored-twins and
   explicit-adapter boundaries.

## Proof obligations

This ADR is not a production-complete claim. Before an external LEM handoff,
the workspace must show an adapter-level journey with two independent scopes,
malformed and scheme-mismatched wire input, duplicate/reordered delivery,
restart/recovery, source-coordinate preservation, bounded payloads, and a
real durable ORIGIN store. The current Keel binary is unavailable, so its
zero-gate and MI-VS result remains unresolved rather than inferred.
