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

This ADR preserves the lower-core isolation law while ADR-0002 governs the
Director-authorized move of Cairn and LEM authority into this workspace. The
current semantic crates are still bounded migration ports: no migration is
canonical until the identity, provenance, durable-ORIGIN, and recovery proof
lands.

The target four-domain authority table is canonical in
[`../architecture/DOMAIN-BOUNDARIES.md`](../architecture/DOMAIN-BOUNDARIES.md):

| Domain | Current workspace relationship | Prohibited authority |
|---|---|---|
| Cairn evidence/ontology | `logical-db-cairn` is a bounded vocabulary/validation compatibility port while Cairn core/ingest/views contracts migrate | canonical episode promotion and effects |
| LEM / ORIGIN | `logical-db-episode` is a bounded local model while the v0↔v1 adapter and durable ORIGIN proof migrate | raw-source ingestion and action execution |
| Forge | planned above LEM by ADR-0004 | direct Cloud/ORIGIN mutation and silent effects |
| Cloud Plane | existing storage, CRDT, protocol, sync, service, and FDB code remains below all semantic/action domains | ontology truth, canonical semantic promotion, effects |

A sibling deprecation pointer alone does not establish migration completion.
ADR-0002 and the applicable domain contract retain the promotion proof.

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
