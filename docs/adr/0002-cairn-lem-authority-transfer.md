# ADR-0002: Transfer Cairn and LEM plane authority into this workspace

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `evidence/SOURCES.md`.

- **Status:** authorized; migration in progress — not canonical until the
  ADR-0001 migration proof lands
- **Date:** 2026-09-16
- **Authority:** Director authorization recorded 2026-09-16 ("properly
  bring in the code from Cairn and Logical-Event-Memory repos") and the
  ratified product definition in `docs/PRD-UNIFIED-SUBSTRATE.md` §5
- **Supersedes:** the read-only-reference posture of ADR-0001 for plane
  *ownership*; ADR-0001's identity, dependency, and proof clauses remain
  binding in full

## Decision

Cairn (observation vocabulary), Logical-Event-Memory (episodic memory /
ORIGIN plane), and Logical-DB (durable typed state) are one product. The
canonical planes move into this workspace:

1. **Code migration.** `wm-core` Rust modules (`claim_schema.rs`,
   `geometry.rs`, `hash.rs`, `schema.rs`, `types.rs`), the Cairn
   vocabulary surface (`world_token.py`, `ontology.py` semantics), and
   the LEM operators plus `LocalWorld`
   (`~/LOGICAL-EVENT-MEMORY/src/lib.rs`) become the authoritative
   implementations inside `logical-db-cairn` and `logical-db-episode`,
   replacing re-expressions.
2. **Versioned identity adapter.** The locked LEM v0 BLAKE3 wire and the
   workspace v1 SHA-256 framing are bridged only by an explicit,
   reviewed adapter recording hash scheme, canonicalization, payload
   location, and migration status. Equal byte width is not identity; no
   silent conversion.
3. **SOT records.** `SOT.okf.md` and `00_CATALOG.okf.yaml` in
   `~/LOGICAL-EVENT-MEMORY` remain the provenance record of what was
   transferred; the frozen repositories stay readable history, not
   import sources.

## Proof obligations (gating "canonical")

From ADR-0001, all required before any canonical claim: two independent
scopes; malformed and scheme-mismatched wire input refused; duplicate
and reordered delivery handled; restart/recovery; source-coordinate
preservation; bounded payloads; a real durable ORIGIN store journey.
Tracked as PRD WP2. Until then, all documentation states "migration in
progress".

## Consequences

- The LEM SOT diff and migration proof required by ADR-0001 are
  satisfied in the following order: this ADR is the Director-approved
  diff; WP2 is the proof.
- Issue #30 is closed as resolved by this authorization.
- Frozen sibling repositories receive no new feature work and accept no
  reverse dependency from this workspace.
