# ADR-001: Canonical DB-Model Authority — This Repo Owns the One Model

**Status:** PROPOSED (Director ratification pending)
**Date:** 2026-08-21
**Context:** Director directive "move DB into a new repo; 1 model across
every repo"; governance structure imported from logic-os-kernel.

## Context

The storage pathway (docs/DB_PATHWAY.md) was researched inside sentinel
2026-08-21 and immediately outgrew it: the Director wants one DB model
consumed by every repo in the constellation, not a sentinel subsystem. A
canonical home must exist, and its authority must be explicit — the
constellation already runs an authority registry (logic-os-kernel
CONSTELLATION-CHARTER: Cid → Braid, Receipt schema → forge-harness, Fact
envelope + Tape contract → logic-os-kernel).

## Decision

1. `logical-DB` is the canonical home of: the row model
   `(class,id,ts,payload)`, the storage classes, the query AST, the ingest
   gauntlet, the port traits, and the use-case pack format.
2. INV-ONE-MODEL-EVERYWHERE (GOVERNANCE §2): consumer repos bind through
   published ports; model changes happen here. No consumer keeps a parallel
   storage truth.
3. This repo does NOT claim authorities owned elsewhere (Cid → Braid;
   Receipt schema → forge-harness; sentinel's evidence/epistemics semantics).
4. Open item for the Director: logic-os-kernel's charter currently claims
   Fact envelope / Tape contract. Ratifying this ADR implies those become
   adapters over this repo's row model (or a registered boundary is drawn).
   That amendment is theirs to record; this repo does not edit their charter.

## Consequences

- sentinel/docs/DB_PATHWAY.md moves here (done in the founding commit);
  sentinel references the pathway via this repo.
- Kernel code lands under `kernel/` only via approved DB-A…DB-E issues.
- Governance structure mirrors logic-os-kernel's (ticketing taxonomy, DoD,
  ADR series) so agents move between the repos without relearning process.
