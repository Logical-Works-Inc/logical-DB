# Logical-DB product map

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `../evidence/SOURCES.md`.

> Status: target organization. “Implemented” claims remain governed by
> `GOVERNANCE.md`; “migration in progress” remains governed by ADR-0002.

Logical-DB is one product with four independently developed domains. They share
versioned contracts, not ambient mutable authority.

| Domain | Product responsibility | Comparable category | Never claims |
|---|---|---|---|
| Cloud Plane | Sovereign durable data, relay, delivery, regional recovery, tenant isolation, and local execution that lowers retained remote data, transfer, and compute cost. | Cloud data platforms, object/compute stacks, consumer sync/data services. | That the current scope-wide FDB relay already delivers >1M active clients, partitioned fan-out, or multi-region resilience. |
| Cairn | Evidence-grounded ontology: source capture, type registry, candidate evidence, typed object/link/action definitions, and reproducible operational views. | Ontology and operational-data platforms. | That an index, embedding, vector, or candidate is canonical semantic truth; that it executes effects. |
| LEM / ORIGIN | Agent memory: immutable episodes, time/provenance, contradictions, consolidation, and bounded LocalWorld answers. | Agent-memory and retrieval systems. | That prompt context is memory, projections are authoritative, or actions belong to memory. |
| Forge | Explicit operations over versioned LocalWorld input, capability grants, and receipts that re-enter the evidence path. | Workflow/action systems. | Silent state mutation or ownership of canonical memory/data. |

## Product loop

```text
sources → Cairn candidate → LEM canonical episode → LocalWorld → Forge action
    ▲                                                                    │
    └──────────────────── receipt candidate ────────────────────────────┘

Cloud Plane provides the durable, versioned, regional infrastructure under the
contracts. It never decides ontology, semantic truth, or actions.
```

## Positioning and proof boundary

The intended Cloud Plane outcome is a **sovereign hyperscale data substrate**:
regional customer/operator control without mandatory foreign data custody, while
preserving Drive-class scale and availability as a measured target. This is not
a present-tense performance claim.

The claim becomes eligible only after the complete nine-axis acceptance contract
in ADR-0003 has receipts. Comparison is against broad credible architecture
classes with matched semantic traces, not a single named competitor. Local
execution is a resource, retention, privacy, latency, and continuity advantage;
it is not the product’s only consumer-facing story.

## Parallel work rule

A team may change one domain independently when it touches only that domain’s
public contracts and passes its domain lane. Contract/public API changes and any
change to the candidate→append→LocalWorld→receipt loop require the cross-plane
lane. `docs/architecture/DOMAIN-BOUNDARIES.md` and
`docs/architecture/DEPENDENCY-RULES.md` define the controlling authority.
