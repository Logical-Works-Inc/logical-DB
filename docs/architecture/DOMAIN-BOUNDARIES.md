# Domain boundaries

Status: target ownership map. It assigns owners; it does not claim the target
crates exist. Product map: `../PRODUCT-MAP.md`. Sovereign cloud acceptance:
`../adr/0003-sovereign-hyperscale-data-plane.md`.

## Domains

| Domain | Owns | Publishes | Never owns |
|---|---|---|---|
| Cloud Plane | durable bytes, replica state, change transport, authorization enforcement, regional placement and recovery | Cloud contracts, the application facade | ontology truth, canonical episodes, actions |
| Cairn | source fidelity and coordinates, content identity, ontology registry, candidate evidence, rebuildable views | candidate and ontology contracts | semantic promotion, effects |
| LEM / ORIGIN | canonical append-only episodes, clocks, provenance, contradiction, consolidation, LocalWorld | episode, LocalWorld and admission contracts | raw ingestion, physical storage, actions |
| Forge | capability-bound action invocation, versioned receipts | action, grant and receipt contracts | memory or database ownership, silent effects |

The facade composes published contracts. It is not a fifth authority.

## Today

Only Cloud Plane code exists in this repo. The bounded Cairn and episode ports
were deleted on 2026-11-24 (#135); their needs are #143 and #145. The FDB store
keeps one frontier per scope: a correct ordered relay, not partitioned
ingestion (ADR-0003 owns that).

## Target layout

```text
crates/
  kernel/  logical-db-model, logical-db-protocol, logical-db-platform
  cloud/   crdt, store, sync, http, service, store-fdb, server, daemon
  cairn/   core, ingest, views, sdk
  lem/     core, projection, adapter
  forge/   contract, runtime
  logical-db/      public facade
  logical-db-e2e/  cross-domain journeys
```

A move adds the domain contract first, moves one crate, proves the graph changed
only by declared edges, moves consumers, then deletes the old path. Nothing is
copied into a parallel crate.

## Legal flow

```text
raw sources -> CAIRN -- candidate --> LEM/ORIGIN -- LocalWorld --> FORGE
                 ^                                                   |
                 +------------------- receipt candidate -------------+
```

The only promotion path is Cairn candidate, then LEM adapter, then ORIGIN
append. Forge never writes ORIGIN or Cloud data directly; its receipts return
through Cairn. Cloud supplies storage under the contracts and never promotes,
decides truth or acts. A direct write across a boundary is a defect.

## Ownership record

The layer order of the workspace crates lives in
`crates/logical-db-e2e/tests/crate_graph.rs`. That test checks it against
`cargo metadata`: every crate must be declared, and every edge must point down.
It proves the import graph only, not that target services exist. The
per-domain `contract/domains/*.toml` files were deleted in #135.

Import rules: `DEPENDENCY-RULES.md`.
