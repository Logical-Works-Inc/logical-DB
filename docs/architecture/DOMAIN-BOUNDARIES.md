# Logical-DB domain boundaries

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `../../evidence/SOURCES.md`.

> Status: proposed target structure. Current crate paths remain stable until their
> migration issue lands. This document assigns ownership; it does not claim that
> the target services are implemented.
>
> Canonical product map: [`../PRODUCT-MAP.md`](../PRODUCT-MAP.md). Sovereign
> cloud acceptance: [`../adr/0003-sovereign-hyperscale-data-plane.md`](../adr/0003-sovereign-hyperscale-data-plane.md).

## Why domains exist

Logical-DB competes across several difficult categories at once. A shared Cargo
workspace must not make every concern a shared mutable implementation. Four
independent domains own distinct facts, authority, and release lanes:

| Domain | Owns | May publish | Never owns |
|---|---|---|---|
| Cloud Plane | durable bytes, replica state, causal-change transport, authorization enforcement, regional placement/delivery/recovery | versioned Cloud contracts and application facade capabilities | ontology truth, canonical episodes, ambient action execution |
| Cairn | source fidelity, source coordinates, content identity, typed ontology registry, candidate evidence, rebuildable operational views | candidate and ontology contracts/SDK | canonical semantic promotion, a source of record, effects |
| LEM / ORIGIN | canonical append-only episodes, clocks, provenance, contradictions, consolidation, bounded LocalWorld | episode, LocalWorld, and candidate-admission contracts | raw-source ingestion, physical storage implementation, actions |
| Forge | explicit capability-bound action invocation and versioned receipts | action, grant, and receipt contracts | canonical memory/database ownership, silent side effects |

The application facade composes published contracts. It is not a fifth authority.

## Current paths and staged target layout

This repository retains current paths while compatibility consumers exist. The
following target layout is an ownership map, not permission to duplicate source:

```text
crates/
  kernel/
    logical-db-model/                 # current target: typed scopes/bounds
    logical-db-protocol/              # current target: canonical wire/action bytes
    logical-db-platform/              # current target: placement helpers

  cloud/
    logical-db-crdt/
    logical-db-store/
    logical-db-sync/
    logical-db-http/
    logical-db-service/
    logical-db-store-fdb/
    logical-db-server/
    logical-db-daemon/
    logical-db-cloud-contract/        # partition routing contract; views/placement ports follow
    logical-db-cloud-e2e/             # future: Cloud-only integration harness

  cairn/
    logical-db-cairn-core/            # future successor of current port
    logical-db-cairn-ingest/
    logical-db-cairn-views/
    logical-db-cairn-sdk/

  lem/
    logical-db-episode/               # current compatibility surface
    logical-db-lem-core/
    logical-db-lem-projection/
    logical-db-lem-adapter/

  forge/
    logical-db-forge-contract/
    logical-db-forge-runtime/

  logical-db/                         # public application facade
  logical-db-e2e/                     # non-published cross-domain journey harness
```

Each migration first adds its domain contract and compatibility re-export, moves
one owned crate, proves the dependency graph is unchanged except for declared
edges, moves consumers, and only then removes the old path. `smb`, ORIGIN,
CRDT, and storage implementations are never copied into parallel crates.

## Legal data and authority flow

```text
raw sources
    │ source bytes + coordinates
    ▼
CAIRN ── candidate contract ──► LEM / ORIGIN ── bounded LocalWorld ──► FORGE
  ▲                                  │                                 │
  └──────── receipt candidate ───────┴─────────────────────────────────┘

Cloud Plane supplies durable/versioned ports beneath the contracts. It cannot
promote candidates, decide semantic truth, or act. Forge cannot directly append
to ORIGIN or mutate Cloud data; it returns receipts through Cairn.
```

The only legal promotion path is `Cairn candidate → LEM adapter → ORIGIN append`.
The only legal closed action loop is `ORIGIN LocalWorld → Forge action → Forge
receipt candidate → Cairn → ORIGIN append`. A direct write across either
boundary is a defect.

## Current-state clarifications

- `logical-db-cairn` and `logical-db-episode` are bounded migration ports. They
  are not yet the complete Cairn or canonical ORIGIN implementations; ADR-0002
  defines their promotion proof.
- `NamespaceDomain` and `synchronize_namespace` filter material at the client
  after a service page is received. They are not an authorized replication-view
  implementation. ADR-0003 owns that future Cloud contract.
- Current `FdbServiceStore` maintains a scope-wide frontier. It is a correct
  current ordered relay, not a claim of partitioned hyperscale ingestion.
- The current local Automerge + redb replica remains a Cloud Plane capability:
  it reduces retained remote data, transfer, and cloud compute while preserving
  a self-hostable distributed data plane.

## Required ownership record

Every new domain component declares in `contract/domains/<domain>.toml`:

1. public crate/API and semantic version owner;
2. durable-state owner, region owner, and key owner;
3. allowed upstream dependencies and prohibited authorities;
4. data classification, retention/excision rule, and recovery authority;
5. domain CI lane, evidence artifacts, and conditions that remain unverified.

`docs/architecture/DEPENDENCY-RULES.md` is the mechanical import policy. Issue
#25’s checked access graph is the execution-time proof complement to this map.

The machine-readable half is `contract/domains/<domain>.toml` plus
`contract/domains/GRAPH.toml`: each record declares its owned `crates`, its
`public_contracts`, its `may_depend_on` owners, and its `forbidden_domains`.
`scripts/domain_contract.py` enforces that map against `cargo metadata` and
compares the committed `evidence/domain-graph.md` receipt; the clean-room
evidence lane runs it. A product domain may import only another domain's
published contracts, while the declared composition layers (facade, adapter,
e2e) may import implementation crates. This proves the declared import graph
only; it does not prove the target services exist or that a runtime authority
path is legal.
