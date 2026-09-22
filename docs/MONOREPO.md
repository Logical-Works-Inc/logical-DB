# Logical-DB monorepo strategy

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `../evidence/SOURCES.md`.

> Status: active development workspace. The target domain map is in
> [`architecture/DOMAIN-BOUNDARIES.md`](architecture/DOMAIN-BOUNDARIES.md);
> migration status is distinct from canonical authority.

## Why one workspace

The monorepo makes shared contracts, one Rust toolchain, and one dependency
policy visible. It does **not** make every product concern one implementation.
Cloud Plane, Cairn, LEM/ORIGIN, and Forge have independent owners, release lanes,
and authority limits. The workspace is the integration point; contract crates are
the only normal cross-domain imports.

## Current source inventory and target owner

| Predecessor responsibility | Current state | Target domain | Canonical/migration rule |
|---|---|---|---|
| Logical-DB model, CRDT, redb replica, sync, HTTP, service, FDB | Existing `logical-db-*` core crates | Cloud Plane | Current implementation; private backend handles remain below public contracts. |
| Cairn `wm-core` vocabulary/registry | Bounded `logical-db-cairn` re-expression | Cairn | Migrate registry and candidate contracts without importing the old workspace or copying `smb`. |
| Cairn `wm-index` source walking/chunking/projections | Not migrated | Cairn | Future ingest/view crates emit source-coordinate candidates only. |
| Cairn `wm-cli` / `wm-eval` | Not migrated | Cairn | Future domain tools consume published Cairn contracts; they do not become ORIGIN or Forge. |
| Cairn `smb` fact-store twin | Frozen predecessor debt | None | Do not migrate or fork it. Any retained behavior must be rederived against a current contract and one canonical owner. |
| LEM episode primitive, clocks, contradictions, LocalWorld | Bounded `logical-db-episode` model | LEM / ORIGIN | ADR-0002 migration in progress; v0 BLAKE3 and v1 SHA-256 remain distinct until adapter proof. |
| LEM durable ORIGIN / FjallFactStore authority | Not migrated | LEM / ORIGIN | Future `logical-db-lem-core` owns semantic append through abstract Cloud ports, never raw FDB/redb. |
| Forge SDK/harness action/receipt discipline | External reference only | Forge | Future action/receipt contract lives above LEM and returns candidate evidence through Cairn. |

The predecessor repositories are readable provenance, not implementation sources.
No reverse path dependency, copied canonical store, or vendored twin is allowed.

## Current crate position

The current workspace’s one-way lower-core DAG remains valid:

```text
logical-db facade
  ├── model / CRDT / embedded store / sync / protocol / platform
  └── optional semantic ports: episode → cairn

service composition: HTTP/server/daemon + service contract + FDB adapter
```

`logical-db-cairn` and `logical-db-episode` remain optional, bounded migration
ports. Storage, CRDT, protocol, sync, HTTP, and service crates must not depend
on them. The future target layout adds Cloud/Cairn/LEM/Forge ownership directories
only in incremental compatibility-preserving migrations.

## Migration protocol

A domain move is a sequence of reviewable changes:

1. add the target public contract and `contract/domains/<domain>.toml` record;
2. add a compatibility re-export at the old path and an external fixture consumer;
3. move one owned implementation or value type, with a generated dependency-graph
   receipt showing only declared edges changed;
4. update consumers and independent domain lane;
5. remove the compatibility path only after every consumer has migrated.

A move that creates a parallel CRDT, store, ORIGIN log, `smb`, or ontology owner
fails the no-twin rule. A domain is not canonical because a directory exists; its
promotion conditions are in its ADR and domain contract.

## Independent work lanes

| Lane | Owns | May run without cross-plane integration |
|---|---|---|
| `cloud-core` / `cloud-service` | durable state, relay, recovery, then partition/view/region contracts | yes, unless public Cloud contract changes |
| `cairn` | registry, source candidates, operational views, SDK | yes, unless candidate contract changes |
| `lem` | episode/ORIGIN operators, LocalWorld, identity adapter | yes, unless candidate/LocalWorld contract changes |
| `forge` | action grants, idempotency, receipts | yes, against fake public ports |
| `cross-plane` | legal candidate→append→LocalWorld→receipt loop | required only for changed cross-domain contract/behavior |
| `sovereign-scale` | capacity, residency, multi-region recovery receipts | provisioned environment only; never substituted by unit CI |

The exact allowed imports and authorities are normative in
[`architecture/DEPENDENCY-RULES.md`](architecture/DEPENDENCY-RULES.md).
