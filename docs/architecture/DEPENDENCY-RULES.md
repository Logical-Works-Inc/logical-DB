# Logical-DB dependency rules

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `../../evidence/SOURCES.md`.

> Status: proposed. This policy is enforced incrementally by the existing
> dependency-contract/evidence verifier; it does not authorize new dependencies.

## One-way domain graph

```text
kernel
  ↑
Cloud Plane ────── immutable contract values only ──────┐
  ↑                                                     │
Cairn ─────────────────────────────────────────────────┤
  ↑                                                     │
LEM / ORIGIN ──────────────────────────────────────────┤
  ↑                                                     │
Forge ─────────────────────────────────────────────────┘
  ↑
applications, product adapters, and end-to-end harnesses
```

Arrows point from a consumer to an allowed dependency. The graph does not imply
that an upper plane can access a lower plane’s private storage or executor
handles: cross-domain use is contract-only.

## Allowed imports

| Consumer | May import | Must not import |
|---|---|---|
| Kernel | Rust core/alloc plus approved estate dependencies | every domain crate |
| Cloud Plane | Kernel and approved Cloud contracts | Cairn, LEM, Forge |
| Cairn | Kernel; published Cloud append/blob/read contracts | Cloud implementation crates, LEM implementation crates, Forge |
| LEM | Kernel; published Cloud ports; Cairn candidate contract via LEM adapter | FDB/redb handles, Cairn ingest/views implementation, Forge runtime |
| Forge | Kernel; published LocalWorld/action/capability contracts | Cloud/LEM/Cairn implementation crates and all physical stores |
| Public `logical-db` facade | published domain boundary crates behind explicit features | domain-private crates |
| E2E | published contracts and explicitly named test adapters | undeclared production-private surfaces |

## Authority prohibitions

- Cloud can persist a supplied canonical record but cannot define its ontology or
  turn a Cairn candidate into a canonical episode.
- Cairn can create a content-addressed candidate but cannot append it as ORIGIN,
  assign truth/trust, or invoke an effect.
- LEM can canonicalize an admitted episode and create LocalWorld projections but
  cannot crawl raw sources or run an action.
- Forge can invoke a capability-scoped action and create a receipt candidate but
  cannot directly write an ORIGIN log, a Cloud implementation, or a Cairn source.
- An application cannot obtain private tuple keys, raw FDB/redb handles, or
  executor authority through a public facade.

## Enforcement

1. Extend the existing dependency contract’s allowed-edge map with domain labels.
2. Add an architecture test for each prohibited edge; it must fail against a
   deliberately injected forbidden dependency.
3. Generate the domain graph from Cargo metadata and compare it with
   `contract/domains/*.toml`.
4. Require a public-fixture consumer for every boundary crate and reject an
   unpublished/private crate in that fixture.
5. Make Issue #25’s transition graph show that canonical promotion and action
   receipts take only the paths in `DOMAIN-BOUNDARIES.md`.

A clean compile is not proof of a legal authority path. A green domain lane is
not proof of the cross-plane loop; only the cross-plane journey may establish
that.
