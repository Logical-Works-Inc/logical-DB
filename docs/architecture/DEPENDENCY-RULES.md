# Logical-DB dependency rules

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `../../evidence/SOURCES.md`.

> Status: enforced for the declared import graph. `scripts/domain_contract.py`
> parses `contract/domains/*.toml` and `contract/domains/GRAPH.toml`, reads the
> live graph from `cargo metadata`, and refuses an undeclared owner, a
> double-owned crate, a disallowed cross-domain edge, a product domain importing
> another domain's implementation crate, and a predecessor twin. The clean-room
> evidence lane runs it and compares the committed `evidence/domain-graph.md`
> receipt; `scripts/test-domain-contract.py` injects a forbidden edge per domain
> and proves the gate refuses it. This does not authorize new dependencies, and
> it does not prove the target crates or services exist.

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

1. [x] Extend the existing dependency contract’s allowed-edge map with domain
   labels. `contract/domains/*.toml` and `contract/domains/GRAPH.toml` declare
   `crates`, `public_contracts`, `may_depend_on`, and `forbidden_domains`; the
   composition layers are declared in `[composition]`.
2. [x] Add an architecture test for each prohibited edge; it must fail against a
   deliberately injected forbidden dependency. `scripts/test-domain-contract.py`
   injects one forbidden edge per domain (Cloud→Cairn, Cairn→Cloud
   implementation, LEM→Cloud implementation, Forge→Cloud) plus undeclared,
   double-owned, and twin crates, and asserts the real checker refuses each.
3. [x] Generate the domain graph from Cargo metadata and compare it with
   `contract/domains/*.toml`. `scripts/domain_contract.py --emit` writes
   `evidence/domain-graph.md`; the verifier fails when the committed receipt no
   longer matches the live graph.
4. [ ] Require a public-fixture consumer for every boundary crate and reject an
   unpublished/private crate in that fixture. This is the external-consumer
   proof tracked by PRD WP2 and is not satisfied by the in-workspace e2e crate.
5. [ ] Make Issue #25’s transition graph show that canonical promotion and action
   receipts take only the paths in `DOMAIN-BOUNDARIES.md`. Issue #25 remains the
   execution-time authority; this gate proves only the declared import graph.

A clean compile is not proof of a legal authority path. A green domain lane is
not proof of the cross-plane loop; only the cross-plane journey may establish
that.
