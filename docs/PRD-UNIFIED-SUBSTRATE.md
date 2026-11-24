# PRD: the unified ontology substrate

- Status: proposed v1 product definition, Director-scoped 2026-09-16.
- Owner: Director.
- 2026-11-24: the bounded `logical-db-cairn` and `logical-db-episode` ports
  were deleted (#135). They had no storage and no caller. The Cairn
  vocabulary need moves to #143 and the episode log need to #145. This PRD
  keeps the target.

## Thesis

One product, four domains, one identity model:

- **Cloud Plane**: sovereign durable data, regional delivery, local replicas,
  tenant isolation. This is the code in this repo today.
- **Cairn**: source evidence, ontology registry, candidate episodes.
- **LEM / ORIGIN**: canonical episodic memory, clocks, provenance,
  contradiction, LocalWorld.
- **Forge**: capability-bound operations that emit receipts.

Legal loop: Cairn candidate, then LEM append, then LocalWorld, then Forge
receipt, back to a Cairn candidate. Cloud is infrastructure, never semantic or
action authority.

Positioning: Supabase's developer ergonomics with Palantir Foundry's ontology,
embedded and AI-native. Palantir cannot ship embedded self-serve without hurting
its enterprise motion. Databricks cannot run embedded or local. Supabase has no
semantic layer, entity resolution, agent memory or offline story. ElectricSQL
joining Databricks showed sync alone is not a business; the substrate that owns
state, identity and memory is.

The contract: the developer hands over the mess (duplicates, conflicts, drift).
The substrate holds dedupe, resolution, provenance, temporal validity and
forgetting as defaults. Opting out is the explicit act.

## Buyers and acceptance

| Persona | Want | Acceptance |
|---|---|---|
| Developer, app 1 | ship fast | 5-minute quickstart compiles first try |
| Developer, app 2 | never rebuild profiles, identity, ontology | zero re-definition; steps-to-first-reuse at most 5 |
| Enterprise | "be as messy as you want" | chaos-corpus ingest to typed graph plus quarantine ledger; zero silent loss; agreed ER precision and recall |
| Agent builder | memory that just works | recalled records cite source spans with validity; beat cosine RAG on the ratified benchmark (#34) |
| ML team | training-ready graph | PyG/OGB export, datasheets, leakage-safe split manifests |

## Planes

- **Ontology**: object, link and action types as versioned code. Actions are
  the only write path: validated, transactional, revertible. Each ontology is
  also an MCP server, read-only by default, with generated typed clients.
- **Identity**: entity resolution is a re-runnable projection over the
  immutable log. Write time assigns stable cluster ids; merge and split are
  versioned, reversible events. Zero-config default: embedding blocking plus a
  calibrated oracle for the ambiguous residue.
- **Reversibility**: append-only log plus bitemporal facts plus generated
  compensating transactions (undo is a computed diff, never hand-written).
  Provenance (W3C PROV-O) is stored as columns. Erasure is an excision event
  that keeps proof of deletion without the payload.
- **Memory**: episodes to LocalWorld projections with inline citations and
  validity, measured on LongMemEval and LoCoMo against Mem0 and Graphiti.
- **ML**: bronze, silver, gold by default; lineage from use; Arrow, PyG and OGB
  export with entity-disjoint, chronological splits.

## Dependencies

Each enters through the estate crates with provenance, licence, pin and
justification.

| Component | Licence | Verdict |
|---|---|---|
| Tantivy | MIT | wrap: ER blocking, keyword recall |
| USearch | Apache-2.0 | wrap: ANN for blocking and recall |
| Arrow/Parquet | Apache-2.0 | build on: batches, export |
| redb | Apache/MIT | core: local store |
| DuckDB | MIT | optional: batch re-projection |
| Splink | MIT | port the method (Fellegi-Sunter), no Python |
| Zingg | AGPL-3.0 | excluded |
| Kuzu | MIT, archived | excluded |
| Mem0, Graphiti, Letta, Cognee | Apache-2.0 | eval baselines only, out of process |

## Metrics

- Second-app reuse: at most 5 steps, zero re-definition.
- Quickstart: at least 90% first-try compile over N generated samples.
- Dedupe: cluster precision and recall on WDC Products plus a chaos corpus,
  zero config; ids stable across model upgrades.
- Reversibility: every action type revertible by generated compensation.
- Memory: significant win over cosine RAG and Mem0/Graphiti; citations on
  every returned memory.
- Ship: every CI lane green on the release head.

## Open research

Memory benchmark headline numbers are vendor-run. LoCoMo is CC-BY-NC-SA, which
limits redistribution. HippoRAG code licence is unverified. The hosted-relay
business model is the Director's call.
