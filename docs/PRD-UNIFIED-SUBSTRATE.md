# PRD: The Unified Ontology Substrate — Logical-DB + Cairn + LEM

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `evidence/SOURCES.md`. Competitive and SOTA research sources are cited
> inline; they were retrieved 2026-09-16 by parallel research agents.

- Status: proposed v1 product definition; Director-scoped 2026-09-16
- Owner: Director; execution: AI agent swarms per `SPEC` section
- Supersedes: nothing; ratifies the recorded rulings in `GOVERNANCE.md`
  (#6 substrate-not-apps, #8 redb-local + FDB-distributed) and extends them
  with the semantic-plane unification of ADR-0001

## 1. Product thesis

One substrate, three planes, one identity model. Cairn (observation
vocabulary), Logical-Event-Memory (episodic memory), and Logical-DB
(durable typed state) are **one product**: an embeddable, local-first
ontology substrate that takes messy enterprise data, deduplicates it from
the first write, keeps every mutation reversible, and exposes a
citation-bearing memory and ML-training-ready graph — without the
developer configuring any of it.

Positioning: **Supabase's developer ergonomics × Palantir Foundry's
ontology**, delivered embedded and AI-native. Palantir proved the ontology
moat (semantic + kinetic layers over messy data; 120%+ net dollar
retention analyses: https://coomia.com/blog/palantir-business-model) but
structurally cannot ship embedded, per-developer, self-serve distribution
without cannibalizing its forward-deployed-enterprise motion
(https://forwarddeployedbook.com/how-palantir-makes-money analysis set).
Databricks proved the mess-to-clean pipeline pattern (medallion:
https://www.databricks.com/glossary/medallion-architecture) and
governance-as-substrate (Unity Catalog:
https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
but cannot be embedded or local. Supabase proved instant-API developer
love (109k+ stars: https://github.com/supabase/supabase) but has **no
semantic layer, no entity resolution, no agent memory, and no offline
story** (inventory: https://supabase.com/llms.txt). ElectricSQL's
absorption into Databricks proved sync alone is not a business
(https://electric-sql.com/blog/2026/08/11/electric-joining-databricks);
the substrate that owns state, identity, and memory is.

**The Tesler's-law bargain, stated as a contract:** complexity is
conserved; someone must hold it. The developer hands us the mess
(dirty records, duplicate identities, contradictory facts, schema drift)
and we hold the complexity permanently: dedupe, resolution, provenance,
temporal validity, and forgetting are substrate defaults, not app code.
SOTA semantics are the *default*, and opting out is the explicit act.

## 2. Why each buyer wants it (Director-stated, made testable)

| Persona | Want | Measurable acceptance |
|---|---|---|
| Developer, app 1 | Ship fast; no data plumbing | 5-minute quickstart compiles first-try; next-token benchmark ≥ 90% |
| Developer, app 2 | **Never rebuild the world** — reuse profiles, identities, ontology | "Second app checklist": onboarding app 2 onto an existing estate requires zero re-definition of user profile, identity, or ontology types; measured as steps-to-first-reuse |
| Enterprise customer | "Be as messy as you want; the DB takes care of it" | Chaos-corpus ingest (nulls, dupes, conflicts, encodings, drift) → typed graph + quarantine ledger; zero silent loss; ER precision/recall ≥ agreed thresholds on dirty benchmarks |
| AI agent builder | "AI agent memories just work" | Episodes with inline citations to source spans + temporal validity; LocalWorld memory beats cosine-RAG baseline on ratified eval (issue #34) |
| ML team | Training-ready graph | PyG/OGB-compatible export with datasheets and leakage-safe split manifests; ER completeness as precondition for entity-disjoint splits |

## 3. Architecture: three planes, two surfaces

```text
applications / agents (MCP + generated SDKs)
   |
   | ONTOLOGY SURFACE  (typed objects, links, actions; schema-as-code)
   |
logical-db embedded core (ships today: CRDT state, indexes, KV,
   |    change feed, bounded sync, FDB relay + recovery)
   |
   |-- MEMORY PLANE    (Cairn vocabulary -> episodes -> LocalWorld)
   |-- IDENTITY PLANE  (entity resolution as derived projection)
   |-- ML PLANE        (export: PyG/OGB/Arrow + datasheets + splits)
```

### 3.1 Ontology surface (new; the Palantir lesson, embedded)

Object types, link types, and action types as versioned artifacts defined
in application code (schema-as-code, derives to typed clients). Actions
are the only sanctioned write path — validated, transactional, and
revertible — which is what makes agent writes safe and auditable
(Palantir pattern: https://www.palantir.com/docs/foundry/action-types/overview/).
Every ontology is also exposed as an MCP server from day one with
read-only-by-default scoping (Supabase MCP pattern:
https://supabase.com/docs/guides/getting-started/mcp), and typed client
SDKs are generated from the ontology subset an application is granted
(OSDK pattern: https://www.palantir.com/docs/foundry/ontology-sdk/overview/).

### 3.2 Identity plane: dedupe from the get-go (the wedge)

Entity resolution is **derived, re-runnable projection over an immutable
log**, never in-place mutation (Splink architecture:
https://github.com/moj-analytical-services/splink). Write-time resolution
assigns stable cluster IDs immediately; a resolution engine upgrade
re-projects without data loss, and cluster merge/split are first-class,
versioned, reversible events (the unsolved gap in current SOTA — no
embeddable system does resolve-at-write with revisit-at-read: see SOTA
gaps in §8 sources). Default profile is zero-config: embedding-based
blocking (USearch/Tantivy) with recall targets + LLM-oracle calibration
for the ambiguous residue; human-in-the-loop match confirmation is an
action, not a UI afterthought.

### 3.3 Full reversibility (the Datomic/XTDB/Dolt lesson, embedded)

Append-only change log (already the Logical-DB core) + bitemporal
validity on ontology facts (SQL:2011 / Graphiti `t_valid`/`t_invalid`
pattern: https://arxiv.org/abs/2501.13956) + **generated compensating
transactions**: undo is computed by diffing current ↔ target state and
emitting the restoring write — inverses are derived from data, never
authored by humans or agents. Point-in-time queries and time travel are
reads, not mechanisms. Provenance (W3C PROV-O:
https://www.w3.org/TR/prov-o/) is captured as columns, not a sidecar.
Right-to-erasure is handled by explicit excision events that preserve
the proof of deletion without the payload.

### 3.4 Memory plane: agent memory that cites (the flagship claim)

Cairn vocabulary → episodes → LocalWorld projections with **inline
citations to source spans and temporal validity** — the provenance-first
memory API no current system ships (Mem0, Graphiti, Letta, Cognee all
return "relevant memories"; none return inline citations with validity:
https://arxiv.org/abs/2504.19413, https://github.com/getzep/graphiti).
The #34 comparative claim (LocalWorld citations beat cosine RAG) is the
headline, measured on ratified benchmarks (LongMemEval:
https://arxiv.org/abs/2410.10813; LoCoMo: https://arxiv.org/abs/2402.17753)
against Mem0/Graphiti baselines.

### 3.5 ML plane: messy-in, graph-out, training-ready

Medallion as the shipped default (bronze raw immutable → silver resolved
entities → gold consumption projections), lineage captured from usage not
declaration (Unity Catalog pattern), and export to PyG/OGB/Arrow with
datasheets (https://arxiv.org/abs/1803.09010) and **leakage-safe split
manifests** — chronological + entity-disjoint splits, where ER
completeness is the precondition that makes splits defensible
(https://arxiv.org/abs/2309.15730, https://arxiv.org/abs/2307.01026).

## 4. Dependency strategy (Director-authorized wrappers, via logicalworks-crates)

Every wrapper enters through `logicalworks-crates` with: provenance
record, license check, version pin, wrap-not-fork boundary, and a written
justification. Research-verified table:

| Component | License | Verdict | Justification |
|---|---|---|---|
| Tantivy | MIT | **wrap** | blocking candidates for ER + memory keyword recall; Rust, mature |
| USearch | Apache-2.0 | **wrap** | ANN for embedding blocking + memory recall |
| Arrow/Parquet | Apache-2.0 | **build on** | record batches + export format |
| redb | Apache/MIT | already core | local store |
| DuckDB (`duckdb-rs`) | MIT | **wrap (optional feature)** | batch ER re-projection / analytics SQL sidecar |
| RocksDB crate | Apache grant | conditional | only if LSM needed; Apache side only |
| Splink | MIT | **methodology port** | Fellegi-Sunter core is small math; do not shell out to Python |
| Zingg | **AGPL-3.0** | **excluded** | copyleft poisons the substrate |
| Datomic | proprietary | excluded | reference only |
| Kùzu | MIT but archived | excluded | dead-end dependency risk |
| Mem0/Graphiti/Letta/Cognee | Apache-2.0 | **eval baselines only** | separate Python processes for benchmarks, never in-process |

## 5. Cairn + LEM migration (Director-authorized 2026-09-16 — "properly bring in the code")

Per ADR-0001 the sibling repos were frozen read-only references with LEM
SOT authority; the Director has now authorized the transfer. Proper
migration contract:

1. **SOT transfer record** — ADR-0002 records the authority move
   (ORIGIN/CAIRN plane ownership into this workspace), the Director
   authorization date, and the frozen-repo pointers. Requires the
   migration proof below before any "canonical" claim is made.
2. **Code migration** — port `wm-core` Rust modules
   (`claim_schema.rs`, `geometry.rs`, `hash.rs`, `schema.rs`, `types.rs`),
   the Cairn vocabulary surface (`world_token.py`, `ontology.py`
   semantics already partially re-expressed in `logical-db-cairn`), and
   LEM operators + `LocalWorld` (`~/LOGICAL-EVENT-MEMORY/src/lib.rs`,
   685 lines) into `logical-db-cairn` / `logical-db-episode` as the
   authoritative implementations, replacing re-expressions.
3. **Versioned identity adapter** — the locked LEM v0 BLAKE3 wire and
   the workspace v1 SHA-256 framing are translated by an explicit
   versioned adapter recording hash scheme, canonicalization, payload
   location, and migration status (ADR-0001 identity clause). No silent
   32-byte equivalence.
4. **Migration proof (acceptance, from ADR-0001 obligations)** — two
   independent scopes; malformed and scheme-mismatched wire input
   refused; duplicate/reordered delivery; restart/recovery;
   source-coordinate preservation; bounded payloads; durable ORIGIN
   store journey. Until the proof lands, docs say "migration in
   progress", never "canonical".

## 6. Execution: agent-swarm spec

Swarm roles: **Build** (implementation), **Verify** (independent
falsification: mutation campaigns, hostile review, black-box journeys),
**Benchmark** (next-token score, ER P/R on dirty corpora, memory eval vs
cosine-RAG + Mem0/Graphiti), **Docs** (quickstart, llms.txt + per-page
.md mirrors, MCP), **Release** (crates.io, semver, changelog).

Work packages (agent-sessions are planning units, not commitments):

| WP | Surface it ships | Size | Human gate |
|---|---|---|---|
| WP0 | Batch orchestrator for external agent fleets (API-key batch runs with receipts) | 1-2 sessions | none |
| WP1 | L0/L1 crate: prelude, put/get sugar, digest helper, KV feature, 5-line quickstart; next-token benchmark ≥90% | 3-5 sessions | crate name + publishing identity |
| WP2 | Cairn+LEM migration per §5 (code port + adapter + proof) | 5-8 sessions | ADR-0002 ratification is recorded (done 2026-09-16); proof review |
| WP3 | Ontology surface: object/link/action types, schema-as-code derives, revertible actions | 8-12 sessions | action-type API shape |
| WP4 | Identity plane: blocking + zero-config default profile + match-confirmation actions | 8-12 sessions | ER thresholds on dirty corpora |
| WP5 | Reversibility: bitemporal facts, generated compensations, PIT queries, excision | 6-10 sessions | none (acceptance is WP-internal) |
| WP6 | Memory surface: episodes→LocalWorld with citations; #34 eval harness + run | 6-10 sessions | corpus + baseline ratification (2026-09-14 ruling) |
| WP7 | ML export: PyG/OGB/Arrow + datasheets + split manifests | 3-5 sessions | none |
| WP8 | MCP server + llms.txt + docs mirrors + hosted-relay surface (existing FDB stack) | 4-6 sessions | relay ops decisions |

Sequence: WP1 ∥ WP2 → WP3 → WP4 ∥ WP5 → WP6 → WP7 ∥ WP8. Every WP ends
at a merged, CI-green PR (self-hosted lanes) with black-box journey
receipts; the top-down ladder rule applies — no WP may add depth below an
unconsumed surface.

## 7. Metrics (all measured, none asserted)

- Second-app reuse: steps-to-first-reuse ≤ 5, zero re-definition of
  profile/identity/ontology types.
- Next-token: ≥ 90% first-try compile over N generated samples.
- Dedupe: cluster-level P/R on WDC Products + chaos corpus with zero
  config; stable-ID guarantee under model upgrade (merge/split events
  only).
- Reversibility: 100% of action types revertible via generated
  compensation; PIT query correctness suite; provenance completeness
  (every value traces to source episode/record).
- Memory: statistically significant win vs cosine-RAG and Mem0/Graphiti
  baselines on ratified eval; inline citations present on 100% of
  returned memories.
- Prod readiness: all REWRITE_MANIFEST gates applicable to shipped
  surface green; self-hosted CI lanes green on release heads; ThinkPad
  baseline measured for the embedded path.

## 8. Open research obligations carried forward

Unverified residuals from this PRD's research (tracked, not hidden):
Motif paper ID unresolved; headline memory-benchmark numbers are
vendor-run; LoCoMo data license (CC-BY-NC-SA) constrains redistribution;
license of HippoRAG code unverified; hosted-relay business model is a
Director decision, not an engineering one.
