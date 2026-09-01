# DB Pathway — one mouldable kernel, a fraction of the compute

Canonical home: `logical-DB` (this repo). Researched inside sentinel 2026-08-21;
moved here 2026-08-21 when the DB became one model across every repo. Sources:
arXiv API + GH API + codegraph explores of six implementations cloned under
`~/wwfd/dbsweep/` (indexes kept for reuse).

## 0. Verdict

Do NOT adopt any single off-the-shelf engine. Sentinel already owns the hard
parts of its epistemics (content-addressed provenance, escalation receipts);
what it lacks is a storage kernel that treats relations, documents, graph
adjacency, and vectors as **storage classes over one append-only,
content-addressed core**, with quantized-by-default vector math. Every
ingredient is proved in production Rust code explored here; none requires a
new dependency today. HelixDB survives as the outboard engine (Director
verdict 2026-08-21: Apache-2.0 verified, may remain).

Positioning: the **new SQLite** — embeddable, zero-daemon, one file per
corpus, universal across use-cases (§8–§11), at a fraction of the compute
(§3). Languages: Rust core, C++ only where a boundary demands it (FDB
adapter, optional FUSE mount). GOVERNANCE §6 governs dependency licenses.

## 1. Evidence base (all `[ran]` unless noted)

| System | Codegraph finding | Steal |
| --- | --- | --- |
| qdrant segment | binary/pq/scalar/turbo quants; RawScorer | Segment immut. |
| arroy | HNSW graph in LMDB u64 keys | ANN as plain KV rows |
| fjall | batched+compressed journal Writer | Batched journal |
| redb | ACID CoW B-tree, savepoints, one file | Commit discipline |
| turso | MVCC: LogicalClock, write sets, tombstones | Rust relational shape |
| helix-db | object_store + tantivy + own HNSW | Object-first segments |

Papers: HNSW `1603.09320` (navigable small-world graphs, no dimensionality
reduction needed); ScaNN `1908.10396` (anisotropic VQ — score asymmetrically:
quantize DB side only); SPANN `2111.08566` (memory holds centroids, disk holds
postings — closure-expansion fixes boundary misses); RaBitQ `2405.12497`
(1-bit codes with theoretical error bound). GNN anchors (design references,
not fresh fetches): GCN `1609.02907`, GAT `1710.10903` (§8).

## 2. Why the blend works: one core, five storage classes

The industry splits along engine lines (Postgres+pgvector, Redis, Neo4j).
That split IS the compute waste: four WALs, four caches, four serialization
paths, four failure models. The blend insight, proved independently by arroy
(graph-in-KV), qdrant payloads (relations-in-vector-DB), and HelixDB v2
(text+vector+graph over objects): **an index is just rows obeying a key
discipline**. Concretely, over one append-only segment store:

    row  = (class, id, ts, payload)
    class ∈ {record, term, edge, vec-code, claim}

- `record`: typed relational rows (evidence packets, file metadata) —
  qdrant payload-index pattern gives secondary lookups without SQL.
- `term`: BM25 postings (FTS5-equivalent semantics; tantivy format if we
  outgrow FTS5).
- `edge`: adjacency as prefix-scanned `(src,dst)` rows — arroy key-packing;
  no separate graph engine.
- `vec-code`: RaBitQ/TurboQuant 1-bit codes + centroid table (SPANN shape).
- `claim`: content-addressed claims with provenance — already built,
  unchanged.

One journal, one cache, one crash story. INV-ONE-CORE-FIVE-CLASSES.

## 3. The compute fraction (the real SOTA)

1. **Quantize by default, rescore rarely.** Store 1-bit RaBitQ codes; scan
   them SIMD-popcount-fast; asymmetric rescore only the top-K (ScaNN). Memory
   and bandwidth drop ~32x vs fp32; recall loss is bounded (RaBitQ's error
   bound) and repaired by rescore.
2. **Segment immutability = zero write amplification at query time.**
   Immutable segments never rewrite live data (qdrant, HelixDB both prove the
   operational win); queries touch cold pages via mmap without copying.
3. **mmap-first reads.** redb/qdrant pattern: page cache IS the buffer pool.
   No bespoke cache invalidation — INV-MMAP-IS-THE-CACHE.
4. **Batched group-commit journal** (fjall Writer): one fsync per batch, not
   per op.
5. **Hydration lifecycle** (HelixDB): hot set resident, cold segments fetched
   from object storage on first touch, retired after idle. Crawl-shaped and
   vault-shaped corpora both fit.

Net effect: brute-force fp32 scan replaced by popcount scan + K rescores;
write path = append + periodic compaction; read path = mmap + SIMD. That is
the "fraction of compute" claim, mechanism named; its measured surface is
the benchmark suite in `docs/FORMAT.md` §4 — a claim without a run there
does not change this document.

## 4. Mouldability contract (what "mouldable" means operationally)

- One port trait per class (`RecordIndex`, `TermIndex`, `Adjacency`,
  `VectorCodes`) with deterministic in-memory reference impls; engines are
  swappable behind them.
- Schemas are data: a table/index registration is itself a row in `record`
  class — moulding = writing config rows, not migrations.
  INV-SCHEMA-IS-DATA. §10 elevates this into the low-code surface.
- Escalation semantics preserved: unknown class → `Capability::Unknown(
  Escalate)`, never a silent fallback.
- HelixDB remains an outboard engine; the kernel must be functional WITHOUT
  it (fail-closed out-of-box rule).

## 5. Pathway phases (status 2026-08-31)

- **DB-A — Kernel spine — LANDED** (row model, framed CRC32 journal,
  MemKv default backend, kill-9 recovery proof; segment packing and
  mmap reads remain future work).
- **DB-B — Record + adjacency classes — LANDED** (`logical-db` crate;
  sqlite bit-for-bit parity was not built; correctness is held by the
  randomized reference-model suite instead).
- **DB-C — Term class — LANDED** (BM25 postings with maintained term
  manifests; FTS5 A/B was not run).
- **DB-D — Vec class — LANDED** for the quantized path (codes +
  centroid index + brute_scan top-K); exact fp32 baseline pending.
- **DB-E — hybrid substrate (ADR-002/003) — IMPLEMENTED, PROVEN LOCALLY**:
  the default profile embeds redb; `OutboardDb` uses the pinned native FDB
  client. Both consume the same account/container/zone `WriteBatch`, real
  Braid runnable proof over the exact request CID and inspected effect,
  idempotency marker, per-zone conflict frontier, and durable zone change feed.
  Embedded recovery and the local live FDB 7.3 two-writer journey are green. A
  remote Linux run and the LEM tape consumer receipt remain mandatory before
  release.

Dependency order A→B→C→D→E; B/C can parallelize after A. §9's filesystem
face and §10's packs ride on B–D; §8's GNN compute rides on D.

## 6. Starting from FoundationDB — the Director's question, answered

Facts `[ran]`: apple/foundationdb 16.6k stars, C++, Apache-2.0; the macOS
installer ships a first-class local `fdbserver`
(documentation/sphinx/source/local-dev.rst `[read]`). Rust bindings
foundationdb-rs 226 stars, Apache-2.0, pushed 2026-08-17 — alive, binds the
stable `fdb_c` ABI.

What FDB gives us free — the hardest DB-A problems, solved with the
strongest correctness culture in the industry (deterministic simulation):
strictly serializable OCC+MVCC transactions, crash-safe by construction,
tuple/range keyspace. All four storage classes are rows; the arroy pattern
ports to FDB unchanged. C++ is a permitted language; Apache-2.0 passes
GOVERNANCE §6.

What it costs — FDB is client-server, not embedded: even local mode runs a
daemon process with recovery logs and an elected coordinator. A fresh
`cargo run` clone would demand an fdbserver before the first query — that
violates the fail-closed out-of-box rule. And the compute-fraction story is
client-side math (quantized scans + rescore); the KV underneath changes
operational weight and the transactional spine, not the vector arithmetic.

Verdict (amended by ADR-002 2026-08-26): FDB is the canonical
substrate after parity, not merely scale-out. Runtime split: device
uses embedded redb (zero daemons, pure Rust) for local authoritative
state; cloud/enterprise uses FDB for distributed serializable
transactions, replication, and fault tolerance. Keys/namespaces stay
byte-compatible across the split so a local world can migrate/sync
without semantic change. Concretely, inside the mouldability
contract (§4):

- DB-A ships the four storage classes over a `KvPort` trait with a
  pure-Rust embedded backend as default (redb/fjall-class, zero
  daemons) — the device profile.
- DB-E now stages F1–F4: FDB canonical migration (F0 recipe ships), not
  just an optional outboard; HelixDB remains outboard. Same kernel,
  strictly serializable multi-node transactions when the corpus outgrows
  one machine via the cloud profile.
- Steal FDB's discipline even where we don't ship its code: DB-A's
  journal and compaction state machine gets a deterministic simulator
  harness with seeded schedules as its regression suite
  (INV-SIMULATE-THE-SPINE).

This keeps "our own" true: the kernel, classes, and semantics are ours;
FDB is the canonical engine after parity, behind the port we own.

## 7. Rejected alternatives (named reasons)

- Adopt SurrealDB/LanceDB wholesale: multi-model breadth is not mouldable
  depth; brings their failure model and license posture (surrealdb =
  NOASSERTION — unverified terms, GOVERNANCE §6 blocks default-path
  adoption unverified); LanceDB fine as an arrow/parquet reader later, not
  the kernel.
- Keep four subsystems (sqlite+fts5+graph-crate+vector-crate): the compute
  waste this pathway exists to remove.
- turso as kernel: excellent relational core, but SQLite-compat surface is
  scope we do not need; MVCC shape noted for DB-A journal design.
- FDB as the embedded default: rejected per §6 — daemon-per-clone breaks
  the out-of-box rule; adopted instead as the scale-out adapter tier.

## 8–11. Future axes (design intent, not v0.1 scope)

Sections 8 (GNN compute), 9 (filesystem face), 10 (use-case packs), and
11 (DiD gauntlet) are the axes this kernel is being built TOWARD, kept
here as design intent with their evidence. None of them is required to
call logical-DB finished at v0.1 (issue #6 §10.7): no filesystem, no
pack interpreter, no sync, no agent surface blocks the library boundary.
They are named future phases, Director-gated, and nothing in them
constrains the shipped crate today.

## 8. GNN-native compute — the DB acts like a GNN model

Director axis 2026-08-21: "act almost like a GNN model; blur the lines
between AI and non-AI." The storage classes already are a labeled property
graph: `record` = node features, `edge` = adjacency, `vec-code` = learned
or quantized embeddings, `claim` = provenance-stamped labels. GNN mechanics
map onto kernel primitives without a new engine:

- **Message passing = k-hop prefix scans + combine.** A GNN layer is:
  gather neighbor rows (`edge` prefix scan from `src`), read their
  `vec-code`/payload rows, apply a combine (weighted sum / attention), write
  the result as new `vec-code` rows. GCN `1609.02907` and GAT `1710.10903`
  are exactly this loop; nothing in it needs a tensor framework for the
  common case — SIMD over quantized codes is the same compute fraction as
  §3.
- **Inference IS a query.** "Rank nodes by propagated importance from seed
  S" is a query plan (scan → propagate → rescore), not a training job.
  Retrieval and learning read the same rows through the same ports.
  INV-GNN-IS-A-QUERY: every graph-learning primitive is expressible as a
  deterministic query over the classes; anything that cannot be is escalated,
  never silently approximated.
- **Blur AI/non-AI.** There is no "AI store" beside the DB. Model weights
  are content-addressed `record` rows; embeddings are `vec-code` rows;
  provenance of any learned artifact is a `claim` row. A heuristic and a
  model output are both just rows — distinguishable only by their claim
  lineage, which the epistemic layer already enforces (no model output
  acquires source authority without a recorded transformation).

## 9. Filesystem face — powering vaults and replacing Spotlight

Director axis 2026-08-21: "work as an overall file system (think powering an
Obsidian, or replacing Spotlight search)." The classes are already a file
system in waiting:

- **File → rows.** A mounted file becomes: `record` (path, mtime, kind,
  content hash), `edge` (hardlinks, wiki-links, directory containment),
  `term` (full text), `vec-code` (semantic embedding). Content addressing is
  free dedupe and free versioning — every write is a new content row; the
  path row points at the tip. Git's model without git's plumbing.
- **Vault use-case (Obsidian-class).** A vault is a directory of markdown;
  wiki-links are `edge` rows written at ingest; backlinks are the reverse
  prefix scan; search is `term` ∪ `vec-code`. No vault-specific code — it is
  a use-case pack (§10).
- **Spotlight replacement.** Spotlight is a closed indexer; ours is an open
  one: a standalone watcher daemon ingests user directories into the store;
  queries hit `term`/`vec-code` with the same API the DB exposes everywhere
  else. Runs as a user daemon, never a kernel extension.
- **Mount surface.** A `VfsPort` trait with a library API as the default and
  an optional FUSE/macFUSE backend later. The mount is never required —
  INV-FS-IS-A-VIEW: the filesystem face is a projection over the same store,
  never a second truth. Out-of-box rule holds: clone, open, query.

## 10. Low-code use-case packs — the clean path for a new use-case

Director axis 2026-08-21: "relatively low code… a clean way for a new
use-case." INV-SCHEMA-IS-DATA (§4) elevated into the product surface:

- **A use-case pack is rows, not code.** Registering "wiki vault" or
  "crawl corpus" or "chat memory" = writing pack rows: schema (typed fields
  per class), index declarations, ingest pipeline bindings, query templates.
  The kernel interprets packs; no recompile, no migration — a new use-case
  is data.
- **Pipelines are declared, not written.** An ingest pipeline is an ordered
  list of typed stages from a fixed registry (parse, chunk, embed, link,
  validate). Custom stages are the ONLY code path, and they implement a
  typed port with escalation semantics — never string-glue.
- **Query templates.** Packs ship named parameterized queries (typed AST,
  §11). The common use-case never composes raw queries at all.
- **Parity with the "new SQLite" bar.** SQLite's magic is: one file, one
  C library, any schema. Ours: one store, one Rust library, any pack —
  and the pack system is what keeps "any schema" from becoming "any
  failure mode".

## 11. DiD by construction — injection-class bugs are structural, not patched

Director axis 2026-08-21: "everything entering this DB is auto heavily
processed; SQL-injection-type stuff becomes a thing of the past."
Defense-in-depth is the architecture, not a sanitizer bolted on:

- **The ingest gauntlet.** Every byte entering the store passes: parse →
  validate (typed schema) → normalize → capability check → content-address →
  journal. Nothing raw is ever persisted; there is no "store then hope"
  path. INV-DID-GAUNTLET: a row exists if and only if it passed every stage.
- **No string-query surface exists to inject into.** Queries are typed ASTs
  built through a builder API; parameters are values bound at plan time,
  never spliced into text. There is no SQL dialect, so there is no SQL
  injection — the bug class is absent by construction, not filtered.
  INV-NO-STRING-QUERY.
- **Blast-radius isolation.** Stages run with capability envelopes; a
  malicious file can poison at most its own parse stage's output, which
  validation then refuses. Provenance (`claim` rows) makes any contamination
  traceable and revocable by content address.
- **Languages.** Rust core; C++ only at named boundaries (FDB adapter,
  optional FUSE backend). `#![forbid(unsafe_code)]` in pure-Rust crates;
  every unsafe boundary documented and fuzz-gated once code lands
  (cargo-fuzz in CI at DB-A).
