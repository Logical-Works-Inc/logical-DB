# logical-DB

One storage kernel for every repo. Append-only, content-addressed, five
storage classes over one core — relational, document, graph, and vector
are storage classes, not engines. Quantized vectors by default: popcount
scans + top-K rescore instead of fp32 brute force — the compute-fraction
mechanism is named in `docs/DB_PATHWAY.md` §3 and its measured surface
in `docs/FORMAT.md`.

**Status:** v0.1 release candidate. The kernel crate (`logical-db`)
ships the row model, all five storage classes (record, term, edge,
vec-code, claim), and two crash stories with one crash story each:
canonical `FdbBackend` (file shim today, native `foundationdb` binding
when `libfdb_c` present) plus the framed CRC32 journal over MemKv as
the deterministic reference and fault oracle (journaled open refuses
durable backends per INV-ONE-CRASH-STORY). Both the `FdbBackend` shim
and MemKv pass the same randomized state-machine suite and the same
process-kill recovery proof; `contains_many` (bounded batch, dedup,
continuation) and `cross_tenant_isolated` are covered, and
planted-corruption negative tests and Kani property proofs cover the
codec; `docs/FORMAT.md` carries the §4 benchmark suite. F2 tuple
namespaces, F3 admitted envelope typestate, and #16 opaque
`contains_many` for kernel#804 are landed and wire-stable; F0 recipe
ships (`scripts/local-fdb-cluster.sh`). What is still ahead: live FDB
cluster parity via the LEM tape consumer (logic-os-kernel#804) — not a
second F4 doc bump before receipt — plus segment packing, mmap-first
reads, and hydration. No `Cid`/Braid type is claimed here; integrity
is storage-internal. Trust state: FUNCTIONAL for the shipped surface
— every claim above traces to a test or a benchmark artifact.

## Try it

    cargo run -p logical-db --example mixed_store_roundtrip

One journal file, zero daemons: open → mixed write txn across four
classes → query → close/reopen → verify replay.

## The five axes

1. **One model across every repo** — sentinel, logic-os-kernel, and
   anything else bind through published ports; nobody forks the storage
   truth.
2. **A fraction of the compute** — quantize by default, rescore rarely;
   mmap-first, immutable segments, and hydration lifecycle are design
   intent pending their phases.
3. **Mouldable / low-code** — schemas and use-case packs are rows; a new
   use-case is data, not a migration or a recompile (design intent).
4. **GNN-native** — message passing over stored edges as a query
   primitive (design intent); AI and non-AI artifacts are rows
   distinguished only by claim lineage.
5. **A filesystem face** — vault-shaped and desktop-search-shaped
   projections over the same store (design intent).

Non-negotiable: **DiD by construction** — everything entering passes the
ingest gauntlet; queries are typed ASTs with bound parameters; there is
no string-query surface, so injection-class bugs are structurally absent.
Rust core; C++ only at named boundaries.

## Read order

`VISION.md` → `docs/DB_PATHWAY.md` → `docs/FORMAT.md` → `GOVERNANCE.md`
→ `laws/governance/`.

## Governance & CI

Governance structure follows logic-os-kernel's (ticketing taxonomy,
definition-of-done, ADR series); the constitution lives in `GOVERNANCE.md`.
CI is keel-derived (`ci/keel-ci.toml` → `.github/workflows/ci.yml`) with
eval parity: local runs go through `.wwfd/local-ci.sh`, which writes the
spend-guard receipt on green; merges are local `--no-ff`, never GitHub
merge commits.
