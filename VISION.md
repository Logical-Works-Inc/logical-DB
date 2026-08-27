# Vision — logical-DB

Minute-1 orientation: what this is, why it exists, the sequence, and the
invariants that do not bend. Deep detail lives in `docs/DB_PATHWAY.md`;
this file is the thesis.

## Thesis

Every serious product in this constellation is drowning in engine sprawl:
a relational store, a full-text store, a graph store, a vector store — four
WALs, four caches, four failure models, four bills. The sprawl IS the
compute waste. One append-only, content-addressed kernel with five storage
classes (record, term, edge, vec-code, claim) replaces all four engines,
quantizes vectors by default, and targets a fraction of the compute
(popcount scans + top-K rescore instead of fp32 brute force; mechanism and
measured surface in DB_PATHWAY §3 and FORMAT.md §4).

We are building the **new SQLite**: embeddable, zero-daemon, one store per
corpus, mouldable to any use-case through data (packs), not code — and
GNN-native, filesystem-faced, and injection-proof by construction.

## What it must be (the five axes)

1. **One model across every repo.** sentinel, logic-os-kernel, and anything
   else bind here; nobody forks the storage truth. (INV-ONE-MODEL-EVERYWHERE)
2. **A fraction of the compute.** Quantize by default; rescore top-K;
   mmap-first; immutable segments; batched journal; hydration lifecycle.
3. **Mouldable.** Schemas are rows; use-case packs are rows; engines are
   swappable behind port traits.
4. **GNN-like.** Message passing over stored edges is a query primitive;
   AI and non-AI artifacts are rows distinguished only by claim lineage.
5. **A filesystem.** Vault-shaped (Obsidian-class) and desktop-search-shaped
   (Spotlight-replacing) faces are projections over the same store.

And non-negotiable: DiD by construction — heavy processing at ingest, no
string-query surface, injection-class bugs structurally absent.

## The wall

The honest wall, updated 2026-08-27: the kernel spine exists — row
model, all five storage classes, randomized state-machine suite run on
`FdbBackend` file shim vs MemKv oracle, corruption negative tests,
Kani proofs, benchmark suite (`docs/FORMAT.md`), and two crash
stories with one crash story each: `FdbBackend` canonical (file shim
today, native `foundationdb` binding when `libfdb_c` present) plus the
framed journal as MemKv oracle (journaled open refuses durable
backends per INV-ONE-CRASH-STORY). F2 tuple namespaces
(`cross_tenant_isolated`), F3 admitted envelope typestate, and #16
opaque `contains_many` (bounded batch, deterministic dedup/order,
continuation, tenant-isolated) are landed for kernel#804. ADR-002
ratifies FDB as canonical after parity; F0 recipe ships
(`scripts/local-fdb-cluster.sh` — verified 2026-08-27 host
 `/tmp/fdb-f0` arm64 7.3.79 + `foundationdb/build:rockylinux9_arm64-latest`
 with `~/Downloads/foundationdb-7.3.77` source). What is still
 ahead: LEM tape parity (logic-os-kernel#804) — live `FdbBackend`
 `get/set/clear + ordered range` already proven in
 `kernel/tests/fdb_live.rs:live_fdb_cluster_roundtrip`; remaining
 is the full FactStore consumer slice — segment packing,
mmap-first reads, hydration. Compute-fraction claims carry mechanism
and measurement in the benchmark artifact; filesystem (§9), packs
(§10), GNN (§8) remain design intent until their phases land. No
`Cid`/Braid type is claimed here; integrity is storage-internal.
Trust state: FUNCTIONAL for the shipped surface.

## Sequence

Landed: DB-A kernel spine → DB-B record+edge → DB-C term → DB-D vec →
DB-E claim/metadata → redb production backend + MemKv parity
(issue #6 §5) → FDB canonical migration F0 recipe (ships) → F1
`FdbBackend` file shim → F2 tuple namespaces
(`cross_tenant_isolated`) → F3 admitted envelope typestate → #16
opaque `contains_many` (bounded batch, continuation, tenant-isolated)
for kernel#804. Next: parity via LEM tape on a live FDB cluster
(logic-os-kernel#804 / #801) — not a second F4 doc bump before
receipt — then segment packing, mmap, hydration per
`docs/DB_PATHWAY.md` §5–§6 and ADR-002. HelixDB interop stays
outboard. Filesystem and GNN faces ride on later phases.

## Day-1 invariants

- Out-of-box: fresh clone opens and queries, zero daemons.
- No silent fallback: unknown escalates.
- Content-addressed rows, segments, weights, claims.
- Rust core, C++ only at named boundaries.
- Compute-fraction claims carry mechanisms and measurements.
