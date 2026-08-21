# Vision — logical-DB

Minute-1 orientation: what this is, why it exists, the sequence, and the
invariants that do not bend. Deep detail lives in `docs/DB_PATHWAY.md`;
this file is the thesis.

## Thesis

Every serious product in this constellation is drowning in engine sprawl:
a relational store, a full-text store, a graph store, a vector store — four
WALs, four caches, four failure models, four bills. The sprawl IS the
compute waste. One append-only, content-addressed kernel with four storage
classes replaces all four, quantizes vectors by default, and costs a
fraction of the compute (popcount scans + top-K rescore instead of fp32
brute force).

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

The honest wall: today there is **no kernel**. There is a researched,
evidence-backed pathway (six production architectures read via codegraph;
four arXiv papers grounding the math) and a phase plan (DB-A…DB-E). Every
claim above is design intent until DB-A lands its crash-consistency proof
and ingest benchmark. Trust state of this document: SOURCE_GROUNDED, not
FUNCTIONAL.

## Sequence

DB-A kernel spine → DB-B record+edge → DB-C term → DB-D vec → DB-E
outboard (HelixDB survives; FoundationDB adapter joins here). Filesystem
and GNN faces ride B–D. Full plan: `docs/DB_PATHWAY.md` §5.

## Day-1 invariants

- Out-of-box: fresh clone opens and queries, zero daemons.
- No silent fallback: unknown escalates.
- Content-addressed rows, segments, weights, claims.
- Rust core, C++ only at named boundaries.
- Compute-fraction claims carry mechanisms and measurements.
