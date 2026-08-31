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

The honest wall, updated 2026-08-31: the row model, five compatibility
classes, corruption tests, Kani proofs, and benchmark suite exist. Hybrid v2
adds one app-facing layer over two transaction drivers: embedded redb and the
native FoundationDB client. Account/container/zone is the ownership axis;
record, revision, change, index, blob-reference, and outbox families are
disjoint beneath it. Every admitted write consumes Braid's opaque
`RunnableInvocation`, binds its snapshot to the exact read version, and
atomically records both an idempotency marker and a resumable zone change.

What is not yet proven is equally important. The native adapter compiles but
has not passed the live cluster parity journey on this branch. The LEM adapter
and its tape receipt do not exist here. Change-token retention/compaction,
large-blob transfer, schema evolution, backup/export, and account erasure are
contracts still to implement. The legacy migration primitive maps v1 rows
losslessly into a chosen v2 scope, but a production migration runner and
cutover receipt remain open. This is a real database kernel, not yet a complete
iCloud or Drive service.

## Sequence

Landed locally: kernel spine → five compatibility classes → redb and MemKv
recovery proofs → tuple namespaces → Braid-bound admitted batches → hybrid v2
scope and key families → native FDB adapter → durable zone change tokens → v1
copy primitive. Next: live FDB semantic parity, then the LEM consumer and tape
receipt, then token retention, blob transfer, schema evolution, export/erase,
and the later segment/mmap/hydration work in `docs/DB_PATHWAY.md`.

## Day-1 invariants

- Out-of-box: fresh clone opens and queries, zero daemons.
- No silent fallback: unknown escalates.
- Content-addressed rows, segments, weights, claims.
- Rust core, C++ only at named boundaries.
- Compute-fraction claims carry mechanisms and measurements.
