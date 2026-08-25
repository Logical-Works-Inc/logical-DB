# logical-DB

One storage kernel for every repo. Append-only, content-addressed, five
storage classes over one core — relational, document, graph, and vector
are storage classes, not engines. Quantized vectors by default: popcount
scans + top-K rescore instead of fp32 brute force — the compute-fraction
mechanism is named in `docs/DB_PATHWAY.md` §3 and its measured surface
in `docs/FORMAT.md`.

**Status:** v0.1 release candidate. The kernel crate (`logical-db`) ships
the row model, framed CRC32 journal, MemKv backend, all five storage
classes (record, term, edge, vec-code, claim), a randomized state-machine
suite, planted-corruption negative tests, process-kill recovery proof,
Kani property proofs, and the §9 benchmark suite (`docs/FORMAT.md`).
Pending for v0.1 done: production redb backend behind KvPort (issue #6
§5). Trust state: FUNCTIONAL — every claim above traces to a test or a
benchmark artifact.

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
