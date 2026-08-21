# logical-DB

One storage kernel for every repo. Append-only, content-addressed, four
storage classes over one core — relational, document, graph, and vector are
storage classes, not engines. Quantized vectors by default: popcount scans +
top-K rescore instead of fp32 brute force, at a fraction of the compute.

**Status:** spec + governance + CI. No kernel code yet — the pathway is
researched, evidence-backed (`docs/DB_PATHWAY.md`), and phased (DB-A…DB-E).
Trust state: SOURCE_GROUNDED.

## The five axes

1. **One model across every repo** — sentinel, logic-os-kernel, and anything
   else bind through published ports; nobody forks the storage truth.
2. **A fraction of the compute** — quantize by default, rescore rarely,
   mmap-first, immutable segments, batched journal, hydration lifecycle.
3. **Mouldable / low-code** — schemas and use-case packs are rows; a new
   use-case is data, not a migration or a recompile.
4. **GNN-native** — message passing over stored edges is a query primitive;
   AI and non-AI artifacts are rows distinguished only by claim lineage.
5. **A filesystem face** — vault-shaped (Obsidian-class) and desktop-search-
   shaped (Spotlight-replacing) projections over the same store.

Non-negotiable: **DiD by construction** — everything entering passes the
ingest gauntlet; queries are typed ASTs with bound parameters; there is no
string-query surface, so injection-class bugs are structurally absent.
Rust core; C++ only at named boundaries.

## Read order

`VISION.md` → `docs/DB_PATHWAY.md` → `GOVERNANCE.md` → `laws/governance/`.

## Governance & CI

Governance structure follows logic-os-kernel's (ticketing taxonomy,
definition-of-done, ADR series); the constitution lives in `GOVERNANCE.md`.
CI is keel-derived (`ci/keel-ci.toml` → `.github/workflows/ci.yml`) with
eval parity: local runs go through `.wwfd/local-ci.sh`, which writes the
spend-guard receipt on green; merges are local `--no-ff`, never GitHub
merge commits.
