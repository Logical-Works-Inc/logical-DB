<!-- GENERATED from repo-map.json by scripts/repo-map.mjs — DO NOT EDIT BY HAND. -->
# REPO MAP

Rendered view of `repo-map.json`. Canon: **ADR-001 + ADR-002 (FDB substrate) + #16 opaque contains_many**. Updated: 2026-08-27.
Reconcile with `node scripts/repo-map.mjs check`.

## Repos (cross-repo)

- `logical-DB` (this repo) — canonical DB model + kernel home (INV-ONE-MODEL-EVERYWHERE)
- `sentinel` — first consumer — evidence/epistemics engine; pathway researched here 2026-08-21
- `logic-os-kernel` — consumer — governance structure donor; Fact envelope / Tape contract adapter pending ADR-001 ratification
- `keel` — CI provider — keel-ci-plan renders and enforces the workflow

## LIVE — authoritative today

| Path | Pillar | What it is |
|------|--------|------------|
| `VISION.md` | spec | Thesis, the wall, sequence, day-1 invariants. |
| `docs/DB_PATHWAY.md` | spec | Spec of record: evidence base, storage classes, compute fraction, phases DB-A…DB-E, axes (GNN §8, filesystem §9, packs §10, DiD §11). (ADR-001) |
| `GOVERNANCE.md` | governance | Project constitution: authority model, invariants, decision process, proof ladder, licenses. |
| `laws/governance/` | governance | Process framework from logic-os-kernel: operating model, ticketing taxonomy, definition-of-done, issue template, collaboration standards. Components: operating-model.md, definition-of-done.md, collaboration-standards.md, issue-template.md, decision-2026-08-25-ci-hermeticity.md. |
| `laws/governance/adr-001-canonical-db-model-authority.md` | governance | PROPOSED: this repo owns the one DB model; constellation registration pending Director ratification. |
| `laws/governance/adr-002-foundationdb-substrate.md` | governance | ACCEPTED: FDB is canonical substrate; add-pin-vet, parity-before-delete, F0-F4. |
| `scripts/local-fdb-cluster.sh` | infra | Hermetic one-process FDB dev cluster (INV-FDB-PIN, INV-FDB-SCRATCH); F0 falsifier. |
| `scripts/spec-lint.py` | infra | 80-column prose contract over markdown; CI-enforced with teeth. |
| `ci/keel-ci.toml` | infra | Keel CI plan: stack-position, change-scope, spec-lint, workspace-gated test/fmt/clippy, property gauntlet, Kani proofs, cargo-mutants teeth, MC/DC slot (keel#480), ci-plan-current, eval-log, cleanup. |
| `.github/workflows/ci.yml` | infra | Rendered workflow — canonical bytes enforced against the plan in CI. |
| `.wwfd/local-ci.sh` | infra | Local CI exec boundary; writes the spend-guard receipt and eval cache on green. |
| `docs/FRANKENSTEIN.md` | spec | Organ map: which engine owns each graft (fjall, qdrant, FDB, redb, turso, arroy), internal donors (secure-authority), and the gap list G1…G6. Donor ideas are re-implemented, never vendored; the organs/ dump itself was deleted at v0.1 RC. |
| `docs/FORMAT.md` | spec | v0.1 format contract: journal frame + row encoding v1, KV key layout v1 (INV-ID-PREFIX-DISJOINT), FDB canonical after F4 parity (Redb retired, journal is MemKv oracle only), what is not yet stable, benchmark results schema and algorithmic gates. |
| `kernel/` | kernel | The kernel crate logical-db (F4 parity landed): row model with framed keys, KvPort/DurableKv/ClockPort seams, FdbBackend canonical (file shim today, native binding when libfdb_c present, ADR-002 F1-F4) + MemKv reference + framed journal oracle, Ldb store (journaled open refuses durable backends per INV-ONE-CRASH-STORY), five storage classes, seven-point parity harness, state-machine parity + kill-recovery + planted-corruption suites, Kani proofs; RedbBackend retired. (ADR-001, ADR-002) |
| `kernel/src/contains.rs` | kernel | Opaque bounded batch existence contains_many: exact present/missing with budget, deterministic dedup+ordering, continuation, tenant-isolated via tuple subspaces (F2); supports kernel#804 missing-set. (ADR-002, #16) |
| `kernel/src/tuple.rs` | kernel | Canonical tuple/subspace layer (F2): estate/subspace/tenant prefix + order-preserving codec; INV-TENANT-PREFIX, cross_tenant_isolated property. |
| `kernel/src/admitted.rs` | kernel | Admitted envelope typestate (F3): Safe/Authority/Justification proofs, INV-UNKNOWN-NEVER-WRITES + INV-JUSTIFICATION-FRESH, receipt capture. |

## META — agent/process tooling

| Path | Pillar | What it is |
|------|--------|------------|
| `README.md` | spec | Public charter: five axes, status, read order. |
| `CODEBOOK.md` | governance | Agent execution index: intent, risk zones, file ownership, delivery loop. |
| `CLAUDE.md` | governance | Runtime entry & agent contract; AGENTS.md symlinks here. |
| `repo-map.json` | infra | Declarative structure source of truth (this file). |
| `scripts/repo-map.mjs` | infra | Validates repo-map.json against its schema and renders REPO_MAP.md; check detects drift. |

## Reading order

1. `VISION.md` — minute-1 orientation: thesis, five axes, wall, sequence
2. `docs/DB_PATHWAY.md` — the spec of record — every claim and its evidence
3. `GOVERNANCE.md` — constitution: invariants that outrank convenience
4. `laws/governance/README.md` — process map: ticketing, DoD, ADRs
