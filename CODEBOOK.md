# CODEBOOK

Version: 1.0 | Updated: 2026-08-21 | Scope: agent execution index

> **Canonical sources — do not duplicate them here:**
> - **Vision / the *why* (read first)** → `VISION.md` — thesis, five axes,
>   wall, sequence, day-1 invariants.
> - **Spec of record** → `docs/DB_PATHWAY.md` — kernel design, evidence base,
>   phases. Amendments cite evidence (GOVERNANCE §3).
> - **Structure** → `repo-map.json` / `REPO_MAP.md`. Reconcile:
>   `node scripts/repo-map.mjs check`.
> - **Decisions** → `laws/governance/adr-*.md`. **Work status** → GitHub
>   Issues (the only authoritative tracker).

This file holds *intent, shape, risk zones, coordination rules* — not
point-in-time state. When in doubt, link, don't copy.

## 1) Project Intent

One storage kernel for every repo in the constellation: append-only,
content-addressed, four storage classes (`record`, `term`, `edge`,
`vec-code`) + provenance `claim` rows; quantized-by-default vectors; GNN
primitives as queries; a filesystem face; low-code use-case packs; DiD by
construction. The new SQLite — at a fraction of the compute.

Named mechanisms behind every claim live in `docs/DB_PATHWAY.md`; never
restate them loosely here.

## 2) What's settled (stable facts, not a status board)

- Pathway researched and ratified-in-principle 2026-08-21 (Director): one
  kernel, four classes; HelixDB survives outboard; FDB = scale-out adapter,
  never embedded default.
- Governance structure imported from logic-os-kernel (ticketing taxonomy,
  DoD, ADR series starting at ADR-001); constitution in `GOVERNANCE.md`.
- CI is keel-derived with eval parity; `.wwfd/local-ci.sh` is the only local
  entry.
- No kernel code exists yet. Build state lives in GitHub Issues + the
  repo-map node statuses (`live | meta | planned | retired`), never in prose.

## 3) Risk zones (issue-backed when they open)

- **Eval-parity trap:** GitHub must never author merge commits; local CI at
  the exact pushed sha is the only green that counts.
- **Authority collision:** logic-os-kernel currently claims Fact envelope /
  Tape contract authorities. ADR-001 records our claim; Director ratifies;
  until then no edits to their charter from this repo.
- **License floor:** NOASSERTION/unverified deps stay off the default path
  (GOVERNANCE §6).
- **Scope gravity:** the filesystem and GNN faces are projections over the
  classes — any PR adding a second store for them is rejected on sight.

## 4) File Ownership & Change Policy

- `laws/**` — decisions/process (append-only; ADR content is historical,
  never rewritten).
- `docs/DB_PATHWAY.md` — amendments only with cited evidence (GOVERNANCE §3).
- `ci/keel-ci.toml` + generated workflow — regenerate after ANY plan edit
  (recipe in the toml header); byte-equality is CI-enforced.
- `kernel/**` (when it exists) — issue-scoped patches only; no bulk rewrites.

## 5) Agent Startup Read Set

- `VISION.md` → `REPO_MAP.md` → `docs/DB_PATHWAY.md` → `GOVERNANCE.md` →
  assigned issue. **No issue, no work.**
- Keel mechanics questions: `wwfd q "<terms>"` before guessing; hook refusals
  name the lesson (`~/wwfd/lessons/L*`).

## 6) Standard Delivery Loop

1. Confirm issue scope, risks, done criteria.
2. Branch `feat/NNN-short-description`; smallest coherent change.
3. Verify: tests cover happy path, edge/unclean inputs, abuse paths,
   regressions — with evidence. Proof ladder in GOVERNANCE §4.
4. `.wwfd/local-ci.sh` green → receipt → push → PR → review verdicts
   (`REJECT`/`REPAIR`/`ACCEPT WITH PROOF`/`UNKNOWN`).
5. Document decisions/evidence/blockers in GitHub; update governance only on
   policy/architecture change.

## 7) Quality bar

- Names state architectural role ("vec-code class", "ingest gauntlet"), not
  implementation incidentals.
- Errors are typed enums; `#![forbid(unsafe_code)]` in pure-Rust crates;
  unsafe only at documented boundaries (FDB/FUSE adapters).
- Zero TODO/HACK/FIXME in tree; unfinished work is an issue.
- Every changed line traces to intent, necessary support, or declared
  coupled rot.

## 8) Coordination

- Agents are peers; one git worktree per agent; same-file conflicts resolved
  before merge.
- Any agent may challenge scope/strategy/safety; block unsafe work with
  alternatives. Unresolved disputes block until the Director decides.
