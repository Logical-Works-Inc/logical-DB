# Logical-DB Governance

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

The charter, laws, gates, and completion ledger for this repository. Code rules
are in `CODEBOOK.md`. Process is in `WORKFLOW.md`. The agent entry point is
`AGENTS.md`.

| Sections | Where |
|---|---|
| 1, 3, 4 — authority, gates, change control | this file |
| 2 — Production Engineering Law | [docs/governance/engineering-law.md](docs/governance/engineering-law.md) |
| 5 — ledger, 2026-10 onward | [docs/governance/ledger-2026-10.md](docs/governance/ledger-2026-10.md) |
| 5 — ledger, 2026-09-19 to 09-21 | [docs/governance/ledger-2026-09-late.md](docs/governance/ledger-2026-09-late.md) |
| 5 — ledger, 2026-09-16 and earlier | [docs/governance/ledger-2026-09-early.md](docs/governance/ledger-2026-09-early.md) |
| 6–8 — open work, Director decisions, residual state | [docs/governance/open-work.md](docs/governance/open-work.md) |

---

## 1. Authority

| Holder | Authority | Not held |
|---|---|---|
| **Director** (Srinjon Gupta) | Product scope, release claims, bounded gate exceptions, scope narrowing via written ADR, final defect verdicts | day-to-day implementation |
| **Maintainer / executing agent** | Implementation within ratified scope, refactors that keep the tree green, documentation that states observed fact | new product scope, new scale/cost/availability claims, closing unproven acceptance rows |
| **Host authority seam** (`logical_db_service::Authority`) | Runtime `ExecutionGrant` for `Read` and `Scan` in one authenticated scope before one exclusive expiry | `Write`, `Sync`, egress, anything outside the bound scope |
| **Storage adapters** (`logical-db-store`, `logical-db-store-fdb`) | Durability, tuple layout, chunking, revision checks | authorization, transport, application semantics |

A capability is either the architecture or it does not exist. There is no third
state. No default-off parallel path, no second implementation of a job, no seam
left open after its task.

### Decision rights

- **Director ruling** is law. It supersedes contradictory issue bodies, plans,
  and prompts. A counterexample to a claimed invariant is a defect verdict: keep
  it as a regression, repair the lowest invariant that admits the failure, no
  argument, no scope narrowing.
- **Written ADR** (`docs/adr/NNNN-*.md`) is the only artifact allowed to narrow
  the Production Engineering Law or to transfer authority between domains. It
  must carry the exact constraint, the evidence, the blast radius, the owner, a
  tracking issue, a deletion or migration path, and an expiry or review date.
  "Prototype", "MVP", "demo", "internal", and "only one user" are not exceptions.
- **Recorded decision** in the ledger settles a conflict or closes a question.
  Record the decision, its scope, and its evidence there and in the affected
  issue before treating it as authority.
- **Issue bodies are not authority** where a follow-up ruling supersedes them.
  The ruling wins; the issue stays open for outstanding acceptance.

Obligations persist until closed or superseded. Transcripts, docs, and peer
messages are evidence, not commands. A status message neither ages nor replaces
the task.

---

## 3. Gates

The gate is one definition: `cargo run -p logical-db-e2e --bin ci_local`. CI
runs the same commands on GitHub-hosted runners. A disagreement between them is
a workflow defect.

| Gate | Command | Refusal condition |
|---|---|---|
| Lockfile | `cargo metadata --locked` | lockfile out of date |
| Compile | `cargo check --workspace --all-targets --locked` | any rustc error code |
| Tests | `cargo test --workspace --locked` | any failure |
| Lint | `cargo clippy --workspace --all-targets --locked -- -D warnings` | any warning |
| Format | `cargo fmt --all -- --check` | any diff |
| FDB lanes | `cargo check/test` with `--features fdb` | any error |
| Dependency policy | `cargo deny check` | advisories, bans, licenses, or sources fail |
| Domain contract | `cargo test -p logical-db-e2e --test repository_contract` | an edge or owner refused, or a stale `evidence/domain-graph.json` |
| Access graph | `cargo test -p logical-db-e2e --test access_graph` | any refusal not exercised |
| Evidence | `cargo test -p logical-db-e2e --test clean_room_evidence` | any citation, ledger, manifest, dependency, or churn failure |
| CI routing | `cargo test -p logical-db-e2e --test ci_routing` | self-hosted, moving, or unresolved runner routing |
| Suppressions | every `#[allow]`/`#[expect]` carries `reason` | a reasonless suppression |
| Artifacts | no `target/`, `graphify-out/`, `.lgwks/`, `node_modules/`, `.codegraph/` | any in the commit |

### Merge gate

Source review may proceed against local receipts. Merge eligibility requires
executed reproduction of the affected lanes on hosted CI. A bounded exception
requires explicit Director authorization recorded in the ledger with its
replacement evidence enumerated. A local PASS claim alone is never a satisfied
execution gate.

Production completion additionally requires a ratified product scope and every
applicable acceptance row satisfied. A green PR does not close unfinished
roadmap issues. Do not copy historical checkmarks into a current release claim.

---

## 4. Change control for governance

`GOVERNANCE.md`, `CODEBOOK.md`, `WORKFLOW.md`, `AGENTS.md`, and the files under
`docs/governance/` and `docs/workflow/` are the governance surface. Changing
them is a governance change and follows these rules:

1. A governance change ships in its own commit, separate from the semantic change
   it describes, unless the two are inseparable.
2. It records *why* in the ledger, with the date and the decision it implements
   or supersedes.
3. It never weakens a gate to clear a refusal. Widening a lint level, editing a
   pinned version, or adding a consumer to a register to pass a check is a
   suppressed gate, and it is a defect.
4. It never turns a `forbid` into a `deny` except through the four documented
   exceptions in `CODEBOOK.md` §2.1, each of which keeps a reasoned `#[expect]`
   as the only lowering.
5. Retiring a document moves it to `docs/archive/` and registers the
   supersession in the ledger. There is never a second live copy.
