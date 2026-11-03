# Logical Works Workflow

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

How work moves through this repository. Code rules are in `CODEBOOK.md`.
Authority, decisions, and the completion ledger are in `GOVERNANCE.md`. The
agent entry point is `AGENTS.md`.

| Sections | Where |
|---|---|
| 1, 7, 10 — local CI, delivery by PR, runner map | this file |
| 2–4 — delivery loop, research, one architecture | [docs/workflow/delivery.md](docs/workflow/delivery.md) |
| 5–6 — change radius, evidence proportionality | [docs/workflow/evidence.md](docs/workflow/evidence.md) |
| 8, 9, 11 — agent economics, data safety, WWFD | [docs/workflow/agents.md](docs/workflow/agents.md) |

---

## 1. Local CI

Local CI is the gate. It runs every check a pull request needs, on this machine,
without GitHub, and writes a receipt.

```sh
cargo run -p logical-db-e2e --bin ci_local                 # full gate
cargo run -p logical-db-e2e --bin ci_local -- --fast       # compile + lint + fmt only
cargo run -p logical-db-e2e --bin ci_local -- --receipt    # also write evidence/ci-local-<sha>.md
```

`crates/logical-db-e2e/src/bin/ci_local.rs` is the single definition of "the
gate". `.github/workflows/ci.yml` calls the same commands, so a green local run
and a green CI run are the same claim. When the two disagree, the workflow is
wrong; fix the workflow and re-run locally.

The repository is public (Director, 2026-11-03), so CI runs on GitHub-hosted
standard runners, which are free for public repositories: native Linux x86-64,
macOS ARM64, and Windows x86-64. Every job names one pinned image label
(`ubuntu-24.04`, `macos-15`, `windows-2025`). A self-hosted runner on a public
repository executes whatever a workflow trigger supplies on the owner's
machine, so `crates/logical-db-e2e/tests/ci_routing.rs` refuses self-hosted
and moving labels, unresolved `${{ }}` labels, reusable-workflow jobs, YAML
anchors, and duplicate keys.

---

## 7. Work ends with a PR

- **Always commit.** Never leave verified-green work uncommitted and never end a
  turn with it in the working tree. Uncommitted work has not carried forward.
- Commit in small, individually-green steps.
- **A local commit is not the finish line.** Work ends with a GitHub PR: branch,
  commits, pushed, PR opened with its evidence. A branch with no PR is unfinished
  work, and a green tree on `main` that was never proposed has not shipped.
- The PR carries the receipts: gate commands and their results. Not a narrative
  about them.

### Merge gate

Source review may proceed against local receipts. Merge eligibility requires
executed reproduction of the affected lanes on hosted CI. A bounded exception
requires explicit Director authorization recorded in the governance ledger with
its replacement evidence enumerated. A local PASS claim alone is never a
satisfied execution gate.

Production completion additionally requires a ratified product scope and every
applicable acceptance row satisfied. A green PR does not close unfinished
roadmap issues. Do not copy historical checkmarks into a current release claim.

---

## 10. Local CI and the runner map

| Lane | Runner | Proves |
|---|---|---|
| `clean-room evidence` | `ubuntu-24.04` | domain graph and receipt, falsifiers, access graph, actionlint |
| `linux / test-clippy` | `ubuntu-24.04` | compile, tests of every crate but `logical-db-e2e`, clippy, package isolation |
| `linux / end-to-end journeys` | `ubuntu-24.04` | dependency register and the `logical-db-e2e` journeys (replicas over real HTTP, crash, kill, recovery) |
| `macos / test-clippy` | `macos-15` (ARM64) | compile, tests, clippy |
| `windows / test-clippy` | `windows-2025` (x86-64 MSVC) | compile, tests, clippy |
| `rustfmt` | `ubuntu-24.04` | formatting |
| `advisory, license, and source policy` | `ubuntu-24.04` | `cargo deny check` (cargo-deny 0.20.2) |
| `official FDB client / daemon feature` | `macos-15` | FDB 7.3.77 feature lanes and live-cluster journeys |
| `official FDB client / linux` | `ubuntu-24.04` | the same on Linux x86-64 |

The FDB lanes share one definition, `.github/actions/fdb-lane/action.yml`.
Native Windows replaces the earlier Wine-emulated Windows-GNU lane, so Windows
results are now native MSVC results.

Every `rust-cache` step restores on every ref and saves from `main` only. The
repository cache holds 10 GiB, a run on `main` cannot restore what a pull
request saved, and one entry per Rust job measured about 3.6 GiB on 2026-11-03,
so pull requests read `main`'s entries and never evict them.
`ci_routing::every_rust_cache_saves_from_main_only` refuses a step without the rule.

Linux is the correct rung wherever a test is `#![cfg(target_os = "linux")]` and
installs a seccomp filter. On a non-Linux host such a test compiles to nothing
and the suite reports green having verified nothing, which is silent coverage
loss. macOS is a reduced rung for quick signal, not an equivalent one.
