# Logical Works Workflow

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

How work moves through this repository. Code rules are in `CODEBOOK.md`.
Authority, decisions, and the completion ledger are in `GOVERNANCE.md`. The
agent entry point is `AGENTS.md`. Sections 3 to 6 live in
[`docs/workflow/engineering.md`](docs/workflow/engineering.md) and sections 7 to
11 in [`docs/workflow/delivery.md`](docs/workflow/delivery.md), under the
120-line law.

---

## 1. Local CI

Local CI is the gate. It runs every check a pull request needs, on this machine,
without GitHub, and writes a receipt.

```sh
cargo install --locked lgwks_deps --version 5.1.2 --bin ci-local   # once
ci-local              # full gate
ci-local --fast       # lockfile, compile, lint, fmt and scans
ci-local --receipt    # also write evidence/ci-local-*.md
```

`scripts/gate-lanes.toml` is the single definition of "the gate", run by the
estate's `ci-local`. `.github/workflows/ci.yml` calls the same commands, so a
green local run and a green CI run are the same claim. When the two disagree,
the workflow is wrong; fix the workflow and re-run locally. The repository
contracts are Rust tests (`docs/adr/0006-contract-checks-in-rust.md`).

Every job declares a literal self-hosted label list.
`crates/logical-db-e2e/tests/repository_contract/ci_routing.rs` refuses hosted
routing, unresolved `${{ }}` labels, aliases, and duplicate `runs-on` keys.

The self-hosted runner is ephemeral: it registers when a job is queued, takes
one job, deregisters, and exits. Nothing stays resident between runs. See §10 in
`docs/workflow/delivery.md` for the per-repo runner map.

---

## 2. The delivery loop

```
1. Read the task and query WWFD            (1 min, mandatory)
2. Find the best OSS implementations       (5 min, non-trivial work only)
3. Navigate with CodeGraph                 (2 min, find code and callers)
4. Implement to the codebook               (CODEBOOK.md)
5. Test and verify                         (affected tests + regression tests)
6. WWFD state sync, then PR                (mandatory before delivery)
```

**Rule 1, first tool call.** The first substantive tool call reads source you
will edit, runs a build or test, writes source, or queries CodeGraph to navigate
to the code you will edit. It is not reading this file, loading a skill, running
`wwfd boot`, or producing a plan document.

**Rule 2, five-minute code.** Within five minutes of session start you have
written or modified a source file, run a build/test/lint command, or named a
specific `file:line` and the exact change it needs.

**Rule 3, narration ratio.** Process words must never exceed 20% of any
response. Code, architecture decisions, error analysis, test results, and diffs
are implementation words.

**Rule 4, one repro then fix.** A prior CI failure is a defect verdict and the
receipt already exists. One repro is authorized only when the cause is genuinely
uncertain from the receipt alone. Fix the code, run the new state once.

**Rule 5, skill budget.** At most two skills loaded before the first code edit.

**Rule 6, table proportionality.** Product runtime changes get the full nine-axis
SLO evidence matrix with measured evidence per axis. Config, docs, CI, README,
and script changes get one line of receipt per file.

**Rule 7, refuse the theater.** No session-start rituals, no prophylactic skill
loading, no re-reading source already held in context.
