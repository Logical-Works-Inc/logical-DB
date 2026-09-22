# Logical Works Workflow

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

How work moves through this repository. Code rules are in `CODEBOOK.md`.
Authority, decisions, and the completion ledger are in `GOVERNANCE.md`. The
agent entry point is `AGENTS.md`.

---

## 1. Local CI

Local CI is the gate. It runs every check a pull request needs, on this machine,
without GitHub, and writes a receipt.

```sh
./scripts/ci-local.sh              # full gate
./scripts/ci-local.sh --fast       # compile + lint + fmt only
./scripts/ci-local.sh --receipt    # also write evidence/ci-local-<sha>.md
```

The script is the single definition of "the gate". `.github/workflows/ci.yml`
calls the same commands in the same order, so a green local run and a green CI
run are the same claim. When the two disagree, the workflow is wrong; fix the
workflow and re-run locally.

Self-hosted runners exist so that GitHub CI can execute at all on this account.
Hosted `*-latest` pools are billing-blocked here and a job routed to them reports
failure in one to two seconds with zero steps recorded, which says nothing about
the code. Every job therefore declares a literal self-hosted label list.
`scripts/verify-ci.py` refuses hosted routing, unresolved `${{ }}` labels, and
duplicate `runs-on` keys.

The self-hosted runner is ephemeral: it registers when a job is queued, takes
one job, deregisters, and exits. Nothing stays resident between runs. See
`WORKFLOW.md` §10 for the per-repo runner map.

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
specific `file:line` and the exact change it needs. "Inventorying owners and
consumers" is not this.

**Rule 3, narration ratio.** Process words must never exceed 20% of any
response. Compliance citations, skill-loading narration, time accounting, and
scope inventorying are process words. Code, architecture decisions, error
analysis, test results, and diffs are implementation words.

**Rule 4, one repro then fix.** A prior CI failure is a defect verdict and the
receipt already exists. One repro is authorized only when the cause is genuinely
uncertain from the receipt alone. A second repro of the same state is forbidden.
Fix the code, run the new state once.

**Rule 5, skill budget.** At most two skills loaded before the first code edit.
Additional skills load only when a specific API question blocks you
mid-implementation.

**Rule 6, table proportionality.** Product runtime changes get the full nine-axis
SLO evidence matrix with measured evidence per axis. Config, docs, CI, README,
and script changes get one line of receipt per file. No table. No axes.

**Rule 7, refuse the theater.** No `wwfd boot` + `mem show` as a session-start
ritual. No prophylactic skill loading. No re-reading source already held in
context. No re-grepping something already found.

---

## 3. RAG yourself into expertise

Before implementing anything non-trivial, find two or three production OSS
implementations of the same thing and study their structural decisions: module
organization, trait design, error handling, concurrency primitives, tests. Five
to ten minutes. Record what you are adopting and why, and what you rejected and
why, in your first edit or message. Then implement at that level.

Skip this for trivial changes under twenty lines with no new abstraction, bug
fixes whose root cause is already identified, and config/docs/CI work.

This is not copying code. It is studying architecture and making an informed
decision. The mechanism is RAG for latent implementation expertise: reading real
code activates specifics that prompts alone do not.

---

## 4. One path, one opinion, one architecture

A capability is either **the** architecture or it does not exist.

- **No default-off parallel paths.** A feature flag that makes an implementation
  a *candidate* is how work fails to carry forward: nothing exercises it, the
  gate never compiles it, and it rots into a second opinion nobody chose. If it
  is how the estate does the thing, it is on by default and the gate runs it.
- **One implementation of a job.** Two builders, two executors, or two
  "equivalent" entry points for the same work is one too many. Pick the right
  one, make it the only one, delete the other including its tests, docs, and
  feature flag.
- **Unification beats a seam.** "Parallel seams, not demolition" governs *how* a
  change lands while the tree stays green. It is not a licence to leave both
  halves standing. The seam closes in the same task.
- A storefront may keep capability features default-off, because selecting is
  its stated purpose. A **consumer** states its opinion: it turns on what it
  uses, in its own defaults, and the workspace gate compiles and tests it.

---

## 5. Compounding, buildable, surgical

**Compounding.** Every unit, trait, or module added makes future tasks easier.
New capabilities plug in through existing traits without rewriting call sites. If
adding a feature requires rewriting half the repo, the previous architecture was
anti-compounding debt.

**Always buildable.** Every single commit compiles and passes tests. Never
commit a broken intermediate state intending to fix it in the next step. If a
refactor breaks the build for more than twenty minutes, the change radius is too
broad.

**Surgical change radius.**

- Touch only the lines the invariant requires. No gratuitous mass renames, no
  cosmetic formatting rewrites, no sweeping module migrations in the same turn
  as a semantic fix.
- Identify before touching: map callers and downstream consumers first. One
  CodeGraph query replaces five grep chains.
- Parallel seams over in-place demolition when replacing a subsystem: introduce
  the new implementation alongside the old, point callers at it one at a time,
  verify tests, then delete the old code.

Time-box is thirty minutes by default. Not terminal within the box: stop, report
the SHA and the receipt, await direction. Identical-state retry is forbidden.

---

## 6. Evidence proportionality

| Change | Evidence |
|---|---|
| Product runtime | full nine-axis SLO matrix, measured per axis |
| Config, docs, CI, README, scripts | one line of receipt per file |
| Instruction or config loading | config-loading evidence only |

**The nine axes.** Frontier (design beats credible alternatives), Hyperscale
(>1M concurrent, correct), Idiomatic (ownership, errors, lifetimes sound),
Generalized (works across declared inputs), Decoupled (components independently
replaceable), Ephemeral (survives process loss), Portable (same semantics on all
targets), Multi-tenant (isolated under concurrent use), Performance (fastest
correct implementation).

Every claim carries evidence: command, exit code, result, artifact ID. Before
completion, account for every requirement, every failed check, and every
unknown. No hidden exclusions.

AI and model output is untrusted first-party evidence until independently
verified. Every capability claim separately states which of these it is:
**planned**; **present in code at an exact commit**; **exercised on the exact
claimed path**; **independently evidenced**; **safe to rely on or merge**. Never
collapse these into "done".

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
executed reproduction of the affected lanes on hosted or self-hosted CI. A
bounded exception requires explicit Director authorization recorded in
`GOVERNANCE.md` with its replacement evidence enumerated. A local PASS claim
alone is never a satisfied execution gate.

Production completion additionally requires a ratified product scope and every
applicable acceptance row satisfied. A green PR does not close unfinished
roadmap issues. Do not copy historical checkmarks into a current release claim.

---

## 8. Agent economics

Agents are the dominant cost in this workflow, orders of magnitude above a tool
call. Spend them like the expensive resource they are.

The escalation ladder, exhausted in order before spawning anything:

1. **CodeGraph** — is it already in this repo? Module DAG, callers, consumers.
2. **Graphify** — is it already in the estate?
3. **A quick 9router search** — one `/v1/search` call answers most "what is the
   standard for X" questions outright.
4. **wwfd** — has the estate already solved this?

Only if still uncertain after all four, spawn an agent. An agent is the last
rung, not the first.

- Fan-out is the cost driver. Batch related questions into one agent with one
  return format. Three agents covering three topics each beats nine covering one
  each.
- Do discovery yourself. A single `curl`, file read, endpoint probe, or `git log`
  is seconds of your own context.
- Default to doing it yourself. Delegate only work that is genuinely parallel,
  independent, and bounded.
- Justify each agent in one clause. If you cannot, it is not one.
- Never re-dispatch to redo. If an agent returns partial work, finish it yourself
  rather than spawning a successor.
- Do not kill running agents to save money. Their cost is already sunk. Change
  the *next* decision instead.
- Front-load the constraint. A wrong channel or a wrong premise costs a whole
  agent run.

Subagent return is not done. The parent owns closure.

---

## 9. Data safety and the guard

Deletion is `trash <path>` or move to `~/.Trash`. Never `rm`, `rmdir`, `shred`,
`truncate`, `git clean`, `git reset --hard`, `find -delete`, or `xargs rm`.
Emptying Trash is human-only. No secrets in output, commits, memory, or queries.
Every resource declares owner, lifetime, and cleanup.

The five enforced rules (`rust-guard`, wired into Claude Code, Codex, OpenCode,
and the global git pre-commit) cover the shell as well as the editor:

1. **OWNERSHIP** — never edit, commit, or sweep a repo the estate does not own.
2. **ARTIFACTS** — never commit derived build output (`graphify-out/`, `target/`,
   `.lgwks/`, `node_modules/`, `.codegraph/`).
3. **SUPPRESSION** — every `#[allow]` / `#[expect]` carries `reason = "..."`.
4. **PRINTS** — no `eprintln!` / `println!` / `writeln!(stderr, ..)` in library
   code. Use `tracing`. `main.rs`, `src/bin/`, `examples/`, `benches/` exempt.
5. **COMPILE** — an edit that leaves the crate not compiling is refused. Judge by
   rustc error codes, never by diagnostic count.

A refusal is a correction, not a discussion and not a stop. Fix it and carry on.
Two responses are forbidden because both are theatre: ending the turn to report
yourself blocked, and writing a paragraph about how you respect the rule.

---

## 10. Local CI and the runner map

| Repo | Gate script | Runner | Workflow |
|---|---|---|---|
| `logical-DB` | `scripts/ci-local.sh` | on-demand ephemeral macOS ARM64 (`logicaldb.runner.watch`) | `.github/workflows/ci.yml`, literal `[self-hosted, macOS, ARM64]` |
| `logicalworks-crates` | `scripts/ci-local.sh` | per-repo LaunchAgent, macOS ARM64 | `.github/workflows/ci.yml`, literal self-hosted labels |
| `logic-os-kernel` | `scripts/ci-local.sh` | Lima `linuxverify` (Linux ARM64, `logic-os-linux`) | `.github/workflows/ci.yml`, literal self-hosted labels |

The self-hosted runner pattern: per-repo runner as a LaunchAgent, workflows ask
for self-hosted by literal label. Hosted Actions does not work on this account.

Linux is the correct rung wherever a test is `#![cfg(target_os = "linux")]` and
installs a seccomp filter. On a non-Linux host such a test compiles to nothing
and the suite reports green having verified nothing, which is silent coverage
loss. macOS is a reduced rung for quick signal, not an equivalent one.

A container (`act`, Docker) is not sufficient for a seccomp gate.

---

## 11. Mandatory WWFD cycle

**Before implementation**, query WWFD (`wwfd q "<error or concept>"` or MCP
`wwfd_query`). Check whether the problem or architecture has past lessons or
established estate standards. Never invent from scratch what WWFD already
solved.

**After verification and before delivery**, run
`wwfd state sync <repo> --task '<task>'` to record genuine state before closing
or opening a PR. No claim of green completion without a sync receipt.
