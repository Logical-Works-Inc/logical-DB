# Workflow: delivery, agents, safety and runners

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `../../evidence/SOURCES.md`.

Sections 7 to 11 of [`WORKFLOW.md`](../../WORKFLOW.md), split out under the
120-line law. The numbering is kept so existing citations still resolve.

## 7. Work ends with a PR

- **Always commit.** Never leave verified-green work uncommitted.
- Commit in small, individually-green steps.
- **A local commit is not the finish line.** Work ends with a GitHub PR: branch,
  commits, pushed, PR opened with its evidence.
- The PR carries the receipts: gate commands and their results.

**Merge gate.** Merge eligibility requires executed reproduction of the affected
lanes on hosted or self-hosted CI. A bounded exception requires explicit
Director authorization recorded in `GOVERNANCE.md` with its replacement evidence
enumerated. A local PASS claim alone is never a satisfied execution gate.
Production completion additionally requires a ratified product scope and every
applicable acceptance row satisfied. A green PR does not close unfinished
roadmap issues.

## 8. Agent economics

Agents are the dominant cost in this workflow. The escalation ladder, exhausted
in order before spawning anything:

1. **CodeGraph** — is it already in this repo? Module DAG, callers, consumers.
2. **Graphify** — is it already in the estate?
3. **A quick 9router search** — one `/v1/search` call.
4. **wwfd** — has the estate already solved this?

- Fan-out is the cost driver. Batch related questions into one agent.
- Do discovery yourself; delegate only work that is genuinely parallel,
  independent, and bounded. Justify each agent in one clause.
- Never re-dispatch to redo; finish partial work yourself. Do not kill running
  agents to save money.
- Front-load the constraint. Subagent return is not done; the parent owns closure.

## 9. Data safety and the guard

Deletion is `trash <path>` or move to `~/.Trash`. Never `rm`, `rmdir`, `shred`,
`truncate`, `git clean`, `git reset --hard`, `find -delete`, or `xargs rm`.
Emptying Trash is human-only. No secrets in output, commits, memory, or queries.
Every resource declares owner, lifetime, and cleanup.

The five enforced rules (`rust-guard`, wired into every harness and the global
git pre-commit) cover the shell as well as the editor:

1. **OWNERSHIP** — never edit, commit, or sweep a repo the estate does not own.
2. **ARTIFACTS** — never commit derived build output (`graphify-out/`, `target/`,
   `.lgwks/`, `node_modules/`, `.codegraph/`).
3. **SUPPRESSION** — every `#[allow]` / `#[expect]` carries `reason = "..."`.
4. **PRINTS** — no `eprintln!` / `println!` / `writeln!(stderr, ..)` in library
   code. Use `tracing`. `main.rs`, `src/bin/`, `examples/`, `benches/` exempt.
5. **COMPILE** — an edit that leaves the crate not compiling is refused.

A refusal is a correction, not a discussion and not a stop. Fix it and carry on.

## 10. Local CI and the runner map

| Repo | Gate | Runner | Workflow |
|---|---|---|---|
| `logical-DB` | `scripts/gate-lanes.toml` via `ci-local` | on-demand ephemeral macOS ARM64 (`logicaldb.runner.watch`) | `.github/workflows/ci.yml`, literal `[self-hosted, macOS, ARM64]` |
| `logicalworks-crates` | `scripts/gate-lanes.toml` via `ci-local` | per-repo LaunchAgent, macOS ARM64 | `.github/workflows/ci.yml`, literal self-hosted labels |

The self-hosted runner pattern: per-repo runner as a LaunchAgent, workflows ask
for self-hosted by literal label.

Linux is the correct rung wherever a test is `#![cfg(target_os = "linux")]` and
installs a seccomp filter. On a non-Linux host such a test compiles to nothing
and the suite reports green having verified nothing. macOS is a reduced rung for
quick signal, not an equivalent one. A container is not sufficient for a
seccomp gate.

## 11. Mandatory WWFD cycle

**Before implementation**, query WWFD (`wwfd q "<error or concept>"` or MCP
`wwfd_query`) for past lessons and estate standards.

**After verification and before delivery**, run
`wwfd state sync <repo> --task '<task>'` to record genuine state before closing
or opening a PR.
