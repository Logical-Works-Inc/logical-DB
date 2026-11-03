# Workflow: agent economics, data safety, WWFD

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

Sections 8, 9, and 11 of [WORKFLOW.md](../../WORKFLOW.md), moved verbatim under
the 120-line document law.

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

## 11. Mandatory WWFD cycle

**Before implementation**, query WWFD (`wwfd q "<error or concept>"` or MCP
`wwfd_query`). Check whether the problem or architecture has past lessons or
established estate standards. Never invent from scratch what WWFD already
solved.

**After verification and before delivery**, run
`wwfd state sync <repo> --task '<task>'` to record genuine state before closing
or opening a PR. No claim of green completion without a sync receipt.
