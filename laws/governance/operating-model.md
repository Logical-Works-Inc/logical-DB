# Operating Model

Adapted from logic-os-kernel (2026-08-21). Changes: pillars renamed to this
repo's lanes; logic-os dual-repo doctrine references dropped; repo-map
reconcile kept.

## Source Of Truth

- GitHub Issues are the authoritative task tracker.
- GitHub Milestones define milestone scope.
- Governance README acts as the map for decisions.
- Governance markdown explains rules; it does not replace GitHub issue state.
- Repo structure: `repo-map.json` (declarative) → `REPO_MAP.md` (rendered);
  reconcile with `node scripts/repo-map.mjs check`.

## Required Workflow

1. Confirm a GitHub issue exists.
2. Confirm the issue has acceptance, security, verification, and done
   criteria (`laws/governance/issue-template.md`).
3. Assign or self-claim ownership in GitHub before parallel work begins.
4. Create a branch using the issue number.
5. Make the smallest coherent change within scope.
6. Document decisions, verification, blockers, and next actions in GitHub.
7. Open a PR that references `Closes #N`.
8. Merge only after checks and required review pass — locally `--no-ff`,
   never via a GitHub merge commit (eval parity).

## Strict Ticketing Taxonomy

| Entity | Definition | Scope | Naming Convention |
| :--- | :--- | :--- | :--- |
| **Milestone** | Release vehicle. | Months. | `M[#]: [Theme]` |
| **Epic** | Major component. | Weeks. | `EPIC: [Component]` |
| **Issue** | Discrete unit of work. | Days. | `type(lane): action` |
| **Sub-Issue** | Splinter of a parent. | Hours. | `type(lane): act (Sub #N)` |

Epics name the pathway phase they carry (`DB-A`…`DB-E`).

### Title Formatting

`[type]([lane]): [action] [target] [context]`

- **Types:** `feat`, `fix`, `arch`, `security`, `docs`, `chore`.
- **Lanes:** `kernel`, `spec`, `governance`, `infra`.

### The Rejection Rule

If a ticket is missing the prefix, the template sections, OR the
agent-interoperability JSON payload, agents MUST halt work, rewrite the
ticket to comply, and log a system comment.

## Branch Naming

- `gov/NNN-short-description` for governance.
- `feat/NNN-short-description` for features.
- `fix/NNN-short-description` for fixes.
- `security/NNN-short-description` for security hardening.
- `docs/NNN-short-description` for documentation-only work.

## Agent Model

All agents are peers. Each owns its parallel workstream independently, takes
scoped directives from peers, and reports status/evidence in GitHub. Agents
must strictly adhere to issue scope and must create or request a new GitHub
issue for new work discovered during execution.

Canonical roles: **Architect** (system shape, tradeoffs, trust boundaries),
**Orchestrator** (coordinates parallel ownership, keeps GitHub state
authoritative), **Coder** (implements scoped patches + verification
evidence), **Hacker** (finds breakage, abuse paths, security gaps — the
independent gate on every trust-boundary change), **QA-Refiner** (validates
behavior, tightens acceptance). Vendor agents map to these roles.

If agents disagree, Orchestrator attempts resolution. Unresolved product,
architecture, or security disputes remain blocked until the Director decides.

## Governance Change Rule

- Governance changes require a GitHub issue.
- Governance history is append-only when recording decisions or lessons.
- If governance docs conflict, record the conflict as an issue before
  resolving it; the constitution (`GOVERNANCE.md`) outranks process files.
