# Governance Map

Single source of truth for architectural decisions and operating models.
Conflict resolution rule: the constitution (`GOVERNANCE.md`) outranks these
process files; among these files, `operating-model.md` dictates precedence.

## Core Directives

| Document | Purpose |
|----------|---------|
| `../GOVERNANCE.md` | Constitution — invariants, proof ladder, licenses. |
| `operating-model.md` | Agent roles, ticketing taxonomy, workflow. |
| `definition-of-done.md` | Pre-merge gates and PR standards. |
| `collaboration-standards.md` | Schemas for commits, PRs, issues. |
| `issue-template.md` | Declarative schema for all GitHub tickets. |

## Architecture Decision Records

> New ADRs start at ADR-001 here — this is a fresh series; logic-os-kernel's
> ADR-004…099 stay theirs. Append-only; never rewrite accepted ADRs.

| ADR | Domain | Decision Summary |
|-----|--------|------------------|
| **ADR-001** | Authority | PROPOSED — owns the one model. |
| **ADR-002** | Substrate | ACCEPTED — FDB canonical; parity-before-delete. |
| **ADR-003** | Hybrid API | ACCEPTED — one admitted layer; redb or FDB. |

## Process lineage

The ticketing taxonomy, definition-of-done, and collaboration standards are
adapted from logic-os-kernel's governance framework (2026-08-21) so agents
move between the repos without relearning process. Divergences live in
each file's header, never silently edited.
