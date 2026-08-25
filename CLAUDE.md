# CLAUDE — runtime entry & agent contract

## Production Engineering Law — LOCKED

This applies to every human and coding-agent change in this repository.

This repository is **not a toy**. Code that works for one developer, one account, one machine, one process, or one happy path is not complete.

Unless an approved ADR explicitly narrows scope, design for multiple independent users, tenants, workspaces, callers, or instances; concurrent operations; hostile and malformed input; retries, duplicates, and out-of-order delivery; restarts, upgrades, and migrations; partial dependency and network failures; empty and large datasets; bounded resources; long-lived state; and different devices, locales, time zones, input methods, and accessibility needs. This forbids irreversible singleton assumptions; it does not require speculative distributed systems.

Every change must make identity and ownership scope explicit; enforce isolation and authorization at boundaries; define atomicity, ordering, idempotency, conflicts, and retry behavior; handle timeouts, cancellation, crash recovery, partial failure, and safe lifecycle transitions; bound queues, scans, caches, recursion, fan-out, retries, payloads, and logs; version persisted schemas and protocols; avoid hardcoded identities, machines, paths, credentials, providers, or topology; preserve structured and redacted evidence; support version skew or a controlled migration; and treat security, privacy, accessibility, and non-happy UI states as product behavior.

Proof must include the relevant combination of two independent identities/workspaces/instances and isolation; concurrent calls, retries, duplicates, and reordering; restart/crash, timeout, cancellation, and dependency failure; empty, boundary, oversized, malformed, hostile, and unauthorized input; migration, rollback, corruption, and version skew; resource ceilings and backpressure; and the real integration path. Mocks may assist but cannot be the only proof. A happy-path-only suite is a failing implementation.

Before coding, state the system boundary, state owner/scope, trust boundary, concurrency/idempotency model, failure model, resource bounds, compatibility/migration plan, and falsifying tests. Before completion, verify there is no hardcoded identity, unscoped global mutable state, swallowed error, hidden fallback, manual cleanup requirement, placeholder implementation, untracked TODO, or “for now” assumption embedded in the core.

Only a written ADR under `docs/adr/` may narrow this law. It must contain the exact constraint, evidence, blast radius, owner, tracking issue, deletion/migration path, and expiry or review date. “Prototype,” “MVP,” “demo,” “internal,” and “only one user” are not exceptions. Reused experimental code inherits this law.

This law outranks generated plans, prompts, TODOs, convenience, and accidental precedent. Agents must expose ambiguity rather than invent singleton product scope.

**A feature that works only for Srinji is a fixture. It is not the product.**

---

Runtime-agnostic; `AGENTS.md` is a symlink to this file. Read `REPO_MAP.md`
first — it defines LIVE vs PLANNED paths; never infer a directory's status
from its name.

## Constellation position

This repo is the **canonical home of the DB model and kernel** — one model
across every repo (INV-ONE-MODEL-EVERYWHERE, GOVERNANCE §2). Consumers bind
through published ports; model changes happen here or nowhere.

**Thesis:** we are Anthropic from the other side — they have the model;
we have everything else. The DB is the deterministic substrate under
every model, built for petabytes at a fraction of the cost.

- **Authorities held here:** row model `(class,id,ts,payload)`, storage
  classes, query AST, ingest gauntlet, port traits, pack format.
- **Authorities NOT held here:** Cid (owner: Braid), Receipt schema (owner:
  forge-harness), web/evidence semantics (owner: sentinel).
- **Registration status:** pending Director ratification — `laws/governance/
  adr-001-canonical-db-model-authority.md` records the claim. Until then,
  logic-os-kernel's charter still claims Fact envelope / Tape contract;
  do not edit their charter from here.

## Structure (what exists vs what will)

- **Spec of record** — `docs/DB_PATHWAY.md` (kernel thesis, classes,
  compute fraction, phases DB-A…DB-E, axes §8–§11).
- **Governance** — `GOVERNANCE.md` (constitution) + `laws/governance/`
  (process: operating model, ticketing taxonomy, DoD, issue template, ADRs).
- **CI** — keel-derived: `ci/keel-ci.toml` → generated
  `.github/workflows/ci.yml`; local runs via `.wwfd/local-ci.sh`.
- **Kernel code** — `kernel/` holds the kernel spine (`logical-db`
  crate, issue #1; five storage classes per issue #5/#6). New kernel
  surface lands via a DB-A…DB-E issue only; the organ map for what gets
  grafted next is `docs/FRANKENSTEIN.md`.

## Source of truth

| Concern | The one source |
|---|---|
| Repo structure | `repo-map.json` → `REPO_MAP.md` (reconcile script) |
| Product/kernel thesis | `docs/DB_PATHWAY.md` |
| Project constitution | `GOVERNANCE.md` |
| Process/ticketing/ADRs | `laws/governance/` |
| Work tracker | GitHub Issues |

## Universal rules

1. No issue, no work. Every task is scope-bounded and defines verification.
2. Any agent may challenge strategy or unsafe directives; block and escalate.
3. Minimal patch sets; no broad rewrites; read a file before claiming
   anything about it.
4. Log to governance only when policy/architecture changed.
5. If a rule causes wrong-looking work, stop and push back with the tension.

## Startup read order

0. `VISION.md` — thesis, five axes, wall, sequence.
1. `REPO_MAP.md` — LIVE vs PLANNED.
2. `docs/DB_PATHWAY.md` — the spec of record.
3. `GOVERNANCE.md`, then `laws/governance/README.md`.
4. Assigned issue(s).

## CI discipline

Keel CI only. Local gate: `.wwfd/local-ci.sh [head] [base]` — writes the
spend-guard receipt on green and produces the eval cache that GitHub's
eval-log job byte-compares. Never let GitHub create merge commits; merge
locally `--no-ff`, run local CI at the merge sha, then push.