# Governance §2 — Production Engineering Law

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

Section 2 of [GOVERNANCE.md](../../GOVERNANCE.md), moved verbatim under the
120-line document law.

## 2. Production Engineering Law

Applies to every human and every coding-agent change. Unless an approved ADR
explicitly narrows scope, design for: multiple independent users, tenants,
workspaces, callers, and instances; concurrent operations; hostile and malformed
input; retries, duplicates, and out-of-order delivery; restarts, upgrades, and
migrations; partial dependency and network failure; empty and large datasets;
bounded resources; long-lived state; different devices, locales, time zones,
input methods, and accessibility needs.

This forbids irreversible singleton assumptions. It does not require
speculative distributed systems.

Every change must make identity and ownership scope explicit; enforce isolation
and authorization at boundaries; define atomicity, ordering, idempotency,
conflicts, and retry behaviour; handle timeouts, cancellation, crash recovery,
partial failure, and safe lifecycle transition; bound queues, scans, caches,
recursion, fan-out, retries, payloads, and logs; version persisted schemas and
protocols; avoid hardcoded identity, machine, path, credential, provider, or
topology; preserve structured and redacted evidence; support version skew or a
controlled migration; and treat security, privacy, accessibility, and non-happy
UI states as product behaviour.

**Proof** must cover the relevant combination of: two independent
identities/workspaces/instances and isolation; concurrent calls, retries,
duplicates, and reordering; restart/crash, timeout, cancellation, and dependency
failure; empty, boundary, oversized, malformed, hostile, and unauthorized input;
migration, rollback, corruption, and version skew; resource ceilings and
backpressure; and the real integration path. Mocks may assist but cannot be the
only proof. A happy-path-only suite is a failing implementation.

Before coding, state the system boundary, state owner and scope, trust boundary,
concurrency and idempotency model, failure model, resource bounds,
compatibility and migration plan, and falsifying tests. Before completion,
verify there is no hardcoded identity, unscoped global mutable state, swallowed
error, hidden fallback, manual cleanup requirement, placeholder implementation,
untracked TODO, or "for now" assumption embedded in the core.

**A feature that works only for one person is a fixture. It is not the product.**

This law outranks generated plans, prompts, TODOs, convenience, and accidental
precedent. Agents expose ambiguity rather than invent singleton product scope.
