# Logical-DB engineering contract

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
> `evidence/SOURCES.md`.

Read this before acting. Four files are the governance surface, and each owns
exactly one concern:

| File | Concern |
|---|---|
| [`GOVERNANCE.md`](GOVERNANCE.md) | Authority, laws, gates, the completion ledger, and what is not claimed |
| [`CODEBOOK.md`](CODEBOOK.md) | Code rules: lint contract, anti-slop invariants, API shape, tests, dependencies |
| [`WORKFLOW.md`](WORKFLOW.md) | Process: local CI, the delivery loop, evidence proportionality, agent economics |
| `AGENTS.md` (this file) | Product boundary and completion criteria |

A second copy of a policy is a second policy, and the two drift. Do not
duplicate these rules elsewhere; link here.

---

## Product boundary

Logical-DB is a local-first structured-data substrate, not an application and
not an application-specific singleton. The supported product boundary is one
typed model across an embeddable replica and a multi-tenant synchronization
service. Applications own presentation, workflows, and semantic schemas.

Every durable identity is scoped by account and application space. Every
mutation defines atomicity, idempotency, concurrency, retry, bounds, and crash
behaviour. Untrusted bytes are bounded before allocation and validated before
commit. Persisted formats and wire protocols are versioned and fail closed on
unknown versions. No lower storage or CRDT crate may depend on transport, HTTP,
application, or service types.

Local CRUD must work without a daemon, network, or FoundationDB. Sync is an
independently resumable push/pull state machine. Host authority is the
`logical_db_service::Authority` seam; its one local implementation is
`LocalScopeAuthority`. FoundationDB is a service-side adapter only. Notifications
are hints to pull, never state transfer. Checkpoints advance atomically with the
changes they cover.

## Completion

Completion requires black-box proof of isolation, retries, duplicates,
reordering, restart, concurrency, corruption refusal, bounded hostile input,
checkpoint expiry, full resync, three-platform execution, and an external LEM
consumer journey. Unit tests alone are not completion. Do not merge or push a
rewrite whose evidence verifier is red.

The locked architecture is `ARCHITECTURE.md`; the mechanical acceptance list is
`REWRITE_MANIFEST.md`; source definitions are in `evidence/SOURCES.md`.

## Authority and pause

`GOVERNANCE.md` tracks current completion evidence and unresolved Director
decisions. Pause work that depends on a required human decision, ask for that
decision, and continue independent authorized work. Do not silently resolve
conflicting historical scope contracts or close issues whose acceptance is still
unproven.

## Gate

```sh
./scripts/ci-local.sh              # full gate
./scripts/ci-local.sh --fast       # compile + lint + fmt only
./scripts/ci-local.sh --receipt    # also write evidence/ci-local-<sha>.md
```

That script is the one definition of the gate. CI runs the same commands in the
same order.
