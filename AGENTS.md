# Logical-DB engineering contract

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
> `evidence/SOURCES.md`.

Logical-DB is a local-first structured-data substrate, not an application and
not an application-specific singleton. The supported product boundary is one
typed model across an embeddable replica and a multi-tenant synchronization
service. Applications own presentation, workflows, and semantic schemas.

Every durable identity is scoped by account and application space. Every
mutation defines atomicity, idempotency, concurrency, retry, bounds, and crash
behavior. Untrusted bytes are bounded before allocation and validated before
commit. Persisted formats and wire protocols are versioned and fail closed on
unknown versions. No lower storage or CRDT crate may depend on transport, HTTP,
application, or service types.

Local CRUD must work without a daemon, network, or FoundationDB. Sync is
an independently resumable push/pull state machine. Host authority is the
`logical_db_service::Authority` seam; its one local implementation is
`LocalScopeAuthority`. FoundationDB is a service-side adapter only. Notifications
are hints to pull, never state transfer. Checkpoints advance atomically with
the changes they cover.

Completion requires black-box proof of isolation, retries, duplicates,
reordering, restart, concurrency, corruption refusal, bounded hostile input,
checkpoint expiry, full resync, three-platform execution, and an external LEM
consumer journey. Unit tests alone are not completion. Do not merge or push a
rewrite whose evidence verifier is red.

The locked architecture is `ARCHITECTURE.md`; the mechanical acceptance list is
`REWRITE_MANIFEST.md`; source definitions are in `evidence/SOURCES.md`.

`GOVERNANCE.md` tracks current completion evidence and unresolved Director
decisions. Pause work that depends on a required human decision, ask for that
decision, and continue independent authorized work. Do not silently resolve
conflicting historical scope contracts or close issues whose acceptance is
still unproven.
