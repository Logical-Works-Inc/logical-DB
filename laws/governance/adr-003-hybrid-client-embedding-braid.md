# ADR-003: One Hybrid Processing Layer, Embedded or Outboard

**Status:** ACCEPTED (Director, 2026-08-31)
**Tracking:** issue #8
**Review:** 2026-11-30 or before the first LEM production migration

## Context

The existing tree has two accidental products: the journaled `Ldb` storage
classes and an FDB-shaped transaction shim backed by redb. Its newer tuple
namespace is parallel to the storage-class key layout, application namespaces
are hardcoded as product names, and the admitted transaction envelope accepts
arbitrary strings where Braid deliberately exposes opaque proofs.

That structure cannot support both an embeddable personal database and a thin
client over FoundationDB without semantic drift. It also lets a caller claim
"safe" or "justified" without passing Braid's verifier and planner.

## Decision

Logical-DB has one processing layer and one versioned logical key contract.
Deployment changes only the physical transaction provider:

```text
application / LEM / kernel adapter
            |
            v
validated account/container/zone request + Braid RunnableInvocation
            |
            v
Logical-DB processing, key, budget, idempotency and receipt binding
            |
            +-------------------+
            |                   |
            v                   v
embedded redb            outboard FoundationDB client
zero daemon              service/cluster deployment
```

The state scope is `account / container / zone`. These are validated opaque
identities, not hardcoded repository names. A zone is the atomicity and change
ordering boundary. Object paths, filenames, UI state, and semantic meaning are
payload or projection concerns and never physical routing authority.

Braid remains the sole owner of `Cid`, canonical capsule admission, authority
set identity, and snapshot-bound justification. Effectful Logical-DB writes
consume a real `braid_run::RunnableInvocation`; string proof references are
forbidden. Logical-DB additionally enforces the external namespace capability
supplied by its caller, because a Braid capability name is not tenant scope.

## Trust and failure boundaries

- Thin-client requests and stored bytes are untrusted.
- The service side obtains authority context; a client cannot self-assert it.
- Transaction conflicts and stale snapshot bindings fail closed and require a
  new Braid invocation. Invocation identity is atomically consumed with its
  operation; only an exact operation retry can reuse it for deduplication.
- Operation IDs are scoped and bounded. Duplicate successful operations return
  the original outcome; the same ID with different bytes is an integrity
  error.
- Keys, values, batches, ranges, retries, and response materialization have
  explicit ceilings. There is no unbounded scan in the hybrid public surface.
- Embedded I/O failure, remote unavailability, timeout, cancellation, version
  skew, and malformed input remain distinguishable errors.

## Format and migration

Hybrid keys use a v2 prefix and are ordered account-first:

```text
LDB / v2 / account / container / zone / family / tuple
```

Families separate current records, revisions, change rows, secondary indexes,
blob references, and the idempotency outbox. The v1 row/tuple formats remain
readable only through their explicit compatibility APIs. No v1 key is silently
reinterpreted as v2. Migration is copy-verify-cutover with an immutable source,
an independently verified destination, and rollback before old state retires.

## Ownership

- FoundationDB/redb: physical durability and transaction mechanics.
- Logical-DB: validated scope, v2 key layout, bounded orchestration, backend
  parity, idempotency, and state/receipt binding.
- Braid: Cid, canonical bytes, verifier, registry, admission and runnable proof.
- Logic OS Kernel/Secure Authority: principal and namespace authorization.
- Forge: canonical receipt schema.
- LEM/Cairn: durable domain objects and derived meaning respectively.

## Falsifying proof

The gate must exercise two accounts and containers, cross-scope denial,
malformed/oversized identities, v1/v2 disjointness, byte-order stability,
duplicate operation IDs, conflicting duplicate payloads, concurrent stale
versions, consumed/stale Braid invocations, process restart, and identical
logical outcomes over embedded and outboard providers. A mock may supplement
but cannot replace the redb and live-FDB journeys.

## Consequences

Applications may embed the processing layer and local store directly. A cloud
deployment runs the same layer beside the FDB client; applications do not gain
raw FDB handles. Logic OS Kernel is a privileged consumer for tape/CID flows,
not Logical-DB's exclusive host. The legacy five storage classes survive as
optional layers until migrated onto v2; they are not a second application API.
