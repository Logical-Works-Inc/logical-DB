# Logical-DB product and architecture direction

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `evidence/SOURCES.md`.
>
> Research receipt: primary documentation and implementation references were
> read through BrowserOS neo on 2026-09-24. The additional research URLs are
> listed in §3. This is a product/architecture direction, not a claim that the
> current workspace already implements the target.

## 1. The soul: what Logical-DB is

Logical-DB is a **local-first, provenance-first application substrate for
safe change**. It turns an ordinary typed application mutation into a durable,
replayable, synchronizable, and explainable fact without making the application
team own distributed-systems mechanics.

The product is not a faster PostgreSQL, a Redis replacement, or a hosted cloud
service wearing a database-shaped API. PostgreSQL and existing SQL remain
useful analytics and legacy systems. Redis remains an excellent disposable hot
store. Logical-DB owns the missing middle: the mutation contract between an
application, a local device, a sovereign service, an ontology, and an agent.

> **USP:** a change is the unit of product truth. Identity, causal context,
> durable receipt, synchronization position, recovery state, and scope are part
> of the write path—not application bookkeeping discovered after an incident.

This is the zero-cognitive-debt bargain: the developer chooses a typed model,
features, and an action's intent; Logical-DB owns the mechanics that otherwise
become a second product for every team.

### The one-sentence test

A feature belongs in the core only if it makes a user-visible state transition
safer, faster to use, cheaper to operate, or more explainable **without exposing
storage topology, retry policy, or authority plumbing to the application**.

### What it is not

- It is not a claim to be the fastest OLTP engine. Raw point-read and
  insert/delete latency are compared against native database engines separately.
- It is not a general SQL executor. Joins, aggregates, and arbitrary SQL remain
  a deliberate sink/adapter concern unless a later decision changes the product
  boundary.
- It is not a cache. A cache is disposable acceleration; Logical-DB's embedded
  replica is the local durable truth that a cache may accelerate.
- It is not Palantir, Datomic, Redis, or a cloud vendor. It borrows explicit
  patterns from those systems while keeping the data, authority, and recovery
  contracts portable and self-hostable.
- It is not an application ontology or memory system by itself. Cairn, LEM, and
  Forge remain separate, contract-first domains.

## 2. The experience contract: zero developer and agent debt

The application supplies intent, not infrastructure:

```text
application / agent
  -> typed model + feature choice
  -> action intent + ordinary domain input
  -> one Logical-DB call
       validate and canonicalize
       bind identity, scope, and causal revision
       commit state, index, outbox, event, and receipt atomically
       return a typed result
  -> optional sync/recovery work happens behind the same facade
```

The developer should not have to implement any of the following:

- document IDs, storage keys, canonical byte encoding, or digest framing;
- an outbox, retry token, checkpoint, cursor, or duplicate-receipt table;
- a conflict merge policy for ordinary concurrent edits;
- schema/projection migration, index rebuild, or version-skew refusal;
- TTL indexes, eviction loops, blob cleanup, backpressure, or cancellation;
- synchronization retries, expiry recovery, partial-page handling, or restart
  state;
- an audit table that claims to explain a mutation but cannot prove it;
- a second authorization system for agents and applications.

The developer does choose the domain model, the enabled feature profile, the
scope/identity integration, and the user-visible action semantics. Those are
real product decisions. The implementation must not push the rest back out to
the caller.

### Defaults that remove cognitive debt

| Failure the team should not have to solve | Logical-DB default |
|---|---|
| Retry after a timeout or crash | `OperationId` + request digest returns the original receipt or a typed conflict. |
| Two devices edit concurrently | CRDT merge where the model permits it; explicit conflict visibility where it does not. |
| A projection changes | Schema/projection digest binding and bounded, resumable migration. |
| A page is lost or reordered | Durable checkpoint plus idempotent install; progress is a report, not app state. |
| A peer falls behind retention | State-bearing baseline recovery or an explicit full-resync transition. |
| An agent asks for too much | Bounded payload/page/fan-out limits and typed refusal. |
| A service call is uncertain | Durable operation identity and exact replay; unknown commit is recovered, not guessed. |
| A value expires or is evicted | Indexed expiry and bounded maintenance; the durable source remains available. |
| The format or protocol is unknown | Fail closed with a versioned error and migration/restore path. |

The library should stay boring at the edges: no ambient registry, no hidden
fallback store, no silent last-writer-wins getter, and no default that moves
distributed complexity into a caller's retry loop.

## 3. What the frontier systems teach us

The research is a set of design lessons, not a shopping list.

| System | Pattern worth adopting | Deliberate boundary for Logical-DB |
|---|---|---|
| **Google Cloud Spanner** | MVCC plus externally ordered commit history; strong reads without blocking writes; split/partitioned serving as scale grows. | Adopt explicit causal frontiers and per-scope ordering. Do not require a managed global clock or make a cloud deployment mandatory. |
| **CockroachDB** | A familiar SQL-shaped contract over a transactional KV substrate; range partitioning, quorum replication, multi-active nodes, and automatic rebalancing. | Adopt stable public contracts, partitionable state, and symmetric service behavior. Do not put a distributed SQL server in every embedded app. |
| **native_db** | Typed Rust models, secondary indexes, transparent serialization, and automatic model migration over a simple embedded store. | Adopt types and migrations as first-class. Keep CRDT, receipt, sync, and ontology authority above the storage engine. |
| **Redis** | A small, sharply defined data-structure server; single-threaded command execution makes common operations easy to reason about; specialized structures solve jobs without a general query engine; persistence is an explicit choice. | Adopt narrow typed capabilities, a hot/cold split, and bounded work per operation. Do not make in-memory data the system of record or copy cache-grade durability. |
| **CloudKit** | Zones, opaque change tokens, persisted sync state, pull-after-hint delivery, and automatic transient-retry classification. | Adopt namespace checkpoints, hints, and resumable state. Keep the transport swappable and the local path offline-first. |
| **Palantir Foundry Ontology** | Objects, links, actions, functions, derived properties, and a governed operational layer over source data. | Adopt a closed, versioned Cairn registry and action gate. Keep the registry portable and keep Cloud from becoming semantic truth. |

Primary research URLs retrieved through BrowserOS neo on 2026-09-24:

- Spanner TrueTime and external consistency:
  <https://docs.cloud.google.com/spanner/docs/true-time-external-consistency>
- Spanner design paper:
  <https://research.google.com/archive/spanner-osdi2012.pdf>
- CockroachDB architecture:
  <https://docs.cockroachlabs.com/docs/stable/architecture/overview>
- CockroachDB transaction layer:
  <https://docs.cockroachlabs.com/docs/stable/architecture/transaction-layer>
- `native_db` implementation and README:
  <https://github.com/vincent-herlemont/native_db>
- Redis data types:
  <https://redis.io/docs/latest/develop/data-types/>
- Redis persistence:
  <https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/>
- Redis cluster specification:
  <https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/>
- Redis stream idempotency:
  <https://redis.io/docs/latest/develop/data-types/streams/idempotency/>
- Palantir Ontology overview:
  <https://www.palantir.com/docs/foundry/ontology/overview/>
- Palantir action types:
  <https://www.palantir.com/docs/foundry/action-types/overview/>

### The important negative conclusion

Logical-DB must not become “Redis plus safety” or “Postgres plus AI.” That
would erase its position. Its defensible combination is:

1. **Redis-grade simplicity on the hot path**—small typed operations and clear
   capability surfaces;
2. **Spanner/Cockroach-grade explicit ordering and partitioning contracts** for
   the service plane;
3. **native_db-grade typed local ergonomics** without surrendering the causal
   change model;
4. **Cairn/Palantir-grade meaning and action governance** above the data plane;
5. **LEM-grade cited, temporal, contradiction-aware memory** above the ledger.

Each layer must be replaceable behind a versioned contract. No layer may quietly
become the owner of another layer's authority.

## 4. The target architecture: four planes, one facade

```text
                         applications / agents
                                  |
                   typed facade: models + actions + memory
                                  |
        +-------------------------+-------------------------+
        |                         |                         |
     Cloud data              semantic meaning             action plane
        |                         |                         |
 local redb + CRDT       Cairn -> LEM -> LocalWorld       Forge
        |                         |                         |
 bounded sync / FDB       candidates -> episodes        receipts
        |                         |                         |
        +--------------- immutable evidence ---------------+
                                  |
                         scoped durable receipt graph
```

### Plane 1: Cloud data plane — durable local state

The embedded path is the first-class path. It must work with no daemon, network,
or FoundationDB. A write commits the user-visible state, causal change, indexes,
outbox, event, and operation receipt as one unit. Local execution is a resource
feature: it lowers retained remote data, transfer, cloud compute, and latency
where the service is available, while the service remains independently
deployable.

The current redb + Automerge + FDB arrangement is a strong implementation of
this plane. It is not yet a claim of consumer-cloud scale or multi-region
recovery.

### Plane 2: Cloud movement plane — changes, not tables

Sync is a resumable state machine, not an application protocol. The service
should eventually partition state by scope and view, use ordered frontiers, and
make region/residency an explicit contract. The public facade should not expose
ranges, leases, tuple keys, or consensus machinery.

A notification is a hint. A checkpoint is a durable progress fact. A receipt is
proof of an accepted mutation. Those three concepts must remain distinct.

### Plane 3: Cairn + LEM — evidence and memory

Cairn is the evidence/ontology plane: source coordinates, content identity,
closed object/link vocabulary, candidate evidence, and rebuildable operational
views. It does not decide truth and cannot execute effects.

LEM/ORIGIN is the canonical episodic memory plane: append-only episodes,
multiple clocks, contradictions, consolidation, and bounded LocalWorld answers.
An answer is useful to an agent only when its claims, validity interval, and
source evidence are inspectable. Similarity is retrieval acceleration, not
memory semantics.

The legal flow is:

```text
source -> Cairn candidate -> LEM append -> LocalWorld
                                      -> Forge action -> receipt candidate
```

There is no direct app or Cloud write into canonical memory.

### Plane 4: Forge — governed action

Forge is the only effect plane. It receives a capability-scoped action over a
versioned LocalWorld, returns a receipt candidate, and routes that receipt back
through Cairn. It never mutates the Cloud store, the ORIGIN log, or the ontology
directly.

This is the Palantir lesson that matters: the ontology must have a governed
kinetic path, but the action gate and data substrate remain portable contracts
rather than vendor runtime features.

## 5. Redis-inspired hot/cold decomposition

Redis is a useful design teacher because it makes the common path legible. It
is not a safe model for durable truth. Logical-DB should adopt the separation,
not the default.

| Tier | Contents | Durability/authority | Access shape |
|---|---|---|---|
| **Hot** | Bounded cache entries, indexes, recently active documents | Disposable acceleration; never the only copy | One typed operation, explicit TTL/eviction, no unbounded memory |
| **Warm** | Embedded documents, projections, local checkpoints | Immediately durable local truth; scoped to one replica | Local reads/writes, offline, low ceremony |
| **Cold** | Immutable changes, receipts, content-addressed blobs, service log segments | Canonical replay and recovery evidence; retention/prune aware | Bounded pages, resumable checkpoints, explicit recovery |
| **Semantic** | Cairn candidates, LEM episodes, LocalWorld, Forge receipts | Separate authority domains; evidence-bearing | Typed semantic and action contracts |

A cache hit may skip remote I/O. A cache miss must not skip the durable source.
A cache eviction must not delete the only explanation of a write. A sync worker
must not become an unbounded queue, and a notification must never be the only
way to discover a change.

The likely ergonomic path is a `logical_db::cache`-style typed facade with
`put/get/delete/watch` semantics over the existing embedded database and
projections. It is an acceleration surface, not a second store or a parallel
source of truth.

## 6. Consumer cloud: local first, cloud when it helps

The consumer-cloud product shape is not “hosted database with a local cache.” It
is:

1. an application embeds one library and works immediately offline;
2. account/space identity and feature choices are explicit;
3. sync is optional and monotonic, with automatic retry and recovery state;
4. a thin client or daemon adds remote delivery without changing the app model;
5. content-addressed data and change-only transfer reduce duplicate work;
6. service partitioning, residency, and regional recovery remain infrastructure
   contracts, not application chores.

The first cloud milestone should be proven as a complete journey: a real local
client, a real transport, a real service, restart, duplicate delivery, partial
page, checkpoint expiry, and recovery. A “managed cloud” label without that
journey is a product lie.

## 7. SQL and PostgreSQL: absorb, do not duplicate

Teams with old-school SQL and PostgreSQL need a low-change-management path, not
a forced rewrite into an exotic model.

- Keep PostgreSQL and existing SQL as the BI/joins/aggregates sink where they
  already win.
- Provide versioned logical export/import and digest-equivalence proofs.
- Add an explicit bridge contract that emits typed Logical-DB change candidates
  or reads a bounded view; do not silently create a second source of truth.
- Make schema and projection semantics part of the durable contract so a bridge
  can be replayed after an outage or a failed migration.
- Keep application writes on one authority path. Dual-write is a migration
  mechanism with receipts, not a permanent application pattern.

The target is “one application-facing substrate, existing systems remain
valuable,” not “replace every database on day one.”

## 8. Implementation order and proof gates

### Near term — make the product legible

1. Publish this direction as the single product narrative and remove competing
   “faster database” language from product-facing docs.
2. Finish the typed cache/feature facade over the existing durable replica; do
   not fork storage or add a second engine.
3. Add namespace/checkpoint policy and a public consumer journey so sync is a
   background capability, not application code.
4. Complete the Cairn/LEM migration adapter and external consumer proof before
   calling semantic ports canonical.
5. Define the first typed action/grant surface and receipt loop for Forge.

### Medium term — make the service boring to operate

1. Partition service state by scope/view and publish the routing contract.
2. Separate serving, durable log, recovery artifact, and retention policy.
3. Add content-addressed blob offload with orphan-safe GC and bounded transfer.
4. Add a Postgres/SQL bridge as a typed export/import or read-sink adapter.
5. Add a reference consumer for a small application and a small AI agent, both
   using only the public facade.

### Long term — earn the frontier claims

- Matched comparisons against native_db/redb/SQLite, Redis, CockroachDB, and
  Spanner at the actual product job, not raw microbenchmarks.
- Sustained, burst, saturation, noisy-neighbor, and recovery tests at the
  declared Cloud scale.
- A cited LocalWorld memory evaluation against credible agent-memory baselines.
- Three-platform end-to-end receipts and a real crash/restart journey.
- A post-cutover or migration receipt proving a team can move away with
  meaning intact.

Until those measurements exist, the repository must continue to say “target” or
“unknown,” never “blazing fast,” “million-user,” or “production complete.”

## 9. Architectural decision rule

For every new feature, answer these questions in the design:

1. Does it reduce developer or agent decisions, or merely move them?
2. Is it a hot-path optimization, durable truth, semantic meaning, or action?
3. What is the owner, scope, atomicity, idempotency, conflict, and recovery
   contract?
4. Can the old SQL/Postgres system continue to own its job during migration?
5. What evidence would make the claim false, and what measurement closes it?

If a feature cannot answer those questions without requiring a new storage
owner, an unbounded queue, ambient authority, or an unowned background task, it
is not ready to enter Logical-DB.
