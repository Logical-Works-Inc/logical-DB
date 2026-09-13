# Logical-DB v3 architecture contract

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `evidence/SOURCES.md`.

## Implemented crate DAG

Arrows point from a consumer to an internal dependency. Storage, CRDT, and
protocol types remain mutually separated; the verifier rejects an undeclared
edge.

```text
application / LEM
  `-- logical-db                         stable embedded facade
        |-- logical-db-model             IDs, authority, indexed values, bounds
        |-- logical-db-crdt              Automerge document boundary
        |     `-- logical-db-model
        |-- logical-db-store             redb transactions + private key axis
        |     `-- logical-db-model
        |-- logical-db-sync              bounded push/pull state machine
        |     |-- logical-db-model
        |     `-- logical-db-protocol    versioned transport-neutral messages
        `-- logical-db-platform          Windows/macOS/Linux data placement

semantic ports (above model, below applications / LEM; non-authoritative)
  |-- logical-db-episode               content-hashed episode log + operators
  |     `-- logical-db-cairn           typed world-token observation vocabulary
  `-- consumed optionally by logical-db (semantics feature) and logical-db-e2e

The semantic ports are bounded local models. They do not own CAIRN ingestion,
canonical ORIGIN persistence, or FORGE receipts. The LEM SOT remains the
authority for those planes; the locked LEM v0 BLAKE3 wire is not interchangeable
with the workspace v1 SHA-256 semantic snapshot. A future adapter must make
that translation and its durable handoff explicit. See
`docs/adr/0001-semantic-plane-boundary.md`.

thin client
  `-- logical-db-http                    pooled async TLS/CBOR transport
        |-- logical-db-sync
        `-- logical-db-protocol

standalone or nested service
  |-- logical-db-daemon                  deployable composition root
  |     |-- logical-db-server            bounded Axum router + auth ingress
  |     |-- logical-db-service           protocol/authority orchestration
  |     |-- logical-db-braid             runnable request/frontier proofs
  |     `-- logical-db-store-fdb         async FDB ServiceStore adapter
  |-- logical-db-server
  |     `-- logical-db-service
  |           |-- logical-db-crdt        native change validation
  |           |-- logical-db-model
  |           `-- logical-db-protocol
  |-- logical-db-store-fdb
  `-- logical-db-braid                   real Braid admission adapter
```

`logical-db-e2e` is a non-published black-box harness. Its in-memory service
journey checks offline convergence, indexes, cursors, and restart. Its retained
`facade_fdb_journey` imports only the public facade and crosses real HTTP,
Braid, and an official FDB cluster. `fdb-ambiguity-journey` enables official
client fault injection and proves a post-commit unknown result, exact replay,
single frontier advance, and invocation consumption. The evidence verifier's
allowed-dependency map must additionally declare the semantic edges
(`logical-db-episode` on `logical-db-cairn`, facade `logical-db` on both, and
`logical-db-e2e` on both); these semantic edges are declared in the verifier.

## Public API boundary

| Boundary | Stable entry points | Deliberately private |
|---|---|---|
| Embedded app | `Database::open`, `open_default`, `open_with_schema`, `edit`, `read`, `query_index`, `changes_since`, `synchronize` | redb tables, key bytes, raw Automerge/FDB handles |
| Structured state | `Reader::{get,get_index,conflicts,conflicts_at,keys,len,text}`, `Editor` map/list/text/counter methods | CRDT actor/op internals except opaque conflict IDs |
| Projection schema | `Projection`, `IndexId`, `IndexValue`, nonzero schema version | reverse rows, ordered value encoding, rebuild mechanics |
| Thin client | `HttpTransport`, async `CredentialProvider`, `SyncLimits`, `SyncReport` | reqwest client, CBOR buffers, retry cursors |
| HTTP service | `logical_db_server::router`, `Authenticator`, `Clock` | listener/TLS/process lifecycle, authorization headers |
| Durable service | `ServiceStore::{frontier,accept,page}`, `RetentionStore::{build_snapshot,seal_snapshot,prune}`, `ExecutionGrant` | FDB database and transaction handles |
| Standalone process | `logical-db-daemon` environment contract | FDB network guard, directory transaction, listener lifecycle |
| Braid | `prepare_request`, `prepare_local_request`, `BraidGrant`, `BraidAuthority`, `LocalBraidAuthority` | grant construction fields and proof fabrication |

An application mutation supplies `OperationId` and an intent
`OperationDigest`. After native CRDT validation, trusted service code computes
an `ActionDigest` over a versioned length-framed encoding of scope, document,
operation, replica, intent, change digest, and exact change bytes. Braid and the
store bind the computed action. Exact retries return the original durable
receipt without rerunning application code. Reusing an operation identity for
different intent/change bytes fails closed.

## Local transaction and subscription contract

```text
load snapshot + revision
  -> run one rollback-safe CRDT edit
  -> derive canonical immutable change
  -> run deterministic schema projections
  -> one immediate redb transaction:
       snapshot + revision
       immutable change
       operation binding + receipt
       outbox sequence
       forward and reverse secondary-index rows
       durable change-feed event
  -> return receipt
```

A wakeup may be dropped; `changes_since(cursor)` is the lossless recovery path.
Remote pages materialize merged snapshots, indexes, newly observed events, and
the replacement peer checkpoint in one transaction. A duplicate page appends no
phantom events. Schema identity is bound before state is opened; drift returns a
typed migration-required error instead of serving stale indexes.
`Database::migrate_schema` records one exact target, scans snapshots in bounded
document-ID order, rechecks each revision while replacing that document's index
rows atomically, and advances its durable cursor in the same transaction. Normal
edits and index reads stay closed during migration; an exact-target retry resumes
and finalization atomically replaces the schema binding and marker.

## Synchronization and API flow

```text
Database outbox
  -> bounded PushRequest
  -> HttpTransport validates + CBOR POST /v1/changes:push
  -> whole-request concurrency admission + deadline
  -> server authenticates credential
  -> validate native change bytes and compute canonical action identity
  -> read scoped FDB frontier
  -> Braid admits capsule + proves exact scope/action/frontier predicate
  -> RunnableInvocation becomes an opaque ExecutionGrant
  -> FDB recomputes action, rechecks frontier, and consumes invocation
  -> exact operation returns accepted / duplicate / recovered-unknown / conflict

durable peer checkpoint
  -> bounded PullRequest
  -> POST /v1/changes:pull
  -> HMAC-authenticated scoped checkpoint opens to ordered FDB position
  -> FDB read transaction rechecks normalized scope/action/frontier
  -> bounded PullPage
  -> CRDT verifies/merges all changes
  -> local snapshots + indexes + events + checkpoint commit atomically
```

Unknown upload outcomes do not acknowledge the outbox. Push responses must have
exact operation cardinality and order, and rejection codes survive to the app.
`SyncReport::more` remains true for either unsent upload work or additional pull
pages. Checkpoint expiry restarts without consuming the installed-page budget;
an empty page that claims more work is rejected as non-progress. A valid expired
checkpoint produces HTTP 410 and an explicit full-resync transition; forged or
cross-scope checkpoints fail rather than guessing continuity. Notifications,
when added by a host, are only pull hints.

## Server baselines and retention

This stack layer adds a state-bearing server recovery artifact and makes it a
prune precondition. `SnapshotArtifact` v2 is content addressed with the
`logical-db-baseline-v2` SHA-256 domain. Its digest covers the immutable scope,
log position, ordered document IDs, canonical native document snapshots, and
each document's tombstone bit. The artifact is bounded to 256 documents and
4 MiB of aggregate snapshot bytes; its fixed descriptor records the version,
position, digest, document count, and byte length.

Position-only v1 `SnapshotRecord`s remain readable for migration and inspection,
but are prune-ineligible. A v2 artifact is required before the retention floor
can advance. FoundationDB reconstructs the artifact from the immutable log,
rechecks the supplied descriptor and document states against the same frontier,
and atomically seals the pointer, document manifests, and snapshot chunks in a
single transaction. The prune transaction revalidates the v2 descriptor and
every manifest and chunk before clearing the collected log prefix, while also
checking the frontier, live checkpoints, and peer-consumption watermark.

This establishes the server-side prune gate; it does not close issue #28. The
next stack layer must provide paged HTTP recovery and one atomic client install
of the recovered document state and checkpoint after expiry. No hosted
full-recovery journey is claimed by this layer.

## Internal axes

The embedded redb table uses a backend-private byte prefix:

```text
("LDB\\x03", account UUID, space UUID, kind, suffix...)
```

Kinds separate snapshots, immutable changes, operation receipts, ordered
outbox entries, local and event sequences, revisions, peer checkpoints,
acknowledgements, the single upstream binding, schema binding, forward index
rows, and reverse index rows.
Index scalar encodings preserve boolean and integer order and escape variable
bytes before the document UUID cursor. Applications never receive these bytes.

The service FDB adapter receives a directory-layer allocated root and uses:

```text
root / (3, account UUID, space UUID) /
  (1, operation UUID)                 exact idempotency manifest
  (2)                                 scoped sequence
  (3, sequence)                       ordered change manifest
  (4, sequence, chunk)                64 KiB canonical-change chunks
  (5, invocation digest)              single-use Braid execution marker
  (6)                                 retention floor and peer-consumption watermark
  (7)                                 v2 snapshot baseline descriptor
  (8, baseline digest, document UUID) state document manifest
  (9, baseline digest, document UUID, chunk)
                                      canonical state snapshot chunks
```

Chunking stays below FDB’s 100 kB value limit. Every adapter transaction applies
its own timeout and retry ceilings without mutating a host-shared FDB handle.
The adapter distinguishes cancellation, version
skew, retry exhaustion, native failure, binding failure, durable corruption,
and a commit that may have succeeded. On an FDB retry marked may-have-committed,
the transaction first reads the exact operation manifest and chunks; equality
returns `RecoveredUnknownCommit` without a second append.

## Braid and deployment separation

Core CRUD never imports Braid. `logical-db-braid` pins real Braid crates at
commit `4b1b3e9`; it admits canonical capsule bytes, evaluates an exact predicate
over a content-addressed `FlowSnapshot`, and calls `RunnableInvocation::prepare`.
The adapter additionally refuses a capsule unless vault-classified request
material flows into the exact scoped capability: reads remain `Read`, durable
writes are `Irreversible`, and synchronization is `Egress` with a vault ceiling.
Write and sync capsules must carry Braid's dangerous-operation confirmation
policy; a capability string alone is insufficient.
The snapshot commits to account, space, exact DB effect (point read and scan are
distinct), canonical action digest, current durable frontier, and exclusive
expiry. The caller's intent digest remains a separate idempotency field.
`BraidGrant` retains that verifier-owned runnable;
the service passes it unchanged to FDB, which atomically consumes its combined
capsule/registry/authority/snapshot/predicate identity with a new operation.
The same invocation cannot authorize another operation, and a proof prepared
against an old frontier cannot execute.

Embedded and service deployment share protocol semantics, not storage handles:
redb is the app’s offline replica; FDB is the multi-tenant ordered server log.
Logic OS may provide authentication, Braid ambient authority, scheduling, and
notifications, but Logical-DB remains independently embeddable and independently
deployable.

The shipped standalone composition boots the FDB client network exactly once,
opens the configured cluster file, obtains `("logical-db", "v3")` through the
Directory Layer, builds a scope-bound `FdbServiceStore`, and retains the FDB guard
through graceful listener shutdown. It accepts only loopback plaintext;
public ingress requires an external TLS terminator and credential issuer. The
reusable router remains listener-neutral so Logic OS can supply its own
lifecycle and Braid-derived authenticator.

`LocalBraidAuthority` refuses irreversible writes and egress synchronization.
Pinned Braid records `HumanConfirm` in the capsule but exposes no independently
verifiable runtime confirmation evidence, so the daemon does not fabricate that
field after bearer authentication. A host supplies `BraidAuthority` or its own
`Authenticator` and separately prepared Braid material. Until that composition
exists, the standalone daemon is a fail-closed shell, not a consumable sync
service.

## Windows contract

Explicit paths are first-class. The optional default helper uses
`LOCALAPPDATA/LogicalDB/<application-id>/replica-v3.redb`, accepts Unicode and
spaces in the root, rejects relative environment-derived roots and path-like
application IDs, and creates no shared
system directory. redb owns locking and immediate durable commits. Native
`windows-latest`, macOS, and Linux jobs are release gates. The Windows job runs
the embedded edit/restart, forced-process-termination, real loopback HTTP, and a
Windows-only default-path/exclusive-lock/Unicode/read-only failure journey. FDB
is a service deployment dependency, not a requirement for a Windows app: the
Windows app embeds redb and uses the rustls HTTP client when cloud sync is enabled.

## Deliberate release blockers

The service log retains immutable ordered entries until collection, and this stack supplies a bounded,
content-addressed v2 server baseline with a transactional retention floor and
peer-aware prune gate. The artifact carries native document state and explicit
tombstone markers. A readable v1 position-only record cannot authorize pruning,
and the FDB transaction validates the v2 artifact before deleting history.
Deleting history is still not a complete client recovery contract: paged HTTP
baseline recovery and atomic client installation remain in the next stack layer,
so issue #28 remains open. The embedded store has a versioned portable
export/restore contract; its direct process-kill-during-restore receipt remains
outstanding. This implementation does not treat the server prune gate as proof
of end-to-end recovery.
The standalone service is additionally blocked on externally verifiable,
request-bound Braid confirmation evidence for irreversible and egress effects.
Multi-node FDB failover/restore, public ingress, mobile sandboxing, and sustained
load remain system tests rather than inferred properties.
