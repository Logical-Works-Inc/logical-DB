# Logical-DB application and service API boundary

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
> S33 S34 S35 S36 S37 S38 S39 S40 S41 S42 S43. Primary URLs:
> `../evidence/SOURCES.md`.

## What an application is allowed to know

An app imports `logical-db`, not redb, Automerge, FoundationDB, the server, or
Braid. Its durable identity axis is:

```text
AccountId / SpaceId / DocumentId
ReplicaId / OperationId / OperationDigest
IndexId / IndexValue / PeerId
```

The embedded surface is deliberately small:

```text
Database::open(path, scope, replica)
Database::open_default(application_id, scope, replica)
Database::open_with_schema(path, scope, replica, version, projections)
Database::edit(document, operation, request_digest, edit_fn)
Database::read(document, read_fn)
Database::query_index(index, value, after_document, limit)
Database::changes_since(after_sequence, limit)
Database::migrate_schema(version, projections, batch_size)
Database::synchronize(peer, transport, limits, cancelled)
Database::delete(document, operation, request_digest)
Database::export()
Database::restore(payload)
Database::restore_cursor()
```

`edit_fn` receives only the typed CRDT `Editor`; `read_fn` and projection
functions receive only `Reader`. Storage handles, tuple keys, transaction
objects, raw CRDT objects, and network clients cannot cross this boundary.
`Reader::conflicts` and `conflicts_at` return every causal candidate plus an
opaque stable causal ID, while `get` and `get_index` return the visible winner.
Lists, nested objects, Unicode-code-point text splice/read, counters, key
discovery, and sequence length are available without importing Automerge.

With feature `http`, an app additionally receives:

```text
logical_db::http::HttpTransport
logical_db::http::{CredentialProvider, StaticBearer, Url}
logical_db::{SyncLimits, SyncReport}
```

Custom network implementations use `logical_db::transport::Transport` and the
versioned protocol types re-exported beside it. They never implement a storage
backend.

## Embedded call graph

```text
app
 `-- Database
      |-- CRDT edit/read and deterministic projection
      |-- one local redb transaction
      |    `-- snapshot + revision + change + operation receipt
      |        + outbox + indexes + event
      `-- optional sync state machine
           `-- Transport (HTTP or app-supplied)
```

This path works offline and imports no Braid or FoundationDB code. It is the
Windows/macOS/Linux app-embedding shape.

## Thin-client and standalone call graph

```text
Database::synchronize
 `-- bind scope to one durable upstream (v1 global upload cursor)
      `-- HttpTransport
      `-- POST versioned bounded CBOR
           `-- server request admission (body + concurrency + deadline)
                `-- Authenticator
                     `-- validate native bytes + compute ActionDigest
                     `-- Authority::authorize(scope,effect,action,frontier,time)
                          `-- Braid RunnableInvocation / ExecutionGrant
                `-- ServiceStore
                     |-- frontier(scope)
                     |-- accept(change, grant)
                     `-- page(PageAction, grant)
                          `-- FoundationDB transaction
```

The daemon is one composition shell for those ports, but its local authority
currently refuses write and egress effects: bearer authentication is not
independent confirmation. Logic OS can nest the same router under its own TLS,
identity, lifecycle, observability, and externally prepared Braid authority;
it does not get a second database implementation and does not own local CRUD.

## Private tuple axes

Embedded redb uses one versioned table whose internal key prefix is:

```text
LDB-v3 / account / space / kind / kind-specific suffix
```

FDB receives a Directory-Layer root and then uses typed tuples:

```text
root / 3 / account / space /
  operation / operation_uuid
  sequence
  log / sequence
  chunk / sequence / chunk_index
  invocation / runnable_digest
```

Account and space are the isolation axes; sequence is the ordered change axis;
operation is the idempotency axis; invocation is the single-use authority axis.
None is a user-visible record model. Documents and projections are materialized
application state above those backend-private encodings.

## Failure ownership

- The embedded facade owns CRDT, schema, projection, local durability, and
  materialization-budget errors.
- The transport owns TLS, credentials, representation, status, body, and
  checkpoint-expiry errors; retryable statuses preserve only numeric
  `Retry-After` delays no greater than one hour.
- The service owns protocol correlation, native validation before authorization,
  canonical action construction, and cross-scope checks. `OperationDigest` is
  application intent/idempotency identity; `ActionDigest` is trusted authority
  identity and is never accepted directly from the wire.
- The FDB adapter owns unknown commit, cancellation, version skew, retry
  exhaustion, action recomputation, native failure, binding failure, and durable
  corruption. Only its
  explicit transient categories cross HTTP as retryable 503; permanent and
  corrupt/configuration failures are 500 without retry advice.
- The host owns account authentication, TLS termination, FDB boot/configuration,
  listener lifecycle, secret rotation, metrics, and rollout.

That separation is the standalone/embedded invariant: deployment may replace
ports, but it cannot fork the data semantics or leak physical backend APIs into
applications.

The embedded facade implements document tombstones and bounded export/restore,
including a durable restore cursor. Retained tests cover tombstone propagation,
reopen, corrupt export refusal, and resuming a restore prefix.

The service has retention operations, but its current baseline is a position
marker rather than recoverable document state. State-bearing baseline recovery
remains blocked by issue #28. Multi-upstream fan-out and range/composite queries
are not implied by the existing API. `GOVERNANCE.md` distinguishes these open
contracts from implemented embedded functionality.
