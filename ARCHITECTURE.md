# Logical-DB architecture

Facts only. Rules and their tests are in `INVARIANTS.md`.

## Crate graph

Arrows point from a consumer to a dependency. `tests/crate_graph.rs`
refuses an undeclared crate and any edge that points upward.

```text
logical-db                facade: Database, sync, export, projections
  |-- logical-db-model    ids, scope, authority values, bounds, baselines
  |-- logical-db-crdt     Automerge document boundary
  |-- logical-db-store    redb transactions, private key axis
  |-- logical-db-sync     bounded push/pull state machine
  |-- logical-db-protocol versioned transport-neutral messages
  `-- logical-db-platform Windows/macOS/Linux data placement
logical-db-http           async HTTP transport (CBOR)
logical-db-kv             cache adapter over the facade
logical-db-service        protocol and authority orchestration
logical-db-server         bounded Axum router, auth ingress
logical-db-store-fdb      FoundationDB ServiceStore adapter
logical-db-daemon         standalone composition root
logical-db-e2e            black-box journeys (not published)
```

Storage, CRDT and protocol crates do not import transport, HTTP or service
types.

## Public surface

| Boundary | Entry points | Private |
|---|---|---|
| Embedded app | `Database::{open, open_default, edit, read, query_index, changes_since, synchronize}` | redb tables, key bytes, Automerge handles |
| State | `Reader`, `Editor` | CRDT actor and op internals |
| Thin client | `HttpTransport`, `CredentialProvider`, `SyncLimits`, `SyncReport` | HTTP client, CBOR buffers |
| Service | `logical_db_server::router`, `Authenticator`, `Clock`, `ServiceStore`, `RetentionStore` | FDB handles |
| Authority | `Authority`, `ExecutionGrant`, `LocalScopeAuthority`, `LocalGrant` | grant internals |

## Local write

One edit is one immediate redb transaction:

```text
CRDT edit -> canonical change -> schema projections -> commit {
  snapshot + revision, change, operation receipt, outbox entry,
  index rows, change-feed event }
```

A dropped wakeup loses nothing: `changes_since(cursor)` replays. A schema
change returns a typed migration error until `Database::migrate_schema` runs.

## Sync

```text
outbox -> PushRequest -> POST /v1/changes:push -> authenticate
  -> validate change, compute ActionDigest -> Authority grants
  -> FDB rechecks frontier, consumes the invocation, appends
peer checkpoint -> PullRequest -> POST /v1/changes:pull
  -> keyed checkpoint opens to an FDB position -> bounded page
  -> merge, then snapshots + indexes + events + checkpoint commit at once
```

An unknown push result never acknowledges the outbox. An expired checkpoint
returns HTTP 410 and a full resync. A forged or cross-scope checkpoint fails.

## Baselines and retention

`SnapshotArtifact` v2 covers scope, position, document ids, native snapshots and
tombstones, bounded to 256 documents and 4 MiB. Prune needs a sealed v2
baseline, the frontier, live checkpoints and the consumption watermark, in one
FDB transaction. A replica behind the floor recovers through
`/v1/recovery:baseline` and installs state and checkpoint atomically.

## Key layout

Embedded redb: `("LDB\x03", account, space, kind, suffix...)`.

FoundationDB, under the directory `("logical-db", "v3")`:

```text
(3, account, space) / (1, operation)        idempotency manifest
                    / (2)                   sequence
                    / (3, seq) (4, seq, n)  change manifest, 64 KiB chunks
                    / (5, invocation)       single-use grant marker
                    / (6) (7)               retention floor, baseline descriptor
                    / (8, digest, doc) (9, digest, doc, n)  baseline state
```

On a may-have-committed retry the adapter reads the manifest first and returns
`RecoveredUnknownCommit` without a second append.

## Deployment

The daemon boots the FDB network once (`logical_db_daemon::boot_network`, the
one `unsafe` in the repo), opens the directory, serves loopback plaintext only
and keeps the network guard until shutdown. Public ingress needs an external TLS
terminator. `LocalScopeAuthority` reads and scans; it refuses writes and sync.

Windows: the default path is `LOCALAPPDATA/LogicalDB/<app>/replica-v3.redb`.
Windows apps embed redb and never need FDB.

## Journeys that prove it

| Journey | Test |
|---|---|
| Kill and restart a writer | `tests/kill_recovery.rs`, `tests/crash_recovery.rs` |
| Two replicas converge | `src/journeys_converge.rs` |
| Baseline recovery | `src/journeys_recovery.rs`, `tests/fdb_recovery_composition.rs` |
| Kill during restore | `tests/fdb_process_kill_restore.rs` |
| FDB unknown commit and replay | `tests/fdb_ambiguity.rs` |
| Seeded network faults | `tests/sim_sync.rs` |

Not yet proven: multi-node FDB failover, public ingress, sustained load.
