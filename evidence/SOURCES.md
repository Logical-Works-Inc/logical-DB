# Primary sources

The `Snn` ids that file headers cite. A citation is a pointer to the spec, not
proof of behaviour; tests prove behaviour (`INVARIANTS.md`). Ids are stable; a
retired id stays retired.

## Local-first state and convergence

- S01 CKSyncEngine: pending changes, bounded batches, retry, conflicts. <https://developer.apple.com/documentation/cloudkit/cksyncengine-4b4w9>
- S02 CloudKit server change tokens: opaque scoped positions. <https://developer.apple.com/documentation/cloudkit/ckserverchangetoken>
- S03 CloudKit zone changes: pagination and `moreComing`. <https://developer.apple.com/documentation/cloudkit/ckdatabase/recordzonechange>
- S04 CloudKit conflict records: client, server, ancestor versions. <https://developer.apple.com/documentation/cloudkit/ckerror/serverrecordchanged>
- S05 CloudKit record change tags: optimistic version checks. <https://developer.apple.com/documentation/cloudkit/ckrecord/recordchangetag>
- S06 Google Drive changes: durable start tokens, paged retrieval. <https://developers.google.com/workspace/drive/api/guides/manage-changes>
- S07 Google Drive notifications: hints, not state transfer. <https://developers.google.com/workspace/drive/api/guides/push>
- S08 Google Drive resumable uploads: resumable sessions, expiry. <https://developers.google.com/workspace/drive/api/guides/manage-uploads>
- S09 CouchDB replication protocol: push/pull, checkpoints, recovery. <https://docs.couchdb.org/en/stable/replication/protocol.html>
- S10 CouchDB conflicts: deterministic visibility, no silent discard. <https://docs.couchdb.org/en/stable/replication/conflicts.html>
- S11 Automerge: CRDT documents, causal changes, sync state. <https://github.com/automerge/automerge>
- S12 Local-first software: offline, multi-device, CRDT/transport split. <https://www.inkandswitch.com/essay/local-first/>
- S13 Algebraic replicated data types: coordination-free convergence. <https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ECOOP.2023.14>

## Storage and transactions

- S14 FoundationDB developer guide: serializable transactions, retries. <https://apple.github.io/foundationdb/developer-guide.html>
- S15 FoundationDB data modeling: tuples, prefix scans. <https://apple.github.io/foundationdb/data-modeling.html>
- S16 FoundationDB directory layer: subspace indirection. <https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/directory/DirectoryLayer.html>
- S17 FoundationDB limitations: key, value, transaction ceilings. <https://apple.github.io/foundationdb/known-limitations.html>
- S18 FoundationDB transaction API: unknown commit outcomes. <https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/Transaction.html>
- S19 SQLite atomic commit: all-or-nothing under interruption. <https://www.sqlite.org/atomiccommit.html>
- S20 SQLite WAL: checkpointing, concurrency, recovery. <https://www.sqlite.org/wal.html>
- S21 SQLite locking: exclusion in the storage engine. <https://www.sqlite.org/lockingv3.html>
- S22 SQLite corruption guidance: fault injection, safe backup. <https://www.sqlite.org/howtocorrupt.html>
- S32 FoundationDB large values: chunking. <https://apple.github.io/foundationdb/largeval.html>
- S33 FoundationDB error codes: unknown commit, timeout, skew. <https://apple.github.io/foundationdb/api-error-codes.html>
- S42 FoundationDB automatic idempotency: exact retry identity. <https://apple.github.io/foundationdb/automatic-idempotency.html>
- S43 FoundationDB client testing: real clusters, fault injection. <https://apple.github.io/foundationdb/client-testing.html>

## Protocol, identity, platforms

- S23 HTTP semantics (RFC 9110): idempotency, retries, status. <https://datatracker.ietf.org/doc/html/rfc9110>
- S24 HTTP problem details (RFC 9457). <https://datatracker.ietf.org/doc/html/rfc9457>
- S25 JSON canonicalization (RFC 8785). <https://datatracker.ietf.org/doc/html/rfc8785>
- S26 UUIDs (RFC 9562): sortable v7. <https://datatracker.ietf.org/doc/html/rfc9562>
- S27 Windows app data: per-user storage. <https://learn.microsoft.com/en-us/windows/apps/develop/data/store-and-retrieve-app-data>
- S28 Windows file writes: completion, oplocks. <https://learn.microsoft.com/en-us/windows/apps/develop/files/best-practices-writing-files>

## Library surfaces

- S29 RustCrypto HMAC. <https://docs.rs/hmac/0.13.0/hmac/>
- S30 RustCrypto SHA-2. <https://docs.rs/sha2/0.11.0/sha2/>
- S31 FoundationDB Rust client 0.11. <https://docs.rs/foundationdb/0.11.0/foundationdb/>
- S37 Reqwest async client. <https://docs.rs/reqwest/0.13.4/reqwest/>
- S38 Axum. <https://docs.rs/axum/0.8.9/axum/>
- S39 Ciborium CBOR. <https://docs.rs/ciborium/0.2.2/ciborium/>

## Retired

S34, S35, S36, S40 and S41 named the removed Braid authority integration. The
host authority seam is now `logical_db_service::Authority`.
