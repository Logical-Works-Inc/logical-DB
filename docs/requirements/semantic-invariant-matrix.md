> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
> S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Primary URLs:
> `../../evidence/SOURCES.md`.

# Semantic invariant matrix — what an end user actually buys

> Scope: **semantic invariants** of a database as the user experiences them
> (meaning of a write, read, retry, merge, recovery, exit). Not a performance
> table. Not a feature checklist. Cells are what the system **guarantees to an
> application author**, not what a blog post asserts.
>
> Legend: **G** = guaranteed, first-class · **P** = partial / config / DIY ·
> **C** = claimed, isolation or limits undocumented · **N** = not offered ·
> **Y** = you build it on top (the Gary tax).
>
> Columns: `Postgres` · `SQL` (MySQL/MariaDB/SQL Server-class) · `SQLite` ·
> `redb` · `native_db` · `RedDB` (reddb-io, **not** redb) · `logical-db`.
>
> RedDB citation: github.com/reddb-io/reddb README + contract matrix (read
> 2026-09-22). Claims are marked **C** where the README is silent on isolation,
> replication, or multi-tenant enforcement.

## 1. Failure semantics — what a crash is allowed to do

| # | Invariant (user language) | Postgres | SQL | SQLite | redb | native_db | RedDB | logical-db |
|---|---|---|---|---|---|---|---|---|
| F1 | A commit survives process death / power loss | G | G | G | G | G | C (double-write buffer, `sync_all`, WAL) | G |
| F2 | A transaction is all-or-nothing | G | G | G | G | G | C (begin/commit/rollback) | G (one commit: snapshot+change+receipt+outbox+indexes+event) |
| F3 | Torn writes are impossible or detected | G | G | G | G | G | C (CRC32 every page + WAL + footer) | G (digest + checksum via store) |
| F4 | Bitrot/corruption is **detected** | P (checksums optional) | P | P | G (checksums) | G (inherits redb) | C (CRC32) | G (export/restore + change digests) |
| F5 | Corruption **fails closed** (never serves bad data silently) | P | P | P | G | G | C | G (restore refuses; live state untouched) |
| F6 | Crash recovery is complete (replay to last commit) | G (WAL) | G (WAL/redo) | G (WAL/journal) | G | G | C (two-phase checkpoint) | G (materialized snapshot outside the process) |
| F7 | A client that times out can retry safely | Y | Y | Y | Y | P (models/keys DIY) | N (no idempotency key contract) | **G** (`OperationId` + request digest → `Duplicate` receipt) |
| F8 | “Did my write commit?” is answerable after crash | Y | Y | Y | Y | Y | N | **G** (operation receipt is in the commit) |

## 2. Concurrency and isolation

| # | Invariant | Postgres | SQL | SQLite | redb | native_db | RedDB | logical-db |
|---|---|---|---|---|---|---|---|---|
| C1 | Multi-threaded writers on one process | G | G | G | G | G | C (server thread-per-conn) | G |
| C2 | **Multi-process** writers on one file | P (cluster) | P (server) | P (WAL, one writer) | **N** (one writer process) | **N** (redb) | **N** (exclusive `flock`) | **N** (one embedded process; Cloud Plane is the multi-node story) |
| C3 | Documented isolation level | G (RC→SSI) | G (varies) | G (serializable/RU) | G (MVCC snapshot) | G (ACID via redb) | **N** (README silent) | P (optimistic revision: `edit_if_revision`) |
| G4 | Lost-update prevention | G | G | G | G | G | N | G (revision CAS + CRDT merge) |
| G5 | Write-skew / phantom control | G (SSI) | P | P | G (snapshot) | G | N | P (per-document causal; not serializable SQL) |
| G6 | Explicit CAS / compare-and-set API | G | G | G | G | G | P | **G** (`edit_if_revision`) |
| G7 | Nested transactions / savepoints | G | G | G | P | P | C | N (batch is one atomic unit, no nested) |

## 3. Read semantics

| # | Invariant | Postgres | SQL | SQLite | redb | native_db | RedDB | logical-db |
|---|---|---|---|---|---|---|---|---|
| R1 | Read-your-writes | G | G | G | G | G | C | G |
| R2 | Causal consistency of a document | G | G | G | G | G | C (per-field reducers) | **G** (CRDT heads + change log) |
| R3 | Linearizable single-key read | G (sync rep optional) | P | P | G (single proc) | G | N | P (local process linear; cross-replica eventual) |
| R4 | Eventual consistency under partition | N (CP) | N | N | N | N | **C** (per-field CRDT-ish reducers) | **G** (CRDT merge) |
| R5 | Monotonic reads for a client | G | G | G | G | G | C | G (revision/frontier) |
| R6 | Secondary indexes match base data **in the same commit** | G | G | G | DIY | **G** | C | **G** (index rows in the same txn) |
| R7 | Cursor pagination stable under concurrent writes | P (keyset) | P | P | DIY | P | N | **G** (document cursor + bounded pages) |
| R8 | Joins / multi-row queries | G | G | G | N | P (index scans) | **G** (SQL/Cypher/…) | **N** (per-document + index probe only) |
| R9 | Aggregates / GROUP BY | G | G | G | N | N | **G** | **N** |
| R10 | Full-text search | G (tsvector) | P | G (FTS5) | N | N | **C** | N (Cairn is the ontology lane) |
| R11 | Vector / similarity search | P (pgvector) | N | P | N | N | **C** (auto-embed) | N |
| R12 | Graph traversal | P | N | N | N | N | **C** (Cypher/Gremlin/SPARQL) | N |
| R13 | Change feed / subscription | G (logical decoding) | P | P | P | **G** (filtered pub/sub) | **C** (`/changes?since_lsn=`) | **G** (change cursor + outbox) |

## 4. Meaning of a mutation (the semantic core)

| # | Invariant | Postgres | SQL | SQLite | redb | native_db | RedDB | logical-db |
|---|---|---|---|---|---|---|---|---|
| M1 | Every mutation has a durable identity | Y (app table) | Y | Y | Y | Y | P (LSN) | **G** (`OperationId`) |
| M2 | Every mutation has a request identity for retry | Y | Y | Y | Y | Y | N | **G** (request digest) |
| M3 | Why is this value X? is answerable (provenance) | Y | Y | Y | N | N | P (append-only tables, `ASK` is LLM) | **G** (receipt + causal frontier) |
| M4 | Deletes converge and cannot resurrect | G (in one node) | G | G | G | G | C | **G** (first-class tombstone) |
| M5 | Concurrent independent edits merge without lost update | N (last writer / locks) | N | N | N (overwrite) | N (overwrite) | **P** (per-field Sum/Max/Last reducers) | **G** (CRDT: lists, text, counters, maps) |
| M6 | Concurrent conflicting edits have a **declared** policy | G (locks/app) | G | G | N | N | **G** (reducer names the policy) | **G** (CRDT + `conflicts` visibility) |
| M7 | A mutation is a single atomic unit across all views | G | G | G | G | G | C | **G** (snapshot+change+receipt+outbox+indexes+event) |
| M8 | Invalid states are unrepresentable at the API | P (constraints) | P | P | N | **G** (typed models) | P | **G** (typed Editor/Reader, no raw rows) |
| M9 | Constraints: NOT NULL / UNIQUE / CHECK / FK | G | G | G | N | P (unique keys) | **C** (tables) | P (typed keys + scope; **no FK graph**) |
| M10 | Schema evolution is explicit and fail-closed | G (DDL) | G | G | DIY | **G** (model migration) | **C** (migration = “git for data” commit) | **G** (`migrate_schema` + projection semantics binding) |

## 5. Multi-replica and sync (where most of these die)

| # | Invariant | Postgres | SQL | SQLite | redb | native_db | RedDB | logical-db |
|---|---|---|---|---|---|---|---|---|
| S1 | Two replicas converge from the same writes | P (streaming rep) | P | DIY | N | N | **N** (backup/PITR only; no multi-master documented) | **G** (CRDT sync) |
| S2 | A peer that falls behind retention gets an **explicit** recovery | P | P | DIY | N | N | N | **G** (checkpoint-expired → full recovery, never a partial prefix) |
| S3 | Unsent work is durable (outbox) | Y | Y | Y | Y | Y | P (queues) | **G** |
| S4 | Exactly-once **delivery** | N (at-least-once + idem) | N | N | N | N | P (queue semantics) | G (acks + receipts; delivery is at-least-once + idempotent install) |
| S5 | Multi-tenant isolation under concurrent use | G (RLS) | P | Y | N | P (separate files) | **C** (collections, not tenancy enforcement) | **G** (`AccountId`/`SpaceId` on every identity) |
| S6 | Cross-tenant negatives are enforceable in the engine | G (RLS) | P | N | N | N | N | **G** (scope bound in the facade) |
| S7 | Geo / multi-region correctness | P (replicas) | P | N | N | N | N | OPEN (ADR-0003, contracts landed) |
| S8 | Scale to >1M concurrent clients | G (with ops) | P | N | N | N | N | OPEN (A02 / issue #72) |

## 6. Lifecycle, retention, exit (the “leave Gary” row)

| # | Invariant | Postgres | SQL | SQLite | redb | native_db | RedDB | logical-db |
|---|---|---|---|---|---|---|---|---|
| L1 | Hot backup is consistent | G | G | G | P | **G** (hot snapshots) | **C** (backup/*) | G (export is a logical consistent dump) |
| L2 | Point-in-time recovery | **G** | G | P | N | N | **C** (PITR backends) | P (versioned export; Cloud PITR is #71) |
| L3 | Restore of corrupt backup **refuses** | P | P | P | P | P | C | **G** (AE3) |
| L4 | Export is a versioned logical format (not a file copy) | P (pg_dump) | P | P | DIY | P | **C** | **G** |
| L5 | Portable exit / vendor independence | G (SQL dump) | G | G | P (file) | P (file) | **P** (BSL 1.1 until 2030; commercial for managed) | G (logical export + archive contract) |
| L6 | Compaction/GC never drops live peer-needed data | G | G | G | P | P | C (TTL/hypertable) | **G** (peer-aware retention floor) |
| L7 | TTL / retention / expiry is first-class | P | P | P | N | P | **G** (timeseries retention, hypertable TTL) | **G** (cache TTL + export bounds) |
| L8 | Right-to-erasure vs immutable audit | P (app) | P | P | N | N | P (append-only vs delete) | P (tombstone + scoped delete; audit lanes are domain work) |
| L9 | Encryption at rest | P (TDE/PGP) | P | P | DIY | DIY | C | P (sovereign archive key identity; local file is host's job) |
| L10 | Tamper-evident audit log | P | P | P | N | N | **P** (append-only tables) | **G** (immutable change + receipts) |

## 7. Operability — the Gary problem

| # | Invariant | Postgres | SQL | SQLite | redb | native_db | RedDB | logical-db |
|---|---|---|---|---|---|---|---|---|
| O1 | An AI can be given **write** authority safely | N | N | N | N | N | **C** (MCP agent mode — authority model undocumented) | **P→G** (scoped grant + exact-request confirmation; D2 deferred but designed) |
| O2 | Every write leaves a receipt an auditor can read | Y | Y | Y | N | N | P | **G** |
| O3 | Deterministic replay of a sequence of mutations | P | P | P | N | N | C | **G** (ordered change log) |
| O4 | The system can explain its schema/invariants | P (DDL) | P | P | N | P (Rust types) | **C** (ASK) | **G** (typed model + projection semantics) |
| O5 | Bounded work per operation (no silent unbounded growth) | P | P | P | G | G | C (MAX_SIZE queues) | **G** (MAX_* bounds + typed refusal) |
| O6 | Byte-identical semantics across OS/arch | G | G | G | G | G | C | P (A07 partial) |
| O7 | Open / documented conformance matrix | G | G | G | G | P (API unstable) | **P** (their PSC matrix shows ⚠️/❌ gaps) | G (`docs/API_BOUNDARY.md`) |
| O8 | License safe for a regulated managed service | G | G | G | Apache/MIT | Apache/MIT | **N** (BSL 1.1 → AGPL 2030) | Apache/MIT-class (estate) |

---

## Systems you did not name (and when they win)

| System | Wins when | Dies on |
|---|---|---|
| **Automerge / Loro / Yjs** | Pure collaborative merge of one doc | No durability contract, no sync recovery, no receipts, no tenancy |
| **FoundationDB** | Ordered, transactional, multi-node substrate | Not an app-facing model; you still own schema, retries, provenance |
| **MongoDB / DynamoDB** | Elastic document/KV at scale | Multi-item ACID (Mongo now has it; Dynamo is limited), explainability, exit cost |
| **CockroachDB / TiDB / Yugabyte** | “Postgres that scales” | Same Gary problem: semantics are SQL-level, not provenance-level; ops weight |
| **Datomic / EventStoreDB** | Immutable history as the product | Query/index ergonomics; niche ops skills |
| **RocksDB / LMDB / sled** | Embedded engine primitives | Everything above the page |
| **DuckDB / ClickHouse** | Analytics over the column | Not a system of record |
| **Realm / PowerSync / ElectricSQL** | Sync UX on SQLite/Postgres | Custody + recovery policy is theirs |
| **CloudKit** | Apple-only sync | Custody, residency, exit |
| **Kafka / Redpanda** | Stream as the source of truth | Not a queryable state store; you rebuild the DB |

---

## Invariants you will make me add later (start here)

1. **Affordability of the invariant** — cost of F7/M1 (receipts) on the hot path. We must disclose the tax.
2. **Write amplification** — we rewrite a full snapshot per edit; Postgres/redb do not.
3. **Query expressiveness floor** — joins/aggregates (R8/R9) are real and we are **N**. PowerBI will need a sink.
4. **Operational skill availability** — can IPC hire for it? (Tesler: must not require a DBA cult.)
5. **Upgrade / downgrade compatibility** across versions (semver of the on-disk format).
6. **Partial availability** under disk full / quota / clock skew.
7. **Clock trust** — what is true when the wall clock jumps.
8. **Human undo** (not just CRDT merge) for a mistaken agent or analyst.
9. **Legal hold / WORM** for regulated books.
10. **Multi-writer multi-process** (C2) — nobody on this list except server databases.
11. **Cross-engine replication** (this ↔ Postgres) for the BI sink.
12. **Cost per explained mutation** — the metric that sells D1 to a CFO.

---

## Reading the table honestly

- **Postgres/SQL** win rows 8–13 and 21–28 (query power) and L2 (PITR). They lose F7, F8, M1–M3, M5, S1–S2 as *guarantees* — those are DIY or features bolted on (triggers, audit tables, logical decoding pipelines).
- **SQLite/redb/native_db** are excellent **one process, one writer** engines. native_db adds typed models + migrations. None of them own multi-replica meaning.
- **RedDB** is the aggressive consolidator: seven models, ASK/RAG, CRUD+queues. Its own contract matrix marks large parts ⚠️/❌ per transport; isolation levels and multi-master are undocumented; one writer via `flock`; license is BSL 1.1. Treat every **C** as “marketing until a conformance test exists.”
- **logical-db** is **N** on R8–R12 (joins, aggregates, FTS, vector, graph). That is on purpose: those are Cairn/LEM/domain work or a sink, not the substrate. We are **G** on the mutation-meaning block (M1–M7), tombstones, fail-closed recovery, and tenant scope. A02/A07/D2 remain OPEN.
