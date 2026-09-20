# Matched-trace evidence: backend capability profiles

> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15 S16
> S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
> `SOURCES.md`.

Date: 2026-09-20. Host: macOS arm64 (aarch64), 15 logical CPUs, 24 GiB RAM.
Toolchain: workspace pinned (`rust-toolchain.toml`), release profile.

## What this closes

`REWRITE_MANIFEST.md` blocked production release on: *"Complete the explicit
backend capability profiles and matched-trace evidence required by the recorded
Director ruling in `GOVERNANCE.md`."* That ruling (issue #8, 2026-09-16) has two
halves:

1. **Capability profiles.** Upper key/namespace/event contracts must be
   compatible, but backend guarantees must be explicit through
   `EmbeddedLocal` and `DistributedTransactional`, and consumers may rely only
   on the common subset unless they explicitly request the stronger profile.
2. **Matched trace.** Run the same 1M-operation trace through both profiles and
   report CPU-seconds, peak/idle RSS, bytes written, p50/p99 latency, cold
   start, crash recovery, and sleep/wake, comparing *common local semantics*
   separately from *distributed guarantees*.

Both halves are now implemented in code, not prose.

## Half 1: capability profiles (implemented)

`logical-db-model` owns one `capability` module:

- `BackendClass` — `EmbeddedLocal` | `DistributedTransactional`.
- `Capability` — one guarantee an application may rely on; a sum type, so an
  unsupported guarantee name cannot be constructed or decoded.
- `CapabilityProfile` — binds a class to a fixed guarantee set. Constructors are
  `embedded_local()` and `distributed_transactional()`; the set is not
  caller-supplied, so a profile cannot advertise a guarantee its backend lacks.
- `CapabilityProfile::require(&[Capability])` — fails closed and names **every**
  unmet guarantee.
- `common_guarantees(a, b)` — resolves the shared subset for a pair.

| Guarantee | EmbeddedLocal | DistributedTransactional |
|---|:--:|:--:|
| `durable_local_commit` | ✓ | — |
| `causal_convergence` | ✓ | — |
| `bounded_sync` | ✓ | — |
| `schema_migration` | ✓ | — |
| `export_restore` | ✓ | — |
| `distributed_serializable_transactions` | — | ✓ |
| `replicated_availability` | — | ✓ |
| `watch_versionstamps` | — | ✓ |
| `cluster_recovery` | — | ✓ |

The common subset between the two declared classes is **empty** by
construction, which is the ruling's point: they are compatible contracts over
different guarantees, not interchangeable backends.

Surfaced where a consumer actually meets it:

- `Database::capability_profile()` → `EmbeddedLocal`;
  `Database::require_guarantees(&[...])` refuses a distributed-only guarantee
  with a typed `DatabaseError::Model(ModelError::UnsupportedGuarantees { class,
  missing })`.
- `ProtocolService::capability_profile()` → `DistributedTransactional`;
  `ProtocolService::require_guarantees(&[...])` refuses an embedded-only
  guarantee with `ServiceError::Model(...)`.

Receipts (all green, workspace suite 331 passed / 0 failed):

- `logical-db-model` `capability` module: 5 unit tests (per-class guarantee
  sets, exactly-one-class partition, fail-closed `require`, empty common subset,
  versioned identity round-trip with unknown-version and unknown-class refusal).
- `logical-db` facade: `embedded_replica_publishes_its_profile_and_refuses_stronger_guarantees`,
  `common_subset_between_embedded_and_distributed_is_empty`.
- `logical-db-service`:
  `service_publishes_distributed_profile_and_refuses_embedded_only_guarantees`.

## Half 2: matched trace (measured)

Harness: `crates/logical-db-e2e/src/bin/matched_trace.rs`.

- One deterministic trace definition drives every profile: round-robin document
  selection over a fixed set and `ordinal % 4` record kind, so two profiles
  replay byte-identical operation sequences. Matched by construction.
- Measured latency is **per durable commit** (one bounded atomic transaction);
  `operations_per_commit` is reported beside it so a reader divides rather than
  assumes.
- The embedded arm runs the real public `Database` facade over redb with
  `Durability::Immediate`, and asserts semantics preserved under measurement:
  an exact retry is honoured without re-applying, an index query returns the
  materialized row, and a delete commits a tombstone that survives reopen.

### Measured — EmbeddedLocal (redb), 32 operations per commit

| Operations | p50 commit | p99 commit | Throughput | Durable bytes | Cold start | Recovery reopen |
|---:|---:|---:|---:|---:|---:|---:|
| 5,000 | 15.2 ms | 26.2 ms | 2,084 ops/s | 10.2 MB | 43.3 ms | 7.1 ms |
| 20,000 | 39.6 ms | 96.9 ms | 781 ops/s | 40.6 MB | 29.0 ms | 7.7 ms |
| 60,000 | 116.2 ms | 378.8 ms | 246 ops/s | 81.3 MB | 35.6 ms | 6.6 ms |

Raw JSON: `evidence/matched-trace/embedded-{5000,20000,60000}.json`.

### Measured — DistributedTransactional (FoundationDB)

**Not measured.** Not compiled into this run: the arm is feature-gated on `fdb`
and requires a staged FoundationDB 7.3.77 cluster plus externally prepared
Braid grants. `matched_trace --profile distributed` reports a typed
`status: "unavailable"` with the reason and exits nonzero; it never fabricates
numbers. Closing this arm is the remaining work in this receipt.

### Unmeasured on both arms

`cpu_seconds` and `peak_rss_bytes` are `null` with the reason recorded in the
payload: `std` exposes no portable process-CPU or resident-set API, and
admitting a platform crate for it is a separate `lgwks-deps` decision. Total
CPU-seconds and peak/idle RSS from the ruling therefore **remain unmeasured**,
not estimated. Idle RSS and idle CPU also remain unmeasured.

Sleep/wake is unmeasured. Crash recovery is partially measured: the
`recovery_reopen_micros` column is a real reopen after handle drop with a
verified surviving tombstone, but it is not a process-kill or power-loss
restart; those live in the retained `crash_recovery`, `kill_recovery`, and
`fdb_process_kill_restore` journeys.

### Open finding: per-commit cost grows with store size

The three points are not a flat line. Throughput falls ~8.5× from 5k to 60k
operations while durable bytes grow ~8×, so p50 commit latency roughly scales
with store size at this batch size (15 ms → 39 ms → 116 ms). The commit path
itself is O(batch) — `commit_local_batch` validates the batch, then writes
snapshot, change, operation, outbox, revision, indexes, and event per member
under one `Durability::Immediate` transaction — so the growth is consistent with
fsync cost rising as the redb file grows, not with a per-commit scan.

This is reported as an **open performance finding**, not a claim: it is
un-attributed (fsync vs. index maintenance vs. page-cache effects are not
separated) and it is measured at one batch size on one host. Cold start and
recovery reopen stay flat (29–43 ms and 6.6–7.7 ms) across the range, so the
growth is in the write path, not in open or recovery.

## Reproduce

```sh
cargo build --release -p logical-db-e2e --bin matched_trace
./target/release/matched_trace --profile embedded --operations 60000 --documents 32 --batch 32 --runs 1
./target/release/matched_trace --profile distributed   # typed "unavailable" without a staged cluster
cargo test --workspace
```
