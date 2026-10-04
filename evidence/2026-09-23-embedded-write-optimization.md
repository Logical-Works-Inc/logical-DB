> Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15 S16
> S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
> `SOURCES.md`.

# Embedded write path optimization

## Change

The embedded edit path now retains a scope-owned CRDT state cache with limits
of 64 documents, 1 MiB per document, and 16 MiB aggregate encoded state. Cache
entries are checked against the durable revision before use. Successful edits
append immutable Automerge change rows to redb; a full checkpoint is written
after 31 suffix changes or before the bounded snapshot byte ceiling is reached.
Checkpoint, suffix pointers, revision, receipt, outbox, indexes, and event remain
one transaction. No new crate was added: Automerge and redb remain owned by the
existing `lgwks_deps` dependency boundary.

## Matched trace

Host: macOS arm64. Release profile. Embedded public `Database` facade. 32
operations per durable commit. Baseline was measured earlier in this session
from the pre-change tree; optimized traces below were measured after the change.

| Metric | Baseline, 20k ops / 32 docs | Optimized, 20k ops / 32 docs | Optimized, 60k ops / 64 docs |
|---|---:|---:|---:|
| Runs | 3 | 3 | 3 |
| Commit samples | 625 | 625 | 1,875 |
| p50 | 21,880 µs | 5,966 µs | 6,858 µs |
| p95 | unavailable in baseline output | 7,739 µs | 8,311 µs |
| p99 | 41,985 µs | 9,966 µs | 10,344 µs |
| Throughput | 1,463 ops/s | 5,293 ops/s | 4,730 ops/s |
| Durable bytes | 40,636,416 | 30,674,944 | 84,938,752 |

At the matched 20k workload, p50 fell 72.7%, p99 fell 76.3%, and throughput
rose 3.62x. The 60k trace retained similar per-commit latency at three times
the operation count and twice as many documents. Correctness probes all
matched: committed operations, exact duplicate retry, index lookup, tombstone,
and reopen. Raw optimized records are `matched-trace/embedded-20000-optimized.json`
and `matched-trace/embedded-60000-optimized.json`.

### Throughput sweep and cache saturation

To select a throughput configuration, the same 60k operations over 256
documents were measured at batches 32, 64, 128, and 256 (three runs per point).
With the original 64-entry cache, the 32-operation batch reached 2,279 ops/s.
After raising the entry cap to 256 while preserving the 16 MiB aggregate byte
limit, that exact workload reached 5,172 ops/s. This confirms the larger
working set had been forcing evictions and replay. All correctness probes passed
at every batch size.

| Batch | p50 commit | p95 commit | p99 commit | Samples | Throughput |
|---:|---:|---:|---:|---:|---:|
| 32 | 6,005 µs | 7,096 µs | 8,115 µs | 1,875 | 5,172 ops/s |
| 64 | 7,983 µs | 9,723 µs | 11,699 µs | 938 | 7,866 ops/s |
| 128 | 11,679 µs | 14,597 µs | 15,989 µs | 469 | 11,104 ops/s |
| 256 | 17,115 µs | 21,866 µs | 27,346 µs | 235 | 14,743 ops/s |

The 256 batch is the current throughput leader and reaches 14,743 ops/s across
three runs, close to the 15k lower target in the supplied chart. A separate
single run measured 15,017 ops/s; it is not substituted for the more stable
three-run result. The higher batch trades increased commit tail latency for
throughput. Raw records are `matched-trace/embedded-60000-d256-b32-before-cache-cap.json`,
`matched-trace/embedded-60000-d256-b32.json`, and the matching `b64`, `b128`,
and `b256` files.

The capacity regression test failed before the limit change with exit 101:
`state_cache_enforces_entry_byte_and_revision_bounds` expected 256 retained
entries and observed 64. It passes after the cache limit change. The same
workload comparison demonstrates the capacity change's throughput effect;
batch 256 demonstrates the throughput/commit-tail tradeoff.

Research line: “Adopting the existing scope-owned LRU and encoded-byte ceiling
to retain up to 256 document states because the 256-document trace exceeded the
64-entry cap; rejecting a separate `lru` crate because it adds another eviction
policy without improving the aggregate byte bound.” The matched same-workload
comparison was 2,279 ops/s at 64 entries versus 5,172 ops/s at 256 entries.

## Memory and process check

`/usr/bin/time -l` around the 60k x 3 run measured 117,161,984 bytes maximum
resident set (111.7 MiB), 112,132,696 bytes peak memory footprint, and zero
swaps. The cache's entry and encoded-byte ceilings are exercised by
`state_cache_enforces_entry_byte_and_revision_bounds`. The measured high-water
RSS does not by itself prove absence of a memory leak; idle and per-run RSS
were not measured, so leak status remains unknown. The immediate post-run
process listing contained no `matched_trace` or `logical-db` process.
At the 60k / 256-document / 256-operation batch point, a separately timed run
measured 127,926,272 bytes maximum RSS, 121,340,504 bytes peak memory footprint,
and zero swaps. The larger cache uses the same aggregate encoded-size ceiling.

## Test-first receipt and gates

The regression test first failed on the old path with exit 101 because ordinary
edits rewrote the checkpoint instead of appending changes. After the change,
`cargo test --release --locked -p logical-db local_edits_append_bounded_suffix_and_reopen_to_latest_value`
passed. `cargo test --release --locked -p logical-db -p logical-db-store`
passed (69 facade tests, 34 store tests, one doctest). Touched-crate Clippy
and workspace formatting passed:

- `cargo clippy --locked -p logical-db -p logical-db-store --all-targets -- -D warnings` — exit 0.
- `cargo fmt --all -- --check` — exit 0.
- `cargo test --release --locked -p logical-db-e2e --test embeddable_acceptance` — exit 101, 26 passed / 3 failed. The failures are empty-batch validation, whitespace-key validation, and a recovery fixture that never publishes its late writes and has no baseline artifact.

The final repository gate `./scripts/ci-local.sh` exited 1. Lockfile, workspace
compile, workspace Clippy, formatting, FDB store check, access-graph,
suppressions, and artifact checks passed. Workspace tests fail on three cases in
the pre-existing untracked `crates/logical-db-e2e/tests/embeddable_acceptance.rs`;
26 of its 29 cases pass. FDB daemon check/lint fail on existing daemon tests
accessing private protocol fields, `cargo deny` exits 2, domain-contract exits
1, and rewrite evidence exits 1. The verifier reports missing citations in
existing requirements/evidence files, missing `logical-db-kv`, and a stale
domain graph. CI routing was skipped because PyYAML is unavailable. These gates
are not presented as green.

GitHub already has PR #90 open for this branch. Its latest hosted run fails the
clean-room evidence job before the other jobs start. This local work has not
been pushed because the repo contract prohibits pushing while that verifier is
red.

## Research decision

Adopting Automerge's immutable change representation with Logical-DB's existing
append-only changes and redb atomic transaction because it avoids replaying or
rewriting the full growing snapshot on every edit. Rejected per-edit full
snapshot saves because the matched baseline showed commit cost rising with
retained store size. The explicit invariant IDs are INV-EMBED-001,
INV-EMBED-002, INV-EMBED-003, and INV-CRDT-001.
