# Format & benchmark contract — v0.1

What is byte-stable in v0.1, what is not yet stable, and the
machine-readable shape of a benchmark run (issue #6 §9–§10). Every
layout fact here is enforced by tests in `kernel/src/model.rs`,
`kernel/src/journal.rs`, and `kernel/tests/planted_corruption.rs` —
this document describes code that exists, nothing aspirational.

## 1. Journal file format v1 (stable)

One append-only file per store. A batch is one frame:

    [payload_len u32 LE][crc32 u32 LE][payload]

- CRC32 IEEE (reflected, poly 0xEDB88320) over the payload bytes.
- INV-BATCH-IS-ONE-FRAME: replay applies a frame whole or not at all.
- Replay stops at the last intact frame; a torn tail is truncated to
  the previous frame boundary.
- A frame that is CRC-valid but fails row decode is a hard error,
  never silently skipped.

Frame payload is a sequence of rows (v1):

    [class_tag u8][op_tag u8][id_len u32 LE][id][ts u64 LE]
    [payload_len u32 LE][payload]

- class_tag: Record=1, Term=2, Edge=3, VecCode=4, Claim=5; tag 0 and
  anything above 5 are rejected at decode — unknown tags are
  unconstructable.
- op_tag: Put=1, Delete=2. Delete encodes payload_len=0.

## 2. KV key layout v1 (stable)

    point key = [class_tag u8][escaped(id)][0x00 0x00][ts u64 BE]

- A literal 0x00 inside an id escapes as `0x00 0xFF`, so the two-byte
  terminator is unambiguous (INV-ID-PREFIX-DISJOINT): no id can be a
  byte-prefix extension of another id's scan range.
- Big-endian ts keeps one entity's versions sorted in commit order.
- Secondary-index ids are composites:
  `[namespace u8][len u16 BE][component]...` — length-prefixed, no
  reserved byte values; arbitrary bytes round-trip.

## 3. Not yet stable in v0.1

- Two persistence paths ship, one crash story each (issue #6 §5):
  production bytes go through redb only (`RedbBackend` behind the
  `DurableKv` seam — its file format is redb's own, not specified
  here); the §1 journal frames serve the MemKv reference path and act
  as the fault oracle for replay/corruption behavior. Never write one
  store's data through both.
- Segment packing, mmap-first reads, and hydration are design intent
  (DB_PATHWAY §3), not shipped format.
- No migration tooling exists yet. Any future change to §1/§2 ships a
  version marker plus an upgrader or a documented dead-end.

## 4. Benchmark results format

`cargo bench -p logical-db` runs `kernel/benches/ship_bench.rs` — a
plain binary, no criterion dependency. Benches run the journaled MemKv
reference path (the deterministic baseline); the redb backend carries
the same semantic suite, not these numbers. It prints one JSON object
per line on stdout:

    {"workload":"<name>","samples":N,"ops":N,
     "throughput_ops_per_s":F,"p50_ns":N,"p95_ns":N,"db_bytes":N}

Two auxiliary lines carry their own fields:
`adjacency_cost_per_row_ns` (`node`, `degree`, `value`) and
`reopen_replay` (`rows_restored`, `p50_ns`, `db_bytes`). Peak RSS is
not measured in-process (stdlib-only harness); measure externally with
`/usr/bin/time -l cargo bench -p logical-db` and read `maxrss`.

### Workloads (issue #6 §9 minimum set)

| Line                            | Measures                                  |
| ------------------------------- | ----------------------------------------- |
| record_insert_1m                | 10^6 record puts, batches of 1000         |
| record_random_latest_reads      | latest() over random populated ids        |
| history_as_of_reads_100_versions| get() as-of across 100 versions/key       |
| adjacency_scan_low_degree_999   | outgoing() over a degree-999 node         |
| adjacency_scan_hub_200k         | outgoing() over a degree-200k hub         |
| adjacency_incoming_degree_800   | incoming() reverse scan                   |
| term_ingest_docs                | 5k-doc BM25 corpus ingest                 |
| term_query_1/3/8_terms          | search() vs query-term count              |
| veccode_brute_scan_5k_x32b_top10| binary-code scan + top-K                  |
| mixed_txn_4_classes             | batches touching four classes             |

### Algorithmic gates (assertions, not vibes)

These hold by construction and are asserted inside the harness; a run
that violates them aborts instead of publishing a number:

- Adjacency scans cost flatly per row: hub per-row cost stays within
  the low-degree per-row cost observed 2026-08-25 (warm-cache effect),
  never orders of magnitude above it.
- Scan row counts are pinned exactly to degree counts, so a key-layout
  regression fails the run rather than producing a number.
- Term query cost must scale with query postings, not corpus size;
  the 1/3/8-term lines exist to expose that slope.
- Scope note: the exact fp32 vector baseline does not exist yet (the
  port ships the quantized path); only the binary-code line is real.
