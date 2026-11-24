# ADR-0008: Save uncompressed, verify at load

## Status

Accepted 2026-11-24 (mined from #90).

## Context

`Document::save` runs on every durable edit. It called automerge
`save_and_verify`, which DEFLATEs every column over 256 bytes and reloads the
whole document: both O(history) per write. matched_trace (embedded, 5000
operations, one per commit, 8-core box) measured p50 1521 to 1623 us and
p99 11461 to 13892 us.

## Decision

- `save` writes `save_with_options` with `deflate: false` and no reload.
- Verification stays where untrusted bytes enter: `Document::load` on
  restore, remote install and export. Automerge's chunk checksum refuses any
  flipped byte (`sim_save_saboteur_any_flipped_byte_is_refused_at_load`).
- Rejected: keep `save_and_verify`. It verifies bytes this process just
  produced from its own in-memory document. Reverse if a load-side check
  ever passes a snapshot that `save_and_verify` would have refused.

## Consequences

- p50 831 us, p99 1740 us, 1130 ops/s (from 457 to 515); durable bytes and
  peak RSS unchanged on the trace.
- Snapshots of long text grow by the compression ratio; `MAX_SNAPSHOT_BYTES`
  still bounds them at save and load.
