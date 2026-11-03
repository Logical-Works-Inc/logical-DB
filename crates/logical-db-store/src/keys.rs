//! Storage keys and bounds; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use logical_db_model::{ChangeDigest, DocumentId, IndexId, OperationId, PeerId, Scope};
use redb::TableDefinition;

/// Versioned table holding every logical-store row for this file format.
pub(crate) const DATA: TableDefinition<&[u8], &[u8]> = TableDefinition::new("logical_data_v3");
/// Versioned magic prefix shared by every scoped storage key.
pub(crate) const FORMAT: &[u8] = b"LDB\x03";
/// Storage key that binds the file to one durable replica identity.
pub(crate) const REPLICA_KEY: &[u8] = b"LDB\x03replica";
/// Row kind for a materialized document snapshot.
pub(crate) const KIND_SNAPSHOT: u8 = 1;
/// Row kind for one immutable CRDT change.
pub(crate) const KIND_CHANGE: u8 = 2;
/// Row kind for one durable operation receipt.
pub(crate) const KIND_OPERATION: u8 = 3;
/// Row kind for one durable local outbox entry.
pub(crate) const KIND_OUTBOX: u8 = 4;
/// Row kind for the scope's highest committed outbox sequence.
pub(crate) const KIND_SEQUENCE: u8 = 5;
/// Row kind for one document's optimistic-concurrency revision.
pub(crate) const KIND_REVISION: u8 = 6;
/// Row kind for one peer's opaque synchronization checkpoint.
pub(crate) const KIND_CHECKPOINT: u8 = 7;
/// Row kind for the scope's global outbox acknowledgement cursor.
pub(crate) const KIND_OUTBOX_ACK: u8 = 8;
/// Row kind for one forward secondary-index entry.
pub(crate) const KIND_INDEX: u8 = 9;
/// Row kind for one reverse secondary-index entry.
pub(crate) const KIND_INDEX_REVERSE: u8 = 10;
/// Row kind for one scope's bound application schema.
pub(crate) const KIND_SCHEMA: u8 = 11;
/// Row kind for the scope's highest materialized-event sequence.
pub(crate) const KIND_EVENT_SEQUENCE: u8 = 12;
/// Row kind for one durable materialized-state invalidation.
pub(crate) const KIND_EVENT: u8 = 13;
/// Row kind for one in-progress schema migration marker.
pub(crate) const KIND_MIGRATION: u8 = 14;
/// Row kind for the scope's durable upstream binding.
pub(crate) const KIND_UPSTREAM: u8 = 15;
/// Row kind for one document's monotonic tombstone flag.
pub(crate) const KIND_TOMBSTONE: u8 = 16;
/// Row kind for one durable restore cursor.
pub(crate) const KIND_RESTORE: u8 = 17;
/// Row kind for the scope's durable baseline generation.
pub(crate) const KIND_BASELINE: u8 = 18;
/// Row kind for one durable baseline invalidation.
pub(crate) const KIND_BASELINE_EVENT: u8 = 19;
/// Magic prefix identifying a versioned logical export payload.
pub(crate) const EXPORT_MAGIC: [u8; 5] = [b'L', b'D', b'B', b'E', 0x01];
/// Version of the logical export payload defined in this module.
pub const EXPORT_VERSION: u16 = 1;
/// Maximum total bytes of one versioned logical export payload.
pub const MAX_EXPORT_BYTES: usize = 67_108_864;
/// Fixed export header: magic, account UUID, and space UUID.
pub(crate) const EXPORT_HEADER_BYTES: usize = 5 + 16 + 16;
/// Minimum export entry wire size: document, operation, request, digest, length.
pub(crate) const EXPORT_ENTRY_PREFIX_BYTES: usize = 16 + 16 + 32 + 32 + 4;
/// Fixed export document prefix: document UUID, tombstone flag, length.
pub(crate) const EXPORT_DOCUMENT_PREFIX_BYTES: usize = 16 + 1 + 4;
/// Durable restore marker: export payload identity plus next-entry cursor.
pub(crate) const RESTORE_MARKER_BYTES: usize = 32 + 8;
/// Maximum aggregate snapshot state retained while installing or migrating one page.
pub const MAX_MATERIALIZED_PAGE_BYTES: usize = 67_108_864;
/// Length of the shared scope prefix: format magic plus account and space UUIDs.
pub(crate) const SCOPE_PREFIX_BYTES: usize = FORMAT.len() + 16 + 16;
/// Length of one kind-prefixed sequence key: scope prefix, kind tag, and `u64`.
pub(crate) const SEQUENCE_KEY_BYTES: usize = SCOPE_PREFIX_BYTES + 1 + 8;
/// Maximum accepted size of one opaque peer checkpoint.
pub(crate) const MAX_CHECKPOINT_BYTES: usize = 4_096;
/// Maximum number of changes accepted in one remote install page.
pub(crate) const MAX_REMOTE_CHANGES: usize = 256;
/// Maximum aggregate change bytes accepted in one remote install page.
pub(crate) const MAX_REMOTE_CHANGE_BYTES: usize = 8_388_608;
/// Encoded size of one durable operation receipt.
pub(crate) const OPERATION_VALUE_BYTES: usize = 88;
/// Encoded size of one durable outbox entry.
pub(crate) const OUTBOX_VALUE_BYTES: usize = 64;
/// Maximum projectable indexes per document.
pub(crate) const MAX_INDEXES_PER_DOCUMENT: usize = 64;
/// Maximum encoded size of one index value key component.
pub(crate) const MAX_INDEX_VALUE_BYTES: usize = 4_096;
/// Encoded size of one bound application schema value.
pub(crate) const SCHEMA_VALUE_BYTES: usize = 36;
/// Encoded size of one schema migration marker.
pub(crate) const MIGRATION_VALUE_BYTES: usize = 53;
/// Encoded size of one materialized-state event value.
pub(crate) const EVENT_VALUE_BYTES: usize = 48;
/// Encoded size of one baseline event value.
pub(crate) const BASELINE_VALUE_BYTES: usize = 8;

/// Builds the shared format-and-scope prefix for every storage key.
pub(crate) fn scope_prefix(scope: Scope) -> Vec<u8> {
    let mut key = Vec::with_capacity(SCOPE_PREFIX_BYTES);
    key.extend_from_slice(FORMAT);
    key.extend_from_slice(scope.account().as_uuid().as_bytes());
    key.extend_from_slice(scope.space().as_uuid().as_bytes());
    key
}

/// Builds one kind-scoped storage key prefix.
pub(crate) fn prefix(scope: Scope, kind: u8) -> Vec<u8> {
    let mut key = scope_prefix(scope);
    key.push(kind);
    key
}

/// Builds the snapshot row key for one document.
pub(crate) fn key_snapshot(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_SNAPSHOT);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the immutable-change row key for one document and digest.
pub(crate) fn key_change(scope: Scope, document: DocumentId, digest: ChangeDigest) -> Vec<u8> {
    let mut key = prefix(scope, KIND_CHANGE);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key.extend_from_slice(&digest.into_bytes());
    key
}

/// Builds the operation-receipt row key for one operation.
pub(crate) fn key_operation(scope: Scope, operation: OperationId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_OPERATION);
    key.extend_from_slice(operation.as_uuid().as_bytes());
    key
}

/// Builds the outbox row key for one sequence.
pub(crate) fn key_outbox(scope: Scope, sequence: u64) -> Vec<u8> {
    let mut key = prefix(scope, KIND_OUTBOX);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// Builds the highest-sequence row key for one scope.
pub(crate) fn key_sequence(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_SEQUENCE)
}

/// Builds the revision row key for one document.
pub(crate) fn key_revision(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_REVISION);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the checkpoint row key for one peer.
pub(crate) fn key_checkpoint(scope: Scope, peer: PeerId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_CHECKPOINT);
    key.extend_from_slice(peer.as_uuid().as_bytes());
    key
}

/// Builds the global outbox acknowledgement row key for one scope.
pub(crate) fn key_outbox_ack(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_OUTBOX_ACK)
}

/// Builds the upstream-binding row key for one scope.
pub(crate) fn key_upstream(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_UPSTREAM)
}

/// Builds the bound-schema row key for one scope.
pub(crate) fn key_schema(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_SCHEMA)
}

/// Builds the highest-event-sequence row key for one scope.
pub(crate) fn key_event_sequence(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_EVENT_SEQUENCE)
}

/// Builds the materialized-event row key for one sequence.
pub(crate) fn key_event(scope: Scope, sequence: u64) -> Vec<u8> {
    let mut key = prefix(scope, KIND_EVENT);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// Builds the baseline-generation row key for one scope.
pub(crate) fn key_baseline(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_BASELINE)
}

/// Builds the baseline-event row key for one sequence.
pub(crate) fn key_baseline_event(scope: Scope, sequence: u64) -> Vec<u8> {
    let mut key = prefix(scope, KIND_BASELINE_EVENT);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// Builds the migration-marker row key for one scope.
pub(crate) fn key_migration(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_MIGRATION)
}

/// Builds the tombstone-flag row key for one document.
pub(crate) fn key_tombstone(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_TOMBSTONE);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the restore-cursor row key for one scope.
pub(crate) fn key_restore(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_RESTORE)
}

/// Builds the forward index-row prefix for one encoded value.
pub(crate) fn index_row_prefix(scope: Scope, index: IndexId, encoded_value: &[u8]) -> Vec<u8> {
    let mut key = prefix(scope, KIND_INDEX);
    key.extend_from_slice(index.as_uuid().as_bytes());
    key.extend_from_slice(encoded_value);
    key
}

/// Builds the forward index-row key for one document.
pub(crate) fn key_index_row(
    scope: Scope,
    index: IndexId,
    encoded_value: &[u8],
    document: DocumentId,
) -> Vec<u8> {
    let mut key = index_row_prefix(scope, index, encoded_value);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the reverse index-row prefix for one document.
pub(crate) fn index_reverse_prefix(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_INDEX_REVERSE);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the reverse index-row key for one index.
pub(crate) fn key_index_reverse(scope: Scope, document: DocumentId, index: IndexId) -> Vec<u8> {
    let mut key = index_reverse_prefix(scope, document);
    key.extend_from_slice(index.as_uuid().as_bytes());
    key
}

/// Returns the exclusive upper bound of one key prefix.
pub(crate) fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    while let Some(last) = next.pop() {
        if last != u8::MAX {
            next.push(last.saturating_add(1));
            return Some(next);
        }
    }
    None
}

/// Reports whether a length is nonzero and at most a maximum.
pub(crate) fn is_nonempty_bounded(length: usize, maximum: usize) -> bool {
    (1..=maximum).contains(&length)
}

/// Reports whether a length is at most a maximum.
pub(crate) fn is_at_most(length: usize, maximum: usize) -> bool {
    length <= maximum
}

/// Adds one snapshot size to the page budget, or signals overflow.
pub(crate) fn add_materialized_bytes(current: usize, additional: usize) -> Option<usize> {
    current
        .checked_add(additional)
        .filter(|total| *total <= MAX_MATERIALIZED_PAGE_BYTES)
}
