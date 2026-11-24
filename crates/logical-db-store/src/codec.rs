//! Value codecs shared by the store modules; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::error::StoreError;
use crate::keys::{
    BASELINE_VALUE_BYTES, EVENT_VALUE_BYTES, FORMAT, KIND_OUTBOX, MIGRATION_VALUE_BYTES,
    OPERATION_VALUE_BYTES, OUTBOX_VALUE_BYTES, SCHEMA_VALUE_BYTES, SEQUENCE_KEY_BYTES,
};
use crate::types::{ChangeEvent, CommittedOperation, LocalChange, OutboxEntry};
use lgwks_std::id::Uuid;
use logical_db_model::{ChangeDigest, DocumentId, DocumentRevision, OperationDigest, OperationId};
use redb::ReadableTable;

/// Splits one fixed-width array off the front of `bytes`, advancing it.
///
/// # Errors
///
/// Returns [`StoreError::CorruptInternalValue`] when fewer than `N` bytes remain.
pub(crate) fn take<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], StoreError> {
    let Some((head, rest)) = bytes.split_first_chunk::<N>() else {
        return Err(StoreError::CorruptInternalValue);
    };
    *bytes = rest;
    Ok(*head)
}

/// Reads one identifier off the front of `bytes`, advancing it.
pub(crate) fn take_uuid(bytes: &mut &[u8]) -> Result<Uuid, StoreError> {
    Ok(Uuid::from(take::<16>(bytes)?))
}

/// Fills a fixed durable value from its concatenated parts.
pub(crate) fn fill<const N: usize>(parts: impl IntoIterator<Item = u8>) -> [u8; N] {
    let mut value = [0; N];
    for (slot, byte) in value.iter_mut().zip(parts) {
        *slot = byte;
    }
    value
}

/// Encodes one schema version and projection digest into its durable value.
pub(crate) fn schema_value(version: u32, digest: [u8; 32]) -> [u8; SCHEMA_VALUE_BYTES] {
    fill(version.to_be_bytes().into_iter().chain(digest))
}

/// Encodes one migration marker from its target and optional cursor.
pub(crate) fn encode_migration(
    target: [u8; SCHEMA_VALUE_BYTES],
    after: Option<DocumentId>,
) -> [u8; MIGRATION_VALUE_BYTES] {
    let (flag, cursor) = match after {
        Some(document) => (1_u8, *document.as_uuid().as_bytes()),
        None => (0_u8, [0_u8; 16]),
    };
    fill(std::iter::once(flag).chain(target).chain(cursor))
}

/// Decodes and validates one migration marker.
pub(crate) fn decode_migration(
    bytes: &[u8],
) -> Result<([u8; SCHEMA_VALUE_BYTES], Option<DocumentId>), StoreError> {
    if bytes.len() != MIGRATION_VALUE_BYTES {
        return Err(StoreError::CorruptInternalValue);
    }
    let mut rest = bytes;
    let [flag] = take::<1>(&mut rest)?;
    let target = take::<SCHEMA_VALUE_BYTES>(&mut rest)?;
    let cursor = take::<16>(&mut rest)?;
    match flag {
        0 if cursor.iter().all(|byte| *byte == 0) => Ok((target, None)),
        1 => Ok((target, Some(DocumentId::from_uuid(Uuid::from(cursor))))),
        _ => Err(StoreError::CorruptInternalValue),
    }
}

/// Encodes one durable operation receipt.
pub(crate) fn encode_operation(
    document: DocumentId,
    request: OperationDigest,
    digest: ChangeDigest,
    sequence: u64,
) -> [u8; OPERATION_VALUE_BYTES] {
    fill(
        (*document.as_uuid().as_bytes())
            .into_iter()
            .chain(request.into_bytes())
            .chain(digest.into_bytes())
            .chain(sequence.to_be_bytes()),
    )
}

/// Decodes and validates one durable operation receipt.
pub(crate) fn decode_operation(bytes: &[u8]) -> Result<CommittedOperation, StoreError> {
    if bytes.len() != OPERATION_VALUE_BYTES {
        return Err(StoreError::CorruptInternalValue);
    }
    let mut rest = bytes;
    Ok(CommittedOperation {
        document: DocumentId::from_uuid(take_uuid(&mut rest)?),
        request: OperationDigest::new(take::<32>(&mut rest)?),
        digest: ChangeDigest::new(take::<32>(&mut rest)?),
        sequence: u64::from_be_bytes(take::<8>(&mut rest)?),
    })
}

/// Encodes one durable outbox entry.
pub(crate) fn encode_outbox(change: LocalChange<'_>) -> [u8; OUTBOX_VALUE_BYTES] {
    fill(
        (*change.document.as_uuid().as_bytes())
            .into_iter()
            .chain(*change.operation.as_uuid().as_bytes())
            .chain(change.digest.into_bytes()),
    )
}

/// Decodes and validates one durable outbox entry.
pub(crate) fn decode_outbox(sequence: u64, bytes: &[u8]) -> Result<OutboxEntry, StoreError> {
    if bytes.len() != OUTBOX_VALUE_BYTES {
        return Err(StoreError::CorruptInternalValue);
    }
    let mut rest = bytes;
    Ok(OutboxEntry {
        sequence,
        document: DocumentId::from_uuid(take_uuid(&mut rest)?),
        operation: OperationId::from_uuid(take_uuid(&mut rest)?),
        digest: ChangeDigest::new(take::<32>(&mut rest)?),
    })
}

/// Encodes one materialized-state event value.
pub(crate) fn encode_event(document: DocumentId, digest: ChangeDigest) -> [u8; EVENT_VALUE_BYTES] {
    fill(
        (*document.as_uuid().as_bytes())
            .into_iter()
            .chain(digest.into_bytes()),
    )
}

/// Decodes and validates one materialized-state event value.
pub(crate) fn decode_event(sequence: u64, bytes: &[u8]) -> Result<ChangeEvent, StoreError> {
    if bytes.len() != EVENT_VALUE_BYTES {
        return Err(StoreError::CorruptInternalValue);
    }
    let mut rest = bytes;
    Ok(ChangeEvent::Document {
        sequence,
        document: DocumentId::from_uuid(take_uuid(&mut rest)?),
        digest: ChangeDigest::new(take::<32>(&mut rest)?),
    })
}

/// Decodes and validates one baseline event value.
pub(crate) fn decode_baseline_event(
    sequence: u64,
    bytes: &[u8],
) -> Result<ChangeEvent, StoreError> {
    if bytes.len() != BASELINE_VALUE_BYTES {
        return Err(StoreError::CorruptInternalValue);
    }
    Ok(ChangeEvent::Baseline {
        sequence,
        generation: decode_u64(bytes)?,
    })
}

/// Collects one bounded outbox page from an ordered key range.
///
/// The outbox page and the history page scan the same durable outbox rows
/// under different cursors, so the bounded range fold lives here instead
/// of once per reader.
pub(crate) fn collect_outbox_page(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Result<Vec<OutboxEntry>, StoreError> {
    table
        .range(start..end)?
        .take(limit)
        .map(|entry| {
            let (key, value) = entry?;
            let sequence = decode_sequence_key(key.value())?;
            decode_outbox(sequence, value.value())
        })
        .collect()
}

/// Decodes one outbox sequence key.
pub(crate) fn decode_sequence_key(bytes: &[u8]) -> Result<u64, StoreError> {
    decode_sequence_key_for_kind(bytes, KIND_OUTBOX)
}

/// Decodes a sequence key after checking its kind tag.
pub(crate) fn decode_sequence_key_for_kind(bytes: &[u8], kind: u8) -> Result<u64, StoreError> {
    if bytes.len() != SEQUENCE_KEY_BYTES {
        return Err(StoreError::CorruptInternalValue);
    }
    let mut rest = bytes;
    let format = take::<{ FORMAT.len() }>(&mut rest)?;
    // The scope's account and space identifiers sit between magic and kind.
    take::<32>(&mut rest)?;
    let [tag] = take::<1>(&mut rest)?;
    if format != *FORMAT || tag != kind {
        return Err(StoreError::CorruptInternalValue);
    }
    decode_u64(rest)
}

/// Decodes one big-endian `u64`.
pub(crate) fn decode_u64(bytes: &[u8]) -> Result<u64, StoreError> {
    Ok(u64::from_be_bytes(<[u8; 8]>::try_from(bytes)?))
}

/// Decodes one document revision from its durable bytes.
pub(crate) fn decode_revision(bytes: &[u8]) -> Result<DocumentRevision, StoreError> {
    decode_u64(bytes).map(DocumentRevision::new)
}

/// Decodes one identifier from exactly its sixteen durable bytes.
///
/// # Errors
///
/// Returns [`StoreError::CorruptIdentifier`] when the width is not sixteen.
pub(crate) fn decode_uuid(bytes: &[u8]) -> Result<Uuid, StoreError> {
    Ok(Uuid::from(<[u8; 16]>::try_from(bytes)?))
}

/// Decodes the identifier that follows a key prefix of `prefix_len` bytes.
///
/// # Errors
///
/// Returns [`StoreError::CorruptInternalValue`] when the key is shorter than
/// the prefix, or [`StoreError::CorruptIdentifier`] when the suffix is not
/// exactly sixteen bytes.
pub(crate) fn decode_uuid_after(key: &[u8], prefix_len: usize) -> Result<Uuid, StoreError> {
    let suffix = key
        .get(prefix_len..)
        .ok_or(StoreError::CorruptInternalValue)?;
    decode_uuid(suffix)
}
