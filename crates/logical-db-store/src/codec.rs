//! Value codecs shared by the store modules; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::error::StoreError;
use crate::keys::{
    BASELINE_VALUE_BYTES, EVENT_VALUE_BYTES, FORMAT, KIND_OUTBOX, MIGRATION_VALUE_BYTES,
    OPERATION_VALUE_BYTES, OUTBOX_VALUE_BYTES, SCHEMA_VALUE_BYTES, SCOPE_PREFIX_BYTES,
    SEQUENCE_KEY_BYTES,
};
use crate::types::{ChangeEvent, CommittedOperation, LocalChange, OutboxEntry};
use logical_db_model::{ChangeDigest, DocumentId, DocumentRevision, OperationDigest, OperationId};
use redb::ReadableTable;

/// Normalizes any durable-value decode failure into [`StoreError::CorruptInternalValue`].
///
/// The concrete decoder error is intentionally discarded: the stable store
/// error is the only information callers may act on, and it fails closed.
pub(crate) fn malformed_store<E: std::fmt::Debug>(cause: E) -> StoreError {
    drop(cause);
    StoreError::CorruptInternalValue
}

/// Encodes one schema version and projection digest into its durable value.
pub(crate) fn schema_value(version: u32, digest: [u8; 32]) -> [u8; SCHEMA_VALUE_BYTES] {
    let mut value = [0; SCHEMA_VALUE_BYTES];
    value[..4].copy_from_slice(&version.to_be_bytes());
    value[4..].copy_from_slice(&digest);
    value
}

/// Encodes one migration marker from its target and optional cursor.
pub(crate) fn encode_migration(
    target: [u8; SCHEMA_VALUE_BYTES],
    after: Option<DocumentId>,
) -> [u8; MIGRATION_VALUE_BYTES] {
    let mut value = [0; MIGRATION_VALUE_BYTES];
    value[1..=SCHEMA_VALUE_BYTES].copy_from_slice(&target);
    if let Some(document) = after {
        value[0] = 1;
        value[1 + SCHEMA_VALUE_BYTES..].copy_from_slice(document.as_uuid().as_bytes());
    }
    value
}

/// Decodes and validates one migration marker.
pub(crate) fn decode_migration(
    bytes: &[u8],
) -> Result<([u8; SCHEMA_VALUE_BYTES], Option<DocumentId>), StoreError> {
    if bytes.len() != MIGRATION_VALUE_BYTES || bytes[0] > 1 {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let target = bytes[1..=SCHEMA_VALUE_BYTES]
        .try_into()
        .map_err(malformed_store)?;
    let cursor = match bytes[0] {
        0 if bytes[1 + SCHEMA_VALUE_BYTES..]
            .iter()
            .all(|byte| *byte == 0) =>
        {
            None
        }
        1 => Some(decode_uuid(&bytes[1 + SCHEMA_VALUE_BYTES..]).map(DocumentId::from_uuid)?),
        _ => {
            let error = StoreError::CorruptInternalValue;
            return Err(error);
        }
    };
    Ok((target, cursor))
}

/// Encodes one durable operation receipt.
pub(crate) fn encode_operation(
    document: DocumentId,
    request: OperationDigest,
    digest: ChangeDigest,
    sequence: u64,
) -> [u8; OPERATION_VALUE_BYTES] {
    let mut value = [0; OPERATION_VALUE_BYTES];
    value[..16].copy_from_slice(document.as_uuid().as_bytes());
    value[16..48].copy_from_slice(&request.into_bytes());
    value[48..80].copy_from_slice(&digest.into_bytes());
    value[80..].copy_from_slice(&sequence.to_be_bytes());
    value
}

/// Decodes and validates one durable operation receipt.
pub(crate) fn decode_operation(bytes: &[u8]) -> Result<CommittedOperation, StoreError> {
    if bytes.len() != OPERATION_VALUE_BYTES {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let document = decode_uuid(&bytes[..16]).map(DocumentId::from_uuid)?;
    let request = bytes[16..48]
        .try_into()
        .map(OperationDigest::new)
        .map_err(malformed_store)?;
    let digest = bytes[48..80]
        .try_into()
        .map(ChangeDigest::new)
        .map_err(malformed_store)?;
    Ok(CommittedOperation {
        document,
        request,
        digest,
        sequence: decode_u64(&bytes[80..])?,
    })
}

/// Encodes one durable outbox entry.
pub(crate) fn encode_outbox(change: LocalChange<'_>) -> [u8; OUTBOX_VALUE_BYTES] {
    let mut value = [0; OUTBOX_VALUE_BYTES];
    value[..16].copy_from_slice(change.document.as_uuid().as_bytes());
    value[16..32].copy_from_slice(change.operation.as_uuid().as_bytes());
    value[32..].copy_from_slice(&change.digest.into_bytes());
    value
}

/// Decodes and validates one durable outbox entry.
pub(crate) fn decode_outbox(sequence: u64, bytes: &[u8]) -> Result<OutboxEntry, StoreError> {
    if bytes.len() != OUTBOX_VALUE_BYTES {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let document = decode_uuid(&bytes[..16]).map(DocumentId::from_uuid)?;
    let operation = decode_uuid(&bytes[16..32]).map(OperationId::from_uuid)?;
    let digest = bytes[32..]
        .try_into()
        .map(ChangeDigest::new)
        .map_err(malformed_store)?;
    Ok(OutboxEntry {
        sequence,
        document,
        operation,
        digest,
    })
}

/// Encodes one materialized-state event value.
pub(crate) fn encode_event(document: DocumentId, digest: ChangeDigest) -> [u8; EVENT_VALUE_BYTES] {
    let mut value = [0; EVENT_VALUE_BYTES];
    value[..16].copy_from_slice(document.as_uuid().as_bytes());
    value[16..].copy_from_slice(&digest.into_bytes());
    value
}

/// Decodes and validates one materialized-state event value.
pub(crate) fn decode_event(sequence: u64, bytes: &[u8]) -> Result<ChangeEvent, StoreError> {
    if bytes.len() != EVENT_VALUE_BYTES {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let document = decode_uuid(&bytes[..16]).map(DocumentId::from_uuid)?;
    let digest = bytes[16..]
        .try_into()
        .map(ChangeDigest::new)
        .map_err(malformed_store)?;
    Ok(ChangeEvent::Document {
        sequence,
        document,
        digest,
    })
}

/// Decodes and validates one baseline event value.
pub(crate) fn decode_baseline_event(
    sequence: u64,
    bytes: &[u8],
) -> Result<ChangeEvent, StoreError> {
    if bytes.len() != BASELINE_VALUE_BYTES {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
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
    if bytes.len() != SEQUENCE_KEY_BYTES
        || bytes[..FORMAT.len()] != *FORMAT
        || bytes[SCOPE_PREFIX_BYTES] != kind
    {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    decode_u64(&bytes[bytes.len().saturating_sub(8)..])
}

/// Decodes one big-endian `u64`.
pub(crate) fn decode_u64(bytes: &[u8]) -> Result<u64, StoreError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(malformed_store)
}

/// Decodes one document revision from its durable bytes.
pub(crate) fn decode_revision(bytes: &[u8]) -> Result<DocumentRevision, StoreError> {
    decode_u64(bytes).map(DocumentRevision::new)
}

/// Decodes one model identifier from its durable bytes through the model's
/// own Deserialize contract, without naming the rival UUID crate.
///
/// The bytes ride a quoted simple-hex JSON string into
/// [`lgwks_std::json::from_slice`]; the target type is inferred at each
/// `from_uuid` call site. Hex encoding goes through the estate owner
/// [`lgwks_std::hex`], and a refusal carries the decode failure.
///
/// # Errors
///
/// Returns [`StoreError::CorruptIdentifier`] when the bytes do not decode.
pub(crate) fn decode_uuid<T>(bytes: &[u8]) -> Result<T, StoreError>
where
    T: for<'de> lgwks_std::json::Deserialize<'de>,
{
    let hex = lgwks_std::hex::encode(bytes);
    let text = format!("\"{hex}\"");
    lgwks_std::json::from_slice::<T>(text.as_bytes()).map_err(StoreError::CorruptIdentifier)
}
