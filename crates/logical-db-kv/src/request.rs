//! Input validation, clocks, document identity, and request digests.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use lgwks_std::hash::Hasher;
use logical_db::{OperationDigest, OperationId};

use crate::{BatchPut, DocumentId, KvError, MAX_BATCH_PUT, NEVER_EXPIRES_AT_MS, custom_id};

/// Validates a key against empty and max-name bounds.
pub(crate) fn validate_key(key: &str) -> Result<(), KvError> {
    if key.is_empty() || key.len() > logical_db_model::MAX_NAME_BYTES {
        return Err(KvError::InvalidKey);
    }
    Ok(())
}

/// Validates a namespace against empty and max-name bounds.
pub(crate) fn validate_namespace(namespace: &str) -> Result<(), KvError> {
    if namespace.is_empty() || namespace.len() > logical_db_model::MAX_NAME_BYTES {
        return Err(KvError::InvalidNamespace);
    }
    Ok(())
}

/// Maps model checkpoint-map errors onto the KV error surface.
pub(crate) fn checkpoint_map_error(error: &logical_db_model::ModelError) -> KvError {
    match *error {
        logical_db_model::ModelError::UnsupportedNamespaceCheckpointVersion { actual } => {
            KvError::UnsupportedCheckpointMapVersion { actual }
        }
        logical_db_model::ModelError::NamespaceCheckpointMapTooLarge { actual, maximum } => {
            KvError::CheckpointMapTooLarge { actual, maximum }
        }
        _ => KvError::CorruptCheckpointMap,
    }
}

/// Returns the current Unix time in milliseconds.
///
/// A clock before the epoch or past `i64` milliseconds fails closed instead
/// of stamping a made-up instant on expiry and recency.
pub(crate) fn now_ms() -> Result<i64, KvError> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    i64::try_from(elapsed.as_millis()).map_err(KvError::ClockOutOfRange)
}

/// Seeds a recency sequence from the current wall clock.
pub(crate) fn seed_sequence() -> Result<u64, KvError> {
    u64::try_from(now_ms()?).map_err(KvError::ClockOutOfRange)
}

/// Converts an optional TTL into an absolute expiry timestamp.
///
/// A TTL that runs past the last representable instant never expires.
pub(crate) fn expiry_for(ttl: Option<Duration>, now: i64) -> i64 {
    let Some(ttl) = ttl else {
        return NEVER_EXPIRES_AT_MS;
    };
    match i64::try_from(ttl.as_millis()) {
        Ok(delta) => match now.checked_add(delta) {
            Some(expires_at_ms) => expires_at_ms,
            None => NEVER_EXPIRES_AT_MS,
        },
        Err(_) => NEVER_EXPIRES_AT_MS,
    }
}

/// Encodes a byte length as the big-endian `u64` every digest frames with,
/// refusing a length no `u64` can carry instead of hashing a stand-in.
fn length_prefix(len: usize) -> Result<[u8; 8], KvError> {
    Ok(u64::try_from(len)?.to_be_bytes())
}

/// Seals a hasher into an operation digest.
fn seal(hasher: Hasher) -> OperationDigest {
    OperationDigest::new(*hasher.finalize().as_bytes())
}

/// Starts a request digest framed by domain, operation, namespace, and key.
fn request_hasher(domain: &[u8], operation: &OperationId, namespace: &str, key: &str) -> Hasher {
    let mut hasher = Hasher::new();
    hasher.update(domain);
    hasher.update(&[0]);
    hasher.update(operation.as_uuid().as_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update(&[0]);
    hasher.update(key.as_bytes());
    hasher
}

/// Frames an absolute expiry and a recency sequence into a request digest.
fn frame_expiry(hasher: &mut Hasher, expires_at_ms: i64, sequence: u64) {
    hasher.update(&expires_at_ms.to_be_bytes());
    hasher.update(&sequence.to_be_bytes());
}

/// Extracts the wall-clock start time encoded in a UUIDv7 operation id.
pub(crate) fn operation_started_at_ms(operation: &OperationId) -> Result<i64, KvError> {
    let uuid = operation.as_uuid();
    if uuid.get_version_num() != 7 {
        return Err(KvError::InvalidOperationId);
    }
    let Some(timestamp) = uuid.get_timestamp() else {
        return Err(KvError::InvalidOperationId);
    };
    let (seconds, nanos) = timestamp.to_unix();
    seconds
        .checked_mul(1_000)
        .and_then(|millis| millis.checked_add(u64::from(nanos.checked_div(1_000_000)?)))
        .and_then(|millis| i64::try_from(millis).ok())
        .ok_or(KvError::InvalidOperationId)
}

/// Derives a unique recency sequence for one index within a batch.
pub(crate) fn batch_sequence(started_at_ms: i64, index: usize) -> Result<u64, KvError> {
    let started_at_ms = u64::try_from(started_at_ms)?;
    let stride = MAX_BATCH_PUT
        .checked_add(1)
        .and_then(|stride| u64::try_from(stride).ok())
        .ok_or(KvError::InvalidBatchSize(MAX_BATCH_PUT))?;
    let index = u64::try_from(index)?;
    started_at_ms
        .checked_mul(stride)
        .and_then(|base| base.checked_add(index))
        .ok_or(KvError::InvalidOperationId)
}

/// Derives a deterministic operation id for one member of a batch.
pub(crate) fn batch_member_operation(
    batch: OperationId,
    index: usize,
) -> Result<OperationId, KvError> {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/batch-member/v1");
    hasher.update(&[0]);
    hasher.update(batch.as_uuid().as_bytes());
    hasher.update(&length_prefix(index)?);
    custom_id(hasher)
}

/// Hashes a full batch request for idempotent apply and deduplication.
pub(crate) fn batch_request_digest(
    namespace: &str,
    entries: &[BatchPut],
    started_at_ms: i64,
) -> Result<OperationDigest, KvError> {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/batch-request/v1");
    hasher.update(&length_prefix(namespace.len())?);
    hasher.update(namespace.as_bytes());
    hasher.update(&length_prefix(entries.len())?);
    for (index, entry) in entries.iter().enumerate() {
        let sequence = batch_sequence(started_at_ms, index)?;
        let expires_at_ms = expiry_for(entry.ttl, started_at_ms);
        hasher.update(&length_prefix(entry.key.len())?);
        hasher.update(entry.key.as_bytes());
        hasher.update(&length_prefix(entry.value.len())?);
        hasher.update(&entry.value);
        frame_expiry(&mut hasher, expires_at_ms, sequence);
    }
    Ok(seal(hasher))
}

/// Binds a member request digest to its parent batch request digest.
pub(crate) fn bind_batch_request(
    request: OperationDigest,
    batch_digest: &OperationDigest,
) -> OperationDigest {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/batch-member-request/v1");
    hasher.update(&request.into_bytes());
    hasher.update(&batch_digest.into_bytes());
    seal(hasher)
}

/// Builds the request digest for an inline put operation.
pub(crate) fn put_digest(
    operation: &OperationId,
    namespace: &str,
    key: &str,
    value: &[u8],
    expires_at_ms: i64,
    sequence: u64,
) -> OperationDigest {
    let mut hasher = request_hasher(b"logical-db-kv/put/v1", operation, namespace, key);
    hasher.update(&[0]);
    hasher.update(value);
    frame_expiry(&mut hasher, expires_at_ms, sequence);
    seal(hasher)
}

/// Builds the request digest for a tiered put operation.
pub(crate) fn tiered_put_digest(
    operation: &OperationId,
    namespace: &str,
    key: &str,
    digest: &[u8; 32],
    size: u64,
    expires_at_ms: i64,
    sequence: u64,
) -> OperationDigest {
    let mut hasher = request_hasher(b"logical-db-kv/tiered-put/v1", operation, namespace, key);
    hasher.update(&[0]);
    hasher.update(digest);
    hasher.update(&size.to_be_bytes());
    frame_expiry(&mut hasher, expires_at_ms, sequence);
    seal(hasher)
}

/// Builds the request digest for a clear/delete operation.
pub(crate) fn clear_digest(operation: &OperationId, namespace: &str, key: &str) -> OperationDigest {
    seal(request_hasher(
        b"logical-db-kv/clear/v1",
        operation,
        namespace,
        key,
    ))
}

/// Derives the deterministic document id for a namespace and key pair.
///
/// The mapping is stable forever (see the crate documentation); a length no
/// `u64` frame can carry is refused rather than hashed as a stand-in.
pub(crate) fn document_for(namespace: &str, key: &str) -> Result<DocumentId, KvError> {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/document/v1");
    for field in [namespace, key] {
        hasher.update(&[0]);
        hasher.update(&length_prefix(field.len())?);
        hasher.update(field.as_bytes());
    }
    custom_id::<DocumentId>(hasher)
}
