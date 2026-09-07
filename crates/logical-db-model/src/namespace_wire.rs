//! Bounded namespace-checkpoint field decoding and cursor advancement.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use super::{MAX_NAME_BYTES, ModelError, NamespaceCheckpoint, validate_namespace};

pub(super) fn take<'bytes>(
    bytes: &'bytes [u8],
    cursor: &mut usize,
    count: usize,
) -> Result<&'bytes [u8], ModelError> {
    let end = cursor
        .checked_add(count)
        .ok_or(ModelError::InvalidNamespaceCheckpointEncoding)?;
    if end > bytes.len() {
        eprintln!("logical-db-model: take: InvalidNamespaceCheckpointEncoding");
        return Err(ModelError::InvalidNamespaceCheckpointEncoding);
    }
    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}
pub(super) fn take_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, ModelError> {
    let raw = take(bytes, cursor, 2)?;
    raw.try_into().map(u16::from_be_bytes).map_err(|error| {
        eprintln!("logical-db-model: ModelError::InvalidNamespaceCheckpointEncoding: {error}");
        ModelError::InvalidNamespaceCheckpointEncoding
    })
}
pub(super) fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<usize, ModelError> {
    let raw = take(bytes, cursor, 4)?;
    let value: u32 = raw.try_into().map(u32::from_be_bytes).map_err(|error| {
        eprintln!("logical-db-model: ModelError::InvalidNamespaceCheckpointEncoding: {error}");
        ModelError::InvalidNamespaceCheckpointEncoding
    })?;
    usize::try_from(value).map_err(|error| {
        eprintln!("logical-db-model: ModelError::InvalidNamespaceCheckpointEncoding: {error}");
        ModelError::InvalidNamespaceCheckpointEncoding
    })
}
pub(super) fn take_namespace<'bytes>(
    bytes: &'bytes [u8],
    cursor: &mut usize,
) -> Result<&'bytes str, ModelError> {
    let name_len = take_u32(bytes, cursor)?;
    if name_len == 0 || name_len > MAX_NAME_BYTES {
        eprintln!(
            "logical-db-model: NamespaceCheckpoints::decode: InvalidNamespaceCheckpointEncoding"
        );
        return Err(ModelError::InvalidNamespaceCheckpointEncoding);
    }
    let name_bytes = take(bytes, cursor, name_len)?;
    let namespace = core::str::from_utf8(name_bytes).map_err(|error| {
        eprintln!("logical-db-model: ModelError::InvalidNamespaceCheckpointEncoding: {error}");
        ModelError::InvalidNamespaceCheckpointEncoding
    })?;
    if validate_namespace(namespace).is_err() {
        eprintln!(
            "logical-db-model: NamespaceCheckpoints::decode: InvalidNamespaceCheckpointEncoding"
        );
        return Err(ModelError::InvalidNamespaceCheckpointEncoding);
    }
    Ok(namespace)
}

pub(super) fn take_checkpoint(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<NamespaceCheckpoint, ModelError> {
    let checkpoint_len = take_u32(bytes, cursor)?;
    if checkpoint_len == 0 {
        eprintln!(
            "logical-db-model: NamespaceCheckpoints::decode: InvalidNamespaceCheckpointEncoding"
        );
        return Err(ModelError::InvalidNamespaceCheckpointEncoding);
    }
    let checkpoint_bytes = take(bytes, cursor, checkpoint_len)?;
    let checkpoint = NamespaceCheckpoint::decode(checkpoint_bytes)?;
    Ok(checkpoint)
}
