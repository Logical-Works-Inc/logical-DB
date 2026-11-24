//! Bounded namespace-checkpoint field decoding and cursor advancement.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use super::{MAX_NAME_BYTES, ModelError, NamespaceCheckpoint, validate_namespace};

/// Borrows exactly `count` bytes at `cursor`, advancing `cursor` past them.
///
/// Fails closed with [`ModelError::InvalidNamespaceCheckpointEncoding`] when
/// the range would run past the end of `bytes` or overflow the cursor.
pub(super) fn take<'bytes>(
    bytes: &'bytes [u8],
    cursor: &mut usize,
    count: usize,
) -> Result<&'bytes [u8], ModelError> {
    let end = cursor
        .checked_add(count)
        .ok_or(ModelError::InvalidNamespaceCheckpointEncoding)?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or(ModelError::InvalidNamespaceCheckpointEncoding)?;
    *cursor = end;
    Ok(slice)
}

/// Copies exactly `N` bytes at `cursor` into an array, advancing past them.
fn take_array<const N: usize>(bytes: &[u8], cursor: &mut usize) -> Result<[u8; N], ModelError> {
    let raw = take(bytes, cursor, N)?;
    match raw.first_chunk::<N>() {
        Some(array) => Ok(*array),
        None => Err(ModelError::InvalidNamespaceCheckpointEncoding),
    }
}

/// Reads a big-endian `u16` at `cursor`, advancing past it.
pub(super) fn take_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, ModelError> {
    take_array::<2>(bytes, cursor).map(u16::from_be_bytes)
}

/// Reads a big-endian `u64` at `cursor`, advancing past it.
pub(super) fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, ModelError> {
    take_array::<8>(bytes, cursor).map(u64::from_be_bytes)
}

/// Reads a big-endian `u32` length at `cursor`, returning it as `usize`.
pub(super) fn take_u32(bytes: &[u8], cursor: &mut usize) -> Result<usize, ModelError> {
    let value = take_array::<4>(bytes, cursor).map(u32::from_be_bytes)?;
    let Ok(length) = usize::try_from(value) else {
        return Err(ModelError::InvalidNamespaceCheckpointEncoding);
    };
    Ok(length)
}

/// Reads a length-prefixed, bounded UTF-8 namespace name at `cursor`.
pub(super) fn take_namespace<'bytes>(
    bytes: &'bytes [u8],
    cursor: &mut usize,
) -> Result<&'bytes str, ModelError> {
    let name_len = take_u32(bytes, cursor)?;
    if name_len == 0 || name_len > MAX_NAME_BYTES {
        return Err(ModelError::InvalidNamespaceCheckpointEncoding);
    }
    let name_bytes = take(bytes, cursor, name_len)?;
    let Ok(namespace) = core::str::from_utf8(name_bytes) else {
        return Err(ModelError::InvalidNamespaceCheckpointEncoding);
    };
    if validate_namespace(namespace).is_err() {
        return Err(ModelError::InvalidNamespaceCheckpointEncoding);
    }
    Ok(namespace)
}

/// Reads a length-prefixed checkpoint record at `cursor`.
pub(super) fn take_checkpoint(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<NamespaceCheckpoint, ModelError> {
    let checkpoint_len = take_u32(bytes, cursor)?;
    if checkpoint_len == 0 {
        return Err(ModelError::InvalidNamespaceCheckpointEncoding);
    }
    let checkpoint_bytes = take(bytes, cursor, checkpoint_len)?;
    let checkpoint = NamespaceCheckpoint::decode(checkpoint_bytes)?;
    Ok(checkpoint)
}
