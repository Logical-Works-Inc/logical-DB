//! Index encode/decode helpers; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::decode_uuid;
use crate::error::StoreError;
use crate::keys::{
    MAX_INDEX_VALUE_BYTES, MAX_INDEXES_PER_DOCUMENT, index_reverse_prefix, is_at_most,
    key_index_reverse, key_index_row, next_prefix,
};
use logical_db_model::{DocumentId, IndexEntry, IndexId, IndexValue, Scope};
use redb::{ReadableTable, Table};
use std::collections::BTreeSet;

/// Rejects duplicate, excessive, or unencodable index projections.
pub(crate) fn validate_indexes(indexes: &[IndexEntry]) -> Result<(), StoreError> {
    if !is_at_most(indexes.len(), MAX_INDEXES_PER_DOCUMENT) {
        let error = StoreError::InvalidIndexes;
        return Err(error);
    }
    let mut identities = BTreeSet::new();
    for entry in indexes {
        if !identities.insert(entry.index) || encode_index_value(&entry.value).is_err() {
            let error = StoreError::InvalidIndexes;
            return Err(error);
        }
    }
    Ok(())
}

/// Replaces all forward and reverse index rows for one document.
pub(crate) fn replace_indexes(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    document: DocumentId,
    indexes: &[IndexEntry],
) -> Result<(), StoreError> {
    let reverse_prefix = index_reverse_prefix(scope, document);
    let reverse_end = next_prefix(&reverse_prefix).ok_or(StoreError::CorruptInternalValue)?;
    let previous = table
        .range(reverse_prefix.as_slice()..reverse_end.as_slice())?
        .map(|entry| {
            let (key, value) = entry?;
            Ok((key.value().to_vec(), value.value().to_vec()))
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    for (reverse_key, encoded_value) in previous {
        if reverse_key.len() != reverse_prefix.len().saturating_add(16) {
            let error = StoreError::CorruptInternalValue;
            return Err(error);
        }
        let index = decode_uuid(&reverse_key[reverse_prefix.len()..]).map(IndexId::from_uuid)?;
        table.remove(key_index_row(scope, index, &encoded_value, document).as_slice())?;
        table.remove(reverse_key.as_slice())?;
    }
    for entry in indexes {
        let encoded = encode_index_value(&entry.value)?;
        table.insert(
            key_index_row(scope, entry.index, &encoded, document).as_slice(),
            [1_u8].as_slice(),
        )?;
        table.insert(
            key_index_reverse(scope, document, entry.index).as_slice(),
            encoded.as_slice(),
        )?;
    }
    Ok(())
}

/// Encodes one order-preserving index value, or rejects it.
pub(crate) fn encode_index_value(value: &IndexValue) -> Result<Vec<u8>, StoreError> {
    let mut encoded = Vec::new();
    match *value {
        IndexValue::Boolean(value) => encoded.extend_from_slice(&[1, u8::from(value)]),
        IndexValue::Integer(value) => {
            encoded.push(2);
            encoded.extend_from_slice(&(value.cast_unsigned() ^ (1_u64 << 63)).to_be_bytes());
        }
        IndexValue::Unsigned(value) => {
            encoded.push(3);
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        IndexValue::Text(ref value) => {
            encoded.push(4);
            encode_escaped(value.as_bytes(), &mut encoded);
        }
        IndexValue::Bytes(ref value) => {
            encoded.push(5);
            encode_escaped(value, &mut encoded);
        }
        IndexValue::Timestamp(value) => {
            encoded.push(6);
            encoded.extend_from_slice(&(value.cast_unsigned() ^ (1_u64 << 63)).to_be_bytes());
        }
        // `IndexValue` is `#[non_exhaustive]`, so a future projected form must
        // be refused as an unsupported encoding rather than silently skipped.
        _ => return Err(StoreError::InvalidIndexes),
    }
    if !is_at_most(encoded.len(), MAX_INDEX_VALUE_BYTES) {
        return Err(StoreError::InvalidIndexes);
    }
    Ok(encoded)
}

/// Decodes a text value from its order-preserving encoding, or `None` for a
/// non-text value. A truncated or ill-formed text encoding is corruption,
/// never a skip: callers must not silently drop rows they cannot read.
pub(crate) fn decode_index_text(encoded: &[u8]) -> Result<Option<String>, StoreError> {
    let Some((tag, body)) = encoded.split_first() else {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    };
    if *tag != 4 {
        return Ok(None);
    }
    let Some(payload_len) = body.len().checked_sub(2) else {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    };
    let Some(payload) = body.get(..payload_len) else {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    };
    if body.get(payload_len..) != Some([0_u8, 0_u8].as_slice()) {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let mut raw = Vec::with_capacity(payload.len());
    let mut rest = payload;
    while let Some((head, tail)) = rest.split_first() {
        if *head == 0 {
            let Some((&escape, tail)) = tail.split_first() else {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            };
            if escape != 0xFF {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            raw.push(0_u8);
            rest = tail;
        } else {
            raw.push(*head);
            rest = tail;
        }
    }
    String::from_utf8(raw)
        .map(Some)
        .map_err(StoreError::CorruptInternalText)
}

/// Appends one zero-escaped byte run plus its terminator.
pub(crate) fn encode_escaped(bytes: &[u8], output: &mut Vec<u8>) {
    for byte in bytes {
        if *byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
}
