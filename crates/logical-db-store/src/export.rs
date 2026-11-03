//! Export codec and restore helper; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::{decode_uuid, malformed_store};
use crate::error::StoreError;
use crate::keys::{
    EXPORT_DOCUMENT_PREFIX_BYTES, EXPORT_ENTRY_PREFIX_BYTES, EXPORT_HEADER_BYTES, EXPORT_MAGIC,
    MAX_EXPORT_BYTES,
};
use crate::types::{ExportDocument, ExportEntry};
use logical_db_model::{
    AccountId, ChangeDigest, DocumentId, MAX_CHANGE_BYTES, MAX_SNAPSHOT_BYTES, OperationDigest,
    OperationId, Scope, SpaceId,
};

/// Encodes one versioned logical export payload.
///
/// The payload is a logical dump — ordered commit history plus document
/// snapshots with tombstone flags — never a file copy: the store refuses
/// foreign files through replica binding, so restore replays these entries
/// through the commit path instead.
///
/// # Errors
///
/// Returns [`StoreError::InvalidExportSize`] when a field or the assembled
/// payload would exceed the export budget.
pub fn encode_export(
    scope: Scope,
    entries: &[ExportEntry],
    documents: &[ExportDocument],
) -> Result<Vec<u8>, StoreError> {
    let entry_count = u32::try_from(entries.len()).map_err(malformed_store)?;
    let document_count = u32::try_from(documents.len()).map_err(malformed_store)?;
    let mut payload = Vec::new();
    push_bounded(&mut payload, &EXPORT_MAGIC)?;
    push_bounded(&mut payload, scope.account().as_uuid().as_bytes())?;
    push_bounded(&mut payload, scope.space().as_uuid().as_bytes())?;
    push_bounded(&mut payload, &entry_count.to_be_bytes())?;
    for entry in entries {
        encode_export_entry(&mut payload, entry)?;
    }
    push_bounded(&mut payload, &document_count.to_be_bytes())?;
    for document in documents {
        encode_export_document(&mut payload, document)?;
    }
    Ok(payload)
}

/// Decodes one versioned logical export payload with bounds before allocation.
///
/// Length prefixes are validated against the remaining input and the resource
/// ceilings before any buffer is allocated; unknown versions, truncation, and
/// trailing bytes fail closed.
///
/// # Errors
///
/// Returns a typed size, version, or structure failure without allocating
/// beyond the validated bounds.
pub fn decode_export(
    bytes: &[u8],
) -> Result<(Scope, Vec<ExportEntry>, Vec<ExportDocument>), StoreError> {
    if bytes.len() > MAX_EXPORT_BYTES {
        let error = StoreError::InvalidExportSize(bytes.len());
        return Err(error);
    }
    if bytes.len() < EXPORT_HEADER_BYTES {
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    if bytes[..EXPORT_MAGIC.len()] != EXPORT_MAGIC {
        if bytes[..4] == *b"LDBE" {
            let error = StoreError::UnknownExportVersion(u16::from(bytes[4]));
            return Err(error);
        }
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    let account = decode_uuid(&bytes[5..21]).map(AccountId::from_uuid)?;
    let space = decode_uuid(&bytes[21..37]).map(SpaceId::from_uuid)?;
    let scope = Scope::new(account, space);
    let mut position = EXPORT_HEADER_BYTES;
    let entry_count = usize::try_from(read_u32(bytes, &mut position)?).map_err(malformed_store)?;
    if entry_count.saturating_mul(EXPORT_ENTRY_PREFIX_BYTES) > bytes.len().saturating_sub(position)
    {
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    let mut entries = Vec::new();
    for _ in 0..entry_count {
        entries.push(decode_export_entry(bytes, &mut position)?);
    }
    let document_count =
        usize::try_from(read_u32(bytes, &mut position)?).map_err(malformed_store)?;
    if document_count.saturating_mul(EXPORT_DOCUMENT_PREFIX_BYTES)
        > bytes.len().saturating_sub(position)
    {
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    let mut documents = Vec::new();
    for _ in 0..document_count {
        documents.push(decode_export_document(bytes, &mut position)?);
    }
    if position != bytes.len() {
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    Ok((scope, entries, documents))
}

/// Appends one bounded export history entry.
pub(crate) fn encode_export_entry(
    payload: &mut Vec<u8>,
    entry: &ExportEntry,
) -> Result<(), StoreError> {
    if entry.bytes.len() > MAX_CHANGE_BYTES {
        let error = StoreError::InvalidExportSize(entry.bytes.len());
        return Err(error);
    }
    push_bounded(payload, entry.document.as_uuid().as_bytes())?;
    push_bounded(payload, entry.operation.as_uuid().as_bytes())?;
    push_bounded(payload, &entry.request.into_bytes())?;
    push_bounded(payload, &entry.digest.into_bytes())?;
    let change_len = u32::try_from(entry.bytes.len()).map_err(malformed_store)?;
    push_bounded(payload, &change_len.to_be_bytes())?;
    push_bounded(payload, &entry.bytes)?;
    Ok(())
}

/// Appends one bounded export document.
pub(crate) fn encode_export_document(
    payload: &mut Vec<u8>,
    document: &ExportDocument,
) -> Result<(), StoreError> {
    if document.snapshot.len() > MAX_SNAPSHOT_BYTES {
        let error = StoreError::InvalidExportSize(document.snapshot.len());
        return Err(error);
    }
    push_bounded(payload, document.document.as_uuid().as_bytes())?;
    push_bounded(payload, &[u8::from(document.tombstone)])?;
    let snapshot_len = u32::try_from(document.snapshot.len()).map_err(malformed_store)?;
    push_bounded(payload, &snapshot_len.to_be_bytes())?;
    push_bounded(payload, &document.snapshot)?;
    Ok(())
}

/// Reads one export history entry, advancing the cursor.
pub(crate) fn decode_export_entry(
    bytes: &[u8],
    position: &mut usize,
) -> Result<ExportEntry, StoreError> {
    let document_bytes = read_array::<16>(bytes, position)?;
    let document_uuid = decode_uuid(&document_bytes)?;
    let document = DocumentId::from_uuid(document_uuid);
    let operation_bytes = read_array::<16>(bytes, position)?;
    let operation_uuid = decode_uuid(&operation_bytes)?;
    let operation = OperationId::from_uuid(operation_uuid);
    let request = read_array::<32>(bytes, position).map(OperationDigest::new)?;
    let digest = read_array::<32>(bytes, position).map(ChangeDigest::new)?;
    let change_len = usize::try_from(read_u32(bytes, position)?).map_err(malformed_store)?;
    if change_len > MAX_CHANGE_BYTES {
        let error = StoreError::InvalidExportSize(change_len);
        return Err(error);
    }
    let change = read_bytes(bytes, position, change_len)?;
    Ok(ExportEntry {
        document,
        operation,
        request,
        digest,
        bytes: change,
    })
}

/// Reads one export document, advancing the cursor.
pub(crate) fn decode_export_document(
    bytes: &[u8],
    position: &mut usize,
) -> Result<ExportDocument, StoreError> {
    let document_bytes = read_array::<16>(bytes, position)?;
    let document_uuid = decode_uuid(&document_bytes)?;
    let document = DocumentId::from_uuid(document_uuid);
    let tombstone = match read_array::<1>(bytes, position)? {
        [0] => false,
        [1] => true,
        _ => {
            let error = StoreError::InvalidExport;
            return Err(error);
        }
    };
    let snapshot_len = usize::try_from(read_u32(bytes, position)?).map_err(malformed_store)?;
    if snapshot_len > MAX_SNAPSHOT_BYTES {
        let error = StoreError::InvalidExportSize(snapshot_len);
        return Err(error);
    }
    let snapshot = read_bytes(bytes, position, snapshot_len)?;
    Ok(ExportDocument {
        document,
        tombstone,
        snapshot,
    })
}

/// Derives the deterministic operation binding for a remotely installed change.
///
/// Export must replay changes that arrived through synchronization and
/// therefore carry no local operation receipt. The binding is a
/// non-cryptographic deterministic function of the document and change
/// digest, so every retry of the same restore replays the exact same
/// identities and short-circuits as duplicates. A collision with a live
/// operation fails closed through [`StoreError::OperationConflict`].
///
/// # Errors
///
/// Returns [`StoreError::CorruptIdentifier`] when the synthesized bytes do
/// not decode, which cannot happen for the fixed sixteen-byte synthesis.
pub fn restore_operation(
    document: DocumentId,
    digest: ChangeDigest,
) -> Result<(OperationId, OperationDigest), StoreError> {
    let operation_tag: &[u8] = b"logical-db-restore-op-v1";
    let request_tag: &[u8] = b"logical-db-restore-req-v1";
    let document = document.as_uuid();
    let digest = digest.into_bytes();
    let first = fnv1a64(
        0x9e37_79b9_7f4a_7c15,
        &[operation_tag, document.as_bytes(), digest.as_slice()],
    );
    let second = fnv1a64(
        0x6a09_e667_f3bc_c909,
        &[operation_tag, document.as_bytes(), digest.as_slice()],
    );
    let mut operation = [0_u8; 16];
    operation[..8].copy_from_slice(&first.to_be_bytes());
    operation[8..].copy_from_slice(&second.to_be_bytes());
    let mut request = [0_u8; 32];
    for (slot, seed) in request.chunks_mut(8).zip([
        0x510e_527f_ade6_82d1,
        0x9b05_688c_2b3e_6c1f,
        0x1f83_d9ab_fb41_bd6b,
        0x5be0_cd19_137e_2179,
    ]) {
        slot.copy_from_slice(
            &fnv1a64(seed, &[request_tag, document.as_bytes(), digest.as_slice()]).to_be_bytes(),
        );
    }
    Ok((
        OperationId::from_uuid(decode_uuid(&operation)?),
        OperationDigest::new(request),
    ))
}

/// Hashes one seeded FNV-1a sequence over the given byte parts.
pub(crate) fn fnv1a64(seed: u64, parts: &[&[u8]]) -> u64 {
    let mut hash = seed;
    for part in parts {
        for byte in *part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

/// Appends bytes while enforcing the export byte budget.
pub(crate) fn push_bounded(payload: &mut Vec<u8>, bytes: &[u8]) -> Result<(), StoreError> {
    if payload.len().saturating_add(bytes.len()) > MAX_EXPORT_BYTES {
        let error = StoreError::InvalidExportSize(payload.len().saturating_add(bytes.len()));
        return Err(error);
    }
    payload.extend_from_slice(bytes);
    Ok(())
}

/// Reads a fixed-size array, advancing the cursor.
pub(crate) fn read_array<const LENGTH: usize>(
    bytes: &[u8],
    position: &mut usize,
) -> Result<[u8; LENGTH], StoreError> {
    let end = position
        .checked_add(LENGTH)
        .filter(|end| *end <= bytes.len())
        .ok_or(StoreError::InvalidExport)?;
    let value: [u8; LENGTH] = bytes[*position..end].try_into().map_err(malformed_store)?;
    *position = end;
    Ok(value)
}

/// Reads a length-prefixed byte run, advancing the cursor.
pub(crate) fn read_bytes(
    bytes: &[u8],
    position: &mut usize,
    length: usize,
) -> Result<Vec<u8>, StoreError> {
    let end = position
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or(StoreError::InvalidExport)?;
    let value = bytes[*position..end].to_vec();
    *position = end;
    Ok(value)
}

/// Reads one big-endian `u32`, advancing the cursor.
pub(crate) fn read_u32(bytes: &[u8], position: &mut usize) -> Result<u32, StoreError> {
    read_array(bytes, position).map(u32::from_be_bytes)
}
