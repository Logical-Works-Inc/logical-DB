//! Export codec and restore helper; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::{fill, take, take_uuid};
use crate::error::StoreError;
use crate::keys::{
    EXPORT_DOCUMENT_PREFIX_BYTES, EXPORT_ENTRY_PREFIX_BYTES, EXPORT_HEADER_BYTES, EXPORT_MAGIC,
    MAX_EXPORT_BYTES,
};
use crate::types::ExportEntry;
use lgwks_std::id::Uuid;
use logical_db_model::{
    AccountId, ChangeDigest, DocumentId, MAX_CHANGE_BYTES, MAX_SNAPSHOT_BYTES, OperationDigest,
    OperationId, Scope, SnapshotDocument, SpaceId,
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
    documents: &[SnapshotDocument],
) -> Result<Vec<u8>, StoreError> {
    let entry_count = u32::try_from(entries.len())?;
    let document_count = u32::try_from(documents.len())?;
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
) -> Result<(Scope, Vec<ExportEntry>, Vec<SnapshotDocument>), StoreError> {
    if bytes.len() > MAX_EXPORT_BYTES {
        return Err(StoreError::InvalidExportSize(bytes.len()));
    }
    if bytes.len() < EXPORT_HEADER_BYTES {
        return Err(StoreError::InvalidExport);
    }
    let mut rest = bytes;
    let magic = take::<{ EXPORT_MAGIC.len() }>(&mut rest)?;
    if magic != EXPORT_MAGIC {
        let [first, second, third, fourth, version] = magic;
        return Err(if [first, second, third, fourth] == *b"LDBE" {
            StoreError::UnknownExportVersion(u16::from(version))
        } else {
            StoreError::InvalidExport
        });
    }
    let account = AccountId::from_uuid(take_uuid(&mut rest)?);
    let space = SpaceId::from_uuid(take_uuid(&mut rest)?);
    let scope = Scope::new(account, space);
    let mut position = EXPORT_HEADER_BYTES;
    let entry_count = usize::try_from(read_u32(bytes, &mut position)?)?;
    if entry_count.saturating_mul(EXPORT_ENTRY_PREFIX_BYTES) > bytes.len().saturating_sub(position)
    {
        return Err(StoreError::InvalidExport);
    }
    let mut entries = Vec::new();
    for _ in 0..entry_count {
        entries.push(decode_export_entry(bytes, &mut position)?);
    }
    let document_count = usize::try_from(read_u32(bytes, &mut position)?)?;
    if document_count.saturating_mul(EXPORT_DOCUMENT_PREFIX_BYTES)
        > bytes.len().saturating_sub(position)
    {
        return Err(StoreError::InvalidExport);
    }
    let mut documents = Vec::new();
    for _ in 0..document_count {
        documents.push(decode_export_document(bytes, &mut position)?);
    }
    if position != bytes.len() {
        return Err(StoreError::InvalidExport);
    }
    Ok((scope, entries, documents))
}

/// Appends one bounded export history entry.
pub(crate) fn encode_export_entry(
    payload: &mut Vec<u8>,
    entry: &ExportEntry,
) -> Result<(), StoreError> {
    if entry.bytes.len() > MAX_CHANGE_BYTES {
        return Err(StoreError::InvalidExportSize(entry.bytes.len()));
    }
    push_bounded(payload, entry.document.as_uuid().as_bytes())?;
    push_bounded(payload, entry.operation.as_uuid().as_bytes())?;
    push_bounded(payload, &entry.request.into_bytes())?;
    push_bounded(payload, &entry.digest.into_bytes())?;
    let change_len = u32::try_from(entry.bytes.len())?;
    push_bounded(payload, &change_len.to_be_bytes())?;
    push_bounded(payload, &entry.bytes)?;
    Ok(())
}

/// Appends one bounded export document.
pub(crate) fn encode_export_document(
    payload: &mut Vec<u8>,
    document: &SnapshotDocument,
) -> Result<(), StoreError> {
    let snapshot = document.snapshot();
    if snapshot.len() > MAX_SNAPSHOT_BYTES {
        return Err(StoreError::InvalidExportSize(snapshot.len()));
    }
    push_bounded(payload, document.document().as_uuid().as_bytes())?;
    push_bounded(payload, &[u8::from(document.is_tombstone())])?;
    let snapshot_len = u32::try_from(snapshot.len())?;
    push_bounded(payload, &snapshot_len.to_be_bytes())?;
    push_bounded(payload, snapshot)?;
    Ok(())
}

/// Reads one export history entry, advancing the cursor.
pub(crate) fn decode_export_entry(
    bytes: &[u8],
    position: &mut usize,
) -> Result<ExportEntry, StoreError> {
    let document = DocumentId::from_uuid(Uuid::from(read_array::<16>(bytes, position)?));
    let operation = OperationId::from_uuid(Uuid::from(read_array::<16>(bytes, position)?));
    let request = read_array::<32>(bytes, position).map(OperationDigest::new)?;
    let digest = read_array::<32>(bytes, position).map(ChangeDigest::new)?;
    let change_len = usize::try_from(read_u32(bytes, position)?)?;
    if change_len > MAX_CHANGE_BYTES {
        return Err(StoreError::InvalidExportSize(change_len));
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
) -> Result<SnapshotDocument, StoreError> {
    let document = DocumentId::from_uuid(Uuid::from(read_array::<16>(bytes, position)?));
    let tombstone = match read_array::<1>(bytes, position)? {
        [0] => false,
        [1] => true,
        _ => return Err(StoreError::InvalidExport),
    };
    let snapshot_len = usize::try_from(read_u32(bytes, position)?)?;
    if snapshot_len > MAX_SNAPSHOT_BYTES {
        return Err(StoreError::InvalidExportSize(snapshot_len));
    }
    let snapshot = read_bytes(bytes, position, snapshot_len)?;
    Ok(SnapshotDocument::new(document, snapshot, tombstone))
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
#[must_use]
pub fn restore_operation(
    document: DocumentId,
    digest: ChangeDigest,
) -> (OperationId, OperationDigest) {
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
    let operation = fill::<16>(first.to_be_bytes().into_iter().chain(second.to_be_bytes()));
    let request = fill::<32>(
        [
            0x510e_527f_ade6_82d1,
            0x9b05_688c_2b3e_6c1f,
            0x1f83_d9ab_fb41_bd6b,
            0x5be0_cd19_137e_2179,
        ]
        .into_iter()
        .flat_map(|seed| {
            fnv1a64(seed, &[request_tag, document.as_bytes(), digest.as_slice()]).to_be_bytes()
        }),
    );
    (
        OperationId::from_uuid(Uuid::from(operation)),
        OperationDigest::new(request),
    )
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
        return Err(StoreError::InvalidExportSize(
            payload.len().saturating_add(bytes.len()),
        ));
    }
    payload.extend_from_slice(bytes);
    Ok(())
}

/// Reads a fixed-size array, advancing the cursor.
pub(crate) fn read_array<const LENGTH: usize>(
    bytes: &[u8],
    position: &mut usize,
) -> Result<[u8; LENGTH], StoreError> {
    let run = read_run(bytes, position, LENGTH)?;
    Ok(<[u8; LENGTH]>::try_from(run)?)
}

/// Reads a length-prefixed byte run, advancing the cursor.
pub(crate) fn read_bytes(
    bytes: &[u8],
    position: &mut usize,
    length: usize,
) -> Result<Vec<u8>, StoreError> {
    read_run(bytes, position, length).map(<[u8]>::to_vec)
}

/// Borrows the next `length` bytes, advancing the cursor past them.
fn read_run<'bytes>(
    bytes: &'bytes [u8],
    position: &mut usize,
    length: usize,
) -> Result<&'bytes [u8], StoreError> {
    let end = position
        .checked_add(length)
        .ok_or(StoreError::InvalidExport)?;
    let run = bytes.get(*position..end).ok_or(StoreError::InvalidExport)?;
    *position = end;
    Ok(run)
}

/// Reads one big-endian `u32`, advancing the cursor.
pub(crate) fn read_u32(bytes: &[u8], position: &mut usize) -> Result<u32, StoreError> {
    read_array(bytes, position).map(u32::from_be_bytes)
}
