//! `FoundationDB` service adapter with private tuple subspaces and chunked values.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S42 S43. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::expect_used, clippy::unwrap_used))]

#[cfg(test)]
use foundationdb_tuple::Subspace;
#[cfg(test)]
use logical_db_model::SnapshotArtifact;
#[cfg(any(feature = "fdb", test))]
use logical_db_model::{
    ChangeDigest, DocumentId, MAX_BASELINE_BYTES, MAX_CHANGE_BYTES, MAX_CHANGES_PER_PAGE,
    MAX_PAGE_BYTES, OperationDigest, OperationId, ReplicaId, Scope, SnapshotBaseline,
    SnapshotDocument, SnapshotRecord,
};
#[cfg(any(feature = "fdb", test))]
use logical_db_protocol::ChangeEnvelope;
#[cfg(any(feature = "fdb", test))]
use thiserror::Error;

#[cfg(any(feature = "fdb", test))]
/// Malformed data
fn malformed_data<E: std::fmt::Debug>(_cause: E) -> FdbDataError {
    FdbDataError::InvalidManifest { at: "manifest" }
}

#[cfg(test)]
/// Malformed baseline
fn malformed_baseline<E: std::fmt::Debug>(_cause: E) -> FdbDataError {
    FdbDataError::BaselineCorrupt
}

#[cfg(any(feature = "fdb", test))]
/// FORMAT VERSION
const FORMAT_VERSION: i64 = 3;
#[cfg(any(feature = "fdb", test))]
/// KIND LOG
const KIND_LOG: i64 = 3;
#[cfg(any(feature = "fdb", test))]
/// CHUNK BYTES
const CHUNK_BYTES: usize = 65_536;
#[cfg(any(feature = "fdb", test))]
/// MANIFEST BYTES
const MANIFEST_BYTES: usize = 118;
#[cfg(any(feature = "fdb", test))]
/// OPERATION BYTES
const OPERATION_BYTES: usize = 158;
#[cfg(any(feature = "fdb", test))]
/// RETENTION VERSION
const RETENTION_VERSION: u16 = 1;
#[cfg(any(feature = "fdb", test))]
/// RETENTION BYTES
const RETENTION_BYTES: usize = 18;
#[cfg(feature = "fdb")]
/// MAX BASELINE CHANGES
const MAX_BASELINE_CHANGES: usize = 4_096;
#[cfg(any(feature = "fdb", test))]
/// BASELINE DOCUMENT VERSION
const BASELINE_DOCUMENT_VERSION: u16 = 1;
#[cfg(any(feature = "fdb", test))]
/// BASELINE DOCUMENT MANIFEST BYTES
const BASELINE_DOCUMENT_MANIFEST_BYTES: usize = 41;

#[cfg(any(feature = "fdb", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// BaselineDocumentManifest
struct BaselineDocumentManifest {
    /// tombstone
    tombstone: bool,
    /// Byte len
    byte_len: usize,
    /// chunks
    chunks: u16,
    /// digest
    digest: [u8; 32],
}

#[cfg(any(feature = "fdb", test))]
/// Encode baseline document
fn encode_baseline_document(document: &SnapshotDocument) -> Result<[u8; 41], FdbDataError> {
    use lgwks_std::hash::blake3;

    if document.snapshot().len() > MAX_BASELINE_BYTES {
        return Err(FdbDataError::BaselineResourceLimit);
    }
    let byte_len = u32::try_from(document.snapshot().len()).map_err(malformed_data)?;
    let chunks =
        chunk_count(document.snapshot().len()).ok_or(FdbDataError::BaselineResourceLimit)?;
    let mut bytes = [0; BASELINE_DOCUMENT_MANIFEST_BYTES];
    bytes[..2].copy_from_slice(&BASELINE_DOCUMENT_VERSION.to_be_bytes());
    bytes[2] = u8::from(document.is_tombstone());
    bytes[3..7].copy_from_slice(&byte_len.to_be_bytes());
    bytes[7..9].copy_from_slice(&chunks.to_be_bytes());
    bytes[9..].copy_from_slice(blake3(document.snapshot()).as_bytes());
    Ok(bytes)
}

#[cfg(any(feature = "fdb", test))]
/// Decode baseline document
fn decode_baseline_document(bytes: &[u8]) -> Result<BaselineDocumentManifest, FdbDataError> {
    let mut fields = Fields(bytes);
    let (Some(version), Some([tombstone]), Some(byte_len), Some(chunks), Some(digest), true) = (
        fields.take::<2>(),
        fields.take::<1>(),
        fields.take::<4>(),
        fields.take::<2>(),
        fields.take::<32>(),
        fields.is_done(),
    ) else {
        return Err(FdbDataError::BaselineCorrupt);
    };
    if u16::from_be_bytes(version) != BASELINE_DOCUMENT_VERSION || tombstone > 1 {
        return Err(FdbDataError::BaselineCorrupt);
    }
    let byte_len = usize::try_from(u32::from_be_bytes(byte_len)).map_err(malformed_data)?;
    let chunks = u16::from_be_bytes(chunks);
    if byte_len > MAX_BASELINE_BYTES || chunk_count(byte_len) != Some(chunks) {
        return Err(FdbDataError::BaselineCorrupt);
    }
    Ok(BaselineDocumentManifest {
        tombstone: tombstone == 1,
        byte_len,
        chunks,
        digest,
    })
}

#[cfg(any(feature = "fdb", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// PageReadPlan
struct PageReadPlan {
    /// limit
    limit: usize,
    /// Target bytes
    target_bytes: usize,
}

#[cfg(any(feature = "fdb", test))]
/// Page read plan
fn page_read_plan(max_changes: u16) -> PageReadPlan {
    PageReadPlan {
        limit: usize::from(max_changes) + 1,
        target_bytes: usize::from(max_changes) * MANIFEST_BYTES,
    }
}

#[cfg(any(feature = "fdb", test))]
/// Valid page limits
fn valid_page_limits(max_changes: u16, max_bytes: u32) -> bool {
    max_changes != 0
        && usize::from(max_changes) <= MAX_CHANGES_PER_PAGE
        && max_bytes != 0
        && matches!(usize::try_from(max_bytes), Ok(bytes) if bytes <= MAX_PAGE_BYTES)
}

#[cfg(any(feature = "fdb", test))]
/// Logical page more
fn logical_page_more(
    frontier: u64,
    position: u64,
    range_more: bool,
    stopped_at_page_boundary: bool,
) -> Result<bool, DataError> {
    if position > frontier || (stopped_at_page_boundary && position == frontier) {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    let more = position < frontier;
    if more && !range_more && !stopped_at_page_boundary {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    Ok(more)
}

#[cfg(any(feature = "fdb", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Record
struct Record {
    /// document
    document: DocumentId,
    /// Replica
    replica: ReplicaId,
    /// operation
    operation: OperationId,
    /// request
    request: OperationDigest,
    /// digest
    digest: ChangeDigest,
    /// byte_len
    byte_len: usize,
    /// chunks
    chunks: u16,
    /// sequence
    sequence: u64,
}

#[cfg(any(feature = "fdb", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// OperationRecord
struct OperationRecord {
    /// record
    record: Record,
    /// Invocation id
    invocation_id: [u8; 32],
}

#[cfg(any(feature = "fdb", test))]
impl Record {
    /// Matches
    fn matches(self, change: &ChangeEnvelope) -> bool {
        self.document == change.document()
            && self.replica == change.replica()
            && self.operation == change.operation()
            && self.request == change.request()
            && self.digest == change.digest()
            && self.byte_len == change.bytes().len()
            && chunk_count(change.bytes().len()) == Some(self.chunks)
    }

    /// Envelope
    fn envelope(self, scope: Scope, bytes: Vec<u8>) -> ChangeEnvelope {
        ChangeEnvelope::new(
            scope,
            self.document,
            self.replica,
            self.operation,
            self.request,
            self.digest,
            bytes,
        )
    }
}

#[cfg(any(feature = "fdb", test))]
/// Encode manifest
fn encode_manifest(
    change: &ChangeEnvelope,
    sequence: u64,
    invocation_id: [u8; 32],
) -> Result<[u8; OPERATION_BYTES], DataError> {
    if !(1..=MAX_CHANGE_BYTES).contains(&change.bytes().len()) {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    let byte_len = u32::try_from(change.bytes().len()).map_err(malformed_data)?;
    let chunks =
        chunk_count(change.bytes().len()).ok_or(DataError::InvalidManifest { at: "chunks" })?;
    let mut bytes = [0; OPERATION_BYTES];
    bytes[..16].copy_from_slice(change.document().as_uuid().as_bytes());
    bytes[16..32].copy_from_slice(change.replica().as_uuid().as_bytes());
    bytes[32..48].copy_from_slice(change.operation().as_uuid().as_bytes());
    bytes[48..80].copy_from_slice(&change.request().into_bytes());
    bytes[80..112].copy_from_slice(&change.digest().into_bytes());
    bytes[112..116].copy_from_slice(&byte_len.to_be_bytes());
    bytes[116..118].copy_from_slice(&chunks.to_be_bytes());
    bytes[118..126].copy_from_slice(&sequence.to_be_bytes());
    bytes[126..158].copy_from_slice(&invocation_id);
    Ok(bytes)
}

#[cfg(any(feature = "fdb", test))]
/// Decode manifest
fn decode_manifest(bytes: &[u8], sequence: u64) -> Result<Record, DataError> {
    let mut fields = Fields(bytes);
    let (Some(manifest), true) = (fields.take::<MANIFEST_BYTES>(), fields.is_done()) else {
        return Err(DataError::InvalidManifest { at: "manifest" });
    };
    decode_record(&manifest, sequence)
}

#[cfg(any(feature = "fdb", test))]
/// Decodes one 158-byte operation record: manifest, sequence, invocation id.
fn decode_operation(bytes: &[u8]) -> Result<OperationRecord, DataError> {
    let mut fields = Fields(bytes);
    let (Some(manifest), Some(sequence), Some(invocation_id), true) = (
        fields.take::<MANIFEST_BYTES>(),
        fields.take::<8>(),
        fields.take::<32>(),
        fields.is_done(),
    ) else {
        return Err(DataError::InvalidManifest { at: "manifest" });
    };
    Ok(OperationRecord {
        record: decode_record(&manifest, u64::from_be_bytes(sequence))?,
        invocation_id,
    })
}

#[cfg(test)]
use std::collections::BTreeMap;

#[cfg(any(feature = "fdb", test))]
/// Encode retention
fn encode_retention(floor: u64, consumed_through: u64) -> [u8; RETENTION_BYTES] {
    let mut bytes = [0; RETENTION_BYTES];
    bytes[..2].copy_from_slice(&RETENTION_VERSION.to_be_bytes());
    bytes[2..10].copy_from_slice(&floor.to_be_bytes());
    bytes[10..18].copy_from_slice(&consumed_through.to_be_bytes());
    bytes
}

#[cfg(any(feature = "fdb", test))]
/// Decode retention
fn decode_retention(bytes: &[u8]) -> Result<(u64, u64), DataError> {
    let mut fields = Fields(bytes);
    let (Some(version), Some(floor), Some(consumed), true) = (
        fields.take::<2>(),
        fields.take::<8>(),
        fields.take::<8>(),
        fields.is_done(),
    ) else {
        return Err(DataError::InvalidManifest { at: "manifest" });
    };
    if u16::from_be_bytes(version) != RETENTION_VERSION {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    Ok((u64::from_be_bytes(floor), u64::from_be_bytes(consumed)))
}

#[cfg(any(feature = "fdb", test))]
/// Decode snapshot
fn decode_snapshot(bytes: &[u8]) -> Result<SnapshotRecord, DataError> {
    SnapshotBaseline::decode(bytes).map_err(malformed_data)
}

/// Durable collection view mirroring one scope's tuple subspaces.
#[cfg(test)]
/// CollectionView
struct CollectionView<'a> {
    /// Log sequence to manifest bytes.
    manifests: &'a BTreeMap<u64, Vec<u8>>,
    /// Log sequence and chunk index to chunk bytes.
    chunks: &'a BTreeMap<(u64, u16), Vec<u8>>,
    /// Operation identity to operation record bytes.
    operations: &'a BTreeMap<OperationId, Vec<u8>>,
    /// Invocation identity to invocation binding bytes.
    invocations: &'a BTreeMap<[u8; 32], Vec<u8>>,
}

#[cfg(test)]
/// BaselineAtlas
struct BaselineAtlas {
    /// manifests
    manifests: BTreeMap<([u8; 32], DocumentId), Vec<u8>>,
    /// Chunks
    chunks: BTreeMap<([u8; 32], DocumentId, u16), Vec<u8>>,
}

#[cfg(test)]
/// Validate baseline atlas
fn validate_baseline_atlas(
    scope: Scope,
    baseline: &SnapshotBaseline,
    atlas: &BaselineAtlas,
) -> Result<SnapshotArtifact, DataError> {
    use lgwks_std::hash::blake3;

    let digest = baseline.digest().into_bytes();
    let mut documents = Vec::new();
    for (&(artifact, document), bytes) in atlas.manifests.iter() {
        if artifact != digest {
            continue;
        }
        let manifest = decode_baseline_document(bytes)?;
        let mut snapshot = Vec::with_capacity(manifest.byte_len);
        for index in 0..manifest.chunks {
            let chunk = atlas
                .chunks
                .get(&(digest, document, index))
                .ok_or(DataError::BaselineCorrupt)?;
            if chunk.is_empty() || chunk.len() > CHUNK_BYTES {
                return Err(DataError::BaselineCorrupt);
            }
            snapshot.extend_from_slice(chunk);
        }
        if snapshot.len() != manifest.byte_len || *blake3(&snapshot).as_bytes() != manifest.digest {
            return Err(DataError::BaselineCorrupt);
        }
        documents.push(SnapshotDocument::new(
            document,
            snapshot,
            manifest.tombstone,
        ));
    }
    let artifact =
        SnapshotArtifact::new(scope, baseline.position(), documents).map_err(malformed_baseline)?;
    if artifact.baseline() != baseline {
        return Err(DataError::BaselineCorrupt);
    }
    Ok(artifact)
}

#[cfg(test)]
/// Check collection consistency
fn check_collection_consistency(view: &CollectionView<'_>) -> Result<(), DataError> {
    for (sequence, manifest) in view.manifests {
        let record = validate_record_links(view, *sequence, manifest)?;
        validate_chunks(view, *sequence, record)?;
    }
    Ok(())
}

#[cfg(test)]
/// Validate record links
fn validate_record_links(
    view: &CollectionView<'_>,
    sequence: u64,
    manifest: &[u8],
) -> Result<Record, DataError> {
    let record = decode_manifest(manifest, sequence)?;
    let operation_bytes = view
        .operations
        .get(&record.operation)
        .ok_or(DataError::InvalidManifest { at: "manifest" })?;
    let operation = decode_operation(operation_bytes)?;
    if operation.record != record {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    let invocation = view
        .invocations
        .get(&operation.invocation_id)
        .ok_or(DataError::MissingInvocation)?;
    if invocation.as_slice() != invocation_binding_record(&record) {
        return Err(DataError::InvalidInvocation);
    }
    Ok(record)
}

#[cfg(test)]
/// Validate chunks
fn validate_chunks(
    view: &CollectionView<'_>,
    sequence: u64,
    record: Record,
) -> Result<(), DataError> {
    let mut total = 0usize;
    for index in 0..record.chunks {
        let chunk = view
            .chunks
            .get(&(sequence, index))
            .ok_or(DataError::MissingChunk)?;
        if chunk.is_empty() || chunk.len() > CHUNK_BYTES {
            return Err(DataError::InvalidChunk);
        }
        total = total
            .checked_add(chunk.len())
            .ok_or(DataError::InvalidManifest { at: "manifest" })?;
    }
    if total != record.byte_len {
        return Err(DataError::InvalidChunk);
    }
    Ok(())
}

#[cfg(any(feature = "fdb", test))]
/// Invocation binding record
fn invocation_binding_record(record: &Record) -> [u8; 80] {
    let mut binding = [0_u8; 80];
    binding[..16].copy_from_slice(record.operation.as_uuid().as_bytes());
    binding[16..48].copy_from_slice(&record.request.into_bytes());
    binding[48..].copy_from_slice(&record.digest.into_bytes());
    binding
}

#[cfg(any(feature = "fdb", test))]
/// Invocation binding
fn invocation_binding(change: &ChangeEnvelope) -> [u8; 80] {
    let mut binding = [0_u8; 80];
    binding[..16].copy_from_slice(change.operation().as_uuid().as_bytes());
    binding[16..48].copy_from_slice(&change.request().into_bytes());
    binding[48..].copy_from_slice(&change.digest().into_bytes());
    binding
}

#[cfg(any(feature = "fdb", test))]
/// Reads fixed-width fields front to back; every read is bounds-checked, so a
/// short or long value is refused instead of indexed.
#[derive(Debug)]
struct Fields<'a>(&'a [u8]);

#[cfg(any(feature = "fdb", test))]
impl Fields<'_> {
    /// The next `N` bytes, or `None` when fewer remain.
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let (head, rest) = self.0.split_first_chunk::<N>()?;
        self.0 = rest;
        Some(*head)
    }

    /// Whether every byte was read.
    fn is_done(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(any(feature = "fdb", test))]
/// Decodes one 118-byte log manifest written by `encode_manifest`.
fn decode_record(bytes: &[u8; MANIFEST_BYTES], sequence: u64) -> Result<Record, DataError> {
    let mut fields = Fields(bytes);
    let (Some(document), Some(replica), Some(operation), Some(request), Some(digest)) = (
        fields.take::<16>(),
        fields.take::<16>(),
        fields.take::<16>(),
        fields.take::<32>(),
        fields.take::<32>(),
    ) else {
        return Err(DataError::InvalidManifest { at: "manifest" });
    };
    let (Some(byte_len), Some(chunks), true) =
        (fields.take::<4>(), fields.take::<2>(), fields.is_done())
    else {
        return Err(DataError::InvalidManifest { at: "manifest" });
    };
    let byte_len = usize::try_from(u32::from_be_bytes(byte_len)).map_err(malformed_data)?;
    let chunks = u16::from_be_bytes(chunks);
    if !(1..=MAX_CHANGE_BYTES).contains(&byte_len) || chunk_count(byte_len) != Some(chunks) {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    Ok(Record {
        document: DocumentId::from_uuid(lgwks_std::id::Uuid::from(document)),
        replica: ReplicaId::from_uuid(lgwks_std::id::Uuid::from(replica)),
        operation: OperationId::from_uuid(lgwks_std::id::Uuid::from(operation)),
        request: OperationDigest::new(request),
        digest: ChangeDigest::new(digest),
        byte_len,
        chunks,
        sequence,
    })
}

#[cfg(any(feature = "fdb", test))]
/// Number of `CHUNK_BYTES` chunks that hold `byte_len` bytes; `None` when the
/// count does not fit the 16-bit manifest field, so no length saturates.
fn chunk_count(byte_len: usize) -> Option<u16> {
    u16::try_from(byte_len.div_ceil(CHUNK_BYTES)).ok()
}

#[cfg(any(feature = "fdb", test))]
/// The tuple-layer form of an estate identifier: the same sixteen bytes, so
/// every packed key is byte-identical to the keys written before.
fn tuple_id(id: lgwks_std::id::Uuid) -> foundationdb_tuple::Uuid {
    foundationdb_tuple::Uuid::from_bytes(*id.as_bytes())
}

#[cfg(any(feature = "fdb", test))]
/// The estate identifier a tuple-layer UUID carries.
fn estate_id(id: foundationdb_tuple::Uuid) -> lgwks_std::id::Uuid {
    lgwks_std::id::Uuid::from(*id.as_bytes())
}

#[cfg(any(feature = "fdb", test))]
/// Decodes one stored big-endian position value.
fn decode_u64(bytes: &[u8]) -> Result<u64, DataError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(malformed_data)
}

#[cfg(any(feature = "fdb", test))]
/// Stable Logical-DB corruption and resource failures decoded inside an FDB transaction.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum FdbDataError {
    /// Snapshot construction exceeded the bounded change, document, or byte budget.
    #[error("FoundationDB snapshot baseline exceeds its resource budget")]
    BaselineResourceLimit,
    /// Snapshot documents do not exactly cover the immutable log prefix.
    #[error("FoundationDB snapshot baseline does not cover the requested log prefix")]
    BaselineCoverage,
    /// Snapshot pointer, document manifest, chunks, or aggregate digest is corrupt.
    #[error("FoundationDB snapshot baseline artifact is corrupt")]
    BaselineCorrupt,
    /// A state-bearing baseline does not reach the requested prune position.
    #[error("FoundationDB snapshot baseline is missing or behind the prune position")]
    BaselineBehind,
    /// Operation or log manifest violated its fixed binary format.
    #[error("FoundationDB service manifest is malformed")]
    InvalidManifest {
        /// Stable diagnostic location for malformed manifest bytes.
        at: &'static str,
    },
    #[cfg(feature = "fdb")]
    #[error("FoundationDB service sequence is exhausted")]
    /// Monotonic scoped log position exhausted its signed tuple domain.
    SequenceExhausted,
    #[error("FoundationDB service change chunk is missing")]
    /// Manifest referenced a chunk key that did not exist.
    MissingChunk,
    #[error("FoundationDB service change chunk is malformed")]
    /// Chunk size, count, or aggregate length did not match its manifest.
    InvalidChunk,
    #[error("FoundationDB service invocation marker is missing")]
    /// Operation record referenced no atomically written invocation marker.
    MissingInvocation,
    #[error("FoundationDB service invocation marker is malformed")]
    /// Invocation marker did not bind operation, request, and change identity.
    InvalidInvocation,
    #[cfg(feature = "fdb")]
    #[error("one stored change exceeds the requested pull byte ceiling")]
    /// A single durable change could not fit the requested bounded page.
    ChangeExceedsPage,
}

#[cfg(any(feature = "fdb", test))]
/// DataError
type DataError = FdbDataError;

#[cfg(feature = "fdb")]
mod runtime;

#[cfg(feature = "fdb")]
pub use runtime::{FdbServiceStore, FdbStoreError};

/// The `FoundationDB` client API this adapter is built on.
#[cfg(feature = "fdb")]
pub use foundationdb;
/// Order-preserving tuple keys, this adapter's keyspace contract.
pub use foundationdb_tuple;

#[cfg(test)]
mod tests;
