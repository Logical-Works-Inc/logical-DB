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

    if document.snapshot.len() > MAX_BASELINE_BYTES {
        return Err(FdbDataError::BaselineResourceLimit);
    }
    let byte_len = u32::try_from(document.snapshot.len()).map_err(malformed_data)?;
    let chunks = chunk_count(document.snapshot.len());
    let mut bytes = [0; BASELINE_DOCUMENT_MANIFEST_BYTES];
    bytes[..2].copy_from_slice(&BASELINE_DOCUMENT_VERSION.to_be_bytes());
    bytes[2] = u8::from(document.tombstone);
    bytes[3..7].copy_from_slice(&byte_len.to_be_bytes());
    bytes[7..9].copy_from_slice(&chunks.to_be_bytes());
    bytes[9..].copy_from_slice(blake3(&document.snapshot).as_bytes());
    Ok(bytes)
}

#[cfg(any(feature = "fdb", test))]
/// Decode baseline document
fn decode_baseline_document(bytes: &[u8]) -> Result<BaselineDocumentManifest, FdbDataError> {
    if bytes.len() != BASELINE_DOCUMENT_MANIFEST_BYTES
        || decode_u16(&bytes[..2])? != BASELINE_DOCUMENT_VERSION
        || bytes[2] > 1
    {
        return Err(FdbDataError::BaselineCorrupt);
    }
    let byte_len = usize::try_from(decode_u32(&bytes[3..7])?).map_err(malformed_data)?;
    let chunks = decode_u16(&bytes[7..9])?;
    if byte_len > MAX_BASELINE_BYTES || chunks != chunk_count(byte_len) {
        return Err(FdbDataError::BaselineCorrupt);
    }
    Ok(BaselineDocumentManifest {
        tombstone: bytes[2] == 1,
        byte_len,
        chunks,
        digest: bytes[9..].try_into().map_err(malformed_data)?,
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
        && usize::try_from(max_bytes).unwrap_or(usize::MAX) <= MAX_PAGE_BYTES
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
        self.document == change.document
            && self.replica == change.replica
            && self.operation == change.operation
            && self.request == change.request
            && self.digest == change.digest
            && self.byte_len == change.bytes.len()
            && self.chunks == chunk_count(change.bytes.len())
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
    if !(1..=MAX_CHANGE_BYTES).contains(&change.bytes.len()) {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    let byte_len = u32::try_from(change.bytes.len()).map_err(malformed_data)?;
    let chunks = chunk_count(change.bytes.len());
    let mut bytes = [0; OPERATION_BYTES];
    bytes[..16].copy_from_slice(change.document.as_uuid().as_bytes());
    bytes[16..32].copy_from_slice(change.replica.as_uuid().as_bytes());
    bytes[32..48].copy_from_slice(change.operation.as_uuid().as_bytes());
    bytes[48..80].copy_from_slice(&change.request.into_bytes());
    bytes[80..112].copy_from_slice(&change.digest.into_bytes());
    bytes[112..116].copy_from_slice(&byte_len.to_be_bytes());
    bytes[116..118].copy_from_slice(&chunks.to_be_bytes());
    bytes[118..126].copy_from_slice(&sequence.to_be_bytes());
    bytes[126..158].copy_from_slice(&invocation_id);
    Ok(bytes)
}

#[cfg(any(feature = "fdb", test))]
/// Decode manifest
fn decode_manifest(bytes: &[u8], sequence: u64) -> Result<Record, DataError> {
    if bytes.len() != MANIFEST_BYTES {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    decode_record(bytes, sequence)
}

#[cfg(any(feature = "fdb", test))]
/// Decode operation
fn decode_operation(bytes: &[u8]) -> Result<OperationRecord, DataError> {
    if bytes.len() != OPERATION_BYTES {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    let record = decode_record(&bytes[..MANIFEST_BYTES], decode_u64(&bytes[118..126])?)?;
    let invocation_id = bytes[126..158].try_into().map_err(malformed_data)?;
    Ok(OperationRecord {
        record,
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
    if bytes.len() != RETENTION_BYTES {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    if decode_u16(&bytes[..2])? != RETENTION_VERSION {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    Ok((decode_u64(&bytes[2..10])?, decode_u64(&bytes[10..18])?))
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
    if &artifact.baseline != baseline {
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
    binding[..16].copy_from_slice(change.operation.as_uuid().as_bytes());
    binding[16..48].copy_from_slice(&change.request.into_bytes());
    binding[48..].copy_from_slice(&change.digest.into_bytes());
    binding
}

#[cfg(any(feature = "fdb", test))]
/// Decode record
fn decode_record(bytes: &[u8], sequence: u64) -> Result<Record, DataError> {
    let document = decode_uuid(&bytes[..16]).map(DocumentId::from_uuid)?;
    let replica = decode_uuid(&bytes[16..32]).map(ReplicaId::from_uuid)?;
    let operation = decode_uuid(&bytes[32..48]).map(OperationId::from_uuid)?;
    let request = bytes[48..80]
        .try_into()
        .map(OperationDigest::new)
        .map_err(malformed_data)?;
    let digest = bytes[80..112]
        .try_into()
        .map(ChangeDigest::new)
        .map_err(malformed_data)?;
    let byte_len = usize::try_from(decode_u32(&bytes[112..116])?).map_err(malformed_data)?;
    let chunks = decode_u16(&bytes[116..118])?;
    if !(1..=MAX_CHANGE_BYTES).contains(&byte_len) || chunks != chunk_count(byte_len) {
        return Err(DataError::InvalidManifest { at: "manifest" });
    }
    Ok(Record {
        document,
        replica,
        operation,
        request,
        digest,
        byte_len,
        chunks,
        sequence,
    })
}

#[cfg(any(feature = "fdb", test))]
/// Chunk count
fn chunk_count(byte_len: usize) -> u16 {
    let chunks = byte_len.div_ceil(CHUNK_BYTES);
    u16::try_from(chunks).unwrap_or(u16::MAX)
}

#[cfg(any(feature = "fdb", test))]
/// Decode uuid
fn decode_uuid(bytes: &[u8]) -> Result<uuid::Uuid, DataError> {
    uuid::Uuid::from_slice(bytes).map_err(malformed_data)
}

#[cfg(any(feature = "fdb", test))]
/// Decode u64
fn decode_u64(bytes: &[u8]) -> Result<u64, DataError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(malformed_data)
}

#[cfg(any(feature = "fdb", test))]
/// Decode u32
fn decode_u32(bytes: &[u8]) -> Result<u32, DataError> {
    bytes
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(malformed_data)
}

#[cfg(any(feature = "fdb", test))]
/// Decode u16
fn decode_u16(bytes: &[u8]) -> Result<u16, DataError> {
    bytes
        .try_into()
        .map(u16::from_be_bytes)
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
/// runtime internals.
mod runtime {
    use super::{
        BaselineDocumentManifest, CHUNK_BYTES, DataError, FORMAT_VERSION, KIND_LOG, MANIFEST_BYTES,
        MAX_BASELINE_CHANGES, Record, decode_baseline_document, decode_manifest, decode_operation,
        decode_retention, decode_snapshot, decode_u64, encode_baseline_document, encode_manifest,
        encode_retention, invocation_binding, invocation_binding_record, logical_page_more,
        page_read_plan, valid_page_limits,
    };
    use foundationdb::{
        Database, FdbBindingError, KeySelector, RangeOption, options::TransactionOption,
    };
    use foundationdb_tuple::Subspace;
    use logical_db_model::{
        DocumentId, Effect, MAX_BASELINE_BYTES, MAX_BASELINE_PAGE_DOCUMENTS, OperationDigest,
        Scope, SnapshotArtifact, SnapshotBaseline, SnapshotDocument, SnapshotRecord,
    };
    use logical_db_protocol::{
        ChangeEnvelope, PageAction, RecoveryAction, mark_consumed_action, prune_action,
        seal_snapshot_action, write_action,
    };
    use logical_db_service::{
        AcceptResult, BaselinePageResult, BaselinePageSlice, ConsumedResult, ExecutionGrant,
        PageResult, PruneReceipt, PruneRefusal, PruneResult, RetentionStore, SealResult,
        ServiceStore, SnapshotMaterializationError, StoreFailureClass, StoredPage,
        materialize_snapshot, prune_decision,
    };
    use thiserror::Error;

    /// KIND OPERATION
    const KIND_OPERATION: i64 = 1;
    /// KIND SEQUENCE
    const KIND_SEQUENCE: i64 = 2;
    /// KIND CHUNK
    const KIND_CHUNK: i64 = 4;
    /// KIND INVOCATION
    const KIND_INVOCATION: i64 = 5;
    /// KIND RETENTION
    const KIND_RETENTION: i64 = 6;
    /// KIND SNAPSHOT
    const KIND_SNAPSHOT: i64 = 7;
    /// KIND SNAPSHOT DOCUMENT
    const KIND_SNAPSHOT_DOCUMENT: i64 = 8;
    /// KIND SNAPSHOT CHUNK
    const KIND_SNAPSHOT_CHUNK: i64 = 9;
    /// TRANSACTION TIMEOUT MS
    const TRANSACTION_TIMEOUT_MS: i32 = 4_000;
    /// TRANSACTION RETRY LIMIT
    const TRANSACTION_RETRY_LIMIT: i32 = 8;

    #[derive(Clone)]
    /// PageSpaces
    struct PageSpaces {
        /// scope
        scope: Subspace,
        /// Sequence key
        sequence_key: Vec<u8>,
        /// retention_key
        retention_key: Vec<u8>,
        /// log
        log: Subspace,
        /// chunk
        chunk: Subspace,
    }

    #[derive(Clone, Copy)]
    /// PageQuery
    struct PageQuery {
        /// scope
        scope: Scope,
        /// Frontier
        frontier: u64,
        /// after
        after: u64,
        /// max_changes
        max_changes: u16,
        /// max_bytes
        max_bytes: u32,
    }

    #[derive(Clone)]
    /// BaselineSpaces
    struct BaselineSpaces {
        /// scope
        scope: Subspace,
        /// Pointer key
        pointer_key: Vec<u8>,
        /// log
        log: Subspace,
        /// change_chunk
        change_chunk: Subspace,
        /// document
        document: Subspace,
        /// snapshot_chunk
        snapshot_chunk: Subspace,
    }

    /// Async multi-tenant service store over a host-booted `FoundationDB` client.
    ///
    /// The host allocates `root` through the directory layer and owns the network
    /// lifecycle. Logical-DB never exposes physical tuple bytes to applications.
    pub struct FdbServiceStore {
        /// database
        database: Database,
        /// Root
        root: Subspace,
    }

    impl FdbServiceStore {
        /// Binds a directory-allocated root subspace without mutating host-wide options.
        ///
        /// # Errors
        ///
        /// Returns the native option failure or refuses an empty global root.
        pub fn new(database: Database, root: Subspace) -> Result<Self, FdbStoreError> {
            if root.bytes().is_empty() {
                return Err(FdbStoreError::UnallocatedRoot);
            }
            Ok(Self { database, root })
        }

        /// Scope space
        fn scope_space(&self, scope: Scope) -> Subspace {
            self.root.subspace(&(
                FORMAT_VERSION,
                scope.account().as_uuid(),
                scope.space().as_uuid(),
            ))
        }
    }

    impl ServiceStore for FdbServiceStore {
        /// Error
        type Error = FdbStoreError;

        /// Classify error
        fn classify_error(error: &Self::Error) -> StoreFailureClass {
            match *error {
                FdbStoreError::CommitMayHaveSucceeded { .. }
                | FdbStoreError::Cancelled { .. }
                | FdbStoreError::VersionSkew { .. }
                | FdbStoreError::RetryExhausted { .. } => StoreFailureClass::Transient,
                FdbStoreError::Client(_)
                | FdbStoreError::UnallocatedRoot
                | FdbStoreError::InvalidChangeEnvelope
                | FdbStoreError::InvalidPageLimits
                | FdbStoreError::Corruption(_)
                | FdbStoreError::Native { .. }
                | FdbStoreError::BindingInvariant(_) => StoreFailureClass::Permanent,
            }
        }

        /// Runs `accept`.
        async fn accept(
            &self,
            change: &ChangeEnvelope,
            grant: &impl ExecutionGrant,
        ) -> Result<AcceptResult, Self::Error> {
            if change.validate().is_err() {
                return Err(FdbStoreError::InvalidChangeEnvelope);
            }
            if grant.scope() != change.scope
                || grant.effect() != Effect::Write
                || grant.action() != write_action(change)
            {
                return Ok(AcceptResult::StaleAuthority);
            }
            let expected_frontier = grant.frontier();
            let invocation_id = grant.invocation_id();
            let change = change.clone();
            let scope_space = self.scope_space(change.scope);
            let operation_key = scope_space.pack(&(KIND_OPERATION, change.operation.as_uuid()));
            let sequence_key = scope_space.pack(&(KIND_SEQUENCE,));
            let retention_key = scope_space.pack(&(KIND_RETENTION,));
            let invocation_key = scope_space.pack(&(KIND_INVOCATION, invocation_id.as_slice()));
            let log_space = scope_space.subspace(&(KIND_LOG,));
            let chunk_space = scope_space.subspace(&(KIND_CHUNK,));
            self.database
                .run(move |transaction, maybe_committed| {
                    let change = change.clone();
                    let operation_key = operation_key.clone();
                    let sequence_key = sequence_key.clone();
                    let retention_key = retention_key.clone();
                    let invocation_key = invocation_key.clone();
                    let scope_space = scope_space.clone();
                    let log_space = log_space.clone();
                    let chunk_space = chunk_space.clone();
                    async move {
                        let operation = AcceptOperation {
                            expected_frontier,
                            invocation_id,
                            change,
                            maybe_committed: bool::from(maybe_committed),
                            operation_key,
                            sequence_key,
                            retention_key,
                            invocation_key,
                            scope_space,
                            log_space,
                            chunk_space,
                        };
                        accept_transaction(&transaction, operation).await
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        /// Runs `frontier`.
        async fn frontier(&self, scope: Scope) -> Result<u64, Self::Error> {
            let sequence_key = self.scope_space(scope).pack(&(KIND_SEQUENCE,));
            self.database
                .run(move |transaction, _maybe_committed| {
                    let sequence_key = sequence_key.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        transaction
                            .get(&sequence_key, false)
                            .await?
                            .map(|value| decode_u64(value.as_ref()).map_err(binding))
                            .transpose()
                            .map(|value| value.unwrap_or(0))
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        /// Runs `retention_floor`.
        async fn retention_floor(&self, scope: Scope) -> Result<u64, Self::Error> {
            let retention_key = self.scope_space(scope).pack(&(KIND_RETENTION,));
            self.database
                .run(move |transaction, _maybe_committed| {
                    let retention_key = retention_key.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        transaction
                            .get(&retention_key, false)
                            .await?
                            .map(|value| {
                                decode_retention(value.as_ref())
                                    .map(|(floor, _)| floor)
                                    .map_err(binding)
                            })
                            .transpose()
                            .map(|value| value.unwrap_or(0))
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        /// Runs `page`.
        async fn page(
            &self,
            action: PageAction,
            grant: &impl ExecutionGrant,
        ) -> Result<PageResult, Self::Error> {
            if !valid_page_limits(action.max_changes, action.max_bytes) {
                return Err(FdbStoreError::InvalidPageLimits);
            }
            if grant.scope() != action.scope
                || grant.effect() != Effect::Sync
                || grant.action() != action.digest()
            {
                return Ok(PageResult::StaleAuthority);
            }
            let expected_frontier = grant.frontier();
            let scope_space = self.scope_space(action.scope);
            let spaces = PageSpaces {
                sequence_key: scope_space.pack(&(KIND_SEQUENCE,)),
                retention_key: scope_space.pack(&(KIND_RETENTION,)),
                log: scope_space.subspace(&(KIND_LOG,)),
                chunk: scope_space.subspace(&(KIND_CHUNK,)),
                scope: scope_space,
            };
            let query = PageQuery {
                scope: action.scope,
                frontier: expected_frontier,
                after: action.after,
                max_changes: action.max_changes,
                max_bytes: action.max_bytes,
            };
            self.database
                .run(move |transaction, _maybe_committed| {
                    let spaces = spaces.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        load_page(&transaction, &spaces, query).await
                    }
                })
                .await
                .map_err(classify_transaction)
        }
    }

    impl RetentionStore for FdbServiceStore {
        /// Runs `consumed_through`.
        async fn consumed_through(&self, scope: Scope) -> Result<u64, Self::Error> {
            let retention_key = self.scope_space(scope).pack(&(KIND_RETENTION,));
            self.database
                .run(move |transaction, _maybe_committed| {
                    let retention_key = retention_key.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        transaction
                            .get(&retention_key, false)
                            .await?
                            .map(|value| {
                                decode_retention(value.as_ref())
                                    .map(|(_, consumed)| consumed)
                                    .map_err(binding)
                            })
                            .transpose()
                            .map(|value| value.unwrap_or(0))
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        /// Runs `snapshot_baseline`.
        async fn snapshot_baseline(
            &self,
            scope: Scope,
        ) -> Result<Option<SnapshotRecord>, Self::Error> {
            let snapshot_key = self.scope_space(scope).pack(&(KIND_SNAPSHOT,));
            self.database
                .run(move |transaction, _maybe_committed| {
                    let snapshot_key = snapshot_key.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        transaction
                            .get(&snapshot_key, false)
                            .await?
                            .map(|value| decode_snapshot(value.as_ref()).map_err(binding))
                            .transpose()
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        /// Runs `build_snapshot`.
        async fn build_snapshot(
            &self,
            scope: Scope,
            through: u64,
        ) -> Result<SnapshotArtifact, Self::Error> {
            let scope_space = self.scope_space(scope);
            let spaces = baseline_spaces(&scope_space);
            self.database
                .run(move |transaction, _maybe_committed| {
                    let spaces = spaces.clone();
                    let scope_space = scope_space.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        let frontier =
                            read_frontier(&transaction, &scope_space.pack(&(KIND_SEQUENCE,)))
                                .await?;
                        if through > frontier {
                            return Err(binding(DataError::BaselineCoverage));
                        }
                        reconstruct_artifact(&transaction, scope, through, &spaces).await
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        /// Runs `read_baseline_page`.
        async fn read_baseline_page(
            &self,
            action: &RecoveryAction,
            descriptor: &SnapshotBaseline,
            grant: &impl ExecutionGrant,
        ) -> Result<BaselinePageResult, Self::Error> {
            if grant.scope() != action.scope
                || grant.effect() != Effect::Sync
                || grant.action() != action.digest()
            {
                return Ok(BaselinePageResult::StaleAuthority);
            }
            let expected_frontier = grant.frontier();
            let scope_space = self.scope_space(action.scope);
            let spaces = baseline_spaces(&scope_space);
            let sequence_key = scope_space.pack(&(KIND_SEQUENCE,));
            let descriptor = descriptor.clone();
            let after = action.after_document.map(DocumentId::as_uuid);
            self.database
                .run(move |transaction, _maybe_committed| {
                    let spaces = spaces.clone();
                    let sequence_key = sequence_key.clone();
                    let descriptor = descriptor.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        if read_frontier(&transaction, &sequence_key).await? != expected_frontier {
                            return Ok(BaselinePageResult::StaleAuthority);
                        }
                        read_baseline_page_transaction(&transaction, &spaces, &descriptor, after)
                            .await
                            .map(BaselinePageResult::Page)
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        /// Runs `seal_snapshot`.
        async fn seal_snapshot(
            &self,
            scope: Scope,
            artifact: &SnapshotArtifact,
            request: OperationDigest,
            grant: &impl ExecutionGrant,
        ) -> Result<SealResult, Self::Error> {
            artifact
                .validate(scope)
                .map_err(|_| FdbStoreError::Corruption(DataError::BaselineCorrupt))?;
            let baseline = artifact.baseline.clone();
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.action() != seal_snapshot_action(scope, request, &baseline)
            {
                return Ok(SealResult::StaleAuthority);
            }
            let expected_frontier = grant.frontier();
            let position = baseline.position();
            let artifact = artifact.clone();
            let scope_space = self.scope_space(scope);
            let sequence_key = scope_space.pack(&(KIND_SEQUENCE,));
            let spaces = baseline_spaces(&scope_space);
            self.database
                .run(move |transaction, _maybe_committed| {
                    let sequence_key = sequence_key.clone();
                    let spaces = spaces.clone();
                    let artifact = artifact.clone();
                    async move {
                        let operation = SealOperation {
                            scope,
                            expected_frontier,
                            position,
                            sequence_key,
                            spaces,
                            artifact,
                        };
                        seal_transaction(&transaction, operation).await
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        /// Runs `mark_consumed`.
        async fn mark_consumed(
            &self,
            scope: Scope,
            through: u64,
            request: OperationDigest,
            grant: &impl ExecutionGrant,
        ) -> Result<ConsumedResult, Self::Error> {
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.action() != mark_consumed_action(scope, request, through)
            {
                return Ok(ConsumedResult::StaleAuthority);
            }
            let expected_frontier = grant.frontier();
            let scope_space = self.scope_space(scope);
            let sequence_key = scope_space.pack(&(KIND_SEQUENCE,));
            let retention_key = scope_space.pack(&(KIND_RETENTION,));
            self.database
                .run(move |transaction, _maybe_committed| {
                    let sequence_key = sequence_key.clone();
                    let retention_key = retention_key.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        let frontier = read_frontier(&transaction, &sequence_key).await?;
                        if frontier != expected_frontier {
                            return Ok(ConsumedResult::StaleAuthority);
                        }
                        if through > frontier {
                            return Ok(ConsumedResult::BeyondFrontier { frontier });
                        }
                        let (floor, consumed) =
                            read_retention(&transaction, &retention_key).await?;
                        if through <= consumed {
                            return Ok(ConsumedResult::Unchanged {
                                consumed_through: consumed,
                            });
                        }
                        transaction.set(&retention_key, &encode_retention(floor, through));
                        Ok(ConsumedResult::Advanced {
                            consumed_through: through,
                        })
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        /// Runs `prune`.
        async fn prune(
            &self,
            scope: Scope,
            up_to: u64,
            min_live: Option<u64>,
            request: OperationDigest,
            grant: &impl ExecutionGrant,
        ) -> Result<PruneResult, Self::Error> {
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.action() != prune_action(scope, request, up_to, min_live)
            {
                return Ok(PruneResult::StaleAuthority);
            }
            let expected_frontier = grant.frontier();
            let scope_space = self.scope_space(scope);
            let sequence_key = scope_space.pack(&(KIND_SEQUENCE,));
            let retention_key = scope_space.pack(&(KIND_RETENTION,));
            let log_space = scope_space.subspace(&(KIND_LOG,));
            let chunk_space = scope_space.subspace(&(KIND_CHUNK,));
            let baseline = baseline_spaces(&scope_space);
            self.database
                .run(move |transaction, _maybe_committed| {
                    let sequence_key = sequence_key.clone();
                    let retention_key = retention_key.clone();
                    let log_space = log_space.clone();
                    let chunk_space = chunk_space.clone();
                    let baseline = baseline.clone();
                    async move {
                        let operation = PruneOperation {
                            scope,
                            expected_frontier,
                            up_to,
                            min_live,
                            sequence_key,
                            retention_key,
                            log_space,
                            chunk_space,
                            baseline,
                        };
                        prune_transaction(&transaction, operation).await
                    }
                })
                .await
                .map_err(classify_transaction)
        }
    }

    /// AcceptOperation
    struct AcceptOperation {
        /// expected_frontier
        expected_frontier: u64,
        /// Invocation id
        invocation_id: [u8; 32],
        /// change
        change: ChangeEnvelope,
        /// maybe_committed
        maybe_committed: bool,
        /// operation_key
        operation_key: Vec<u8>,
        /// sequence_key
        sequence_key: Vec<u8>,
        /// retention_key
        retention_key: Vec<u8>,
        /// invocation_key
        invocation_key: Vec<u8>,
        /// scope_space
        scope_space: Subspace,
        /// log_space
        log_space: Subspace,
        /// chunk_space
        chunk_space: Subspace,
    }

    /// Runs `accept_transaction`.
    async fn accept_transaction(
        transaction: &foundationdb::RetryableTransaction,
        operation: AcceptOperation,
    ) -> Result<AcceptResult, FdbBindingError> {
        let AcceptOperation {
            expected_frontier,
            invocation_id,
            change,
            maybe_committed,
            operation_key,
            sequence_key,
            retention_key,
            invocation_key,
            scope_space,
            log_space,
            chunk_space,
        } = operation;
        configure_transaction(transaction)?;
        if let Some(result) = existing_accept_result(
            transaction,
            &scope_space,
            &chunk_space,
            &operation_key,
            &retention_key,
            &change,
            maybe_committed,
        )
        .await?
        {
            return Ok(result);
        }
        if transaction.get(&invocation_key, false).await?.is_some() {
            return Ok(AcceptResult::InvocationConsumed);
        }
        let previous = transaction
            .get(&sequence_key, false)
            .await?
            .map(|value| decode_u64(value.as_ref()).map_err(binding))
            .transpose()?
            .unwrap_or(0);
        if previous != expected_frontier {
            return Ok(AcceptResult::StaleAuthority);
        }
        let sequence = previous
            .checked_add(1)
            .filter(|value| i64::try_from(*value).is_ok())
            .ok_or_else(|| binding(DataError::SequenceExhausted))?;
        let manifest = encode_manifest(&change, sequence, invocation_id).map_err(binding)?;
        for (index, bytes) in change.bytes.chunks(CHUNK_BYTES).enumerate() {
            let index = u16::try_from(index).map_err(binding_invalid)?;
            transaction.set(&chunk_space.pack(&(sequence, index)), bytes);
        }
        transaction.set(&log_space.pack(&(sequence,)), &manifest[..MANIFEST_BYTES]);
        transaction.set(&operation_key, &manifest);
        transaction.set(&sequence_key, &sequence.to_be_bytes());
        let invocation_record = invocation_binding(&change);
        transaction.set(&invocation_key, &invocation_record);
        Ok(AcceptResult::Accepted)
    }

    /// PruneOperation
    struct PruneOperation {
        /// scope
        scope: Scope,
        /// Expected frontier
        expected_frontier: u64,
        /// up_to
        up_to: u64,
        /// min_live
        min_live: Option<u64>,
        /// sequence_key
        sequence_key: Vec<u8>,
        /// retention_key
        retention_key: Vec<u8>,
        /// log_space
        log_space: Subspace,
        /// chunk_space
        chunk_space: Subspace,
        /// baseline
        baseline: BaselineSpaces,
    }

    /// SealOperation
    struct SealOperation {
        /// scope
        scope: Scope,
        /// Expected frontier
        expected_frontier: u64,
        /// position
        position: u64,
        /// sequence_key
        sequence_key: Vec<u8>,
        /// spaces
        spaces: BaselineSpaces,
        /// artifact
        artifact: SnapshotArtifact,
    }

    /// Runs `seal_transaction`.
    async fn seal_transaction(
        transaction: &foundationdb::RetryableTransaction,
        operation: SealOperation,
    ) -> Result<SealResult, FdbBindingError> {
        configure_transaction(transaction)?;
        let frontier = read_frontier(transaction, &operation.sequence_key).await?;
        if frontier != operation.expected_frontier {
            return Ok(SealResult::StaleAuthority);
        }
        if operation.position > frontier {
            return Ok(SealResult::BeyondFrontier { frontier });
        }
        let existing = read_snapshot_record(transaction, &operation.spaces.pointer_key).await?;
        let existing_position = match existing.as_ref() {
            Some(&SnapshotRecord::LegacyPosition(position)) => position,
            Some(&SnapshotRecord::State(ref state)) => state.position(),
            None => 0,
            // A future descriptor class cannot authorize a seal; fail closed
            // as corrupt rather than guessing its coverage position.
            Some(_) => return Err(binding(DataError::BaselineCorrupt)),
        };
        if existing_position > operation.position {
            return Err(binding(DataError::BaselineCoverage));
        }
        let rebuilt = reconstruct_artifact(
            transaction,
            operation.scope,
            operation.position,
            &operation.spaces,
        )
        .await?;
        if rebuilt != operation.artifact {
            return Err(binding(DataError::BaselineCoverage));
        }
        if let Some(SnapshotRecord::State(previous)) = existing
            && previous.digest() != operation.artifact.baseline.digest()
        {
            clear_artifact(transaction, previous.digest(), &operation.spaces);
        }
        write_artifact(transaction, &operation.artifact, &operation.spaces)?;
        Ok(SealResult::Sealed {
            position: operation.position,
        })
    }

    /// Baseline spaces
    fn baseline_spaces(scope: &Subspace) -> BaselineSpaces {
        BaselineSpaces {
            scope: scope.clone(),
            pointer_key: scope.pack(&(KIND_SNAPSHOT,)),
            log: scope.subspace(&(KIND_LOG,)),
            change_chunk: scope.subspace(&(KIND_CHUNK,)),
            document: scope.subspace(&(KIND_SNAPSHOT_DOCUMENT,)),
            snapshot_chunk: scope.subspace(&(KIND_SNAPSHOT_CHUNK,)),
        }
    }

    /// Write artifact
    fn write_artifact(
        transaction: &foundationdb::RetryableTransaction,
        artifact: &SnapshotArtifact,
        spaces: &BaselineSpaces,
    ) -> Result<(), FdbBindingError> {
        let digest = artifact.baseline.digest().into_bytes();
        for document in &artifact.documents {
            let manifest = encode_baseline_document(document).map_err(binding)?;
            let key = spaces
                .document
                .pack(&(digest.as_slice(), document.document.as_uuid()));
            transaction.set(&key, &manifest);
            for (index, chunk) in document.snapshot.chunks(CHUNK_BYTES).enumerate() {
                let index = u16::try_from(index).map_err(binding_invalid)?;
                let key = spaces.snapshot_chunk.pack(&(
                    digest.as_slice(),
                    document.document.as_uuid(),
                    index,
                ));
                transaction.set(&key, chunk);
            }
        }
        transaction.set(&spaces.pointer_key, &artifact.baseline.encode());
        Ok(())
    }

    /// Clear artifact
    fn clear_artifact(
        transaction: &foundationdb::RetryableTransaction,
        digest: logical_db_model::BaselineDigest,
        spaces: &BaselineSpaces,
    ) {
        let digest = digest.into_bytes();
        for space in [&spaces.document, &spaces.snapshot_chunk] {
            let artifact = space.subspace(&(digest.as_slice(),));
            let (begin, end) = artifact.range();
            transaction.clear_range(&begin, &end);
        }
    }

    /// Runs `reconstruct_artifact`.
    async fn reconstruct_artifact(
        transaction: &foundationdb::RetryableTransaction,
        scope: Scope,
        through: u64,
        spaces: &BaselineSpaces,
    ) -> Result<SnapshotArtifact, FdbBindingError> {
        let mut position = 0_u64;
        let mut replayed_bytes = 0_usize;
        let mut base = None;
        if let Some(record) = read_snapshot_record(transaction, &spaces.pointer_key).await?
            && let SnapshotRecord::State(baseline) = record
            && baseline.position() <= through
        {
            let artifact = load_artifact(transaction, scope, &baseline, spaces).await?;
            position = baseline.position();
            base = Some(artifact);
        }
        let end = through
            .checked_add(1)
            .map_or_else(|| spaces.log.range().1, |next| spaces.log.pack(&(next,)));
        // A single range read may be truncated by the client before either the
        // row or byte budget is reached, so continue from the last sequence
        // until the range is exhausted or the change budget is exceeded.
        let mut manifests = Vec::new();
        let mut next_sequence = position.saturating_add(1);
        while manifests.len() <= MAX_BASELINE_CHANGES {
            let begin = spaces.log.pack(&(next_sequence,));
            let remaining = MAX_BASELINE_CHANGES
                .saturating_add(1)
                .saturating_sub(manifests.len());
            let options = RangeOption {
                begin: KeySelector::first_greater_or_equal(begin.as_slice()),
                end: KeySelector::first_greater_or_equal(end.as_slice()),
                limit: Some(remaining.max(1)),
                target_bytes: MAX_BASELINE_BYTES,
                ..RangeOption::default()
            };
            let values = transaction.get_range(&options, 1, false).await?;
            let more = values.more();
            manifests.extend(
                values
                    .iter()
                    .map(|value| (value.key().to_vec(), value.value().to_vec())),
            );
            if manifests.len() > MAX_BASELINE_CHANGES {
                return Err(binding(DataError::BaselineResourceLimit));
            }
            if !more {
                break;
            }
            let Some(&(ref last, _)) = manifests.last() else {
                return Err(binding(DataError::BaselineResourceLimit));
            };
            let (sequence,) = spaces.log.unpack::<(u64,)>(last).map_err(binding_invalid)?;
            next_sequence = sequence.saturating_add(1);
        }
        if manifests.len() > MAX_BASELINE_CHANGES {
            return Err(binding(DataError::BaselineResourceLimit));
        }
        let mut changes = Vec::with_capacity(manifests.len());
        for (key, value) in manifests {
            let (sequence, change, next_bytes) = load_baseline_change(
                transaction,
                spaces,
                scope,
                position,
                replayed_bytes,
                &key,
                &value,
            )
            .await?;
            changes.push(change);
            position = sequence;
            replayed_bytes = next_bytes;
        }
        if position != through {
            return Err(binding(DataError::BaselineCoverage));
        }
        materialize_snapshot(scope, through, base.as_ref(), &changes).map_err(|error| {
            let data = match error {
                SnapshotMaterializationError::Coverage => DataError::BaselineCoverage,
                SnapshotMaterializationError::ResourceLimit => DataError::BaselineResourceLimit,
                SnapshotMaterializationError::InvalidBase { context: _ }
                | SnapshotMaterializationError::ScopeMismatch
                | SnapshotMaterializationError::InvalidChange => DataError::BaselineCorrupt,
                // Unknown materialization classes fail closed as corrupt
                // rather than being assumed benign outside the defining crate.
                _ => DataError::BaselineCorrupt,
            };
            binding(data)
        })
    }

    /// Runs `load_baseline_change`.
    async fn load_baseline_change(
        transaction: &foundationdb::RetryableTransaction,
        spaces: &BaselineSpaces,
        scope: Scope,
        previous: u64,
        replayed_bytes: usize,
        key: &[u8],
        value: &[u8],
    ) -> Result<(u64, ChangeEnvelope, usize), FdbBindingError> {
        let (sequence,) = spaces.log.unpack::<(u64,)>(key).map_err(binding_invalid)?;
        if previous.checked_add(1) != Some(sequence) {
            return Err(binding(DataError::BaselineCoverage));
        }
        let record = decode_manifest(value, sequence).map_err(binding)?;
        let next_bytes = replayed_bytes
            .checked_add(record.byte_len)
            .filter(|total| *total <= MAX_BASELINE_BYTES)
            .ok_or_else(|| binding(DataError::BaselineResourceLimit))?;
        verify_operation(transaction, &spaces.scope, &record).await?;
        let bytes = load_change_bytes(transaction, &spaces.change_chunk, sequence, &record).await?;
        Ok((sequence, record.envelope(scope, bytes), next_bytes))
    }

    /// Runs `read_snapshot_record`.
    async fn read_snapshot_record(
        transaction: &foundationdb::RetryableTransaction,
        pointer_key: &[u8],
    ) -> Result<Option<SnapshotRecord>, FdbBindingError> {
        transaction
            .get(pointer_key, false)
            .await?
            .map(|bytes| decode_snapshot(bytes.as_ref()).map_err(binding))
            .transpose()
    }

    /// Runs `scan_baseline_manifests`.
    async fn scan_baseline_manifests(
        transaction: &foundationdb::RetryableTransaction,
        space_begin: &[u8],
        space_end: &[u8],
        expected_count: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, FdbBindingError> {
        let mut manifests = Vec::new();
        let mut cursor = space_begin.to_vec();
        let mut exclusive = false;
        while manifests.len() <= expected_count {
            let remaining = expected_count
                .saturating_add(1)
                .saturating_sub(manifests.len());
            let begin = if exclusive {
                KeySelector::first_greater_than(cursor.as_slice())
            } else {
                KeySelector::first_greater_or_equal(cursor.as_slice())
            };
            let options = RangeOption {
                begin,
                end: KeySelector::first_greater_or_equal(space_end),
                limit: Some(remaining.max(1)),
                target_bytes: MAX_BASELINE_BYTES,
                ..RangeOption::default()
            };
            let values = transaction.get_range(&options, 1, false).await?;
            let more = values.more();
            let last = values.iter().last().map(|value| value.key().to_vec());
            manifests.extend(
                values
                    .iter()
                    .map(|value| (value.key().to_vec(), value.value().to_vec())),
            );
            if manifests.len() > expected_count {
                return Err(binding(DataError::BaselineCorrupt));
            }
            if !more {
                break;
            }
            let Some(last) = last else {
                return Err(binding(DataError::BaselineCorrupt));
            };
            cursor = last;
            exclusive = true;
        }
        if manifests.len() > expected_count {
            return Err(binding(DataError::BaselineCorrupt));
        }
        if manifests.len() != expected_count {
            return Err(binding(DataError::BaselineCorrupt));
        }
        Ok(manifests)
    }

    /// Runs `load_baseline_documents`.
    async fn load_baseline_documents(
        transaction: &foundationdb::RetryableTransaction,
        document_space: &foundationdb_tuple::Subspace,
        spaces: &BaselineSpaces,
        digest: [u8; 32],
        expected_bytes: usize,
        manifests: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<SnapshotDocument>, FdbBindingError> {
        let mut documents = Vec::with_capacity(manifests.len());
        let mut aggregate_bytes = 0_usize;
        for (key, value) in manifests {
            let manifest = decode_baseline_document(&value).map_err(binding)?;
            let Some(next_total) = aggregate_bytes
                .checked_add(manifest.byte_len)
                .filter(|total| *total <= MAX_BASELINE_BYTES && *total <= expected_bytes)
            else {
                return Err(binding(DataError::BaselineCorrupt));
            };
            let document =
                load_artifact_document(transaction, document_space, spaces, digest, &key, manifest)
                    .await?;
            aggregate_bytes = next_total;
            documents.push(document);
        }
        if aggregate_bytes != expected_bytes {
            return Err(binding(DataError::BaselineCorrupt));
        }
        Ok(documents)
    }

    /// Verify baseline artifact
    fn verify_baseline_artifact(
        scope: Scope,
        baseline: &logical_db_model::SnapshotBaseline,
        documents: Vec<SnapshotDocument>,
    ) -> Result<SnapshotArtifact, FdbBindingError> {
        let artifact = SnapshotArtifact::new(scope, baseline.position(), documents)
            .map_err(binding_baseline)?;
        if &artifact.baseline != baseline {
            return Err(binding(DataError::BaselineCorrupt));
        }
        let native = materialize_snapshot(scope, baseline.position(), Some(&artifact), &[])
            .map_err(|_| binding(DataError::BaselineCorrupt))?;
        if native != artifact {
            return Err(binding(DataError::BaselineCorrupt));
        }
        Ok(artifact)
    }

    /// Runs `load_artifact`.
    async fn load_artifact(
        transaction: &foundationdb::RetryableTransaction,
        scope: Scope,
        baseline: &logical_db_model::SnapshotBaseline,
        spaces: &BaselineSpaces,
    ) -> Result<SnapshotArtifact, FdbBindingError> {
        let digest = baseline.digest().into_bytes();
        let document_space = spaces.document.subspace(&(digest.as_slice(),));
        let (space_begin, space_end) = document_space.range();
        let expected_count = usize::try_from(baseline.document_count()).map_err(binding_invalid)?;
        let expected_bytes = usize::try_from(baseline.byte_len()).map_err(binding_invalid)?;

        let manifests = scan_baseline_manifests(
            transaction,
            space_begin.as_slice(),
            space_end.as_slice(),
            expected_count,
        )
        .await?;

        let documents = load_baseline_documents(
            transaction,
            &document_space,
            spaces,
            digest,
            expected_bytes,
            manifests,
        )
        .await?;

        verify_baseline_artifact(scope, baseline, documents)
    }

    /// Runs `read_baseline_page_transaction`.
    async fn read_baseline_page_transaction(
        transaction: &foundationdb::RetryableTransaction,
        spaces: &BaselineSpaces,
        baseline: &SnapshotBaseline,
        after: Option<uuid::Uuid>,
    ) -> Result<BaselinePageSlice, FdbBindingError> {
        let digest = baseline.digest().into_bytes();
        let document_space = spaces.document.subspace(&(digest.as_slice(),));
        let (space_begin, space_end) = document_space.range();
        let begin = after.map_or_else(
            || KeySelector::first_greater_or_equal(space_begin),
            |cursor| KeySelector::first_greater_than(document_space.pack(&(cursor,))),
        );
        let options = RangeOption {
            begin,
            end: KeySelector::first_greater_or_equal(space_end),
            limit: Some(MAX_BASELINE_PAGE_DOCUMENTS + 1),
            target_bytes: MAX_BASELINE_BYTES,
            ..RangeOption::default()
        };
        let values = transaction.get_range(&options, 1, false).await?;
        let more = values.len() > MAX_BASELINE_PAGE_DOCUMENTS || values.more();
        let manifests = values
            .iter()
            .take(MAX_BASELINE_PAGE_DOCUMENTS)
            .map(|value| (value.key().to_vec(), value.value().to_vec()))
            .collect::<Vec<_>>();
        drop(values);
        let mut documents = Vec::with_capacity(manifests.len());
        let mut aggregate_bytes = 0_usize;
        for (key, value) in manifests {
            let manifest = decode_baseline_document(&value).map_err(binding)?;
            aggregate_bytes = aggregate_bytes
                .checked_add(manifest.byte_len)
                .filter(|total| *total <= MAX_BASELINE_BYTES)
                .ok_or_else(|| binding(DataError::BaselineCorrupt))?;
            documents.push(
                load_artifact_document(
                    transaction,
                    &document_space,
                    spaces,
                    digest,
                    &key,
                    manifest,
                )
                .await?,
            );
        }
        Ok(BaselinePageSlice::new(documents, more))
    }

    /// Runs `load_artifact_document`.
    async fn load_artifact_document(
        transaction: &foundationdb::RetryableTransaction,
        document_space: &Subspace,
        spaces: &BaselineSpaces,
        digest: [u8; 32],
        key: &[u8],
        manifest: BaselineDocumentManifest,
    ) -> Result<SnapshotDocument, FdbBindingError> {
        use lgwks_std::hash::blake3;

        let (document_id,) = document_space
            .unpack::<(uuid::Uuid,)>(key)
            .map_err(binding_invalid)?;
        let snapshot = load_snapshot_bytes(
            transaction,
            &spaces.snapshot_chunk,
            digest,
            document_id,
            manifest,
        )
        .await?;
        if *blake3(&snapshot).as_bytes() != manifest.digest {
            return Err(binding(DataError::BaselineCorrupt));
        }
        Ok(SnapshotDocument::new(
            DocumentId::from_uuid(document_id),
            snapshot,
            manifest.tombstone,
        ))
    }

    /// Runs `load_snapshot_bytes`.
    async fn load_snapshot_bytes(
        transaction: &foundationdb::RetryableTransaction,
        chunk_space: &Subspace,
        artifact: [u8; 32],
        document: uuid::Uuid,
        manifest: BaselineDocumentManifest,
    ) -> Result<Vec<u8>, FdbBindingError> {
        let mut bytes = Vec::with_capacity(manifest.byte_len);
        for index in 0..manifest.chunks {
            let key = chunk_space.pack(&(artifact.as_slice(), document, index));
            let chunk = transaction
                .get(&key, false)
                .await?
                .ok_or_else(|| binding(DataError::BaselineCorrupt))?;
            if chunk.is_empty() || chunk.len() > CHUNK_BYTES {
                return Err(binding(DataError::BaselineCorrupt));
            }
            bytes.extend_from_slice(chunk.as_ref());
        }
        if bytes.len() != manifest.byte_len {
            return Err(binding(DataError::BaselineCorrupt));
        }
        Ok(bytes)
    }

    /// Runs `prune_transaction`.
    async fn prune_transaction(
        transaction: &foundationdb::RetryableTransaction,
        operation: PruneOperation,
    ) -> Result<PruneResult, FdbBindingError> {
        let PruneOperation {
            scope,
            expected_frontier,
            up_to,
            min_live,
            sequence_key,
            retention_key,
            log_space,
            chunk_space,
            baseline,
        } = operation;
        configure_transaction(transaction)?;
        let frontier = read_frontier(transaction, &sequence_key).await?;
        if frontier != expected_frontier {
            return Ok(PruneResult::StaleAuthority);
        }
        let (floor, consumed) = read_retention(transaction, &retention_key).await?;
        let through = match prune_decision(frontier, floor, up_to, min_live, consumed) {
            Ok(value) => value,
            Err(refusal) => return Ok(PruneResult::Refused(refusal)),
        };
        if through <= floor {
            return Ok(PruneResult::Pruned(PruneReceipt::new(floor, floor)));
        }
        let baseline_record = read_snapshot_record(transaction, &baseline.pointer_key).await?;
        let state = match baseline_record {
            Some(SnapshotRecord::State(state)) if state.position() >= through => state,
            Some(SnapshotRecord::State(state)) => {
                return Ok(PruneResult::Refused(PruneRefusal::BaselineBehind {
                    position: state.position(),
                }));
            }
            Some(SnapshotRecord::LegacyPosition(_)) | None => {
                return Ok(PruneResult::Refused(PruneRefusal::MissingBaseline));
            }
            // A future descriptor class cannot prove coverage of the prunable
            // prefix; refuse collection as if no state-bearing baseline exists.
            Some(_) => {
                return Ok(PruneResult::Refused(PruneRefusal::MissingBaseline));
            }
        };
        load_artifact(transaction, scope, &state, &baseline).await?;
        if through > floor {
            let log_begin = log_space.pack(&(floor.saturating_add(1),));
            let log_end = through
                .checked_add(1)
                .map_or_else(|| log_space.range().1, |next| log_space.pack(&(next,)));
            transaction.clear_range(&log_begin, &log_end);
            let chunk_begin = chunk_space.pack(&(floor.saturating_add(1),));
            let chunk_end = through
                .checked_add(1)
                .map_or_else(|| chunk_space.range().1, |next| chunk_space.pack(&(next,)));
            transaction.clear_range(&chunk_begin, &chunk_end);
            transaction.set(&retention_key, &encode_retention(through, consumed));
        }
        Ok(PruneResult::Pruned(PruneReceipt::new(
            through,
            through.max(floor),
        )))
    }

    /// Runs `existing_accept_result`.
    async fn existing_accept_result(
        transaction: &foundationdb::RetryableTransaction,
        scope_space: &Subspace,
        chunk_space: &Subspace,
        operation_key: &[u8],
        retention_key: &[u8],
        change: &ChangeEnvelope,
        maybe_committed: bool,
    ) -> Result<Option<AcceptResult>, FdbBindingError> {
        let Some(existing) = transaction.get(operation_key, false).await? else {
            return Ok(None);
        };
        let operation = decode_operation(existing.as_ref()).map_err(binding)?;
        let stored_invocation_key =
            scope_space.pack(&(KIND_INVOCATION, operation.invocation_id.as_slice()));
        let stored_binding = transaction
            .get(&stored_invocation_key, false)
            .await?
            .ok_or_else(|| binding(DataError::MissingInvocation))?;
        if stored_binding.as_ref() != invocation_binding_record(&operation.record) {
            return Err(binding(DataError::InvalidInvocation));
        }
        let record = operation.record;
        let floor = transaction
            .get(retention_key, false)
            .await?
            .map(|value| {
                decode_retention(value.as_ref())
                    .map(|(floor, _)| floor)
                    .map_err(binding)
            })
            .transpose()?
            .unwrap_or(0);
        let exact_bytes = if record.sequence <= floor {
            // Chunks below the floor were collected by design; the manifest
            // digest remains the content identity for the retry decision.
            None
        } else {
            Some(load_change_bytes(transaction, chunk_space, record.sequence, &record).await?)
        };
        let exact_retry = record.matches(change)
            && exact_bytes
                .as_ref()
                .is_none_or(|bytes| *bytes == change.bytes);
        Ok(Some(if exact_retry {
            if maybe_committed {
                AcceptResult::RecoveredUnknownCommit
            } else {
                AcceptResult::Duplicate
            }
        } else {
            AcceptResult::Conflict
        }))
    }

    /// Runs `load_page`.
    async fn load_page(
        transaction: &foundationdb::RetryableTransaction,
        spaces: &PageSpaces,
        query: PageQuery,
    ) -> Result<PageResult, FdbBindingError> {
        let frontier = transaction
            .get(&spaces.sequence_key, false)
            .await?
            .map(|value| decode_u64(value.as_ref()).map_err(binding))
            .transpose()?
            .unwrap_or(0);
        if frontier != query.frontier {
            return Ok(PageResult::StaleAuthority);
        }
        let floor = transaction
            .get(&spaces.retention_key, false)
            .await?
            .map(|value| {
                decode_retention(value.as_ref())
                    .map(|(floor, _)| floor)
                    .map_err(binding)
            })
            .transpose()?
            .unwrap_or(0);
        if query.after < floor {
            return Ok(PageResult::BehindFloor);
        }
        if query.after > frontier {
            return Err(binding(DataError::InvalidManifest { at: "manifest" }));
        }
        let begin = spaces.log.pack(&(query.after.saturating_add(1),));
        let (_, end) = spaces.log.range();
        let plan = page_read_plan(query.max_changes);
        let mut options = RangeOption::from((begin.as_slice(), end.as_slice()));
        options.limit = Some(plan.limit);
        options.target_bytes = plan.target_bytes;
        let values = transaction.get_range(&options, 1, false).await?;
        let range_more = values.more();
        let manifests = values
            .iter()
            .map(|value| (value.key().to_vec(), value.value().to_vec()))
            .collect::<Vec<_>>();
        drop(values);
        materialize_page(transaction, spaces, query, frontier, range_more, manifests).await
    }

    /// Configure transaction
    fn configure_transaction(
        transaction: &foundationdb::RetryableTransaction,
    ) -> Result<(), FdbBindingError> {
        transaction.set_option(TransactionOption::Timeout(TRANSACTION_TIMEOUT_MS))?;
        transaction.set_option(TransactionOption::RetryLimit(TRANSACTION_RETRY_LIMIT))?;
        Ok(())
    }

    /// Runs `read_frontier`.
    async fn read_frontier(
        transaction: &foundationdb::RetryableTransaction,
        key: &[u8],
    ) -> Result<u64, FdbBindingError> {
        let value = transaction.get(key, false).await?;
        let decoded = value
            .map(|bytes| decode_u64(bytes.as_ref()).map_err(binding))
            .transpose()?;
        Ok(decoded.unwrap_or(0))
    }

    /// Runs `read_retention`.
    async fn read_retention(
        transaction: &foundationdb::RetryableTransaction,
        key: &[u8],
    ) -> Result<(u64, u64), FdbBindingError> {
        let value = transaction.get(key, false).await?;
        let decoded = value
            .map(|bytes| decode_retention(bytes.as_ref()).map_err(binding))
            .transpose()?;
        Ok(decoded.unwrap_or((0, 0)))
    }

    /// Runs `materialize_page`.
    async fn materialize_page(
        transaction: &foundationdb::RetryableTransaction,
        spaces: &PageSpaces,
        query: PageQuery,
        frontier: u64,
        range_more: bool,
        manifests: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<PageResult, FdbBindingError> {
        let mut changes = Vec::with_capacity(usize::from(query.max_changes));
        let mut aggregate = 0usize;
        let mut position = query.after;
        let mut stopped_at_page_boundary = false;
        for (key, value) in manifests {
            if changes.len() == usize::from(query.max_changes) {
                stopped_at_page_boundary = true;
                break;
            }
            let (sequence,) = spaces.log.unpack::<(u64,)>(&key).map_err(binding_invalid)?;
            if position.checked_add(1) != Some(sequence) {
                return Err(binding(DataError::InvalidManifest { at: "manifest" }));
            }
            let record = match decode_manifest(&value, sequence).map_err(binding) {
                Ok(record) => record,
                Err(error) => {
                    return Err(error);
                }
            };
            if let Err(error) = verify_operation(transaction, &spaces.scope, &record).await {
                return Err(error);
            }
            let Some(next_bytes) = aggregate.checked_add(record.byte_len) else {
                return Err(binding(DataError::InvalidManifest { at: "manifest" }));
            };
            let max_bytes = usize::try_from(query.max_bytes).unwrap_or(usize::MAX);
            if next_bytes > max_bytes {
                if changes.is_empty() {
                    return Err(binding(DataError::ChangeExceedsPage));
                }
                stopped_at_page_boundary = true;
                break;
            }
            let bytes = match load_change_bytes(transaction, &spaces.chunk, sequence, &record).await
            {
                Ok(bytes) => bytes,
                Err(error) => {
                    return Err(error);
                }
            };
            changes.push(record.envelope(query.scope, bytes));
            aggregate = next_bytes;
            position = sequence;
        }
        let has_entry = match log_has_entry_after(transaction, &spaces.log, frontier).await {
            Ok(has_entry) => has_entry,
            Err(error) => {
                return Err(error);
            }
        };
        if position == frontier && has_entry {
            return Err(binding(DataError::InvalidManifest { at: "manifest" }));
        }
        let more = match logical_page_more(frontier, position, range_more, stopped_at_page_boundary)
            .map_err(binding)
        {
            Ok(more) => more,
            Err(error) => {
                return Err(error);
            }
        };
        Ok(PageResult::Page(StoredPage::new(changes, position, more)))
    }

    /// Runs `log_has_entry_after`.
    async fn log_has_entry_after(
        transaction: &foundationdb::RetryableTransaction,
        log: &Subspace,
        frontier: u64,
    ) -> Result<bool, FdbBindingError> {
        let terminal = log.pack(&(frontier,));
        let (_, end) = log.range();
        let options = RangeOption {
            begin: KeySelector::first_greater_than(terminal),
            end: KeySelector::first_greater_or_equal(end),
            limit: Some(1),
            target_bytes: MANIFEST_BYTES,
            ..RangeOption::default()
        };
        Ok(!transaction.get_range(&options, 1, false).await?.is_empty())
    }

    /// Runs `verify_operation`.
    async fn verify_operation(
        transaction: &foundationdb::RetryableTransaction,
        scope_space: &Subspace,
        record: &Record,
    ) -> Result<(), FdbBindingError> {
        let operation_key = scope_space.pack(&(KIND_OPERATION, record.operation.as_uuid()));
        let operation_bytes = transaction
            .get(&operation_key, false)
            .await?
            .ok_or_else(|| binding(DataError::InvalidManifest { at: "manifest" }))?;
        let operation = decode_operation(operation_bytes.as_ref()).map_err(binding)?;
        if operation.record != *record {
            return Err(binding(DataError::InvalidManifest { at: "manifest" }));
        }
        let invocation_key =
            scope_space.pack(&(KIND_INVOCATION, operation.invocation_id.as_slice()));
        let invocation = transaction
            .get(&invocation_key, false)
            .await?
            .ok_or_else(|| binding(DataError::MissingInvocation))?;
        if invocation.as_ref() != invocation_binding_record(record) {
            return Err(binding(DataError::InvalidInvocation));
        }
        Ok(())
    }

    /// Runs `load_change_bytes`.
    async fn load_change_bytes(
        transaction: &foundationdb::RetryableTransaction,
        chunk_space: &Subspace,
        sequence: u64,
        record: &Record,
    ) -> Result<Vec<u8>, FdbBindingError> {
        let mut bytes = Vec::with_capacity(record.byte_len);
        for index in 0..record.chunks {
            let chunk = transaction
                .get(&chunk_space.pack(&(sequence, index)), false)
                .await?
                .ok_or_else(|| binding(DataError::MissingChunk))?;
            if chunk.is_empty() || chunk.len() > CHUNK_BYTES {
                return Err(binding(DataError::InvalidChunk));
            }
            bytes.extend_from_slice(chunk.as_ref());
        }
        if bytes.len() != record.byte_len {
            return Err(binding(DataError::InvalidChunk));
        }
        Ok(bytes)
    }

    /// Binding
    fn binding(error: DataError) -> FdbBindingError {
        FdbBindingError::new_custom_error(Box::new(error))
    }

    /// Binding invalid
    fn binding_invalid<E: std::fmt::Debug>(_cause: E) -> FdbBindingError {
        binding(DataError::InvalidManifest { at: "manifest" })
    }

    /// Binding baseline
    fn binding_baseline<E: std::fmt::Debug>(_cause: E) -> FdbBindingError {
        binding(DataError::BaselineCorrupt)
    }

    /// Classify transaction
    fn classify_transaction(error: FdbBindingError) -> FdbStoreError {
        if let FdbBindingError::CustomError(ref custom) = error
            && let Some(data) = custom.downcast_ref::<DataError>()
        {
            return FdbStoreError::Corruption(*data);
        }
        if let Some(native) = error.get_fdb_error() {
            let code = native.code();
            if code == 1_025 {
                return FdbStoreError::Cancelled { code };
            }
            if code == 1_039 {
                return FdbStoreError::VersionSkew {
                    code,
                    may_have_committed: native.is_maybe_committed(),
                };
            }
            if native.is_maybe_committed() {
                return FdbStoreError::CommitMayHaveSucceeded { code };
            }
            if native.is_retryable() || native.is_retryable_not_committed() {
                return FdbStoreError::RetryExhausted { code };
            }
            return FdbStoreError::Native { code };
        }
        FdbStoreError::BindingInvariant(error)
    }

    /// `FoundationDB` adapter initialization or transaction failure.
    #[derive(Debug, Error)]
    #[non_exhaustive]
    pub enum FdbStoreError {
        /// Database option or client call failed before a retryable transaction.
        #[error("FoundationDB client rejected configuration: {0}")]
        Client(#[from] foundationdb::FdbError),
        /// Directory layer did not allocate a nonempty application subspace.

        #[error("FoundationDB adapter requires a directory-allocated nonempty root")]
        UnallocatedRoot,
        /// Caller bypassed the versioned protocol's per-change envelope bounds.
        #[error("FoundationDB adapter rejected an invalid change envelope")]
        InvalidChangeEnvelope,
        /// Caller bypassed the versioned protocol's page count or byte bounds.
        #[error("FoundationDB adapter rejected invalid page limits")]
        InvalidPageLimits,
        /// Commit returned without proof of whether the exact operation landed.
        #[error("FoundationDB commit may have succeeded (code {code}); retry exact operation")]
        CommitMayHaveSucceeded {
            /// Native FDB error code.
            code: i32,
        },
        /// The host or client cancelled transaction execution.
        #[error("FoundationDB transaction was cancelled (code {code})")]
        Cancelled {
            /// Native FDB error code.
            code: i32,
        },
        /// Client/cluster protocol versions changed while the transaction was active.
        #[error(
            "FoundationDB cluster version changed (code {code}, may_have_committed={may_have_committed})"
        )]
        VersionSkew {
            /// Native FDB error code.
            code: i32,
            /// Whether FDB classifies this particular result as ambiguous commit.
            may_have_committed: bool,
        },
        /// Native retry machinery exhausted the configured retry/timeout budget.
        #[error("FoundationDB retry budget was exhausted (code {code})")]
        RetryExhausted {
            /// Native FDB error code.
            code: i32,
        },
        /// Durable tuple/chunk bytes violated Logical-DB's format invariants.
        #[error("FoundationDB durable data is corrupt: {0}")]
        Corruption(super::FdbDataError),
        /// Non-retryable native failure not covered by a narrower stable category.
        #[error("FoundationDB native transaction failed (code {code})")]
        Native {
            /// Native FDB error code.
            code: i32,
        },
        /// Binding structural failure without an underlying native error code.
        #[error("FoundationDB binding invariant failed: {0:?}")]
        BindingInvariant(FdbBindingError),
    }

    #[cfg(test)]
    mod tests {
        use foundationdb::FdbError;

        use super::*;

        #[test]
        /// Native and corruption failures keep stable recovery categories
        fn native_and_corruption_failures_keep_stable_recovery_categories()
        -> Result<(), Box<dyn std::error::Error>> {
            assert!(matches!(
                classify_transaction(FdbBindingError::from(FdbError::from_code(1_021))),
                FdbStoreError::CommitMayHaveSucceeded { code: 1_021 }
            ));
            assert!(matches!(
                classify_transaction(FdbBindingError::from(FdbError::from_code(1_025))),
                FdbStoreError::Cancelled { code: 1_025 }
            ));
            assert!(matches!(
                classify_transaction(FdbBindingError::from(FdbError::from_code(1_039))),
                FdbStoreError::VersionSkew { code: 1_039, .. }
            ));
            assert!(matches!(
                classify_transaction(binding(DataError::MissingChunk)),
                FdbStoreError::Corruption(DataError::MissingChunk)
            ));
            assert!(matches!(
                classify_transaction(FdbBindingError::from(FdbError::from_code(1_007))),
                FdbStoreError::RetryExhausted { code: 1_007 }
            ));
            assert!(matches!(
                classify_transaction(FdbBindingError::from(FdbError::from_code(2_000))),
                FdbStoreError::Native { code: 2_000 }
            ));
            assert!(matches!(
                classify_transaction(FdbBindingError::ReferenceToTransactionKept),
                FdbStoreError::BindingInvariant(_)
            ));
            assert_eq!(
                FdbServiceStore::classify_error(&FdbStoreError::CommitMayHaveSucceeded {
                    code: 1_021
                }),
                StoreFailureClass::Transient
            );
            assert_eq!(
                FdbServiceStore::classify_error(&FdbStoreError::UnallocatedRoot),
                StoreFailureClass::Permanent
            );
            Ok(())
        }
    }
}

#[cfg(feature = "fdb")]
pub use runtime::{FdbServiceStore, FdbStoreError};

/// The `FoundationDB` client API this adapter is built on.
#[cfg(feature = "fdb")]
pub use foundationdb;
/// Order-preserving tuple keys, this adapter's keyspace contract.
pub use foundationdb_tuple;

#[cfg(test)]
mod tests {
    use logical_db_model::{AccountId, SpaceId};
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    /// Tuple axis is scope isolated and sequence ordered
    fn tuple_axis_is_scope_isolated_and_sequence_ordered() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = Subspace::from_bytes(b"directory-allocated/".to_vec());
        let first = Scope::new(AccountId::new(), SpaceId::new());
        let second = Scope::new(AccountId::new(), SpaceId::new());
        let first_log = root.subspace(&(
            FORMAT_VERSION,
            first.account().as_uuid(),
            first.space().as_uuid(),
            KIND_LOG,
        ));
        let second_log = root.subspace(&(
            FORMAT_VERSION,
            second.account().as_uuid(),
            second.space().as_uuid(),
            KIND_LOG,
        ));
        assert_ne!(first_log.bytes(), second_log.bytes());
        assert!(first_log.pack(&(1_u64,)) < first_log.pack(&(2_u64,)));
        assert!(first_log.is_start_of(&first_log.pack(&(99_u64,))));
        assert!(!first_log.is_start_of(&second_log.pack(&(1_u64,))));
        Ok(())
    }

    /// Change
    fn change(byte_len: usize) -> ChangeEnvelope {
        ChangeEnvelope::new(
            Scope::new(AccountId::new(), SpaceId::new()),
            DocumentId::new(),
            ReplicaId::new(),
            OperationId::new(),
            OperationDigest::new([7; 32]),
            ChangeDigest::new([9; 32]),
            vec![3; byte_len],
        )
    }

    #[test]
    /// Manifest round trip preserves every axis and rejects each rebinding
    fn manifest_round_trip_preserves_every_axis_and_rejects_each_rebinding()
    -> Result<(), Box<dyn std::error::Error>> {
        let change = change(CHUNK_BYTES + 1);
        let invocation_id = [0x5a; 32];
        let binding = invocation_binding(&change);
        assert_eq!(&binding[..16], change.operation.as_uuid().as_bytes());
        assert_eq!(&binding[16..48], &change.request.into_bytes());
        assert_eq!(&binding[48..], &change.digest.into_bytes());
        let encoded = encode_manifest(&change, 41, invocation_id)?;
        assert_ne!(encoded, [0; OPERATION_BYTES]);
        let operation = decode_operation(&encoded)?;
        assert_eq!(operation.invocation_id, invocation_id);
        let record = operation.record;
        assert_eq!(record.sequence, 41);
        assert!(record.matches(&change));
        assert_eq!(record.envelope(change.scope, change.bytes.clone()), change);
        assert_eq!(
            decode_manifest(&encoded[..MANIFEST_BYTES], 99)?.sequence,
            99
        );

        let mut altered = change.clone();
        altered.document = DocumentId::new();
        assert!(!record.matches(&altered));
        altered = change.clone();
        altered.replica = ReplicaId::new();
        assert!(!record.matches(&altered));
        altered = change.clone();
        altered.operation = OperationId::new();
        assert!(!record.matches(&altered));
        altered = change.clone();
        altered.request = OperationDigest::new([8; 32]);
        assert!(!record.matches(&altered));
        altered = change.clone();
        altered.digest = ChangeDigest::new([10; 32]);
        assert!(!record.matches(&altered));
        altered = change.clone();
        altered.bytes.pop();
        assert!(!record.matches(&altered));
        assert!(
            !Record {
                chunks: record.chunks + 1,
                ..record
            }
            .matches(&change)
        );
        Ok(())
    }

    #[test]
    /// Page limits and frontiers fail closed without a cluster
    fn page_limits_and_frontiers_fail_closed_without_a_cluster()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(CHUNK_BYTES, 65_536);
        assert!(valid_page_limits(1, 1));
        assert!(valid_page_limits(
            u16::try_from(MAX_CHANGES_PER_PAGE)?,
            u32::try_from(MAX_PAGE_BYTES)?
        ));
        assert!(!valid_page_limits(0, 1));
        assert!(!valid_page_limits(1, 0));
        assert!(!valid_page_limits(
            u16::try_from(MAX_CHANGES_PER_PAGE + 1)?,
            1
        ));
        assert!(!valid_page_limits(1, u32::try_from(MAX_PAGE_BYTES + 1)?));
        assert_eq!(MANIFEST_BYTES, 118);
        assert_eq!(OPERATION_BYTES, 158);
        assert_eq!(
            page_read_plan(256),
            PageReadPlan {
                limit: 257,
                target_bytes: 30_208
            }
        );
        assert_eq!(logical_page_more(10, 10, true, false), Ok(false));
        assert_eq!(logical_page_more(10, 9, true, false), Ok(true));
        assert_eq!(logical_page_more(10, 9, false, true), Ok(true));
        assert_eq!(
            logical_page_more(10, 11, false, false),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        assert_eq!(
            logical_page_more(10, 10, false, true),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        assert_eq!(
            logical_page_more(10, 9, false, false),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        Ok(())
    }

    #[test]
    /// Manifest chunk bounds reject inconsistent lengths
    fn manifest_chunk_bounds_reject_inconsistent_lengths() -> Result<(), Box<dyn std::error::Error>>
    {
        assert_eq!(chunk_count(0), 0);
        assert_eq!(chunk_count(1), 1);
        assert_eq!(chunk_count(CHUNK_BYTES), 1);
        assert_eq!(chunk_count(CHUNK_BYTES + 1), 2);
        assert_eq!(chunk_count(usize::MAX), u16::MAX);
        assert_eq!(
            encode_manifest(&change(0), 1, [1; 32]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        assert_eq!(
            encode_manifest(&change(MAX_CHANGE_BYTES + 1), 1, [1; 32]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        let encoded = encode_manifest(&change(1), u64::MAX, [2; 32])?;
        assert_eq!(decode_operation(&encoded)?.record.sequence, u64::MAX);
        assert_eq!(
            decode_operation(&encoded[..OPERATION_BYTES - 1]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        assert_eq!(
            decode_manifest(&encoded[..MANIFEST_BYTES - 1], 1),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        let mut zero_length = encoded;
        zero_length[112..116].copy_from_slice(&0_u32.to_be_bytes());
        zero_length[116..118].copy_from_slice(&0_u16.to_be_bytes());
        assert_eq!(
            decode_operation(&zero_length),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        let mut zero_chunks = encoded;
        zero_chunks[116..118].copy_from_slice(&0_u16.to_be_bytes());
        assert_eq!(
            decode_operation(&zero_chunks),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        let mut wrong_chunks = encoded;
        wrong_chunks[116..118].copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            decode_operation(&wrong_chunks),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        let mut hostile_length = encoded;
        hostile_length[112..116].copy_from_slice(&u32::MAX.to_be_bytes());
        hostile_length[116..118].copy_from_slice(&u16::MAX.to_be_bytes());
        assert_eq!(
            decode_operation(&hostile_length),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        Ok(())
    }

    #[test]
    /// Scalar record codecs reject truncation
    fn scalar_record_codecs_reject_truncation() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(decode_u64(&41_u64.to_be_bytes())?, 41);
        assert_eq!(decode_u32(&42_u32.to_be_bytes())?, 42);
        assert_eq!(decode_u16(&43_u16.to_be_bytes())?, 43);
        assert_eq!(
            decode_u64(&[0; 7]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        assert_eq!(
            decode_u32(&[0; 3]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        assert_eq!(
            decode_u16(&[0; 1]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        assert_eq!(
            decode_uuid(&[0; 15]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        Ok(())
    }

    #[test]
    /// Retention record codec is versioned and fail closed
    fn retention_record_codec_is_versioned_and_fail_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(RETENTION_BYTES, 18);
        assert_eq!(RETENTION_VERSION, 1);
        let encoded = encode_retention(7, 9);
        assert_eq!(decode_retention(&encoded), Ok((7, 9)));
        assert_eq!(
            decode_retention(&[0; 18]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        assert_eq!(
            decode_retention(&encoded[..RETENTION_BYTES - 1]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        let mut unknown = encoded;
        unknown[..2].copy_from_slice(&(RETENTION_VERSION + 1).to_be_bytes());
        assert_eq!(
            decode_retention(&unknown),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        let mut regressed = encoded;
        regressed[2..10].copy_from_slice(&u64::MAX.to_be_bytes());
        regressed[10..18].copy_from_slice(&0_u64.to_be_bytes());
        assert_eq!(decode_retention(&regressed), Ok((u64::MAX, 0)));
        Ok(())
    }

    #[test]
    /// Snapshot record round trips through the versioned model codec
    fn snapshot_record_round_trips_through_the_versioned_model_codec()
    -> Result<(), Box<dyn std::error::Error>> {
        use logical_db_model::SnapshotBaseline;

        let baseline =
            SnapshotBaseline::new(41, logical_db_model::BaselineDigest::new([7; 32]), 0, 0);
        let encoded = baseline.encode();
        assert_eq!(
            decode_snapshot(&encoded),
            Ok(SnapshotRecord::State(baseline))
        );
        assert_eq!(
            decode_snapshot(&encoded[..encoded.len() - 1]),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        let mut foreign = encoded;
        foreign[..2].copy_from_slice(&0_u16.to_be_bytes());
        assert_eq!(
            decode_snapshot(&foreign),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        Ok(())
    }

    #[test]
    /// State baseline atlas rejects missing or corrupt chunks
    fn state_baseline_atlas_rejects_missing_or_corrupt_chunks()
    -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document =
            SnapshotDocument::new(DocumentId::new(), b"bounded snapshot".to_vec(), false);
        let artifact = SnapshotArtifact::new(scope, 7, vec![document.clone()])?;
        let digest = artifact.baseline.digest().into_bytes();
        let manifest = encode_baseline_document(&document)?;
        assert_eq!(manifest.len(), BASELINE_DOCUMENT_MANIFEST_BYTES);
        assert_eq!(
            decode_baseline_document(&manifest[..manifest.len() - 1]),
            Err(DataError::BaselineCorrupt)
        );
        let mut atlas = BaselineAtlas {
            manifests: BTreeMap::from([((digest, document.document), manifest.to_vec())]),
            chunks: BTreeMap::from([((digest, document.document, 0), document.snapshot.clone())]),
        };
        assert_eq!(
            validate_baseline_atlas(scope, &artifact.baseline, &atlas),
            Ok(artifact.clone())
        );
        let Some(chunk) = atlas.chunks.remove(&(digest, document.document, 0)) else {
            return Err(std::io::Error::other("baseline fixture chunk must exist").into());
        };
        assert_eq!(
            validate_baseline_atlas(scope, &artifact.baseline, &atlas),
            Err(DataError::BaselineCorrupt)
        );
        atlas.chunks.insert((digest, document.document, 0), chunk);
        let Some(corruptible) = atlas.chunks.get_mut(&(digest, document.document, 0)) else {
            return Err(
                std::io::Error::other("baseline fixture chunk must exist after reinsert").into(),
            );
        };
        corruptible.push(0xff);
        assert_eq!(
            validate_baseline_atlas(scope, &artifact.baseline, &atlas),
            Err(DataError::BaselineCorrupt)
        );
        Ok(())
    }

    /// CollectionAtlas
    struct CollectionAtlas {
        /// manifests
        manifests: BTreeMap<u64, Vec<u8>>,
        /// Chunks
        chunks: BTreeMap<(u64, u16), Vec<u8>>,
        /// operations
        operations: BTreeMap<OperationId, Vec<u8>>,
        /// invocations
        invocations: BTreeMap<[u8; 32], Vec<u8>>,
    }

    /// Build atlas
    fn build_atlas(
        sequences: &[u64],
    ) -> Result<(CollectionAtlas, Vec<Record>), Box<dyn std::error::Error>> {
        let mut atlas = CollectionAtlas {
            manifests: BTreeMap::new(),
            chunks: BTreeMap::new(),
            operations: BTreeMap::new(),
            invocations: BTreeMap::new(),
        };
        let mut records = Vec::new();
        for (index, sequence) in sequences.iter().enumerate() {
            let byte_len = index.saturating_add(3);
            let change = change(byte_len);
            let tag = u8::try_from(index)?;
            let invocation_id = [tag.wrapping_add(1); 32];
            let manifest = encode_manifest(&change, *sequence, invocation_id)?;
            let record = decode_operation(&manifest)?.record;
            let mut remaining = byte_len;
            for chunk_index in 0..record.chunks {
                let take = remaining.min(CHUNK_BYTES);
                atlas
                    .chunks
                    .insert((*sequence, chunk_index), vec![tag.wrapping_add(9); take]);
                remaining = remaining.saturating_sub(take);
            }
            atlas
                .manifests
                .insert(*sequence, manifest[..MANIFEST_BYTES].to_vec());
            atlas.operations.insert(record.operation, manifest.to_vec());
            atlas
                .invocations
                .insert(invocation_id, invocation_binding(&change).to_vec());
            records.push(record);
        }
        Ok((atlas, records))
    }

    /// Atlas view
    fn atlas_view(atlas: &CollectionAtlas) -> CollectionView<'_> {
        CollectionView {
            manifests: &atlas.manifests,
            chunks: &atlas.chunks,
            operations: &atlas.operations,
            invocations: &atlas.invocations,
        }
    }

    #[test]
    /// Prune plan keeps idempotency axes while dropping the pruned prefix
    fn prune_plan_keeps_idempotency_axes_while_dropping_the_pruned_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        use logical_db_service::{PruneRefusal, prune_decision};

        let (mut atlas, _records) = build_atlas(&[1, 2, 3, 4])?;
        assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
        assert_eq!(prune_decision(4, 0, 2, None, 4), Ok(2));
        assert_eq!(
            prune_decision(4, 0, 2, None, 1),
            Err(PruneRefusal::PendingOperations {
                consumed_through: 1
            })
        );
        atlas.manifests.remove(&1);
        atlas.manifests.remove(&2);
        atlas.chunks.retain(|&(sequence, _), _| sequence > 2u64);
        assert!(!atlas.manifests.contains_key(&1));
        assert!(!atlas.manifests.contains_key(&2));
        assert!(atlas.chunks.keys().all(|&(sequence, _)| sequence > 2u64));
        assert_eq!(atlas.operations.len(), 4);
        assert_eq!(atlas.invocations.len(), 4);
        assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
        Ok(())
    }

    #[test]
    /// Collection consistency checker rejects each broken cross reference
    fn collection_consistency_checker_rejects_each_broken_cross_reference()
    -> Result<(), Box<dyn std::error::Error>> {
        let (mut atlas, records) = build_atlas(&[1, 2])?;
        assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
        let missing_chunk = atlas.chunks.remove(&(2, 0)).ok_or("expected Some")?;
        assert_eq!(
            check_collection_consistency(&atlas_view(&atlas)),
            Err(DataError::MissingChunk)
        );
        atlas.chunks.insert((2, 0), missing_chunk);
        assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
        let missing_operation = atlas
            .operations
            .remove(&records[1].operation)
            .ok_or("expected Some")?;
        assert_eq!(
            check_collection_consistency(&atlas_view(&atlas)),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        atlas
            .operations
            .insert(records[1].operation, missing_operation);
        assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
        atlas.manifests.insert(2, vec![7; MANIFEST_BYTES]);
        assert_eq!(
            check_collection_consistency(&atlas_view(&atlas)),
            Err(DataError::InvalidManifest { at: "manifest" })
        );
        Ok(())
    }
}
