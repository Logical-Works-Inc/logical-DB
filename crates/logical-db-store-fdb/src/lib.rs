//! `FoundationDB` service adapter with private tuple subspaces and chunked values.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S42 S43. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

#[cfg(test)]
use foundationdb_tuple::Subspace;
#[cfg(any(feature = "fdb", test))]
use logical_db_model::{
    ChangeDigest, DocumentId, MAX_CHANGE_BYTES, MAX_CHANGES_PER_PAGE, MAX_PAGE_BYTES,
    OperationDigest, OperationId, ReplicaId, Scope, SnapshotBaseline,
};
#[cfg(any(feature = "fdb", test))]
use logical_db_protocol::ChangeEnvelope;
#[cfg(any(feature = "fdb", test))]
use thiserror::Error;

#[cfg(any(feature = "fdb", test))]
const FORMAT_VERSION: i64 = 3;
#[cfg(any(feature = "fdb", test))]
const KIND_LOG: i64 = 3;
#[cfg(any(feature = "fdb", test))]
const CHUNK_BYTES: usize = 65_536;
#[cfg(any(feature = "fdb", test))]
const MANIFEST_BYTES: usize = 118;
#[cfg(any(feature = "fdb", test))]
const OPERATION_BYTES: usize = 158;
#[cfg(any(feature = "fdb", test))]
const RETENTION_VERSION: u16 = 1;
#[cfg(any(feature = "fdb", test))]
const RETENTION_BYTES: usize = 18;

#[cfg(any(feature = "fdb", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PageReadPlan {
    limit: usize,
    target_bytes: usize,
}

#[cfg(any(feature = "fdb", test))]
fn page_read_plan(max_changes: u16) -> PageReadPlan {
    PageReadPlan {
        limit: usize::from(max_changes) + 1,
        target_bytes: usize::from(max_changes) * MANIFEST_BYTES,
    }
}

#[cfg(any(feature = "fdb", test))]
fn valid_page_limits(max_changes: u16, max_bytes: u32) -> bool {
    max_changes != 0
        && usize::from(max_changes) <= MAX_CHANGES_PER_PAGE
        && max_bytes != 0
        && max_bytes as usize <= MAX_PAGE_BYTES
}

#[cfg(any(feature = "fdb", test))]
fn logical_page_more(
    frontier: u64,
    position: u64,
    range_more: bool,
    stopped_at_page_boundary: bool,
) -> Result<bool, DataError> {
    if position > frontier || (stopped_at_page_boundary && position == frontier) {
        return Err(DataError::InvalidManifest);
    }
    let more = position < frontier;
    if more && !range_more && !stopped_at_page_boundary {
        return Err(DataError::InvalidManifest);
    }
    Ok(more)
}

#[cfg(any(feature = "fdb", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Record {
    document: DocumentId,
    replica: ReplicaId,
    operation: OperationId,
    request: OperationDigest,
    digest: ChangeDigest,
    byte_len: usize,
    chunks: u16,
    sequence: u64,
}

#[cfg(any(feature = "fdb", test))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OperationRecord {
    record: Record,
    invocation_id: [u8; 32],
}

#[cfg(any(feature = "fdb", test))]
impl Record {
    fn matches(self, change: &ChangeEnvelope) -> bool {
        self.document == change.document
            && self.replica == change.replica
            && self.operation == change.operation
            && self.request == change.request
            && self.digest == change.digest
            && self.byte_len == change.bytes.len()
            && self.chunks == chunk_count(change.bytes.len())
    }

    fn envelope(self, scope: Scope, bytes: Vec<u8>) -> ChangeEnvelope {
        ChangeEnvelope {
            scope,
            document: self.document,
            replica: self.replica,
            operation: self.operation,
            request: self.request,
            digest: self.digest,
            bytes,
        }
    }
}

#[cfg(any(feature = "fdb", test))]
fn encode_manifest(
    change: &ChangeEnvelope,
    sequence: u64,
    invocation_id: [u8; 32],
) -> Result<[u8; OPERATION_BYTES], DataError> {
    if !(1..=MAX_CHANGE_BYTES).contains(&change.bytes.len()) {
        return Err(DataError::InvalidManifest);
    }
    let byte_len = u32::try_from(change.bytes.len()).map_err(|_| DataError::InvalidManifest)?;
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
fn decode_manifest(bytes: &[u8], sequence: u64) -> Result<Record, DataError> {
    if bytes.len() != MANIFEST_BYTES {
        return Err(DataError::InvalidManifest);
    }
    decode_record(bytes, sequence)
}

#[cfg(any(feature = "fdb", test))]
fn decode_operation(bytes: &[u8]) -> Result<OperationRecord, DataError> {
    if bytes.len() != OPERATION_BYTES {
        return Err(DataError::InvalidManifest);
    }
    let record = decode_record(&bytes[..MANIFEST_BYTES], decode_u64(&bytes[118..126])?)?;
    let invocation_id = bytes[126..158]
        .try_into()
        .map_err(|_| DataError::InvalidManifest)?;
    Ok(OperationRecord {
        record,
        invocation_id,
    })
}

#[cfg(test)]
use std::collections::BTreeMap;

#[cfg(any(feature = "fdb", test))]
fn encode_retention(floor: u64, consumed_through: u64) -> [u8; RETENTION_BYTES] {
    let mut bytes = [0; RETENTION_BYTES];
    bytes[..2].copy_from_slice(&RETENTION_VERSION.to_be_bytes());
    bytes[2..10].copy_from_slice(&floor.to_be_bytes());
    bytes[10..18].copy_from_slice(&consumed_through.to_be_bytes());
    bytes
}

#[cfg(any(feature = "fdb", test))]
fn decode_retention(bytes: &[u8]) -> Result<(u64, u64), DataError> {
    if bytes.len() != RETENTION_BYTES {
        return Err(DataError::InvalidManifest);
    }
    if decode_u16(&bytes[..2])? != RETENTION_VERSION {
        return Err(DataError::InvalidManifest);
    }
    Ok((decode_u64(&bytes[2..10])?, decode_u64(&bytes[10..18])?))
}

#[cfg(any(feature = "fdb", test))]
fn decode_snapshot(bytes: &[u8]) -> Result<SnapshotBaseline, DataError> {
    SnapshotBaseline::decode(bytes).map_err(|_| DataError::InvalidManifest)
}

/// Durable collection view mirroring one scope's tuple subspaces.
#[cfg(test)]
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
fn check_collection_consistency(view: &CollectionView<'_>) -> Result<(), DataError> {
    for (sequence, manifest) in view.manifests {
        let record = decode_manifest(manifest, *sequence)?;
        let operation_bytes = view
            .operations
            .get(&record.operation)
            .ok_or(DataError::InvalidManifest)?;
        let operation = decode_operation(operation_bytes)?;
        if operation.record != record {
            return Err(DataError::InvalidManifest);
        }
        let invocation = view
            .invocations
            .get(&operation.invocation_id)
            .ok_or(DataError::MissingInvocation)?;
        if invocation.as_slice() != invocation_binding_record(&record) {
            return Err(DataError::InvalidInvocation);
        }
        let mut total = 0usize;
        for index in 0..record.chunks {
            let chunk = view
                .chunks
                .get(&(*sequence, index))
                .ok_or(DataError::MissingChunk)?;
            if chunk.is_empty() || chunk.len() > CHUNK_BYTES {
                return Err(DataError::InvalidChunk);
            }
            total = total
                .checked_add(chunk.len())
                .ok_or(DataError::InvalidManifest)?;
        }
        if total != record.byte_len {
            return Err(DataError::InvalidChunk);
        }
    }
    Ok(())
}

#[cfg(any(feature = "fdb", test))]
fn invocation_binding(change: &ChangeEnvelope) -> [u8; 80] {
    let mut binding = [0_u8; 80];
    binding[..16].copy_from_slice(change.operation.as_uuid().as_bytes());
    binding[16..48].copy_from_slice(&change.request.into_bytes());
    binding[48..].copy_from_slice(&change.digest.into_bytes());
    binding
}

#[cfg(any(feature = "fdb", test))]
fn invocation_binding_record(record: &Record) -> [u8; 80] {
    let mut binding = [0_u8; 80];
    binding[..16].copy_from_slice(record.operation.as_uuid().as_bytes());
    binding[16..48].copy_from_slice(&record.request.into_bytes());
    binding[48..].copy_from_slice(&record.digest.into_bytes());
    binding
}

#[cfg(any(feature = "fdb", test))]
fn decode_record(bytes: &[u8], sequence: u64) -> Result<Record, DataError> {
    let document = decode_uuid(&bytes[..16]).map(DocumentId::from_uuid)?;
    let replica = decode_uuid(&bytes[16..32]).map(ReplicaId::from_uuid)?;
    let operation = decode_uuid(&bytes[32..48]).map(OperationId::from_uuid)?;
    let request = bytes[48..80]
        .try_into()
        .map(OperationDigest::new)
        .map_err(|_| DataError::InvalidManifest)?;
    let digest = bytes[80..112]
        .try_into()
        .map(ChangeDigest::new)
        .map_err(|_| DataError::InvalidManifest)?;
    let byte_len =
        usize::try_from(decode_u32(&bytes[112..116])?).map_err(|_| DataError::InvalidManifest)?;
    let chunks = decode_u16(&bytes[116..118])?;
    if !(1..=MAX_CHANGE_BYTES).contains(&byte_len) || chunks != chunk_count(byte_len) {
        return Err(DataError::InvalidManifest);
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
fn chunk_count(byte_len: usize) -> u16 {
    let chunks = byte_len.saturating_add(CHUNK_BYTES - 1) / CHUNK_BYTES;
    u16::try_from(chunks).unwrap_or(u16::MAX)
}

#[cfg(any(feature = "fdb", test))]
fn decode_uuid(bytes: &[u8]) -> Result<uuid::Uuid, DataError> {
    uuid::Uuid::from_slice(bytes).map_err(|_| DataError::InvalidManifest)
}

#[cfg(any(feature = "fdb", test))]
fn decode_u64(bytes: &[u8]) -> Result<u64, DataError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_| DataError::InvalidManifest)
}

#[cfg(any(feature = "fdb", test))]
fn decode_u32(bytes: &[u8]) -> Result<u32, DataError> {
    bytes
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| DataError::InvalidManifest)
}

#[cfg(any(feature = "fdb", test))]
fn decode_u16(bytes: &[u8]) -> Result<u16, DataError> {
    bytes
        .try_into()
        .map(u16::from_be_bytes)
        .map_err(|_| DataError::InvalidManifest)
}

#[cfg(any(feature = "fdb", test))]
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
/// Stable Logical-DB corruption and resource failures decoded inside an FDB transaction.
pub enum FdbDataError {
    /// Operation or log manifest violated its fixed binary format.
    #[error("FoundationDB service manifest is malformed")]
    InvalidManifest,
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
type DataError = FdbDataError;

#[cfg(feature = "fdb")]
mod runtime {
    use super::{
        CHUNK_BYTES, DataError, FORMAT_VERSION, KIND_LOG, MANIFEST_BYTES, Record, decode_manifest,
        decode_operation, decode_retention, decode_u64, encode_manifest, encode_retention,
        invocation_binding, invocation_binding_record, logical_page_more, page_read_plan,
        valid_page_limits,
    };
    use foundationdb::{
        Database, FdbBindingError, KeySelector, RangeOption, options::TransactionOption,
    };
    use foundationdb_tuple::Subspace;
    use logical_db_model::{Effect, OperationDigest, Scope, SnapshotBaseline};
    use logical_db_protocol::ChangeEnvelope;
    use logical_db_service::{
        AcceptResult, ConsumedResult, ExecutionGrant, PageResult, PruneReceipt, PruneResult,
        RetentionStore, SealResult, ServiceStore, StoreFailureClass, StoredPage, prune_decision,
    };
    use thiserror::Error;

    const KIND_OPERATION: i64 = 1;
    const KIND_SEQUENCE: i64 = 2;
    const KIND_CHUNK: i64 = 4;
    const KIND_INVOCATION: i64 = 5;
    const KIND_RETENTION: i64 = 6;
    const KIND_SNAPSHOT: i64 = 7;
    const TRANSACTION_TIMEOUT_MS: i32 = 4_000;
    const TRANSACTION_RETRY_LIMIT: i32 = 8;

    #[derive(Clone)]
    struct PageSpaces {
        scope: Subspace,
        sequence_key: Vec<u8>,
        retention_key: Vec<u8>,
        log: Subspace,
        chunk: Subspace,
    }

    #[derive(Clone, Copy)]
    struct PageQuery {
        scope: Scope,
        frontier: u64,
        after: u64,
        max_changes: u16,
        max_bytes: u32,
    }

    /// Async multi-tenant service store over a host-booted `FoundationDB` client.
    ///
    /// The host allocates `root` through the directory layer and owns the network
    /// lifecycle. Logical-DB never exposes physical tuple bytes to applications.
    pub struct FdbServiceStore {
        database: Database,
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

        fn scope_space(&self, scope: Scope) -> Subspace {
            self.root.subspace(&(
                FORMAT_VERSION,
                scope.account().as_uuid(),
                scope.space().as_uuid(),
            ))
        }
    }

    #[allow(async_fn_in_trait)]
    impl ServiceStore for FdbServiceStore {
        type Error = FdbStoreError;

        fn classify_error(error: &Self::Error) -> StoreFailureClass {
            match error {
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
                || grant.request() != change.request
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
                        configure_transaction(&transaction)?;
                        if let Some(result) = existing_accept_result(
                            &transaction,
                            &scope_space,
                            &chunk_space,
                            &operation_key,
                            &retention_key,
                            &change,
                            bool::from(maybe_committed),
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
                        let manifest =
                            encode_manifest(&change, sequence, invocation_id).map_err(binding)?;
                        for (index, bytes) in change.bytes.chunks(CHUNK_BYTES).enumerate() {
                            let index = u16::try_from(index)
                                .map_err(|_| binding(DataError::InvalidManifest))?;
                            transaction.set(&chunk_space.pack(&(sequence, index)), bytes);
                        }
                        transaction.set(&log_space.pack(&(sequence,)), &manifest[..MANIFEST_BYTES]);
                        transaction.set(&operation_key, &manifest);
                        transaction.set(&sequence_key, &sequence.to_be_bytes());
                        let invocation_record = invocation_binding(&change);
                        transaction.set(&invocation_key, &invocation_record);
                        Ok(AcceptResult::Accepted)
                    }
                })
                .await
                .map_err(classify_transaction)
        }

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

        async fn page(
            &self,
            scope: Scope,
            request: OperationDigest,
            after: u64,
            max_changes: u16,
            max_bytes: u32,
            grant: &impl ExecutionGrant,
        ) -> Result<PageResult, Self::Error> {
            if !valid_page_limits(max_changes, max_bytes) {
                return Err(FdbStoreError::InvalidPageLimits);
            }
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.request() != request
            {
                return Ok(PageResult::StaleAuthority);
            }
            let expected_frontier = grant.frontier();
            let scope_space = self.scope_space(scope);
            let spaces = PageSpaces {
                sequence_key: scope_space.pack(&(KIND_SEQUENCE,)),
                retention_key: scope_space.pack(&(KIND_RETENTION,)),
                log: scope_space.subspace(&(KIND_LOG,)),
                chunk: scope_space.subspace(&(KIND_CHUNK,)),
                scope: scope_space,
            };
            let query = PageQuery {
                scope,
                frontier: expected_frontier,
                after,
                max_changes,
                max_bytes,
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

    #[allow(async_fn_in_trait)]
    impl RetentionStore for FdbServiceStore {
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

        async fn snapshot_baseline(
            &self,
            scope: Scope,
        ) -> Result<Option<SnapshotBaseline>, Self::Error> {
            use super::decode_snapshot;

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

        async fn seal_snapshot(
            &self,
            scope: Scope,
            baseline: SnapshotBaseline,
            request: OperationDigest,
            grant: &impl ExecutionGrant,
        ) -> Result<SealResult, Self::Error> {
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.request() != request
            {
                return Ok(SealResult::StaleAuthority);
            }
            let expected_frontier = grant.frontier();
            let position = baseline.position();
            let encoded = baseline.encode();
            let scope_space = self.scope_space(scope);
            let sequence_key = scope_space.pack(&(KIND_SEQUENCE,));
            let snapshot_key = scope_space.pack(&(KIND_SNAPSHOT,));
            self.database
                .run(move |transaction, _maybe_committed| {
                    let sequence_key = sequence_key.clone();
                    let snapshot_key = snapshot_key.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        let frontier = transaction
                            .get(&sequence_key, false)
                            .await?
                            .map(|value| decode_u64(value.as_ref()).map_err(binding))
                            .transpose()?
                            .unwrap_or(0);
                        if frontier != expected_frontier {
                            return Ok(SealResult::StaleAuthority);
                        }
                        if position > frontier {
                            return Ok(SealResult::BeyondFrontier { frontier });
                        }
                        transaction.set(&snapshot_key, &encoded);
                        Ok(SealResult::Sealed { position })
                    }
                })
                .await
                .map_err(classify_transaction)
        }

        async fn mark_consumed(
            &self,
            scope: Scope,
            through: u64,
            request: OperationDigest,
            grant: &impl ExecutionGrant,
        ) -> Result<ConsumedResult, Self::Error> {
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.request() != request
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
                        let frontier = transaction
                            .get(&sequence_key, false)
                            .await?
                            .map(|value| decode_u64(value.as_ref()).map_err(binding))
                            .transpose()?
                            .unwrap_or(0);
                        if frontier != expected_frontier {
                            return Ok(ConsumedResult::StaleAuthority);
                        }
                        if through > frontier {
                            return Ok(ConsumedResult::BeyondFrontier { frontier });
                        }
                        let (floor, consumed) = transaction
                            .get(&retention_key, false)
                            .await?
                            .map(|value| decode_retention(value.as_ref()).map_err(binding))
                            .transpose()?
                            .unwrap_or((0, 0));
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
                || grant.request() != request
            {
                return Ok(PruneResult::StaleAuthority);
            }
            let expected_frontier = grant.frontier();
            let scope_space = self.scope_space(scope);
            let sequence_key = scope_space.pack(&(KIND_SEQUENCE,));
            let retention_key = scope_space.pack(&(KIND_RETENTION,));
            let log_space = scope_space.subspace(&(KIND_LOG,));
            let chunk_space = scope_space.subspace(&(KIND_CHUNK,));
            self.database
                .run(move |transaction, _maybe_committed| {
                    let sequence_key = sequence_key.clone();
                    let retention_key = retention_key.clone();
                    let log_space = log_space.clone();
                    let chunk_space = chunk_space.clone();
                    async move {
                        configure_transaction(&transaction)?;
                        let frontier = transaction
                            .get(&sequence_key, false)
                            .await?
                            .map(|value| decode_u64(value.as_ref()).map_err(binding))
                            .transpose()?
                            .unwrap_or(0);
                        if frontier != expected_frontier {
                            return Ok(PruneResult::StaleAuthority);
                        }
                        let (floor, consumed) = transaction
                            .get(&retention_key, false)
                            .await?
                            .map(|value| decode_retention(value.as_ref()).map_err(binding))
                            .transpose()?
                            .unwrap_or((0, 0));
                        let through =
                            match prune_decision(frontier, floor, up_to, min_live, consumed) {
                                Ok(through) => through,
                                Err(refusal) => return Ok(PruneResult::Refused(refusal)),
                            };
                        if through > floor {
                            let log_begin = log_space.pack(&(floor.saturating_add(1),));
                            let log_end = through.checked_add(1).map_or_else(
                                || log_space.range().1,
                                |next| log_space.pack(&(next,)),
                            );
                            transaction.clear_range(&log_begin, &log_end);
                            let chunk_begin = chunk_space.pack(&(floor.saturating_add(1),));
                            let chunk_end = through.checked_add(1).map_or_else(
                                || chunk_space.range().1,
                                |next| chunk_space.pack(&(next,)),
                            );
                            transaction.clear_range(&chunk_begin, &chunk_end);
                            transaction.set(&retention_key, &encode_retention(through, consumed));
                        }
                        Ok(PruneResult::Pruned(PruneReceipt {
                            pruned_through: through,
                            floor: through.max(floor),
                        }))
                    }
                })
                .await
                .map_err(classify_transaction)
        }
    }

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
            return Err(binding(DataError::InvalidManifest));
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

    fn configure_transaction(
        transaction: &foundationdb::RetryableTransaction,
    ) -> Result<(), FdbBindingError> {
        transaction.set_option(TransactionOption::Timeout(TRANSACTION_TIMEOUT_MS))?;
        transaction.set_option(TransactionOption::RetryLimit(TRANSACTION_RETRY_LIMIT))?;
        Ok(())
    }

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
            let (sequence,) = spaces
                .log
                .unpack::<(u64,)>(&key)
                .map_err(|_| binding(DataError::InvalidManifest))?;
            if position.checked_add(1) != Some(sequence) {
                return Err(binding(DataError::InvalidManifest));
            }
            let record = decode_manifest(&value, sequence).map_err(binding)?;
            verify_operation(transaction, &spaces.scope, &record).await?;
            let next_bytes = aggregate
                .checked_add(record.byte_len)
                .ok_or_else(|| binding(DataError::InvalidManifest))?;
            if next_bytes > query.max_bytes as usize {
                if changes.is_empty() {
                    return Err(binding(DataError::ChangeExceedsPage));
                }
                stopped_at_page_boundary = true;
                break;
            }
            let bytes = load_change_bytes(transaction, &spaces.chunk, sequence, &record).await?;
            changes.push(record.envelope(query.scope, bytes));
            aggregate = next_bytes;
            position = sequence;
        }
        if position == frontier && log_has_entry_after(transaction, &spaces.log, frontier).await? {
            return Err(binding(DataError::InvalidManifest));
        }
        let more = logical_page_more(frontier, position, range_more, stopped_at_page_boundary)
            .map_err(binding)?;
        Ok(PageResult::Page(StoredPage {
            changes,
            position,
            more,
        }))
    }

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

    async fn verify_operation(
        transaction: &foundationdb::RetryableTransaction,
        scope_space: &Subspace,
        record: &Record,
    ) -> Result<(), FdbBindingError> {
        let operation_key = scope_space.pack(&(KIND_OPERATION, record.operation.as_uuid()));
        let operation_bytes = transaction
            .get(&operation_key, false)
            .await?
            .ok_or_else(|| binding(DataError::InvalidManifest))?;
        let operation = decode_operation(operation_bytes.as_ref()).map_err(binding)?;
        if operation.record != *record {
            return Err(binding(DataError::InvalidManifest));
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

    fn binding(error: DataError) -> FdbBindingError {
        FdbBindingError::new_custom_error(Box::new(error))
    }

    fn classify_transaction(error: FdbBindingError) -> FdbStoreError {
        if let FdbBindingError::CustomError(custom) = &error
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
        fn native_and_corruption_failures_keep_stable_recovery_categories() {
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
        }
    }
}

#[cfg(feature = "fdb")]
pub use runtime::{FdbServiceStore, FdbStoreError};

#[cfg(test)]
mod tests {
    use logical_db_model::{AccountId, SpaceId};
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn tuple_axis_is_scope_isolated_and_sequence_ordered() {
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
    }

    fn change(byte_len: usize) -> ChangeEnvelope {
        ChangeEnvelope {
            scope: Scope::new(AccountId::new(), SpaceId::new()),
            document: DocumentId::new(),
            replica: ReplicaId::new(),
            operation: OperationId::new(),
            request: OperationDigest::new([7; 32]),
            digest: ChangeDigest::new([9; 32]),
            bytes: vec![3; byte_len],
        }
    }

    #[test]
    fn manifest_round_trip_preserves_every_axis_and_rejects_each_rebinding() {
        let change = change(CHUNK_BYTES + 1);
        let invocation_id = [0x5a; 32];
        let binding = invocation_binding(&change);
        assert_eq!(&binding[..16], change.operation.as_uuid().as_bytes());
        assert_eq!(&binding[16..48], &change.request.into_bytes());
        assert_eq!(&binding[48..], &change.digest.into_bytes());
        let encoded = encode_manifest(&change, 41, invocation_id).unwrap();
        assert_ne!(encoded, [0; OPERATION_BYTES]);
        let operation = decode_operation(&encoded).unwrap();
        assert_eq!(operation.invocation_id, invocation_id);
        let record = operation.record;
        assert_eq!(record.sequence, 41);
        assert!(record.matches(&change));
        assert_eq!(record.envelope(change.scope, change.bytes.clone()), change);
        assert_eq!(
            decode_manifest(&encoded[..MANIFEST_BYTES], 99)
                .unwrap()
                .sequence,
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
    }

    #[test]
    fn manifest_and_chunk_boundaries_fail_closed_without_a_cluster() {
        assert_eq!(CHUNK_BYTES, 65_536);
        assert!(valid_page_limits(1, 1));
        assert!(valid_page_limits(
            u16::try_from(MAX_CHANGES_PER_PAGE).unwrap(),
            u32::try_from(MAX_PAGE_BYTES).unwrap()
        ));
        assert!(!valid_page_limits(0, 1));
        assert!(!valid_page_limits(1, 0));
        assert!(!valid_page_limits(
            u16::try_from(MAX_CHANGES_PER_PAGE + 1).unwrap(),
            1
        ));
        assert!(!valid_page_limits(
            1,
            u32::try_from(MAX_PAGE_BYTES + 1).unwrap()
        ));
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
            Err(DataError::InvalidManifest)
        );
        assert_eq!(
            logical_page_more(10, 10, false, true),
            Err(DataError::InvalidManifest)
        );
        assert_eq!(
            logical_page_more(10, 9, false, false),
            Err(DataError::InvalidManifest)
        );
        assert_eq!(chunk_count(0), 0);
        assert_eq!(chunk_count(1), 1);
        assert_eq!(chunk_count(CHUNK_BYTES), 1);
        assert_eq!(chunk_count(CHUNK_BYTES + 1), 2);
        assert_eq!(chunk_count(usize::MAX), u16::MAX);
        assert_eq!(
            encode_manifest(&change(0), 1, [1; 32]),
            Err(DataError::InvalidManifest)
        );
        assert_eq!(
            encode_manifest(&change(MAX_CHANGE_BYTES + 1), 1, [1; 32]),
            Err(DataError::InvalidManifest)
        );
        let encoded = encode_manifest(&change(1), u64::MAX, [2; 32]).unwrap();
        assert_eq!(
            decode_operation(&encoded).unwrap().record.sequence,
            u64::MAX
        );
        assert_eq!(
            decode_operation(&encoded[..OPERATION_BYTES - 1]),
            Err(DataError::InvalidManifest)
        );
        assert_eq!(
            decode_manifest(&encoded[..MANIFEST_BYTES - 1], 1),
            Err(DataError::InvalidManifest)
        );
        let mut zero_length = encoded;
        zero_length[112..116].copy_from_slice(&0_u32.to_be_bytes());
        zero_length[116..118].copy_from_slice(&0_u16.to_be_bytes());
        assert_eq!(
            decode_operation(&zero_length),
            Err(DataError::InvalidManifest)
        );
        let mut zero_chunks = encoded;
        zero_chunks[116..118].copy_from_slice(&0_u16.to_be_bytes());
        assert_eq!(
            decode_operation(&zero_chunks),
            Err(DataError::InvalidManifest)
        );
        let mut wrong_chunks = encoded;
        wrong_chunks[116..118].copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            decode_operation(&wrong_chunks),
            Err(DataError::InvalidManifest)
        );
        let mut hostile_length = encoded;
        hostile_length[112..116].copy_from_slice(&u32::MAX.to_be_bytes());
        hostile_length[116..118].copy_from_slice(&u16::MAX.to_be_bytes());
        assert_eq!(
            decode_operation(&hostile_length),
            Err(DataError::InvalidManifest)
        );
        assert_eq!(decode_u64(&41_u64.to_be_bytes()).unwrap(), 41);
        assert_eq!(decode_u32(&42_u32.to_be_bytes()).unwrap(), 42);
        assert_eq!(decode_u16(&43_u16.to_be_bytes()).unwrap(), 43);
        assert_eq!(decode_u64(&[0; 7]), Err(DataError::InvalidManifest));
        assert_eq!(decode_u32(&[0; 3]), Err(DataError::InvalidManifest));
        assert_eq!(decode_u16(&[0; 1]), Err(DataError::InvalidManifest));
        assert_eq!(decode_uuid(&[0; 15]), Err(DataError::InvalidManifest));
    }

    #[test]
    fn retention_record_codec_is_versioned_and_fail_closed() {
        assert_eq!(RETENTION_BYTES, 18);
        assert_eq!(RETENTION_VERSION, 1);
        let encoded = encode_retention(7, 9);
        assert_eq!(decode_retention(&encoded), Ok((7, 9)));
        assert_eq!(decode_retention(&[0; 18]), Err(DataError::InvalidManifest));
        assert_eq!(
            decode_retention(&encoded[..RETENTION_BYTES - 1]),
            Err(DataError::InvalidManifest)
        );
        let mut unknown = encoded;
        unknown[..2].copy_from_slice(&(RETENTION_VERSION + 1).to_be_bytes());
        assert_eq!(decode_retention(&unknown), Err(DataError::InvalidManifest));
        let mut regressed = encoded;
        regressed[2..10].copy_from_slice(&u64::MAX.to_be_bytes());
        regressed[10..18].copy_from_slice(&0_u64.to_be_bytes());
        assert_eq!(decode_retention(&regressed), Ok((u64::MAX, 0)));
    }

    #[test]
    fn snapshot_record_round_trips_through_the_versioned_model_codec() {
        use logical_db_model::SnapshotBaseline;

        let baseline = SnapshotBaseline::new(41);
        let encoded = baseline.encode();
        assert_eq!(decode_snapshot(&encoded), Ok(baseline));
        assert_eq!(
            decode_snapshot(&encoded[..encoded.len() - 1]),
            Err(DataError::InvalidManifest)
        );
        let mut foreign = encoded;
        foreign[..2].copy_from_slice(&0_u16.to_be_bytes());
        assert_eq!(decode_snapshot(&foreign), Err(DataError::InvalidManifest));
    }

    struct CollectionAtlas {
        manifests: BTreeMap<u64, Vec<u8>>,
        chunks: BTreeMap<(u64, u16), Vec<u8>>,
        operations: BTreeMap<OperationId, Vec<u8>>,
        invocations: BTreeMap<[u8; 32], Vec<u8>>,
    }

    fn build_atlas(sequences: &[u64]) -> (CollectionAtlas, Vec<Record>) {
        let mut atlas = CollectionAtlas {
            manifests: BTreeMap::new(),
            chunks: BTreeMap::new(),
            operations: BTreeMap::new(),
            invocations: BTreeMap::new(),
        };
        let mut records = Vec::new();
        for (index, sequence) in sequences.iter().enumerate() {
            let byte_len = 3 + index;
            let change = change(byte_len);
            let tag = u8::try_from(index).unwrap();
            let invocation_id = [tag.wrapping_add(1); 32];
            let manifest = encode_manifest(&change, *sequence, invocation_id).unwrap();
            let record = decode_operation(&manifest).unwrap().record;
            let mut remaining = byte_len;
            for chunk_index in 0..record.chunks {
                let take = remaining.min(CHUNK_BYTES);
                atlas
                    .chunks
                    .insert((*sequence, chunk_index), vec![tag.wrapping_add(9); take]);
                remaining -= take;
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
        (atlas, records)
    }

    fn atlas_view(atlas: &CollectionAtlas) -> CollectionView<'_> {
        CollectionView {
            manifests: &atlas.manifests,
            chunks: &atlas.chunks,
            operations: &atlas.operations,
            invocations: &atlas.invocations,
        }
    }

    #[test]
    fn prune_plan_keeps_idempotency_axes_while_dropping_the_pruned_prefix() {
        use logical_db_service::{PruneRefusal, prune_decision};

        let (mut atlas, _records) = build_atlas(&[1, 2, 3, 4]);
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
        atlas.chunks.retain(|(sequence, _), _| *sequence > 2);
        assert!(!atlas.manifests.contains_key(&1));
        assert!(!atlas.manifests.contains_key(&2));
        assert!(atlas.chunks.keys().all(|(sequence, _)| *sequence > 2));
        assert_eq!(atlas.operations.len(), 4);
        assert_eq!(atlas.invocations.len(), 4);
        assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
    }

    #[test]
    fn collection_consistency_checker_rejects_each_broken_cross_reference() {
        let (mut atlas, records) = build_atlas(&[1, 2]);
        assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
        let missing_chunk = atlas.chunks.remove(&(2, 0)).unwrap();
        assert_eq!(
            check_collection_consistency(&atlas_view(&atlas)),
            Err(DataError::MissingChunk)
        );
        atlas.chunks.insert((2, 0), missing_chunk);
        assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
        let missing_operation = atlas.operations.remove(&records[1].operation).unwrap();
        assert_eq!(
            check_collection_consistency(&atlas_view(&atlas)),
            Err(DataError::InvalidManifest)
        );
        atlas
            .operations
            .insert(records[1].operation, missing_operation);
        assert_eq!(check_collection_consistency(&atlas_view(&atlas)), Ok(()));
        atlas.manifests.insert(2, vec![7; MANIFEST_BYTES]);
        assert_eq!(
            check_collection_consistency(&atlas_view(&atlas)),
            Err(DataError::InvalidManifest)
        );
    }
}
