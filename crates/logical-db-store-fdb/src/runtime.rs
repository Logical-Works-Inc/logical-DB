//! `FoundationDB` runtime: the store, its retention contract and failure classes.

mod baseline;
mod transaction;

use baseline::{
    baseline_spaces, read_baseline_page_transaction, reconstruct_artifact, seal_transaction,
};
use foundationdb::{Database, FdbBindingError};
use foundationdb_tuple::Subspace;
use logical_db_model::{
    Effect, OperationDigest, Scope, SnapshotArtifact, SnapshotBaseline, SnapshotRecord,
};
use logical_db_protocol::{
    ChangeEnvelope, PageAction, RecoveryAction, mark_consumed_action, prune_action,
    seal_snapshot_action, write_action,
};
use logical_db_service::{
    AcceptResult, BaselinePageResult, ConsumedResult, ExecutionGrant, PageResult, PruneResult,
    RetentionStore, SealResult, ServiceStore, StoreFailureClass,
};
use thiserror::Error;
use transaction::{
    accept_transaction, binding, classify_transaction, configure_transaction, load_page,
    prune_transaction, read_frontier, read_retention,
};

use crate::{
    DataError, FORMAT_VERSION, KIND_LOG, decode_retention, decode_snapshot, decode_u64,
    encode_retention, tuple_id, valid_page_limits,
};

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

impl std::fmt::Debug for FdbServiceStore {
    /// Shows the root prefix only; the client handle has no stable rendering.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FdbServiceStore")
            .field("root", &self.root.bytes())
            .finish_non_exhaustive()
    }
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
            tuple_id(scope.account().as_uuid()),
            tuple_id(scope.space().as_uuid()),
        ))
    }

    /// Reads one position key in its own transaction. An absent key is a scope
    /// that never committed, so it stands at the log origin, zero.
    async fn read_position(
        &self,
        key: Vec<u8>,
        decode: fn(&[u8]) -> Result<u64, DataError>,
    ) -> Result<u64, FdbStoreError> {
        self.database
            .run(move |transaction, _maybe_committed| {
                let key = key.clone();
                async move {
                    configure_transaction(&transaction)?;
                    match transaction.get(&key, false).await? {
                        Some(value) => decode(value.as_ref()).map_err(binding),
                        None => Ok(0),
                    }
                }
            })
            .await
            .map_err(classify_transaction)
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
            | FdbStoreError::InvalidArtifact(_)
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
        if grant.scope() != change.scope()
            || grant.effect() != Effect::Write
            || grant.action() != write_action(change)
        {
            return Ok(AcceptResult::StaleAuthority);
        }
        let expected_frontier = grant.frontier();
        let invocation_id = grant.invocation_id();
        let change = change.clone();
        let scope_space = self.scope_space(change.scope());
        let operation_key =
            scope_space.pack(&(KIND_OPERATION, tuple_id(change.operation().as_uuid())));
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
        let key = self.scope_space(scope).pack(&(KIND_SEQUENCE,));
        self.read_position(key, decode_u64).await
    }

    /// Runs `retention_floor`.
    async fn retention_floor(&self, scope: Scope) -> Result<u64, Self::Error> {
        let key = self.scope_space(scope).pack(&(KIND_RETENTION,));
        self.read_position(key, |bytes| Ok(decode_retention(bytes)?.0))
            .await
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
        let key = self.scope_space(scope).pack(&(KIND_RETENTION,));
        self.read_position(key, |bytes| Ok(decode_retention(bytes)?.1))
            .await
    }

    /// Runs `snapshot_baseline`.
    async fn snapshot_baseline(&self, scope: Scope) -> Result<Option<SnapshotRecord>, Self::Error> {
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
                        read_frontier(&transaction, &scope_space.pack(&(KIND_SEQUENCE,))).await?;
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
        let after = action
            .after_document
            .map(|document| tuple_id(document.as_uuid()));
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
            .map_err(FdbStoreError::InvalidArtifact)?;
        let baseline = artifact.baseline().clone();
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
                    let (floor, consumed) = read_retention(&transaction, &retention_key).await?;
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
    /// A snapshot artifact offered for sealing failed its model validation.
    #[error("FoundationDB adapter rejected an invalid snapshot artifact")]
    InvalidArtifact(#[source] logical_db_model::ModelError),
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
            FdbServiceStore::classify_error(&FdbStoreError::CommitMayHaveSucceeded { code: 1_021 }),
            StoreFailureClass::Transient
        );
        assert_eq!(
            FdbServiceStore::classify_error(&FdbStoreError::UnallocatedRoot),
            StoreFailureClass::Permanent
        );
    }
}
