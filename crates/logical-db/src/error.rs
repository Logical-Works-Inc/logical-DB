//! Stable failure taxonomy for the embedded facade.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use logical_db_crdt::CrdtError;
use logical_db_model::{ChangeDigest, DocumentId, OperationId};
use logical_db_platform::PlatformError;
use logical_db_store::StoreError;
use logical_db_store::projection_indexed::IndexedError;
use logical_db_store::projection_snapshot::SnapshotError;
use thiserror::Error;

/// Stable failures exposed by the embedded facade.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum DatabaseError {
    /// Structured document processing failed.
    #[error("structured document failed: {0}")]
    Crdt(#[from] CrdtError),
    /// Durable local storage failed.
    #[error("local replica storage failed: {0}")]
    Store(#[from] StoreError),
    /// Native per-user placement policy failed.
    #[error("local replica placement failed: {0}")]
    Platform(#[from] PlatformError),
    /// Private replica directory creation failed.
    #[error("local replica directory failed: {0}")]
    Io(#[from] std::io::Error),
    /// Platform helper produced a path with no parent directory.
    #[error("native local replica path has no parent")]
    InvalidDefaultPath,
    /// Two projection functions claimed the same stable index identity.
    #[error("projection set contains a duplicate index identity")]
    DuplicateProjection,
    /// An atomic edit batch attempted to update one document more than once.
    #[error("atomic edit batch contains duplicate document {0}")]
    DuplicateBatchDocument(DocumentId),
    /// Schema version zero is reserved.
    #[error("application schema version must be nonzero")]
    InvalidSchemaVersion,
    /// Migration pages are bounded to keep transactions and memory predictable.
    #[error("migration batch size {0} is outside 1..=256")]
    InvalidMigrationBatchSize(usize),
    /// Atomic edit batches are bounded before materialization and transaction setup.
    #[error("edit batch size {0} exceeds the maximum of 256")]
    InvalidEditBatchSize(usize),
    /// One atomic install would retain too much aggregate materialized state.
    #[error("materialized page exceeds the bounded in-memory installation budget")]
    MaterializationBudgetExceeded,
    /// One public edit must commit exactly one immutable causal change.
    #[error("edit produced {0} changes; expected exactly one")]
    ChangeCardinality(usize),
    /// Durable outbox metadata referenced missing immutable change bytes.
    #[error("outbox references missing change {0:?}")]
    MissingChange(ChangeDigest),
    /// Durable outbox metadata referenced a missing exact operation binding.
    #[error("outbox references missing operation {0}")]
    MissingOperation(OperationId),
    /// The document is deleted and only accepts further tombstones.
    ///
    /// Edits are refused on tombstoned documents so a fresh write can never
    /// silently overwrite converged absence; reconcile through
    /// [`crate::Database::conflicts`] instead. Classified as
    /// [`DatabaseErrorKind::Other`].
    #[error("document is deleted and only accepts further tombstones")]
    DocumentDeleted,
    /// A versioned export was produced for a different ownership scope.
    ///
    /// Restoring a foreign scope would interleave two histories, so the
    /// payload is refused before any durable state changes. Classified as
    /// [`DatabaseErrorKind::Other`].
    #[error("logical export scope does not match this replica scope")]
    ExportScopeMismatch,
    /// A recovery baseline violated its versioned content identity.
    ///
    /// The artifact is refused before any durable mutation, so a truncated,
    /// reordered, or substituted baseline leaves state and checkpoint
    /// unchanged. Classified as [`DatabaseErrorKind::Other`].
    #[error("recovery baseline artifact is malformed or mismatched")]
    InvalidBaseline,
    /// A required backend guarantee is not provided by this replica's profile.
    ///
    /// The refusal happens before any durable work, so a caller needing a
    /// stronger class is never silently served by the local replica.
    /// Classified as [`DatabaseErrorKind::Other`].
    #[error("backend capability refused: {0}")]
    Model(#[from] logical_db_model::ModelError),
    /// The durable store returned a commit outcome this replica does not recognize.
    ///
    /// Fail closed rather than guessing Applied versus Duplicate: the caller
    /// must not observe a fabricated receipt for an unknown transition.
    /// Classified as [`DatabaseErrorKind::Other`].
    #[error("store returned an unrecognized commit outcome")]
    UnknownCommitOutcome,
    /// A text-plus-vector projection view failed: an over-long vector seed, a
    /// malformed snapshot, or snapshot file I/O.
    ///
    /// The message names the refusal; durable state is unchanged.
    /// Classified as [`DatabaseErrorKind::Other`].
    #[error("projection view failed: {0}")]
    ProjectionView(String),
}

impl From<IndexedError> for DatabaseError {
    /// Adapts an in-memory hybrid query refusal.
    fn from(error: IndexedError) -> Self {
        Self::ProjectionView(error.to_string())
    }
}

impl From<SnapshotError> for DatabaseError {
    /// Adapts a durable snapshot read or query refusal.
    fn from(error: SnapshotError) -> Self {
        Self::ProjectionView(error.to_string())
    }
}

/// Stable recovery decision for application-facing database failures.
///
/// This keeps storage-engine variants private while allowing an embedding layer
/// to distinguish a caller bug from optimistic concurrency and an ambiguous
/// durability result. New categories may be added without exposing internals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DatabaseErrorKind {
    /// One operation identity was reused for a different canonical request.
    OperationConflict,
    /// Another successor committed first; rebuild the edit on current state.
    ConcurrentWrite,
    /// Durability is ambiguous; retry the exact operation and request identity.
    RetryExactRequest,
    /// No specialized recovery contract applies.
    Other,
}

impl DatabaseError {
    /// Classifies recovery without leaking the embedded store's error types.
    #[must_use]
    pub const fn kind(&self) -> DatabaseErrorKind {
        match *self {
            Self::Store(StoreError::OperationConflict) => DatabaseErrorKind::OperationConflict,
            Self::Store(StoreError::StaleDocument) => DatabaseErrorKind::ConcurrentWrite,
            Self::Store(StoreError::CommitOutcomeUnknown(_)) => {
                DatabaseErrorKind::RetryExactRequest
            }
            _ => DatabaseErrorKind::Other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn facade_error_kinds_preserve_recovery_decisions_without_store_types()
    -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            DatabaseError::Store(StoreError::StaleDocument).kind(),
            DatabaseErrorKind::ConcurrentWrite
        );
        assert_eq!(
            DatabaseError::InvalidSchemaVersion.kind(),
            DatabaseErrorKind::Other
        );
        Ok(())
    }
}
