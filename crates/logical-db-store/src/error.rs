//! Store error type; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use logical_db_model::{PeerId, ReplicaId};
use redb::{
    CommitError, DatabaseError, SetDurabilityError, StorageError, TableError, TransactionError,
};
use thiserror::Error;

/// Stable embedded storage failures.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Opening or creating the database failed.
    #[error("database open failed: {0}")]
    Database(#[from] DatabaseError),
    /// Starting a read or write transaction failed.
    #[error("database transaction failed: {0}")]
    Transaction(#[from] TransactionError),
    /// Opening or using the v3 table failed.
    #[error("database table failed: {0}")]
    Table(#[from] TableError),
    /// Reading or writing a table value failed.
    #[error("database storage failed: {0}")]
    Storage(#[from] StorageError),
    /// Commit returned without proving whether the durable transition landed.
    /// Callers must retry the exact operation identity and request digest.
    #[error("database commit outcome is unknown; retry the exact request: {0}")]
    CommitOutcomeUnknown(#[from] CommitError),
    /// Selecting the required immediate durability mode failed.
    #[error("database durability configuration failed: {0}")]
    Durability(#[from] SetDurabilityError),
    /// Immutable change bytes violated the local resource bound.
    #[error("change byte length {0} is outside local bounds")]
    InvalidChangeSize(usize),
    /// A local batch had no transitions to commit.
    #[error("local commit batch is empty")]
    EmptyLocalBatch,
    /// A local batch attempted to update one document more than once.
    #[error("local commit batch contains a duplicate document")]
    DuplicateBatchDocument,
    /// A local batch attempted to reuse one operation identity.
    #[error("local commit batch contains a duplicate operation")]
    DuplicateBatchOperation,
    /// A local batch mixed ownership scopes.
    #[error("local commit batch contains multiple scopes")]
    MixedBatchScope,
    /// Complete snapshot bytes violated the local resource bound.
    #[error("snapshot byte length {0} is outside local bounds")]
    InvalidSnapshotSize(usize),
    /// An operation identity was reused for a different change.
    #[error("operation identity was already committed with different bytes")]
    OperationConflict,
    /// The document changed after its input snapshot was read.
    #[error("document changed before the successor snapshot committed")]
    StaleDocument,
    /// A non-tombstone successor was attempted on a deleted document.
    ///
    /// Tombstones are monotonic: only another tombstone commit may follow.
    /// Callers holding fresh writes for a deleted document must reconcile
    /// against its conflict candidates instead of overwriting absence.
    #[error("document is deleted and only accepts further tombstones")]
    DocumentDeleted,
    /// A scope consumed every possible monotonic sequence.
    #[error("scope sequence is exhausted")]
    SequenceExhausted,
    /// A materialized document revision exhausted its numeric space.
    #[error("document revision is exhausted")]
    RevisionExhausted,
    /// A caller requested an empty or excessive outbox page.
    #[error("outbox limit {0} is outside 1..=256")]
    InvalidOutboxLimit(usize),
    /// A caller requested an empty or excessive secondary-index page.
    #[error("index query limit {0} is outside 1..=256")]
    InvalidIndexLimit(usize),
    /// A caller requested an empty or excessive change-feed page.
    #[error("change-feed limit {0} is outside 1..=256")]
    InvalidEventLimit(usize),
    /// A projection set was duplicate, excessive, or contained an oversized value.
    #[error("materialized secondary-index entries violate storage bounds")]
    InvalidIndexes,
    /// A service acknowledged changes that were never committed locally.
    #[error("outbox acknowledgement {through} exceeds committed sequence {highest}")]
    InvalidAcknowledgement {
        /// Requested inclusive acknowledgement.
        through: u64,
        /// Highest local sequence that exists.
        highest: u64,
    },
    /// One globally acknowledged outbox cannot safely target two services.
    #[error("scope is bound to upstream {configured}, not requested upstream {requested}")]
    UpstreamMismatch {
        /// Durable synchronization service identity.
        configured: PeerId,
        /// Conflicting service identity supplied by the caller.
        requested: PeerId,
    },
    /// Legacy durable checkpoints identify more than one possible upstream.
    #[error("scope has legacy checkpoints for multiple upstreams; explicit migration is required")]
    AmbiguousLegacyUpstream,
    /// A legacy global acknowledgement cannot be attributed to an upstream.
    #[error("scope has an acknowledged legacy outbox without an attributable upstream")]
    UnattributedLegacyAcknowledgement,
    /// An opaque checkpoint violated its durable resource bound.
    #[error("checkpoint byte length {0} is outside local bounds")]
    InvalidCheckpointSize(usize),
    /// A remote page violated count, aggregate byte, or document coverage rules.
    #[error("remote page violates local atomic-install bounds")]
    InvalidRemotePage,
    /// Equal change identities were presented with unequal immutable bytes.
    #[error("change digest was reused for different immutable bytes")]
    ChangeDigestConflict,
    /// Durable internal bytes did not match the v3 format.
    #[error("durable v3 internal bytes are malformed")]
    CorruptInternalValue,
    /// Durable identifier bytes did not decode through the model contract.
    #[error("durable identifier bytes failed model decoding: {0}")]
    CorruptIdentifier(#[from] lgwks_std::json::Error),
    /// Durable text bytes were not valid UTF-8.
    #[error("durable v3 text bytes are not valid UTF-8: {0}")]
    CorruptInternalText(#[from] std::string::FromUtf8Error),
    /// An existing file was opened under a different replica installation.
    #[error("database replica mismatch: expected {expected}, received {actual}")]
    ReplicaMismatch {
        /// Replica identity already bound to the file.
        expected: ReplicaId,
        /// Replica identity supplied by this open request.
        actual: ReplicaId,
    },
    /// Durable data existed without the required replica metadata.
    #[error("database contains v3 data without a durable replica binding")]
    MissingReplicaBinding,
    /// The file contains tables from another or unsupported schema version.
    #[error("database file contains an unsupported schema and requires migration")]
    UnsupportedSchema,
    /// Schema version zero is reserved and cannot identify application state.
    #[error("application schema version must be nonzero")]
    InvalidSchemaVersion,
    /// Durable state belongs to another projection/schema contract.
    #[error("application schema differs from durable state and requires migration")]
    SchemaMigrationRequired,
    /// A durable schema migration excludes writes and index-dependent reads.
    #[error("application schema migration is in progress")]
    MigrationInProgress,
    /// A caller tried to resume a migration using a different target schema.
    #[error("active schema migration belongs to a different target")]
    MigrationTargetMismatch,
    /// A migration page was requested or committed without a durable marker.
    #[error("no schema migration is in progress")]
    MigrationNotInProgress,
    /// A migration reader requested an empty or excessive page.
    #[error("migration page limit {0} is outside 1..=256")]
    InvalidMigrationLimit(usize),
    /// A migration batch was empty, excessive, unordered, or skipped a cursor.
    #[error("schema migration page violates ordering or resource bounds")]
    InvalidMigrationPage,
    /// Another worker atomically advanced the migration cursor first.
    #[error("schema migration page cursor is stale")]
    StaleMigrationPage,
    /// Target schema publication was attempted before every snapshot was rebuilt.
    #[error("schema migration still has unprocessed documents")]
    MigrationIncomplete,
    /// An export scan page was requested with an empty or excessive limit.
    #[error("export scan limit {0} is outside 1..=256")]
    InvalidExportLimit(usize),
    /// A logical export payload or one of its fields violates size bounds.
    #[error("logical export byte length {0} is outside local bounds")]
    InvalidExportSize(usize),
    /// A logical export payload declares an unsupported version.
    #[error("logical export version {0} is not supported")]
    UnknownExportVersion(u16),
    /// A logical export payload is structurally malformed.
    #[error("logical export payload is malformed")]
    InvalidExport,
    /// A second restore payload was attempted while another restore holds the scope cursor.
    #[error("a versioned restore is already in progress for this scope")]
    RestoreInProgress,
    /// A restore cursor was advanced or finalized without an active restore.
    #[error("no versioned restore is in progress for this scope")]
    RestoreNotInProgress,
    /// A restore cannot finalize because durable revisions diverge from its replay.
    #[error("versioned restore cannot finalize before its replayed revisions match")]
    RestoreIncomplete,
}

/// Rejects an out-of-range page size before any table opens.
///
/// The outbox, export, and history pages share the 1..=256 bound with
/// different typed errors, so the refusal lives here instead of once per
/// reader.
pub(crate) fn require_page_limit(
    limit: usize,
    refused: fn(usize) -> StoreError,
) -> Result<(), StoreError> {
    if !(1..=256).contains(&limit) {
        return Err(refused(limit));
    }
    Ok(())
}
