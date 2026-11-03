//! Embeddable local-first application API.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! The optional `semantics` feature re-exports the bounded, non-authoritative
//! `logical-db-cairn` and `logical-db-episode` ports for hosts that model
//! semantic observations. It does not provide canonical LEM ORIGIN storage or
//! LEM v0 wire compatibility. The feature stays off by default to preserve the
//! minimal embedded footprint.

#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::expect_used, clippy::unwrap_used))]

mod db;
mod digest;
mod edit;
mod error;
mod export;
#[cfg(feature = "http")]
pub mod http;
mod identity;
mod open;
mod outcomes;
pub mod prelude;
mod projection;
mod read;
mod schema;
mod sync;
#[cfg(test)]
pub(crate) mod test_helpers;
#[cfg(test)]
mod tests_export;
#[cfg(test)]
mod tests_schema;
pub mod transport;
mod views;

pub use crate::db::Database;
pub use crate::digest::content_digest;
pub use crate::error::{DatabaseError, DatabaseErrorKind};
pub use crate::outcomes::{
    BatchEdit, DeleteOutcome, DeleteReceipt, DuplicateReceipt, EditOutcome, EditReceipt,
    MigrationReport, PendingChange, RemoteChange, RestoreReport,
};
pub use crate::projection::{Projection, ProjectionSemantics};
pub use crate::views::{IndexedHit, SnapshotBuildReport};
pub use logical_db_crdt::{
    ConflictValue, CrdtError, Editor, Item, Object, ObjectKind, Reader, Scalar,
};
pub use logical_db_model::{
    AccountId, ActionDigest, BackendClass, CAPABILITY_PROFILE_BYTES, CAPABILITY_PROFILE_VERSION,
    Capability, CapabilityProfile, ChangeDigest, DocumentId, DocumentRevision, IndexEntry, IndexId,
    IndexValue, OperationDigest, OperationId, PeerId, ReplicaId, Scope, SnapshotArtifact, SpaceId,
    common_guarantees,
};
pub use logical_db_platform::PlatformError;
pub use logical_db_store::projection_relational::{
    ProjectionAction, ProjectionError, RelationalProjection, SqliteRelationalProjection,
};
pub use logical_db_store::projection_snapshot::{
    SnapshotError, query_snapshot, snapshot_row_count, verify_snapshot_integrity,
};
pub use logical_db_store::snapshot_backend::RedbSnapshotStore;
pub use logical_db_store::snapshot_envelope::{
    SnapshotEnvelope, SnapshotEnvelopeError, SnapshotMetadata, SnapshotStore,
};
pub use logical_db_store::snapshot_file::FileProjectionIndex;
pub use logical_db_store::snapshot_restore::{
    RestorableProjection, RestoreStats, SnapshotInput, create_snapshot, restore_or_replay,
};
pub use logical_db_store::{ChangeEvent, EXPORT_VERSION, MAX_EXPORT_BYTES, RestoreCursor};
pub use logical_db_sync::{SyncLimits, SyncReport, Transport};

/// Opt-in semantic vocabularies; off by default to keep the embedded minimal.
#[cfg(feature = "semantics")]
pub use logical_db_cairn;
/// Opt-in episode log; off by default to keep the embedded minimal.
#[cfg(feature = "semantics")]
pub use logical_db_episode;
