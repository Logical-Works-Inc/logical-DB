//! Portable transactional storage for one embedded local replica.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

pub(crate) mod codec;
pub mod commit;
pub mod error;
pub mod export;
pub mod indexes;
pub mod keys;
pub(crate) mod outbox;
pub mod projection_indexed;
pub mod projection_relational;
pub mod projection_snapshot;
pub mod read;
pub(crate) mod remote;
pub mod restore;
pub mod schema;
pub mod snapshot_backend;
pub mod snapshot_envelope;
pub mod snapshot_file;
pub mod snapshot_restore;
pub mod store;
pub(crate) mod types;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests_export;
#[cfg(test)]
mod tests_index;
#[cfg(test)]
mod tests_keys;
#[cfg(test)]
mod tests_restore;
#[cfg(test)]
mod tests_schema;
#[cfg(test)]
mod tests_store;
#[cfg(test)]
mod tests_sync;

pub use error::StoreError;
pub use export::{decode_export, encode_export, restore_operation};
pub use keys::{EXPORT_VERSION, MAX_EXPORT_BYTES, MAX_MATERIALIZED_PAGE_BYTES};
pub use logical_db_model::MAX_SNAPSHOT_BYTES;
pub use remote::InstallMode;
pub use store::EmbeddedStore;
pub use types::{
    ChangeEvent, CommitOutcome, CommittedOperation, ExportDocument, ExportEntry, ExportScanPage,
    IndexHit, LocalChange, LocalCommit, MigrationDocument, MigrationIndexes, MigrationPage,
    OutboxEntry, RemoteChange, RemoteDocument, RestoreCursor, StoredSnapshot, TextIndexPage,
};
