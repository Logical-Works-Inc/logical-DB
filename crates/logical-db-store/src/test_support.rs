//! Shared test support; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::error::StoreError;
use crate::keys::DATA;
use crate::store::EmbeddedStore;
use crate::types::{CommitOutcome, LocalChange, LocalCommit, MigrationIndexes, MigrationPage};
use logical_db_model::{
    ChangeDigest, DocumentId, IndexEntry, IndexId, IndexValue, OperationDigest, OperationId, Scope,
};
use std::error::Error;

/// Typed failure for store unit tests: every fallible fixture step names its
/// error instead of erasing it behind a box.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TestError {
    /// The embedded store refused or failed the operation.
    #[error("store fixture failed: {0}")]
    Store(StoreError),
    /// Fixture file cleanup failed.
    #[error("fixture cleanup failed: {0}")]
    Io(std::io::Error),
}

impl From<StoreError> for TestError {
    /// Adapts a store refusal without discarding its variant.
    fn from(error: StoreError) -> Self {
        Self::Store(error)
    }
}

impl From<std::io::Error> for TestError {
    /// Adapts a fixture cleanup failure without discarding its kind.
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Builds one unique temporary database path for a named fixture.
pub(crate) fn test_path(name: &str) -> std::path::PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let sequence = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "logical-db-v3-{name}-{}-{sequence}.redb",
        std::process::id()
    ))
}

/// Builds one bounded local change from deterministic fixture bytes.
pub(crate) fn change(
    scope: Scope,
    document: DocumentId,
    operation: OperationId,
    request_byte: u8,
    digest_byte: u8,
) -> Result<LocalChange<'static>, StoreError> {
    LocalChange::new(
        scope,
        document,
        operation,
        OperationDigest::new([request_byte; 32]),
        ChangeDigest::new([digest_byte; 32]),
        b"verified-change",
    )
}

/// Commits one live change with no expected revision.
pub(crate) fn commit_live<'a>(
    store: &EmbeddedStore,
    change: LocalChange<'a>,
    snapshot: &'a [u8],
) -> Result<CommitOutcome, StoreError> {
    store.commit_local(LocalCommit::new(change, None, snapshot, &[], false))
}

/// Writes one raw legacy row inside its own immediate transaction.
pub(crate) fn insert_legacy_value(
    store: &EmbeddedStore,
    key: &[u8],
    value: &[u8],
) -> Result<(), StoreError> {
    let transaction = store.database.begin_write()?;
    {
        let mut table = transaction.open_table(DATA)?;
        table.insert(key, value)?;
    }
    transaction.commit()?;
    Ok(())
}

/// Applies one migration page using the fixed target schema and new index.
pub(crate) fn apply_migration_documents(
    store: &EmbeddedStore,
    scope: Scope,
    index: IndexId,
    page: &MigrationPage,
) -> Result<(), StoreError> {
    let indexes = [IndexEntry::new(index, IndexValue::Text("new".into()))];
    let updates = page
        .documents
        .iter()
        .map(|document| MigrationIndexes {
            document: document.document,
            expected_revision: document.revision,
            indexes: &indexes,
        })
        .collect::<Vec<_>>();
    store.apply_migration_page(scope, 2, [2; 32], page.after, &updates)
}

/// Binds the initial schema and commits three indexed snapshot documents.
pub(crate) fn seed_migration_documents(
    store: &EmbeddedStore,
    scope: Scope,
    index: IndexId,
    documents: [DocumentId; 3],
) -> Result<(), Box<dyn Error>> {
    store.bind_schema(scope, 1, [1; 32])?;
    for (offset, document) in documents.into_iter().enumerate() {
        store.commit_local(LocalCommit {
            change: change(
                scope,
                document,
                OperationId::new(),
                u8::try_from(offset.saturating_add(1))?,
                u8::try_from(offset.saturating_add(11))?,
            )?,
            expected_revision: None,
            snapshot: b"snapshot",
            indexes: &[IndexEntry::new(index, IndexValue::Text("old".into()))],
            tombstone: false,
        })?;
    }
    Ok(())
}
