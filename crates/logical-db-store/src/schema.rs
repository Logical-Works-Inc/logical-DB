//! Schema migration implementation; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::{
    decode_migration, decode_revision, decode_uuid, encode_migration, schema_value,
};
use crate::error::StoreError;
use crate::indexes::{replace_indexes, validate_indexes};
use crate::keys::{
    DATA, KIND_SNAPSHOT, add_materialized_bytes, is_at_most, is_nonempty_bounded, key_migration,
    key_revision, key_schema, next_prefix, prefix, scope_prefix,
};
use crate::store::EmbeddedStore;
use crate::types::{MigrationDocument, MigrationIndexes, MigrationPage};
use logical_db_model::{DocumentId, DocumentRevision, MAX_SNAPSHOT_BYTES, Scope};
use redb::{Durability, ReadableDatabase, ReadableTable};

/// Reads the durable migration marker value, refusing a missing one.
///
/// Both migration pages load the marker before touching rows, so the
/// missing-marker refusal exists once instead of once per page.
fn load_migration_marker(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
) -> Result<Vec<u8>, StoreError> {
    let marker_key = key_migration(scope);
    let marker_value = table
        .get(marker_key.as_slice())?
        .ok_or(StoreError::MigrationNotInProgress)?;
    Ok(marker_value.value().to_vec())
}

/// Rejects a zero schema version before any table opens.
///
/// Binding and migration both reserve zero, so the refusal lives here
/// instead of once per entry point.
fn require_schema_version(version: u32) -> Result<(), StoreError> {
    if version == 0 {
        return Err(StoreError::InvalidSchemaVersion);
    }
    Ok(())
}

/// Start/end keys for one bounded snapshot-row scan resuming after `after`.
///
/// Export and migration pages share this cursor instead of repeating the
/// prefix, end, and resume-key construction once per page reader.
pub(crate) struct SnapshotRowRange {
    /// Ordered row prefix scoping the scan to one scope's snapshots.
    pub(crate) prefix: Vec<u8>,
    /// Resume cursor: the prefix itself, or just past the last returned row.
    pub(crate) start: Vec<u8>,
    /// Exclusive scan end past this scope's snapshot rows.
    pub(crate) end: Vec<u8>,
}

/// Computes the snapshot-row scan range shared by export and migration pages.
///
/// # Errors
///
/// Returns [`StoreError::CorruptInternalValue`] when the scope prefix has no
/// successor key, which cannot happen for a bounded non-empty prefix.
pub(crate) fn snapshot_row_range(
    scope: Scope,
    after: Option<DocumentId>,
) -> Result<SnapshotRowRange, StoreError> {
    let prefix = prefix(scope, KIND_SNAPSHOT);
    let end = next_prefix(&prefix).ok_or(StoreError::CorruptInternalValue)?;
    let mut start = prefix.clone();
    if let Some(document) = after {
        start.extend_from_slice(document.as_uuid().as_bytes());
        start.push(0);
    }
    Ok(SnapshotRowRange { prefix, start, end })
}

impl EmbeddedStore {
    /// Binds one scope to an exact application schema version and projection
    /// digest before document state is opened.
    ///
    /// # Errors
    ///
    /// Refuses a schema change or legacy scoped data without an explicit
    /// migration. A new empty scope is bound atomically.
    pub fn bind_schema(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
    ) -> Result<(), StoreError> {
        require_schema_version(version)?;
        let key = key_schema(scope);
        let expected = schema_value(version, digest);
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let migration_marker = table.get(key_migration(scope).as_slice())?.is_some();
        if migration_marker {
            let error = StoreError::MigrationInProgress;
            return Err(error);
        }
        let existing = {
            let value = table.get(key.as_slice())?;
            value.map(|value| value.value().to_vec())
        };
        match existing.as_deref() {
            Some(existing) if existing == expected => return Ok(()),
            Some(_) => {
                let error = StoreError::SchemaMigrationRequired;
                return Err(error);
            }
            None => {
                let has_scope_entries = scope_has_records(&table, scope)?;
                if has_scope_entries {
                    let error = StoreError::SchemaMigrationRequired;
                    return Err(error);
                }
                table.insert(key.as_slice(), expected.as_slice())?;
            }
        }
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Starts or resumes an explicit schema/index migration to one exact target.
    ///
    /// A durable marker excludes local writes, remote installs, and index reads
    /// until every snapshot is reprojected and the target schema swaps in.
    ///
    /// # Errors
    ///
    /// Refuses zero versions or a different target while another migration is
    /// in progress.
    pub fn begin_schema_migration(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
    ) -> Result<(), StoreError> {
        require_schema_version(version)?;
        let target = schema_value(version, digest);
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let key = key_migration(scope);
        let marker_value = table.get(key.as_slice())?;
        let marker = marker_value.map(|value| value.value().to_vec());
        match marker.as_deref() {
            Some(value) => {
                let (existing, _) = decode_migration(value)?;
                if existing != target {
                    let error = StoreError::MigrationTargetMismatch;
                    return Err(error);
                }
            }
            None => {
                table.insert(key.as_slice(), encode_migration(target, None).as_slice())?;
            }
        }
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Reads the next bounded snapshot page for an active schema migration.
    ///
    /// # Errors
    ///
    /// Refuses an invalid page size, inactive/different migration, malformed
    /// durable state, or a snapshot without its matching revision.
    pub fn migration_page(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
        limit: usize,
    ) -> Result<MigrationPage, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidMigrationLimit(limit);
            return Err(error);
        }
        let target = schema_value(version, digest);
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let marker_value = table.get(key_migration(scope).as_slice())?;
        let marker = marker_value.ok_or(StoreError::MigrationNotInProgress)?;
        let (existing, after) = decode_migration(marker.value())?;
        if existing != target {
            let error = StoreError::MigrationTargetMismatch;
            return Err(error);
        }
        let range = snapshot_row_range(scope, after)?;
        let mut documents = Vec::with_capacity(limit);
        let mut materialized_bytes = 0usize;
        let entries = table.range(range.start.as_slice()..range.end.as_slice())?;
        for entry in entries.take(limit) {
            let (key, value) = entry?;
            let snapshot_bytes = value.value().len();
            if !is_nonempty_bounded(snapshot_bytes, MAX_SNAPSHOT_BYTES) {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let Some(next_bytes) = add_materialized_bytes(materialized_bytes, snapshot_bytes)
            else {
                break;
            };
            let document =
                decode_uuid(&key.value()[range.prefix.len()..]).map(DocumentId::from_uuid)?;
            let revision = stored_revision(&table, scope, document)?;
            documents.push(MigrationDocument {
                document,
                revision,
                snapshot: value.value().to_vec(),
            });
            materialized_bytes = next_bytes;
        }
        Ok(MigrationPage { after, documents })
    }

    /// Atomically replaces all index rows for one migration page and advances
    /// its durable cursor.
    ///
    /// # Errors
    ///
    /// Refuses empty, excessive, unordered, stale, skipped, or target-mismatched
    /// batches. No cursor or index row changes on failure.
    pub fn apply_migration_page(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
        after: Option<DocumentId>,
        documents: &[MigrationIndexes<'_>],
    ) -> Result<(), StoreError> {
        if documents.is_empty() || !is_at_most(documents.len(), 256) {
            let error = StoreError::InvalidMigrationPage;
            return Err(error);
        }
        let target = schema_value(version, digest);
        let mut previous = after;
        for document in documents {
            if previous.is_some_and(|cursor| document.document <= cursor) {
                let error = StoreError::InvalidMigrationPage;
                return Err(error);
            }
            validate_indexes(document.indexes)?;
            previous = Some(document.document);
        }
        let last = previous.ok_or(StoreError::InvalidMigrationPage)?;
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let marker_key = key_migration(scope);
        let marker = load_migration_marker(&table, scope)?;
        let (existing, durable_after) = decode_migration(&marker)?;
        if existing != target {
            let error = StoreError::MigrationTargetMismatch;
            return Err(error);
        }
        if durable_after != after {
            let error = StoreError::StaleMigrationPage;
            return Err(error);
        }
        let row_prefix = prefix(scope, KIND_SNAPSHOT);
        let row_end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
        let mut row_start = row_prefix.clone();
        if let Some(document) = durable_after {
            row_start.extend_from_slice(document.as_uuid().as_bytes());
            row_start.push(0);
        }
        let expected_entries = table.range(row_start.as_slice()..row_end.as_slice())?;
        let expected_documents = expected_entries
            .take(documents.len())
            .map(|entry| {
                let (key, _value) = entry?;
                if key.value().len() != row_prefix.len().saturating_add(16) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                decode_uuid(&key.value()[row_prefix.len()..]).map(DocumentId::from_uuid)
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        if expected_documents.len() != documents.len()
            || expected_documents
                .iter()
                .zip(documents)
                .any(|(expected, supplied)| *expected != supplied.document)
        {
            let error = StoreError::InvalidMigrationPage;
            return Err(error);
        }
        for document in documents {
            let revision = stored_revision(&table, scope, document.document)?;
            if revision != document.expected_revision {
                let error = StoreError::StaleDocument;
                return Err(error);
            }
            replace_indexes(&mut table, scope, document.document, document.indexes)?;
        }
        table.insert(
            marker_key.as_slice(),
            encode_migration(target, Some(last)).as_slice(),
        )?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Atomically publishes the target schema after proving no snapshots remain
    /// beyond the durable migration cursor.
    ///
    /// # Errors
    ///
    /// Refuses an incomplete, inactive, or target-mismatched migration.
    pub fn finish_schema_migration(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
    ) -> Result<(), StoreError> {
        let target = schema_value(version, digest);
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let marker_key = key_migration(scope);
        let marker = load_migration_marker(&table, scope)?;
        let (existing, after) = decode_migration(&marker)?;
        if existing != target {
            let error = StoreError::MigrationTargetMismatch;
            return Err(error);
        }
        let row_prefix = prefix(scope, KIND_SNAPSHOT);
        let end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
        let mut start = row_prefix;
        if let Some(document) = after {
            start.extend_from_slice(document.as_uuid().as_bytes());
            start.push(0);
        }
        let mut remaining_entries = table.range(start.as_slice()..end.as_slice())?;
        let has_remaining = remaining_entries.next().transpose()?.is_some();
        if has_remaining {
            let error = StoreError::MigrationIncomplete;
            return Err(error);
        }
        table.insert(key_schema(scope).as_slice(), target.as_slice())?;
        table.remove(marker_key.as_slice())?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }
}

/// Loads the stored revision for one document, failing closed when absent.
pub(crate) fn stored_revision(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    document: DocumentId,
) -> Result<DocumentRevision, StoreError> {
    let value = table.get(key_revision(scope, document).as_slice())?;
    let value = value.ok_or(StoreError::CorruptInternalValue)?;
    decode_revision(value.value())
}

/// Reports whether a scope owns any durable row.
pub(crate) fn scope_has_records(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
) -> Result<bool, StoreError> {
    let scope_prefix = scope_prefix(scope);
    let scope_end = next_prefix(&scope_prefix).ok_or(StoreError::CorruptInternalValue)?;
    let mut scope_entries = table.range(scope_prefix.as_slice()..scope_end.as_slice())?;
    let has_scope_entries = scope_entries.next().transpose()?.is_some();
    Ok(has_scope_entries)
}
