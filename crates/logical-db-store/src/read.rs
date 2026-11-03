//! Scan and query view implementation; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::{
    decode_baseline_event, decode_event, decode_operation, decode_outbox, decode_revision,
    decode_sequence_key_for_kind, decode_u64, decode_uuid,
};
use crate::error::StoreError;
use crate::indexes::{decode_index_text, encode_index_value};
use crate::keys::{
    DATA, KIND_BASELINE_EVENT, KIND_EVENT, KIND_INDEX, add_materialized_bytes, index_row_prefix,
    is_nonempty_bounded, key_baseline, key_baseline_event, key_change, key_event,
    key_index_reverse, key_operation, key_outbox, key_revision, key_snapshot, key_tombstone,
    next_prefix, prefix,
};
use crate::store::{EmbeddedStore, ensure_not_migrating};
use crate::types::{
    ChangeEvent, CommittedOperation, ExportDocument, ExportScanPage, IndexHit, OutboxEntry,
    StoredSnapshot, TextIndexPage,
};
use logical_db_model::{
    ChangeDigest, DocumentId, IndexId, IndexValue, MAX_CHANGE_BYTES, MAX_SNAPSHOT_BYTES,
    OperationId, Scope,
};
use redb::{ReadableDatabase, ReadableTable};

impl EmbeddedStore {
    /// Reads the next bounded document page for a versioned logical export.
    ///
    /// Documents arrive in stable identifier order with their complete
    /// snapshot and tombstone flag, so an interrupted export resumes from
    /// `after` without re-reading the prefix. Snapshots stay bounded by the
    /// same aggregate materialized budget as migration pages.
    ///
    /// # Errors
    ///
    /// Refuses an invalid page size, an active schema migration, or malformed
    /// durable state.
    pub fn export_scan(
        &self,
        scope: Scope,
        after: Option<DocumentId>,
        limit: usize,
    ) -> Result<ExportScanPage, StoreError> {
        crate::error::require_page_limit(limit, StoreError::InvalidExportLimit)?;
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        let range = crate::schema::snapshot_row_range(scope, after)?;
        let mut documents = Vec::new();
        let mut materialized_bytes = 0usize;
        for entry in table
            .range(range.start.as_slice()..range.end.as_slice())?
            .take(limit)
        {
            let (key, value) = entry?;
            if key.value().len() != range.prefix.len().saturating_add(16) {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let snapshot = value.value();
            if !is_nonempty_bounded(snapshot.len(), MAX_SNAPSHOT_BYTES) {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let Some(next_bytes) = add_materialized_bytes(materialized_bytes, snapshot.len())
            else {
                break;
            };
            documents.push(export_document(
                &table,
                scope,
                key.value(),
                range.prefix.len(),
                snapshot,
            )?);
            materialized_bytes = next_bytes;
        }
        Ok(ExportScanPage { after, documents })
    }

    /// Reads the full ordered commit history including acknowledged entries.
    ///
    /// Unlike [`EmbeddedStore::outbox_page`], acknowledgement only advances
    /// the transport cursor and never hides history from an export: a backup
    /// must replay every commit to reproduce outbox and event cursors.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, decode, read, or table error.
    pub fn history_page(
        &self,
        scope: Scope,
        after: u64,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, StoreError> {
        self.outbox_history_page(scope, after, limit)
    }

    /// Loads the durable exact-request binding for an operation, when present.
    ///
    /// # Errors
    ///
    /// Returns a typed read or corruption error.
    pub fn operation(
        &self,
        scope: Scope,
        operation: OperationId,
    ) -> Result<Option<CommittedOperation>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        let key = key_operation(scope, operation);
        let committed = table
            .get(key.as_slice())?
            .map(|value| decode_operation(value.value()))
            .transpose()?;
        if let Some(committed) = committed {
            verify_operation_links(&table, scope, operation, committed)?;
        }
        Ok(committed)
    }

    /// Loads the latest complete document snapshot, when present.
    ///
    /// # Errors
    ///
    /// Returns a typed read or table error when durable state cannot be read.
    pub fn load_snapshot(
        &self,
        scope: Scope,
        document: DocumentId,
    ) -> Result<Option<StoredSnapshot>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let snapshot_key = key_snapshot(scope, document);
        let revision_key = key_revision(scope, document);
        let snapshot = table
            .get(snapshot_key.as_slice())?
            .map(|value| {
                if !is_nonempty_bounded(value.value().len(), MAX_SNAPSHOT_BYTES) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                Ok(value.value().to_vec())
            })
            .transpose()?;
        let revision = table
            .get(revision_key.as_slice())?
            .map(|value| decode_revision(value.value()))
            .transpose()?;
        match (snapshot, revision) {
            (None, None) => Ok(None),
            (Some(bytes), Some(revision)) => Ok(Some(StoredSnapshot { bytes, revision })),
            _ => Err(StoreError::CorruptInternalValue),
        }
    }

    /// Reads whether one document carries a first-class tombstone.
    ///
    /// The flag is monotonic: once a local delete or a remote tombstone page
    /// commits it, neither restarts nor later non-tombstone pages clear it.
    /// Test-only inspection: the production read path derives absence from
    /// the materialized snapshot instead of consulting this flag.
    ///
    /// # Errors
    ///
    /// Returns a typed read or table error when durable state cannot be read.
    #[cfg(test)]
    pub(crate) fn is_tombstoned(
        &self,
        scope: Scope,
        document: DocumentId,
    ) -> Result<bool, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        Ok(table
            .get(key_tombstone(scope, document).as_slice())?
            .is_some_and(|value| value.value() == [1_u8]))
    }

    /// Reads durable state invalidations after an exclusive subscription cursor.
    ///
    /// # Errors
    ///
    /// Returns a bound, corruption, or storage failure.
    pub fn changes_since(
        &self,
        scope: Scope,
        after: u64,
        limit: usize,
    ) -> Result<Vec<ChangeEvent>, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidEventLimit(limit);
            return Err(error);
        }
        if after == u64::MAX {
            return Ok(Vec::new());
        }
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        let mut events = Vec::new();
        let start = key_event(
            scope,
            after.checked_add(1).ok_or(StoreError::SequenceExhausted)?,
        );
        let end =
            next_prefix(&prefix(scope, KIND_EVENT)).ok_or(StoreError::CorruptInternalValue)?;
        for entry in table.range(start.as_slice()..end.as_slice())?.take(limit) {
            let (key, value) = entry?;
            let sequence = decode_sequence_key_for_kind(key.value(), KIND_EVENT)?;
            let event = decode_event(sequence, value.value())?;
            let ChangeEvent::Document {
                document, digest, ..
            } = event
            else {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            };
            let change = table
                .get(key_change(scope, document, digest).as_slice())?
                .ok_or(StoreError::CorruptInternalValue)?;
            if !is_nonempty_bounded(change.value().len(), MAX_CHANGE_BYTES) {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            validate_snapshot_revision(&table, scope, document)?;
            events.push(event);
        }
        let start = key_baseline_event(
            scope,
            after.checked_add(1).ok_or(StoreError::SequenceExhausted)?,
        );
        let end = next_prefix(&prefix(scope, KIND_BASELINE_EVENT))
            .ok_or(StoreError::CorruptInternalValue)?;
        for entry in table.range(start.as_slice()..end.as_slice())?.take(limit) {
            let (key, value) = entry?;
            let sequence = decode_sequence_key_for_kind(key.value(), KIND_BASELINE_EVENT)?;
            events.push(decode_baseline_event(sequence, value.value())?);
        }
        events.sort_by_key(ChangeEvent::sequence);
        events.truncate(limit);
        Ok(events)
    }

    /// Reads the scope's durable baseline generation, `0` before any install.
    ///
    /// # Errors
    ///
    /// Returns a corruption or storage failure.
    pub fn baseline_generation(&self, scope: Scope) -> Result<u64, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        let stored = table
            .get(key_baseline(scope).as_slice())?
            .map(|value| decode_u64(value.value()))
            .transpose()?;
        let mut generation = 0;
        if let Some(next) = stored {
            generation = next;
        }
        Ok(generation)
    }

    /// Loads one immutable change by its native digest.
    ///
    /// # Errors
    ///
    /// Returns a typed read or table error when durable state cannot be read.
    pub fn load_change(
        &self,
        scope: Scope,
        document: DocumentId,
        digest: ChangeDigest,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let key = key_change(scope, document, digest);
        table
            .get(key.as_slice())?
            .map(|value| {
                if !is_nonempty_bounded(value.value().len(), MAX_CHANGE_BYTES) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                Ok(value.value().to_vec())
            })
            .transpose()
    }

    /// Reads an exact-value secondary-index page in stable document order.
    ///
    /// # Errors
    ///
    /// Returns a validation, corruption, or storage failure. `after` is an
    /// exclusive document cursor from the preceding page.
    pub fn query_index(
        &self,
        scope: Scope,
        index: IndexId,
        value: &IndexValue,
        after: Option<DocumentId>,
        limit: usize,
    ) -> Result<Vec<IndexHit>, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidIndexLimit(limit);
            return Err(error);
        }
        let encoded = encode_index_value(value)?;
        let row_prefix = index_row_prefix(scope, index, &encoded);
        let end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
        let mut start = row_prefix.clone();
        if let Some(document) = after {
            start.extend_from_slice(document.as_uuid().as_bytes());
            start.push(0);
        }
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        table
            .range(start.as_slice()..end.as_slice())?
            .take(limit)
            .map(|entry| {
                let (key, value) = entry?;
                if key.value().len() != row_prefix.len().saturating_add(16) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                if value.value() != [1_u8] {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                let document =
                    decode_uuid(&key.value()[row_prefix.len()..]).map(DocumentId::from_uuid)?;
                let reverse = table
                    .get(key_index_reverse(scope, document, index).as_slice())?
                    .ok_or(StoreError::CorruptInternalValue)?;
                if reverse.value() != encoded.as_slice() {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                validate_snapshot_revision(&table, scope, document)?;
                Ok(IndexHit { document })
            })
            .collect()
    }

    /// Scans text-valued rows of one secondary index in key order.
    ///
    /// Non-text rows are skipped deterministically; tombstoned or otherwise
    /// unmaterialized documents are refused as corruption, mirroring
    /// [`Self::query_index`]. `after` is an opaque cursor from a preceding
    /// [`TextIndexPage::next`]; forging one fails closed.
    ///
    /// # Errors
    ///
    /// Returns a validation, corruption, or storage failure.
    pub fn scan_index_text(
        &self,
        scope: Scope,
        index: IndexId,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<TextIndexPage, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidIndexLimit(limit);
            return Err(error);
        }
        let mut row_prefix = prefix(scope, KIND_INDEX);
        row_prefix.extend_from_slice(index.as_uuid().as_bytes());
        let end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
        let start = match after {
            None => row_prefix.clone(),
            Some(cursor) => {
                if !cursor.starts_with(&row_prefix) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                next_prefix(cursor).ok_or(StoreError::CorruptInternalValue)?
            }
        };
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        let mut rows = Vec::new();
        let mut iterated = 0_usize;
        let mut last_key: Option<Vec<u8>> = None;
        for entry in table.range(start.as_slice()..end.as_slice())?.take(limit) {
            iterated = iterated.saturating_add(1);
            let (key, value) = entry?;
            if value.value() != [1_u8] {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let Some(document_start) = key.value().len().checked_sub(16) else {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            };
            if document_start <= row_prefix.len() {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let document =
                decode_uuid(&key.value()[document_start..]).map(DocumentId::from_uuid)?;
            let reverse = table
                .get(key_index_reverse(scope, document, index).as_slice())?
                .ok_or(StoreError::CorruptInternalValue)?;
            if reverse.value() != &key.value()[row_prefix.len()..document_start] {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            validate_snapshot_revision(&table, scope, document)?;
            if let Some(text) = decode_index_text(reverse.value())? {
                rows.push((document, text));
            }
            last_key = Some(key.value().to_vec());
        }
        let next = if iterated == limit { last_key } else { None };
        Ok(TextIndexPage { rows, next })
    }
}

/// Builds one export document from a snapshot row and its tombstone flag.
pub(crate) fn export_document(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    key: &[u8],
    prefix_length: usize,
    snapshot: &[u8],
) -> Result<ExportDocument, StoreError> {
    let document = decode_uuid(&key[prefix_length..]).map(DocumentId::from_uuid)?;
    let tombstone = table
        .get(key_tombstone(scope, document).as_slice())?
        .is_some_and(|value| value.value() == [1_u8]);
    Ok(ExportDocument {
        document,
        tombstone,
        snapshot: snapshot.to_vec(),
    })
}

/// Checks that a stored operation receipt still matches its change and outbox rows.
pub(crate) fn verify_operation_links(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    operation: OperationId,
    committed: CommittedOperation,
) -> Result<(), StoreError> {
    if table
        .get(key_change(scope, committed.document, committed.digest).as_slice())?
        .is_none()
    {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let outbox = table
        .get(key_outbox(scope, committed.sequence).as_slice())?
        .ok_or(StoreError::CorruptInternalValue)?;
    let outbox = decode_outbox(committed.sequence, outbox.value())?;
    if outbox.document != committed.document
        || outbox.operation != operation
        || outbox.digest != committed.digest
    {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    Ok(())
}

/// Validates one document's snapshot row and revision link: absent, empty,
/// or oversized snapshots and undecodable revisions are corruption. The
/// three read paths that materialize documents share this gate instead of
/// repeating it.
fn validate_snapshot_revision(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    document: DocumentId,
) -> Result<(), StoreError> {
    let snapshot = table
        .get(key_snapshot(scope, document).as_slice())?
        .ok_or(StoreError::CorruptInternalValue)?;
    if !is_nonempty_bounded(snapshot.value().len(), MAX_SNAPSHOT_BYTES) {
        return Err(StoreError::CorruptInternalValue);
    }
    let revision = table
        .get(key_revision(scope, document).as_slice())?
        .ok_or(StoreError::CorruptInternalValue)?;
    decode_revision(revision.value())?;
    Ok(())
}
