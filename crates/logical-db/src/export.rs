//! Versioned logical backup export and crash-safe restore.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::collections::{BTreeMap, BTreeSet};

use lgwks_std::hash::blake3;
use logical_db_crdt::{Document, EncodedChange};
use logical_db_model::{DocumentId, DocumentRevision};
use logical_db_store::{
    ChangeEvent, ExportDocument, ExportEntry, LocalChange, LocalCommit, StoreError, decode_export,
    encode_export, restore_operation,
};

use super::db::Database;
use super::error::DatabaseError;
use super::outcomes::RestoreReport;

impl Database {
    /// Exports one versioned logical backup of this scope.
    ///
    /// The payload is a versioned logical dump — ordered commit history plus
    /// document snapshots with tombstone flags — never a file copy: the store
    /// refuses foreign files through replica binding, so restore replays
    /// these entries through the commit path instead. Entries follow the
    /// durable event order, so histories that arrive through
    /// synchronization keep their causal positions with deterministic
    /// synthesized operation bindings, and tombstones stay converged after
    /// restore. The
    /// full history, including acknowledged outbox entries, is exported so a
    /// restored replica reproduces outbox order, operation bindings, and event
    /// cursors; replayed outbox entries attribute the restoring installation
    /// as their presenter.
    ///
    /// Restored indexes are recomputed from the target handle's projections:
    /// open the restore target with the same projection set to reproduce
    /// index rows exactly.
    ///
    /// # Errors
    ///
    /// Returns a typed corruption, bound, migration, or storage failure.
    pub fn export(&self) -> Result<Vec<u8>, DatabaseError> {
        let mut bindings = BTreeMap::new();
        let mut after = 0_u64;
        loop {
            let page = self.store.history_page(self.scope, after, 256)?;
            if page.is_empty() {
                break;
            }
            for item in &page {
                let binding = self
                    .store
                    .operation(self.scope, item.operation)?
                    .ok_or(DatabaseError::MissingOperation(item.operation))?;
                bindings
                    .entry((item.document, item.digest))
                    .or_insert((item.operation, binding.request));
                after = item.sequence;
            }
            if page.len() < 256 {
                break;
            }
        }
        let mut entries = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let events = self.store.changes_since(self.scope, cursor, 256)?;
            if events.is_empty() {
                break;
            }
            let complete = events.len() < 256;
            for event in &events {
                let &ChangeEvent::Document {
                    sequence,
                    document,
                    digest,
                } = event
                else {
                    // A baseline install carries no replayable change of its
                    // own; its snapshots are exported by the scan below.
                    cursor = event.sequence();
                    continue;
                };
                let bytes = self
                    .store
                    .load_change(self.scope, document, digest)?
                    .ok_or(DatabaseError::MissingChange(digest))?;
                let (operation, request) = match bindings.get(&(document, digest)) {
                    Some(binding) => *binding,
                    None => restore_operation(document, digest)?,
                };
                entries.push(ExportEntry::new(
                    document, operation, request, digest, bytes,
                ));
                cursor = sequence;
            }
            if complete {
                break;
            }
        }
        let mut documents = Vec::new();
        let mut scan = None;
        loop {
            let page = self.store.export_scan(self.scope, scan, 64)?;
            if page.documents().is_empty() {
                break;
            }
            let complete = page.documents().len() < 64;
            for document in page.documents() {
                documents.push(ExportDocument::new(
                    document.document,
                    document.tombstone,
                    document.snapshot().to_vec(),
                ));
                scan = Some(document.document);
            }
            if complete {
                break;
            }
        }
        Ok(encode_export(self.scope, &entries, &documents)?)
    }

    /// Restores one versioned logical backup into this scope.
    ///
    /// The payload is bounded before allocation and fully validated before
    /// the first commit: unknown versions, oversized inputs, scope mismatch,
    /// malformed structure, and corrupt change or snapshot bytes are refused
    /// without touching live state. Valid entries replay through the commit
    /// path with their original operation identities, so retrying the same
    /// payload short-circuits as exact duplicates.
    ///
    /// An interrupted restore leaves a durable cursor bound to the payload
    /// identity; retrying with the same bytes resumes from the cursor and
    /// completes, while a different payload is refused until the active one
    /// finishes. Finalization verifies every replayed revision before the
    /// cursor clears. Restore into a fresh scope; a target that already holds
    /// divergent documents fails closed at the first conflicting entry with
    /// the cursor retained for inspection.
    ///
    /// # Errors
    ///
    /// Returns a typed export, scope, CRDT, revision, cursor, or storage
    /// failure. No document, outbox, event, or checkpoint row changes unless
    /// the payload validates end to end.
    pub fn restore(&self, payload: &[u8]) -> Result<RestoreReport, DatabaseError> {
        let identity = export_identity(payload);
        let (scope, entries, documents) = decode_export(payload)?;
        if scope != self.scope {
            return Err(DatabaseError::ExportScopeMismatch);
        }
        if entries.is_empty() && documents.is_empty() {
            return Ok(RestoreReport {
                documents: 0,
                entries: 0,
                applied: 0,
                resumed: false,
            });
        }
        let documented = check_restore_coverage(&documents, &entries)?;
        let mut loaded = BTreeMap::new();
        for document in &documented {
            let revision = self
                .store
                .load_snapshot(self.scope, *document)?
                .map(|snapshot| snapshot.revision);
            loaded.insert(*document, revision);
        }
        self.check_restore_replay(&documents, &entries)?;
        if self
            .store
            .restore_cursor(self.scope)?
            .is_some_and(|cursor| cursor.payload != identity)
        {
            return Err(StoreError::RestoreInProgress.into());
        }
        let cursor = self.store.restore_begin(self.scope, &identity)?;
        let resumed = cursor.next > 0;
        let start = usize::try_from(cursor.next)?;
        if start > entries.len() {
            return Err(StoreError::RestoreIncomplete.into());
        }
        let mut current = loaded;
        let applied = self.replay_restore_entries(&entries, &identity, start, &mut current)?;
        let expected = current
            .into_iter()
            .map(|(document, revision)| {
                revision
                    .map(|revision| (document, revision))
                    .ok_or(StoreError::RestoreIncomplete)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.store
            .restore_finish(self.scope, &identity, &expected)?;
        Ok(RestoreReport {
            documents: documents.len(),
            entries: entries.len(),
            applied,
            resumed,
        })
    }

    /// Check restore replay
    fn check_restore_replay(
        &self,
        documents: &[ExportDocument],
        entries: &[ExportEntry],
    ) -> Result<(), DatabaseError> {
        let mut duplicate = BTreeSet::new();
        for entry in entries {
            if let Some(committed) = self.store.operation(self.scope, entry.operation)? {
                if committed.document != entry.document
                    || committed.request != entry.request
                    || committed.digest != entry.digest
                {
                    return Err(StoreError::OperationConflict.into());
                }
                let stored = self
                    .store
                    .load_change(self.scope, entry.document, entry.digest)?
                    .ok_or(DatabaseError::MissingChange(entry.digest))?;
                if stored.as_slice() != entry.bytes() {
                    return Err(StoreError::OperationConflict.into());
                }
                duplicate.insert(entry.operation);
            }
        }
        let mut states: BTreeMap<DocumentId, Document> = BTreeMap::new();
        let mut last_flag = BTreeMap::new();
        for entry in entries {
            let state = states.entry(entry.document).or_default();
            state.apply(std::slice::from_ref(&EncodedChange::new(
                entry.digest,
                entry.bytes().to_vec(),
            )))?;
            last_flag.insert(entry.document, state.is_deleted()?);
        }
        for document in documents {
            if last_flag.get(&document.document) != Some(&document.tombstone) {
                return Err(StoreError::InvalidExport.into());
            }
        }
        Ok(())
    }

    /// Replay restore entries
    fn replay_restore_entries(
        &self,
        entries: &[ExportEntry],
        identity: &[u8; 32],
        start: usize,
        current: &mut BTreeMap<DocumentId, Option<DocumentRevision>>,
    ) -> Result<usize, DatabaseError> {
        let mut states: BTreeMap<DocumentId, Document> = BTreeMap::new();
        let mut applied = 0_usize;
        for (offset, entry) in entries.iter().enumerate() {
            let state = states.entry(entry.document).or_default();
            let did_apply = self.replay_restore_entry(entry, offset, start, state, current)?;
            applied = applied.saturating_add(usize::from(did_apply));
            let next = u64::try_from(offset.saturating_add(1))?;
            self.store.restore_advance(self.scope, identity, next)?;
        }
        Ok(applied)
    }

    /// Replay restore entry
    fn replay_restore_entry(
        &self,
        entry: &ExportEntry,
        offset: usize,
        start: usize,
        state: &mut Document,
        current: &mut BTreeMap<DocumentId, Option<DocumentRevision>>,
    ) -> Result<bool, DatabaseError> {
        state.apply(std::slice::from_ref(&EncodedChange::new(
            entry.digest,
            entry.bytes().to_vec(),
        )))?;
        if offset < start {
            return Ok(false);
        }
        let tombstoned = state.is_deleted()?;
        let indexes = if tombstoned {
            Vec::new()
        } else {
            self.project(state)?
        };
        let snapshot = state.save()?;
        let expected = current.get(&entry.document).copied().flatten();
        let change = LocalChange::new(
            self.scope,
            entry.document,
            entry.operation,
            entry.request,
            entry.digest,
            entry.bytes(),
        )?;
        let outcome = self.store.commit_local(LocalCommit::new(
            change, expected, &snapshot, &indexes, tombstoned,
        ))?;
        if !matches!(outcome, logical_db_store::CommitOutcome::Applied { .. }) {
            return Ok(false);
        }
        let next = match expected {
            Some(revision) => revision.next().ok_or(StoreError::RevisionExhausted)?,
            None => DocumentRevision::new(1),
        };
        current.insert(entry.document, Some(next));
        Ok(true)
    }

    /// Loads the durable restore cursor for this scope, when one is active.
    ///
    /// A present cursor means a previous restore was interrupted; retrying
    /// [`Database::restore`] with the same payload resumes from it. Test-only:
    /// no shipped path inspects the cursor directly, so it stays out of the
    /// library surface until an external caller needs it.
    ///
    /// # Errors
    ///
    /// Returns a typed durable read or corruption error.
    #[cfg(test)]
    pub fn restore_cursor(&self) -> Result<Option<logical_db_store::RestoreCursor>, DatabaseError> {
        Ok(self.store.restore_cursor(self.scope)?)
    }
}

/// Export identity
fn export_identity(payload: &[u8]) -> [u8; 32] {
    *blake3(payload).as_bytes()
}

/// Check restore coverage
fn check_restore_coverage(
    documents: &[ExportDocument],
    entries: &[ExportEntry],
) -> Result<BTreeSet<DocumentId>, DatabaseError> {
    let documented = documents
        .iter()
        .map(|document| document.document)
        .collect::<BTreeSet<_>>();
    let mut covered = BTreeSet::new();
    for entry in entries {
        if !documented.contains(&entry.document) {
            return Err(StoreError::InvalidExport.into());
        }
        covered.insert(entry.document);
    }
    if covered != documented {
        return Err(StoreError::InvalidExport.into());
    }
    let mut bindings = BTreeMap::new();
    for entry in entries {
        let binding = (
            entry.document,
            entry.request,
            entry.digest,
            entry.bytes().to_vec(),
        );
        if let Some(previous) = bindings.insert(entry.operation, binding.clone()) {
            if previous == binding {
                return Err(StoreError::InvalidExport.into());
            }
            return Err(StoreError::OperationConflict.into());
        }
    }
    for entry in entries {
        EncodedChange::new(entry.digest, entry.bytes().to_vec()).validate()?;
    }
    for document in documents {
        let mut state = Document::load(document.snapshot())?;
        if state.is_deleted()? != document.tombstone {
            return Err(StoreError::InvalidExport.into());
        }
    }
    Ok(documented)
}
