//! Structured edits, atomic batches, and first-class deletes.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::collections::BTreeSet;

use logical_db_crdt::{CrdtError, Document, Editor, EncodedChange};
use logical_db_model::{ChangeDigest, DocumentId, DocumentRevision, OperationDigest, OperationId};
use logical_db_store::{CommitOutcome, LocalChange, LocalCommit, StoreError};

use super::db::Database;
use super::error::DatabaseError;
use super::outcomes::{
    BatchEdit, DeleteOutcome, DeleteReceipt, DuplicateReceipt, EditOutcome, EditReceipt,
    PreparedBatchEdit,
};

/// MAX EDIT BATCH
pub(crate) const MAX_EDIT_BATCH: usize = 256;

impl Database {
    /// Existing batch operation
    pub(crate) fn existing_batch_operation(
        &self,
        edit: &BatchEdit,
    ) -> Result<Option<DuplicateReceipt>, DatabaseError> {
        let Some(existing) = self.store.operation(self.scope, edit.operation)? else {
            return Ok(None);
        };
        if existing.document != edit.document || existing.request != edit.request {
            return Err(StoreError::OperationConflict.into());
        }
        let bytes = self
            .store
            .load_change(self.scope, edit.document, existing.digest)?
            .ok_or(DatabaseError::MissingChange(existing.digest))?;
        EncodedChange::new(existing.digest, bytes).validate()?;
        Ok(Some(DuplicateReceipt {
            sequence: existing.sequence,
            digest: existing.digest,
        }))
    }

    /// Validates an exact single-operation retry against its stored change.
    ///
    /// An unknown operation is new work, a reused identity with different
    /// bytes fails closed, and an exact retry returns its stored receipt
    /// without staging a new change.
    fn existing_single_operation(
        &self,
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
    ) -> Result<Option<DuplicateReceipt>, DatabaseError> {
        let Some(existing) = self.store.operation(self.scope, operation)? else {
            return Ok(None);
        };
        if existing.document != document || existing.request != request {
            return Err(StoreError::OperationConflict.into());
        }
        let bytes = self
            .store
            .load_change(self.scope, document, existing.digest)?
            .ok_or(DatabaseError::MissingChange(existing.digest))?;
        EncodedChange::new(existing.digest, bytes).validate()?;
        Ok(Some(DuplicateReceipt {
            sequence: existing.sequence,
            digest: existing.digest,
        }))
    }

    /// Stages one validated local change for commit.
    fn stage_local_change<'a>(
        &self,
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
        digest: ChangeDigest,
        bytes: &'a [u8],
    ) -> Result<LocalChange<'a>, DatabaseError> {
        LocalChange::new(self.scope, document, operation, request, digest, bytes)
            .map_err(DatabaseError::Store)
    }

    /// Applies one structured CRDT transaction and atomically saves its
    /// snapshot, immutable change, idempotency identity, and outbox entry.
    ///
    /// # Errors
    ///
    /// Returns a typed CRDT or storage failure. Concurrent successors of the
    /// same snapshot return [`DatabaseError::Store`] containing
    /// [`StoreError::StaleDocument`] rather than overwriting state. An exact
    /// retry returns [`EditOutcome::Duplicate`] without rerunning `edit`.
    pub fn edit<T>(
        &self,
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
        edit: impl FnOnce(&mut Editor<'_>) -> Result<T, CrdtError>,
    ) -> Result<EditOutcome<T>, DatabaseError> {
        self.edit_with_revision(document, None, operation, request, edit)
    }

    /// Applies one structured edit only if the document still has the supplied
    /// revision. Exact operation retries retain the normal duplicate result.
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::Store`] containing
    /// [`StoreError::StaleDocument`] when the observed revision changed before
    /// the edit could commit.
    pub fn edit_if_revision<T>(
        &self,
        document: DocumentId,
        expected_revision: DocumentRevision,
        operation: OperationId,
        request: OperationDigest,
        edit: impl FnOnce(&mut Editor<'_>) -> Result<T, CrdtError>,
    ) -> Result<EditOutcome<T>, DatabaseError> {
        self.edit_with_revision(document, Some(expected_revision), operation, request, edit)
    }

    /// Edit with revision
    fn edit_with_revision<T>(
        &self,
        document: DocumentId,
        required_revision: Option<DocumentRevision>,
        operation: OperationId,
        request: OperationDigest,
        edit: impl FnOnce(&mut Editor<'_>) -> Result<T, CrdtError>,
    ) -> Result<EditOutcome<T>, DatabaseError> {
        if let Some(receipt) = self.existing_single_operation(document, operation, request)? {
            return Ok(EditOutcome::Duplicate(receipt));
        }
        let stored = self.store.load_snapshot(self.scope, document)?;
        let actual_revision = stored.as_ref().map(|snapshot| snapshot.revision);
        if required_revision.is_some_and(|expected| actual_revision != Some(expected)) {
            return Err(StoreError::StaleDocument.into());
        }
        let mut state = match stored {
            Some(snapshot) => Document::load(snapshot.bytes())?,
            None => Document::new(),
        };
        if state.is_deleted()? {
            return Err(DatabaseError::DocumentDeleted);
        }
        let (value, mut changes) = state.edit(edit)?;
        if changes.len() != 1 {
            return Err(DatabaseError::ChangeCardinality(changes.len()));
        }
        let change = changes.pop().ok_or(DatabaseError::ChangeCardinality(0))?;
        let indexes = self.project(&mut state)?;
        let snapshot = state.save()?;
        let local_change =
            self.stage_local_change(document, operation, request, change.digest, &change.bytes)?;
        let outcome = self.store.commit_local(LocalCommit::new(
            local_change,
            actual_revision,
            &snapshot,
            &indexes,
            false,
        ))?;
        match outcome {
            CommitOutcome::Applied { sequence } => Ok(EditOutcome::Applied(EditReceipt {
                value,
                sequence,
                digest: change.digest,
            })),
            CommitOutcome::Duplicate { sequence } => Ok(EditOutcome::Duplicate(DuplicateReceipt {
                sequence,
                digest: change.digest,
            })),
            // Fail closed on a future commit outcome rather than assuming the
            // enum is closed outside the defining crate.
            _ => Err(DatabaseError::UnknownCommitOutcome),
        }
    }

    /// Prepare batch edit
    fn prepare_batch_edit(
        &self,
        index: usize,
        edit: BatchEdit,
    ) -> Result<PreparedBatchEdit, DatabaseError> {
        let stored = self.store.load_snapshot(self.scope, edit.document)?;
        let expected_revision = stored.as_ref().map(|snapshot| snapshot.revision);
        let mut state = match stored {
            Some(snapshot) => Document::load(snapshot.bytes())?,
            None => Document::new(),
        };
        if state.is_deleted()? {
            return Err(DatabaseError::DocumentDeleted);
        }
        let ((), mut changes) = state.edit(edit.edit)?;
        if changes.len() != 1 {
            return Err(DatabaseError::ChangeCardinality(changes.len()));
        }
        let change = changes.pop().ok_or(DatabaseError::ChangeCardinality(0))?;
        let indexes = self.project(&mut state)?;
        let snapshot = state.save()?;
        Ok(PreparedBatchEdit {
            index,
            document: edit.document,
            operation: edit.operation,
            request: edit.request,
            digest: change.digest,
            bytes: change.bytes,
            expected_revision,
            snapshot,
            indexes,
        })
    }

    /// Applies several structured edits and atomically saves all resulting
    /// snapshots, immutable changes, operation receipts, indexes, and outbox
    /// entries in one durable transaction.
    ///
    /// Every edit is materialized and checked before the first durable write.
    /// A stale document, malformed edit, or storage failure leaves the whole
    /// batch absent. Duplicate document identities are rejected before any
    /// edit closure runs. The input is bounded before materialization or
    /// transaction setup.
    ///
    /// # Errors
    ///
    /// Returns a typed CRDT, duplicate-document, stale-read, operation, or
    /// storage failure.
    pub fn edit_many(&self, edits: Vec<BatchEdit>) -> Result<Vec<EditOutcome<()>>, DatabaseError> {
        if edits.is_empty() {
            return Ok(Vec::new());
        }
        if edits.len() > MAX_EDIT_BATCH {
            return Err(DatabaseError::InvalidEditBatchSize(edits.len()));
        }
        let mut documents = BTreeSet::new();
        for edit in &edits {
            if !documents.insert(edit.document) {
                return Err(DatabaseError::DuplicateBatchDocument(edit.document));
            }
        }

        let mut duplicates = Vec::with_capacity(edits.len());
        for edit in &edits {
            duplicates.push(self.existing_batch_operation(edit)?);
        }
        let mut prepared = Vec::with_capacity(edits.len());
        for (index, edit) in edits.into_iter().enumerate() {
            if duplicates[index].is_some() {
                continue;
            }
            prepared.push(self.prepare_batch_edit(index, edit)?);
        }
        let mut results: Vec<Option<EditOutcome<()>>> =
            (0..duplicates.len()).map(|_| None).collect();
        for (index, duplicate) in duplicates.into_iter().enumerate() {
            if let Some(duplicate) = duplicate {
                results[index] = Some(EditOutcome::Duplicate(duplicate));
            }
        }
        if prepared.is_empty() {
            return results
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or(DatabaseError::ChangeCardinality(0));
        }
        let mut commits = Vec::with_capacity(prepared.len());
        for edit in &prepared {
            let change = LocalChange::new(
                self.scope,
                edit.document,
                edit.operation,
                edit.request,
                edit.digest,
                &edit.bytes,
            )?;
            commits.push(LocalCommit::new(
                change,
                edit.expected_revision,
                &edit.snapshot,
                &edit.indexes,
                false,
            ));
        }
        let outcomes = self.store.commit_local_batch(&commits)?;
        for (outcome, edit) in outcomes.into_iter().zip(prepared) {
            results[edit.index] = Some(match outcome {
                CommitOutcome::Applied { sequence } => EditOutcome::Applied(EditReceipt {
                    value: (),
                    sequence,
                    digest: edit.digest,
                }),
                CommitOutcome::Duplicate { sequence } => EditOutcome::Duplicate(DuplicateReceipt {
                    sequence,
                    digest: edit.digest,
                }),
                // Fail closed on a future commit outcome rather than assuming
                // the enum is closed outside the defining crate. EditOutcome
                // has no failure variant, so the whole batch returns Err.
                _ => return Err(DatabaseError::UnknownCommitOutcome),
            });
        }
        results
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(DatabaseError::ChangeCardinality(0))
    }

    /// Commits a first-class tombstone that converges to absent on every
    /// replica, including replicas offline at deletion time.
    ///
    /// The tombstone travels the same single-transaction discipline as an
    /// edit (snapshot, change, operation receipt, outbox, indexes, event) and
    /// carries the same operation-identity exact-retry contract: an exact
    /// retry returns [`DeleteOutcome::Duplicate`] without staging a new
    /// change, while a reused operation identity with different bytes fails
    /// closed. Deleting a never-created document still commits a tombstone so
    /// a late replica cannot resurrect the value. Tombstoned documents keep
    /// their snapshot for conflict inspection while reads report absent.
    ///
    /// # Errors
    ///
    /// Returns a typed CRDT or storage failure. Concurrent successors of the
    /// same snapshot return [`DatabaseError::Store`] containing
    /// [`StoreError::StaleDocument`] rather than overwriting state. An exact
    /// retry returns [`DeleteOutcome::Duplicate`] without staging a new
    /// change.
    pub fn delete(
        &self,
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
    ) -> Result<DeleteOutcome, DatabaseError> {
        if let Some(receipt) = self.existing_single_operation(document, operation, request)? {
            return Ok(DeleteOutcome::Duplicate(receipt));
        }
        let stored = self.store.load_snapshot(self.scope, document)?;
        let expected_revision = stored.as_ref().map(|snapshot| snapshot.revision);
        let mut state = match stored {
            Some(snapshot) => Document::load(snapshot.bytes())?,
            None => Document::new(),
        };
        let mut changes = state.delete()?;
        if changes.len() != 1 {
            return Err(DatabaseError::ChangeCardinality(changes.len()));
        }
        let change = changes.pop().ok_or(DatabaseError::ChangeCardinality(0))?;
        let snapshot = state.save()?;
        let local_change =
            self.stage_local_change(document, operation, request, change.digest, &change.bytes)?;
        let outcome = self.store.commit_local(LocalCommit::new(
            local_change,
            expected_revision,
            &snapshot,
            &[],
            true,
        ))?;
        match outcome {
            CommitOutcome::Applied { sequence } => Ok(DeleteOutcome::Applied(DeleteReceipt {
                sequence,
                digest: change.digest,
            })),
            CommitOutcome::Duplicate { sequence } => {
                Ok(DeleteOutcome::Duplicate(DuplicateReceipt {
                    sequence,
                    digest: change.digest,
                }))
            }
            // Fail closed on a future commit outcome rather than assuming the
            // enum is closed outside the defining crate.
            _ => Err(DatabaseError::UnknownCommitOutcome),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use logical_db_crdt::{Item, Object, Scalar};
    use logical_db_model::{AccountId, OperationDigest, OperationId, ReplicaId, Scope, SpaceId};
    use logical_db_store::StoreError;

    use super::*;
    use crate::error::DatabaseErrorKind;
    use crate::test_helpers::path;

    /// Opens one database seeded with a single `v1` title row.
    fn seed_titled_document()
    -> Result<(std::path::PathBuf, Scope, Database, DocumentId), Box<dyn std::error::Error>> {
        let db_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let database = Database::open(&db_path, scope, ReplicaId::new())?;
        let document = DocumentId::new();
        database.edit(
            document,
            OperationId::new(),
            OperationDigest::new([1; 32]),
            |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
        )?;
        Ok((db_path, scope, database, document))
    }

    #[test]
    fn embedded_edit_read_restart_and_outbox_need_no_braid_or_network()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let document = DocumentId::new();
        let operation = OperationId::new();
        let request = OperationDigest::new([1; 32]);
        {
            let database = Database::open(&path, scope, replica)?;
            let outcome = database.edit(document, operation, request, |editor| {
                editor.put(
                    &Object::root(),
                    "title",
                    Scalar::String("local first".into()),
                )
            })?;
            let EditOutcome::Applied(receipt) = outcome else {
                return Err(std::io::Error::other("first edit must apply").into());
            };
            assert_eq!(receipt.sequence, 1);
            assert_eq!(database.pending_changes(0, 10)?.len(), 1);
        }
        {
            let database = Database::open(&path, scope, replica)?;
            let title = database.read(document, |state| state.get(&Object::root(), "title"))?;
            assert_eq!(
                title,
                Some(Some(Item::Scalar(Scalar::String("local first".into()))))
            );
        }
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn exact_retry_does_not_rerun_application_code() -> Result<(), Box<dyn std::error::Error>> {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let database = Database::open(&path, scope, ReplicaId::new())?;
        let document = DocumentId::new();
        let operation = OperationId::new();
        let request = OperationDigest::new([3; 32]);
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let applied = database.edit(document, operation, request, |editor| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            editor.put(&Object::root(), "value", Scalar::Integer(1))
        })?;
        let EditOutcome::Applied(receipt) = applied else {
            return Err(std::io::Error::other("first request must apply").into());
        };
        let duplicate = database.edit(document, operation, request, |_editor| {
            calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(
            duplicate,
            EditOutcome::Duplicate(DuplicateReceipt { sequence, digest })
                if sequence == receipt.sequence && digest == receipt.digest
        ));
        for (other_document, other_request) in [
            (document, OperationDigest::new([4; 32])),
            (DocumentId::new(), request),
        ] {
            let Err(error) =
                database.edit(other_document, operation, other_request, |_editor| Ok(()))
            else {
                return Err("a conflicting edit must be refused".into());
            };
            assert!(matches!(
                &error,
                DatabaseError::Store(StoreError::OperationConflict)
            ));
            assert_eq!(error.kind(), DatabaseErrorKind::OperationConflict);
        }
        drop(database);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn atomic_batch_retries_duplicates_without_rerunning_and_preserves_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let database = Database::open(&path, scope, ReplicaId::new())?;
        let first_document = DocumentId::new();
        let second_document = DocumentId::new();
        let third_document = DocumentId::new();
        let first_operation = OperationId::new();
        let second_operation = OperationId::new();
        let third_operation = OperationId::new();
        let first_request = OperationDigest::new([5; 32]);
        let second_request = OperationDigest::new([6; 32]);
        let third_request = OperationDigest::new([7; 32]);
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let first_calls = std::sync::Arc::clone(&calls);
        let second_calls = std::sync::Arc::clone(&calls);
        let applied = database.edit_many(vec![
            BatchEdit::new(
                first_document,
                first_operation,
                first_request,
                move |editor| {
                    first_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    editor.put(&Object::root(), "value", Scalar::Integer(1))
                },
            ),
            BatchEdit::new(
                second_document,
                second_operation,
                second_request,
                move |editor| {
                    second_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    editor.put(&Object::root(), "value", Scalar::Integer(2))
                },
            ),
        ])?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(matches!(applied[0], EditOutcome::Applied(_)));
        assert!(matches!(applied[1], EditOutcome::Applied(_)));

        let duplicate_calls = std::sync::Arc::clone(&calls);
        let third_calls = std::sync::Arc::clone(&calls);
        let retried = database.edit_many(vec![
            BatchEdit::new(
                first_document,
                first_operation,
                first_request,
                move |_editor| {
                    duplicate_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                },
            ),
            BatchEdit::new(
                third_document,
                third_operation,
                third_request,
                move |editor| {
                    third_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    editor.put(&Object::root(), "value", Scalar::Integer(3))
                },
            ),
        ])?;
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert!(matches!(retried[0], EditOutcome::Duplicate(_)));
        assert!(matches!(retried[1], EditOutcome::Applied(_)));
        drop(database);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn atomic_batch_bound_rejects_before_materialization() -> Result<(), Box<dyn std::error::Error>>
    {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let database = Database::open(&path, scope, ReplicaId::new())?;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let edits = (0..=MAX_EDIT_BATCH)
            .map(|_| {
                let calls = std::sync::Arc::clone(&calls);
                BatchEdit::new(
                    DocumentId::new(),
                    OperationId::new(),
                    OperationDigest::new([8; 32]),
                    move |_editor| {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(())
                    },
                )
            })
            .collect();
        assert!(matches!(
            database.edit_many(edits),
            Err(DatabaseError::InvalidEditBatchSize(actual))
                if actual == MAX_EDIT_BATCH + 1
        ));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(database);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn duplicate_delete_short_circuits_exact_retry() -> Result<(), Box<dyn std::error::Error>> {
        let (db_path, _scope, database, document) = seed_titled_document()?;
        let operation = OperationId::new();
        let request = OperationDigest::new([2; 32]);
        let DeleteOutcome::Applied(applied) = database.delete(document, operation, request)? else {
            return Err(std::io::Error::other("first delete must apply").into());
        };
        let duplicate = database.delete(document, operation, request)?;
        assert!(matches!(
            duplicate,
            DeleteOutcome::Duplicate(DuplicateReceipt { sequence, digest })
                if sequence == applied.sequence && digest == applied.digest
        ));
        assert_eq!(database.pending_changes(0, 10)?.len(), 2);
        let Err(conflict) = database.delete(document, operation, OperationDigest::new([3; 32]))
        else {
            return Err("a conflicting delete must be refused".into());
        };
        assert!(matches!(
            &conflict,
            DatabaseError::Store(StoreError::OperationConflict)
        ));
        assert_eq!(conflict.kind(), DatabaseErrorKind::OperationConflict);
        drop(database);
        fs::remove_file(db_path)?;
        Ok(())
    }

    #[test]
    fn edit_after_delete_is_refused_without_leaking_store_types()
    -> Result<(), Box<dyn std::error::Error>> {
        let (db_path, _scope, database, document) = seed_titled_document()?;
        database.delete(document, OperationId::new(), OperationDigest::new([2; 32]))?;
        let Err(error) = database.edit(
            document,
            OperationId::new(),
            OperationDigest::new([3; 32]),
            |editor| editor.put(&Object::root(), "title", Scalar::String("rival".into())),
        ) else {
            return Err("an edit on a deleted document must be refused".into());
        };
        assert!(matches!(&error, DatabaseError::DocumentDeleted));
        assert_eq!(error.kind(), DatabaseErrorKind::Other);
        let missing = DocumentId::new();
        assert!(
            database
                .read(missing, |reader| reader.get(&Object::root(), "title"))?
                .is_none()
        );
        assert!(!database.is_deleted(missing)?);
        assert!(
            database
                .conflicts(missing, &Object::root(), "title")?
                .is_none()
        );
        let candidates = database
            .conflicts(document, &Object::root(), "title")?
            .ok_or("expected conflicts")?;
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].value,
            Item::Scalar(Scalar::String("v1".into()))
        );
        drop(database);
        fs::remove_file(db_path)?;
        Ok(())
    }
}
