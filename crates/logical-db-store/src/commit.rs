//! Commit batch implementation; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::{
    decode_operation, decode_revision, decode_u64, encode_event, encode_operation, encode_outbox,
};
use crate::error::StoreError;
use crate::indexes::{replace_indexes, validate_indexes};
use crate::keys::{
    DATA, is_nonempty_bounded, key_change, key_event, key_event_sequence, key_operation,
    key_outbox, key_revision, key_sequence, key_snapshot,
};
use crate::store::{
    EmbeddedStore, check_tombstone_transition, ensure_not_migrating, write_tombstone,
};
use crate::types::{CommitOutcome, LocalCommit};
use logical_db_model::{DocumentRevision, MAX_SNAPSHOT_BYTES, Scope};
use redb::{Durability, ReadableTable, Table};
use std::collections::BTreeSet;

impl EmbeddedStore {
    /// Atomically saves a document snapshot, immutable change, operation
    /// result, scope sequence, and outbox entry.
    ///
    /// # Errors
    ///
    /// Returns a validation or storage error. Reusing an operation identity
    /// for different change bytes returns [`StoreError::OperationConflict`].
    pub fn commit_local(&self, commit: LocalCommit<'_>) -> Result<CommitOutcome, StoreError> {
        self.commit_local_batch(std::slice::from_ref(&commit))?
            .pop()
            .ok_or(StoreError::EmptyLocalBatch)
    }

    /// Atomically saves several local document transitions in one durable
    /// transaction. Every snapshot and revision is checked before any row is
    /// written, so a stale or invalid member leaves the complete batch absent.
    ///
    /// # Errors
    ///
    /// Returns a validation, stale-read, operation, or storage error. A
    /// successful commit returns one outcome per input in input order.
    pub fn commit_local_batch(
        &self,
        commits: &[LocalCommit<'_>],
    ) -> Result<Vec<CommitOutcome>, StoreError> {
        if commits.is_empty() {
            return Ok(Vec::new());
        }
        let mut documents = BTreeSet::new();
        let mut operations = BTreeSet::new();
        for commit in commits {
            if !is_nonempty_bounded(commit.snapshot.len(), MAX_SNAPSHOT_BYTES) {
                let error = StoreError::InvalidSnapshotSize(commit.snapshot.len());
                return Err(error);
            }
            validate_indexes(commit.indexes)?;
            if !documents.insert(commit.change.document) {
                return Err(StoreError::DuplicateBatchDocument);
            }
            if !operations.insert(commit.change.operation) {
                return Err(StoreError::DuplicateBatchOperation);
            }
        }

        let scope = commits[0].change.scope;
        if commits.iter().any(|commit| commit.change.scope != scope) {
            return Err(StoreError::MixedBatchScope);
        }
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;

        let mut outcomes = Vec::with_capacity(commits.len());
        let mut current_revisions = Vec::with_capacity(commits.len());
        for commit in commits {
            let operation_key = key_operation(scope, commit.change.operation);
            if let Some(sequence) = existing_local_sequence(&table, commit, &operation_key)? {
                outcomes.push(Some(CommitOutcome::Duplicate { sequence }));
                current_revisions.push(None);
                continue;
            }
            let revision_key = key_revision(scope, commit.change.document);
            let current_revision = table
                .get(revision_key.as_slice())?
                .map(|value| decode_revision(value.value()))
                .transpose()?;
            if current_revision != commit.expected_revision {
                let error = StoreError::StaleDocument;
                return Err(error);
            }
            check_tombstone_transition(&table, scope, commit.change.document, commit.tombstone)?;
            outcomes.push(None);
            current_revisions.push(current_revision);
        }

        let sequence_key = key_sequence(scope);
        let stored_sequence = table
            .get(sequence_key.as_slice())?
            .map(|value| decode_u64(value.value()))
            .transpose()?;
        let mut sequence = 0;
        if let Some(next) = stored_sequence {
            sequence = next;
        }
        let event_sequence_key = key_event_sequence(scope);
        let stored_event = table
            .get(event_sequence_key.as_slice())?
            .map(|value| decode_u64(value.value()))
            .transpose()?;
        let mut event_sequence = 0;
        if let Some(next) = stored_event {
            event_sequence = next;
        }
        for (index, commit) in commits.iter().enumerate() {
            if outcomes[index].is_some() {
                continue;
            }
            commit_local_member(
                &mut table,
                scope,
                commit,
                current_revisions[index],
                &mut sequence,
                &mut event_sequence,
            )?;
            outcomes[index] = Some(CommitOutcome::Applied { sequence });
        }
        table.insert(sequence_key.as_slice(), sequence.to_be_bytes().as_slice())?;
        table.insert(
            event_sequence_key.as_slice(),
            event_sequence.to_be_bytes().as_slice(),
        )?;
        drop(table);
        transaction.commit()?;
        outcomes
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(StoreError::CorruptInternalValue)
    }
}

/// Writes one local commit-batch member and advances both sequence cursors.
pub(crate) fn commit_local_member(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    commit: &LocalCommit<'_>,
    current_revision: Option<DocumentRevision>,
    sequence: &mut u64,
    event_sequence: &mut u64,
) -> Result<(), StoreError> {
    let next_revision = match current_revision {
        Some(revision) => revision.next().ok_or(StoreError::RevisionExhausted)?,
        None => DocumentRevision::new(1),
    };
    *sequence = sequence
        .checked_add(1)
        .ok_or(StoreError::SequenceExhausted)?;
    *event_sequence = event_sequence
        .checked_add(1)
        .ok_or(StoreError::SequenceExhausted)?;
    let operation_key = key_operation(scope, commit.change.operation);
    let operation_value = encode_operation(
        commit.change.document,
        commit.change.request,
        commit.change.digest,
        *sequence,
    );
    let outbox_value = encode_outbox(commit.change);
    table.insert(
        key_snapshot(scope, commit.change.document).as_slice(),
        commit.snapshot,
    )?;
    table.insert(
        key_change(scope, commit.change.document, commit.change.digest).as_slice(),
        commit.change.bytes,
    )?;
    table.insert(operation_key.as_slice(), operation_value.as_slice())?;
    table.insert(
        key_outbox(scope, *sequence).as_slice(),
        outbox_value.as_slice(),
    )?;
    table.insert(
        key_revision(scope, commit.change.document).as_slice(),
        next_revision.get().to_be_bytes().as_slice(),
    )?;
    if commit.tombstone {
        write_tombstone(table, scope, commit.change.document)?;
    }
    replace_indexes(table, scope, commit.change.document, commit.indexes)?;
    table.insert(
        key_event(scope, *event_sequence).as_slice(),
        encode_event(commit.change.document, commit.change.digest).as_slice(),
    )?;
    Ok(())
}

/// Returns the sequence of an exact operation retry, or fails closed on conflict.
pub(crate) fn existing_local_sequence(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    commit: &LocalCommit<'_>,
    operation_key: &[u8],
) -> Result<Option<u64>, StoreError> {
    let scope = commit.change.scope;
    let stored_operation = table.get(operation_key)?;
    let Some(existing) = stored_operation else {
        return Ok(None);
    };
    let existing = decode_operation(existing.value())?;
    if existing.document == commit.change.document
        && existing.request == commit.change.request
        && existing.digest == commit.change.digest
    {
        let change_key = key_change(scope, existing.document, existing.digest);
        let existing_bytes = table
            .get(change_key.as_slice())?
            .ok_or(StoreError::CorruptInternalValue)?;
        if existing_bytes.value() == commit.change.bytes {
            return Ok(Some(existing.sequence));
        }
    }
    let error = StoreError::OperationConflict;
    Err(error)
}
