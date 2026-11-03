//! Remote page installation; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::{decode_revision, decode_u64, encode_event};
use crate::error::StoreError;
use crate::indexes::{replace_indexes, validate_indexes};
use crate::keys::{
    DATA, MAX_CHECKPOINT_BYTES, MAX_REMOTE_CHANGE_BYTES, MAX_REMOTE_CHANGES,
    add_materialized_bytes, is_at_most, is_nonempty_bounded, key_baseline, key_baseline_event,
    key_change, key_checkpoint, key_event, key_event_sequence, key_revision, key_snapshot,
};
use crate::outbox::bind_upstream_in_table;
use crate::store::{EmbeddedStore, ensure_not_migrating, write_tombstone};
use crate::types::{RemoteChange, RemoteDocument};
use logical_db_model::{DocumentRevision, MAX_CHANGE_BYTES, MAX_SNAPSHOT_BYTES, PeerId, Scope};
use redb::{Durability, ReadableTable, Table};
use std::collections::BTreeSet;

/// How one remote page lands: as ordinary entries, or as a scope-replacing
/// baseline that also advances the generation and invalidates subscribers.
///
/// One entry point keeps the two install paths from drifting into
/// near-identical twins; the baseline flag rides this mode instead.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InstallMode {
    /// Ordinary entries; no generation advance, no invalidation.
    Remote,
    /// A recovered baseline page, installed atomically with its generation
    /// advance and invalidation.
    Baseline,
}

impl EmbeddedStore {
    /// Atomically installs a verified remote page and its replacement
    /// checkpoint. Changes never enter the local outbox.
    ///
    /// A baseline install advances the scope's monotonic baseline generation
    /// and publishes a [`ChangeEvent::Baseline`] invalidation in the same
    /// transaction, so a subscription cursor taken before the install can
    /// never observe a replaced scope as though nothing happened.
    ///
    /// # Errors
    ///
    /// Fails before commit for stale snapshots, duplicate document entries,
    /// conflicting immutable bytes, or resource-bound violations.
    pub fn install_remote_page(
        &self,
        scope: Scope,
        peer: PeerId,
        documents: &[RemoteDocument<'_>],
        changes: &[RemoteChange<'_>],
        checkpoint: &[u8],
        mode: InstallMode,
    ) -> Result<(), StoreError> {
        self.install_remote_page_inner(
            scope,
            peer,
            documents,
            changes,
            checkpoint,
            mode == InstallMode::Baseline,
        )
    }

    /// Shared body of remote-page and baseline installation.
    fn install_remote_page_inner(
        &self,
        scope: Scope,
        peer: PeerId,
        documents: &[RemoteDocument<'_>],
        changes: &[RemoteChange<'_>],
        checkpoint: &[u8],
        baseline: bool,
    ) -> Result<(), StoreError> {
        validate_remote_page(documents, changes, checkpoint)?;

        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        bind_upstream_in_table(&mut table, scope, peer)?;
        for document in documents {
            let revision_key = key_revision(scope, document.document);
            let current_value = table.get(revision_key.as_slice())?;
            let current = current_value
                .map(|value| decode_revision(value.value()))
                .transpose()?;
            if current != document.expected_revision {
                let error = StoreError::StaleDocument;
                return Err(error);
            }
        }
        let mut new_changes = Vec::new();
        for change in changes {
            let key = key_change(scope, change.document, change.digest);
            let existing = table
                .get(key.as_slice())?
                .map(|value| value.value().to_vec());
            if let Some(existing) = existing {
                if existing != change.bytes {
                    let error = StoreError::ChangeDigestConflict;
                    return Err(error);
                }
            } else {
                table.insert(key.as_slice(), change.bytes)?;
                new_changes.push((change.document, change.digest));
            }
        }
        for document in documents {
            install_remote_document(&mut table, scope, document)?;
        }
        let event_sequence_value = table.get(key_event_sequence(scope).as_slice())?;
        let mut event_sequence = 0;
        if let Some(next) = event_sequence_value
            .map(|value| decode_u64(value.value()))
            .transpose()?
        {
            event_sequence = next;
        }
        for (document, digest) in new_changes {
            event_sequence = event_sequence
                .checked_add(1)
                .ok_or(StoreError::SequenceExhausted)?;
            table.insert(
                key_event(scope, event_sequence).as_slice(),
                encode_event(document, digest).as_slice(),
            )?;
        }
        if baseline {
            let stored = table
                .get(key_baseline(scope).as_slice())?
                .map(|value| decode_u64(value.value()))
                .transpose()?;
            let mut baseline = 0;
            if let Some(next) = stored {
                baseline = next;
            }
            let generation = baseline
                .checked_add(1)
                .ok_or(StoreError::SequenceExhausted)?;
            event_sequence = event_sequence
                .checked_add(1)
                .ok_or(StoreError::SequenceExhausted)?;
            table.insert(
                key_baseline_event(scope, event_sequence).as_slice(),
                generation.to_be_bytes().as_slice(),
            )?;
            table.insert(
                key_baseline(scope).as_slice(),
                generation.to_be_bytes().as_slice(),
            )?;
        }
        table.insert(
            key_event_sequence(scope).as_slice(),
            event_sequence.to_be_bytes().as_slice(),
        )?;
        table.insert(key_checkpoint(scope, peer).as_slice(), checkpoint)?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }
}

/// Materializes one remotely merged document inside a write transaction.
pub(crate) fn install_remote_document(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    document: &RemoteDocument<'_>,
) -> Result<(), StoreError> {
    let next = match document.expected_revision {
        Some(revision) => revision.next().ok_or(StoreError::RevisionExhausted)?,
        None => DocumentRevision::new(1),
    };
    table.insert(
        key_snapshot(scope, document.document).as_slice(),
        document.snapshot,
    )?;
    table.insert(
        key_revision(scope, document.document).as_slice(),
        next.get().to_be_bytes().as_slice(),
    )?;
    if document.tombstone {
        write_tombstone(table, scope, document.document)?;
    }
    replace_indexes(table, scope, document.document, document.indexes)?;
    Ok(())
}

/// Validates every count, byte, and coverage bound of one remote page.
pub(crate) fn validate_remote_page(
    documents: &[RemoteDocument<'_>],
    changes: &[RemoteChange<'_>],
    checkpoint: &[u8],
) -> Result<(), StoreError> {
    if !is_nonempty_bounded(checkpoint.len(), MAX_CHECKPOINT_BYTES) {
        let error = StoreError::InvalidCheckpointSize(checkpoint.len());
        return Err(error);
    }
    if !is_at_most(changes.len(), MAX_REMOTE_CHANGES) {
        let error = StoreError::InvalidRemotePage;
        return Err(error);
    }
    let mut total_change_bytes = 0usize;
    for change in changes {
        if !is_nonempty_bounded(change.bytes.len(), MAX_CHANGE_BYTES) {
            let error = StoreError::InvalidChangeSize(change.bytes.len());
            return Err(error);
        }
        total_change_bytes = total_change_bytes
            .checked_add(change.bytes.len())
            .filter(|total| is_at_most(*total, MAX_REMOTE_CHANGE_BYTES))
            .ok_or(StoreError::InvalidRemotePage)?;
    }
    let mut seen_documents = BTreeSet::new();
    let mut materialized_bytes = 0usize;
    for document in documents {
        if !is_nonempty_bounded(document.snapshot.len(), MAX_SNAPSHOT_BYTES) {
            let error = StoreError::InvalidSnapshotSize(document.snapshot.len());
            return Err(error);
        }
        materialized_bytes = add_materialized_bytes(materialized_bytes, document.snapshot.len())
            .ok_or(StoreError::InvalidRemotePage)?;
        if !seen_documents.insert(document.document) {
            let error = StoreError::InvalidRemotePage;
            return Err(error);
        }
        validate_indexes(document.indexes)?;
    }
    if changes
        .iter()
        .any(|change| !seen_documents.contains(&change.document))
    {
        let error = StoreError::InvalidRemotePage;
        return Err(error);
    }
    Ok(())
}
