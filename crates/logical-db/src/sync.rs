//! Push/pull synchronization and remote page installation.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::collections::BTreeMap;

use logical_db_crdt::{Document, EncodedChange};
use logical_db_model::{DocumentId, PeerId, SnapshotArtifact};
use logical_db_store::{
    InstallMode, MAX_MATERIALIZED_PAGE_BYTES, MAX_SNAPSHOT_BYTES, RemoteDocument,
};
use logical_db_sync::{
    InboundChange, OutboundChange, ReplicaPort, SyncError, SyncLimits, SyncReport, Transport,
    synchronize as run_synchronization,
};

use super::db::Database;
use super::error::DatabaseError;
use super::outcomes::{MaterializedRemoteDocument, PendingChange, RemoteChange};

impl Database {
    /// Returns a bounded ordered page of locally committed changes ready for a
    /// synchronization transport.
    ///
    /// # Errors
    ///
    /// Returns a store failure or [`DatabaseError::MissingChange`] if durable
    /// outbox metadata refers to absent change bytes.
    pub fn pending_changes(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<Vec<PendingChange>, DatabaseError> {
        self.store
            .outbox_page(self.scope, after, limit)?
            .into_iter()
            .map(|entry| {
                let committed = self
                    .store
                    .operation(self.scope, entry.operation)?
                    .ok_or(DatabaseError::MissingOperation(entry.operation))?;
                let bytes = self
                    .store
                    .load_change(self.scope, entry.document, entry.digest)?
                    .ok_or(DatabaseError::MissingChange(entry.digest))?;
                EncodedChange::new(entry.digest, bytes.clone()).validate()?;
                Ok(PendingChange {
                    sequence: entry.sequence,
                    envelope: logical_db_protocol::ChangeEnvelope::new(
                        self.scope,
                        entry.document,
                        self.replica,
                        entry.operation,
                        committed.request,
                        entry.digest,
                        bytes,
                    ),
                })
            })
            .collect()
    }

    /// Atomically installs a verified remote page and its opaque peer
    /// checkpoint after CRDT merge.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, CRDT, stale-read, or durable storage error.
    /// No document or checkpoint becomes visible if any entry fails.
    pub fn install_remote_page(
        &self,
        peer: PeerId,
        changes: &[RemoteChange],
        checkpoint: &[u8],
    ) -> Result<(), DatabaseError> {
        let mut grouped: BTreeMap<DocumentId, Vec<EncodedChange>> = BTreeMap::new();
        for change in changes {
            let encoded = EncodedChange::new(change.digest, change.bytes.clone());
            encoded.validate()?;
            grouped.entry(change.document).or_default().push(encoded);
        }

        let mut prepared = Vec::with_capacity(grouped.len());
        let mut materialized_bytes = 0usize;
        for (document, encoded) in &grouped {
            let materialized = self.materialize_remote_document(*document, encoded)?;
            materialized_bytes = materialized_bytes
                .checked_add(materialized.2.len())
                .ok_or(DatabaseError::MaterializationBudgetExceeded)?;
            if materialized_bytes > MAX_MATERIALIZED_PAGE_BYTES {
                return Err(DatabaseError::MaterializationBudgetExceeded);
            }
            prepared.push(materialized);
        }

        let durable_documents = prepared
            .iter()
            .map(
                |&(document, expected_revision, ref snapshot, ref indexes, tombstoned)| {
                    RemoteDocument::new(document, expected_revision, snapshot, indexes, tombstoned)
                },
            )
            .collect::<Vec<_>>();
        let durable_changes = changes
            .iter()
            .map(|change| {
                logical_db_store::RemoteChange::new(change.document, change.digest, &change.bytes)
            })
            .collect::<Vec<_>>();
        self.store.install_remote_page(
            self.scope,
            peer,
            &durable_documents,
            &durable_changes,
            checkpoint,
            InstallMode::Remote,
        )?;
        Ok(())
    }

    /// Atomically installs a verified state-bearing baseline and its checkpoint.
    ///
    /// The artifact's content identity is verified before any durable mutation.
    /// Each baseline document is then merged with the replica's current
    /// convergent state rather than replacing it, so a local edit the baseline
    /// predates survives recovery; the merged state is re-projected and the whole
    /// set commits with the replacement checkpoint in one transaction. Local
    /// changes are never lost: every local change is either covered by the
    /// baseline, retained by the merge, or re-delivered by the log suffix the
    /// client resumes from, so recovery converges.
    ///
    /// # Errors
    ///
    /// Returns a typed failure when the artifact is malformed, a document does
    /// not load, or the durable transaction fails. Both state and checkpoint
    /// stay unchanged on every failure path.
    pub fn install_baseline(
        &self,
        peer: PeerId,
        artifact: &SnapshotArtifact,
        checkpoint: &[u8],
    ) -> Result<(), DatabaseError> {
        artifact.validate(self.scope)?;
        let mut materialized = Vec::with_capacity(artifact.documents().len());
        for document in artifact.documents() {
            let local = self.store.load_snapshot(self.scope, document.document())?;
            let expected_revision = local.as_ref().map(|snapshot| snapshot.revision());
            let mut state = Document::load(document.snapshot())?;
            if state.is_deleted()? != document.is_tombstone() {
                return Err(DatabaseError::InvalidBaseline);
            }
            if let Some(ref local) = local {
                let mut current = Document::load(local.bytes())?;
                state.merge(&mut current)?;
            }
            let tombstoned = state.is_deleted()?;
            let indexes = if tombstoned {
                Vec::new()
            } else {
                self.project(&mut state)?
            };
            let snapshot = state.save()?;
            materialized.push((
                document.document(),
                expected_revision,
                snapshot,
                indexes,
                tombstoned,
            ));
        }
        let durable_documents = materialized
            .iter()
            .map(
                |&(document, expected_revision, ref snapshot, ref indexes, tombstoned)| {
                    RemoteDocument::new(document, expected_revision, snapshot, indexes, tombstoned)
                },
            )
            .collect::<Vec<_>>();
        self.store.install_remote_page(
            self.scope,
            peer,
            &durable_documents,
            &[],
            checkpoint,
            InstallMode::Baseline,
        )?;
        Ok(())
    }

    /// Returns this scope's durable baseline generation, `0` before any install.
    ///
    /// A subscription that tracks both its [`Self::changes_since`] cursor and
    /// this generation can detect a baseline install even when the install
    /// carried no change of its own, because installing a baseline advances the
    /// generation atomically with the replaced state and checkpoint.
    ///
    /// # Errors
    ///
    /// Returns a storage or corruption failure.
    pub fn baseline_generation(&self) -> Result<u64, DatabaseError> {
        Ok(self.store.baseline_generation(self.scope)?)
    }

    /// Returns the last atomically installed opaque checkpoint for a peer.
    ///
    /// # Errors
    ///
    /// Returns a typed durable read or corruption error.
    pub fn checkpoint(&self, peer: PeerId) -> Result<Option<Vec<u8>>, DatabaseError> {
        Ok(self.store.checkpoint(self.scope, peer)?)
    }

    /// Marks all local outbox entries through an inclusive sequence as
    /// accepted by the configured synchronization service.
    ///
    /// # Errors
    ///
    /// Refuses a mismatched upstream or acknowledgements beyond the local
    /// durable sequence.
    pub(crate) fn acknowledge_uploaded(
        &self,
        peer: PeerId,
        through: u64,
    ) -> Result<(), DatabaseError> {
        Ok(self.store.acknowledge_outbox(self.scope, peer, through)?)
    }

    /// Runs one bounded push/pull cycle over an application-supplied transport.
    ///
    /// # Errors
    ///
    /// Preserves replica, transport, protocol, rejection, and cancellation
    /// failures. Unknown transport outcomes never acknowledge the local outbox.
    pub async fn synchronize<T: Transport>(
        &self,
        peer: PeerId,
        transport: &mut T,
        limits: SyncLimits,
        cancelled: impl FnMut() -> bool,
    ) -> Result<SyncReport, SyncError<DatabaseError, T::Error>> {
        run_synchronization(self, peer, transport, limits, cancelled).await
    }

    /// Materialize remote document
    pub(crate) fn materialize_remote_document(
        &self,
        document: DocumentId,
        encoded: &[EncodedChange],
    ) -> Result<MaterializedRemoteDocument, DatabaseError> {
        let stored = self.store.load_snapshot(self.scope, document)?;
        let expected_revision = stored.as_ref().map(|snapshot| snapshot.revision());
        let mut state = match stored {
            Some(snapshot) => Document::load(snapshot.bytes())?,
            None => Document::new(),
        };
        state.apply(encoded)?;
        let tombstoned = state.is_deleted()?;
        let indexes = if tombstoned {
            Vec::new()
        } else {
            self.project(&mut state)?
        };
        let snapshot = state.save()?;
        if snapshot.len() > MAX_SNAPSHOT_BYTES {
            return Err(DatabaseError::MaterializationBudgetExceeded);
        }
        Ok((document, expected_revision, snapshot, indexes, tombstoned))
    }
}

impl ReplicaPort for Database {
    /// Error
    type Error = DatabaseError;

    /// Scope
    fn scope(&self) -> logical_db_model::Scope {
        Database::scope(self)
    }

    /// Replica
    fn replica(&self) -> logical_db_model::ReplicaId {
        Database::replica(self)
    }

    /// Bind upstream
    fn bind_upstream(&self, peer: PeerId) -> Result<(), Self::Error> {
        Ok(self.store.bind_upstream(self.scope, peer)?)
    }

    /// Pending
    fn pending(&self, limit: usize) -> Result<Vec<OutboundChange>, Self::Error> {
        Database::pending_changes(self, 0, limit).map(|changes| {
            changes
                .into_iter()
                .map(|change| {
                    OutboundChange::new(
                        change.sequence,
                        change.document(),
                        change.operation(),
                        change.request(),
                        change.digest(),
                        change.envelope.bytes().to_vec(),
                    )
                })
                .collect()
        })
    }

    /// Acknowledge
    fn acknowledge(&self, peer: PeerId, through: u64) -> Result<(), Self::Error> {
        Database::acknowledge_uploaded(self, peer, through)
    }

    /// Checkpoint
    fn checkpoint(&self, peer: PeerId) -> Result<Option<Vec<u8>>, Self::Error> {
        Database::checkpoint(self, peer)
    }

    /// Install
    fn install(
        &self,
        peer: PeerId,
        changes: &[InboundChange],
        checkpoint: &[u8],
    ) -> Result<(), Self::Error> {
        let changes = changes
            .iter()
            .map(|change| {
                RemoteChange::new(change.document(), change.digest(), change.bytes().to_vec())
            })
            .collect::<Vec<_>>();
        Database::install_remote_page(self, peer, &changes, checkpoint)
    }

    /// Install baseline
    fn install_baseline(
        &self,
        peer: PeerId,
        artifact: &SnapshotArtifact,
        checkpoint: &[u8],
    ) -> Result<(), Self::Error> {
        Database::install_baseline(self, peer, artifact, checkpoint)
    }
}

#[cfg(test)]
mod tests;
