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
        let mut materialized = Vec::with_capacity(artifact.documents.len());
        for document in &artifact.documents {
            let local = self.store.load_snapshot(self.scope, document.document)?;
            let expected_revision = local.as_ref().map(|snapshot| snapshot.revision);
            let mut state = Document::load(&document.snapshot)?;
            if state.is_deleted()? != document.tombstone {
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
                document.document,
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
        let expected_revision = stored.as_ref().map(|snapshot| snapshot.revision);
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
                        change.document,
                        change.operation,
                        change.request,
                        change.digest,
                        change.envelope.bytes,
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
            .map(|change| RemoteChange {
                document: change.document,
                digest: change.digest,
                bytes: change.bytes.clone(),
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
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use logical_db_crdt::{Item, Object, Scalar};
    use logical_db_model::{
        AccountId, DocumentId, IndexId, IndexValue, OperationDigest, OperationId, PeerId,
        ReplicaId, Scope, SpaceId,
    };
    use logical_db_sync::SyncError;

    use super::*;
    use crate::outcomes::DeleteOutcome;
    use crate::projection::{Projection, ProjectionSemantics};
    use crate::test_helpers::{path, remote_page};

    /// Opens one two-replica sync case with a fresh scope and peer.
    fn sync_case() -> (PathBuf, PathBuf, Scope, DocumentId, PeerId) {
        (
            path(),
            path(),
            Scope::new(AccountId::new(), SpaceId::new()),
            DocumentId::new(),
            PeerId::new(),
        )
    }

    #[test]
    fn public_synchronization_delegates_every_replica_port_transition()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let peer = PeerId::new();
        let database = Database::open(&path, scope, replica)?;
        database.edit(
            DocumentId::new(),
            OperationId::new(),
            OperationDigest::new([42; 32]),
            |editor| editor.put(&Object::root(), "value", Scalar::Integer(1)),
        )?;
        assert_eq!(ReplicaPort::replica(&database), replica);
        assert_eq!(ReplicaPort::pending(&database, 10)?.len(), 1);
        assert_eq!(ReplicaPort::checkpoint(&database, peer)?, None);
        let report = lgwks_std::task::block_on(database.synchronize(
            peer,
            &mut crate::test_helpers::AcceptingTransport,
            SyncLimits::default(),
            || false,
        ))?;
        // SyncReport is non_exhaustive: compare fields instead of a struct literal.
        assert_eq!(report.pushed, 1);
        assert_eq!(report.pulled, 0);
        assert_eq!(report.pages, 1);
        assert!(!report.full_resync);
        assert!(!report.recovered);
        assert!(!report.more);
        assert!(ReplicaPort::pending(&database, 10)?.is_empty());
        assert_eq!(ReplicaPort::checkpoint(&database, peer)?, Some(vec![9]));
        let second_peer = PeerId::new();
        assert!(matches!(
            lgwks_std::task::block_on(database.synchronize(
                second_peer,
                &mut crate::test_helpers::AcceptingTransport,
                SyncLimits::default(),
                || false,
            )),
            Err(SyncError::Replica(DatabaseError::Store(
                logical_db_store::StoreError::UpstreamMismatch {
                    configured,
                    requested,
                }
            ))) if configured == peer && requested == second_peer
        ));
        drop(database);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn offline_replicas_converge_and_checkpoint_install_is_durable()
    -> Result<(), Box<dyn std::error::Error>> {
        let (first_path, second_path, scope, document, peer) = sync_case();
        let first_replica = ReplicaId::new();
        let second_replica = ReplicaId::new();
        {
            let first = Database::open(&first_path, scope, first_replica)?;
            let second = Database::open(&second_path, scope, second_replica)?;
            first.edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("A".into())),
            )?;
            second.edit(
                document,
                OperationId::new(),
                OperationDigest::new([2; 32]),
                |editor| editor.put(&Object::root(), "count", Scalar::Integer(2)),
            )?;

            let first_changes = first.pending_changes(0, 10)?;
            let second_changes = second.pending_changes(0, 10)?;
            first.install_remote_page(
                peer,
                &second_changes
                    .iter()
                    .map(|change| RemoteChange {
                        document: change.document,
                        digest: change.digest,
                        bytes: change.bytes.clone(),
                    })
                    .collect::<Vec<_>>(),
                b"checkpoint-a",
            )?;
            second.install_remote_page(
                peer,
                &first_changes
                    .iter()
                    .map(|change| RemoteChange {
                        document: change.document,
                        digest: change.digest,
                        bytes: change.bytes.clone(),
                    })
                    .collect::<Vec<_>>(),
                b"checkpoint-b",
            )?;

            for database in [&first, &second] {
                assert_eq!(
                    database.read(document, |reader| reader.get(&Object::root(), "title"))?,
                    Some(Some(Item::Scalar(Scalar::String("A".into()))))
                );
                assert_eq!(
                    database.read(document, |reader| reader.get(&Object::root(), "count"))?,
                    Some(Some(Item::Scalar(Scalar::Integer(2))))
                );
            }
            first.acknowledge_uploaded(peer, 1)?;
            assert!(first.pending_changes(0, 10)?.is_empty());
        }
        let first = Database::open(&first_path, scope, first_replica)?;
        assert_eq!(first.checkpoint(peer)?, Some(b"checkpoint-a".to_vec()));
        drop(first);
        fs::remove_file(first_path)?;
        fs::remove_file(second_path)?;
        Ok(())
    }

    #[test]
    fn delete_syncs_to_absent_and_clears_indexes_on_both_replicas()
    -> Result<(), Box<dyn std::error::Error>> {
        let (first_path, second_path, scope, document, peer) = sync_case();
        let index = IndexId::new();
        let projection = Projection::new(index, ProjectionSemantics::V1, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        });
        let first = Database::open_with_projections(
            &first_path,
            scope,
            ReplicaId::new(),
            vec![projection.clone()],
        )?;
        let second = Database::open_with_projections(
            &second_path,
            scope,
            ReplicaId::new(),
            vec![projection],
        )?;
        first.edit(
            document,
            OperationId::new(),
            OperationDigest::new([1; 32]),
            |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
        )?;
        let base = first.pending_changes(0, 10)?;
        second.install_remote_page(peer, &remote_page(&base), b"checkpoint-base")?;
        assert_eq!(
            second.query_index(index, &IndexValue::Text("note".into()), None, 10)?,
            vec![document]
        );

        let outcome = first.delete(document, OperationId::new(), OperationDigest::new([2; 32]))?;
        let DeleteOutcome::Applied(receipt) = outcome else {
            return Err(std::io::Error::other("first delete must apply").into());
        };
        assert_eq!(receipt.sequence, 2);
        assert!(
            first
                .read(document, |reader| reader.get(&Object::root(), "kind"))?
                .is_none()
        );
        assert!(first.is_deleted(document)?);
        assert!(
            first
                .query_index(index, &IndexValue::Text("note".into()), None, 10)?
                .is_empty()
        );

        let pending = first.pending_changes(0, 10)?;
        assert_eq!(pending.len(), 2);
        second.install_remote_page(peer, &remote_page(&pending), b"checkpoint-deleted")?;
        for database in [&first, &second] {
            assert!(
                database
                    .read(document, |reader| reader.get(&Object::root(), "kind"))?
                    .is_none()
            );
            assert!(database.is_deleted(document)?);
            assert!(
                database
                    .query_index(index, &IndexValue::Text("note".into()), None, 10)?
                    .is_empty()
            );
        }
        drop(first);
        drop(second);
        fs::remove_file(first_path)?;
        fs::remove_file(second_path)?;
        Ok(())
    }

    #[test]
    fn offline_replica_and_late_joiner_converge_without_resurrection()
    -> Result<(), Box<dyn std::error::Error>> {
        let origin_path = path();
        let offline_path = path();
        let late_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let origin = Database::open(&origin_path, scope, ReplicaId::new())?;
        let offline = Database::open(&offline_path, scope, ReplicaId::new())?;
        origin.edit(
            document,
            OperationId::new(),
            OperationDigest::new([1; 32]),
            |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
        )?;
        let base = origin.pending_changes(0, 10)?;
        offline.install_remote_page(peer, &remote_page(&base), b"checkpoint-base")?;
        origin.delete(document, OperationId::new(), OperationDigest::new([2; 32]))?;
        let tombstone_page = origin.pending_changes(0, 10)?;
        offline.install_remote_page(peer, &remote_page(&tombstone_page), b"checkpoint-deleted")?;
        assert!(
            offline
                .read(document, |reader| reader.get(&Object::root(), "title"))?
                .is_none()
        );
        let offline_pending = offline.pending_changes(0, 10)?;
        origin.install_remote_page(peer, &remote_page(&offline_pending), b"checkpoint-back")?;
        assert!(
            origin
                .read(document, |reader| reader.get(&Object::root(), "title"))?
                .is_none()
        );

        let late = Database::open(&late_path, scope, ReplicaId::new())?;
        late.install_remote_page(peer, &remote_page(&tombstone_page), b"checkpoint-late")?;
        assert!(
            late.read(document, |reader| reader.get(&Object::root(), "title"))?
                .is_none()
        );
        assert!(late.is_deleted(document)?);
        drop(origin);
        drop(offline);
        drop(late);
        fs::remove_file(origin_path)?;
        fs::remove_file(offline_path)?;
        fs::remove_file(late_path)?;
        Ok(())
    }

    #[test]
    fn concurrent_edit_and_delete_surface_candidates_with_absent_visible()
    -> Result<(), Box<dyn std::error::Error>> {
        let (first_path, second_path, scope, document, peer) = sync_case();
        let first = Database::open(&first_path, scope, ReplicaId::new())?;
        let second = Database::open(&second_path, scope, ReplicaId::new())?;
        first.edit(
            document,
            OperationId::new(),
            OperationDigest::new([1; 32]),
            |editor| editor.put(&Object::root(), "title", Scalar::String("base".into())),
        )?;
        let base = first.pending_changes(0, 10)?;
        second.install_remote_page(peer, &remote_page(&base), b"checkpoint-base")?;
        first.delete(document, OperationId::new(), OperationDigest::new([2; 32]))?;
        second.edit(
            document,
            OperationId::new(),
            OperationDigest::new([3; 32]),
            |editor| editor.put(&Object::root(), "title", Scalar::String("rival".into())),
        )?;
        for (source, target, checkpoint) in [
            (&first, &second, b"checkpoint-a".as_slice()),
            (&second, &first, b"checkpoint-b".as_slice()),
        ] {
            let page = source.pending_changes(0, 10)?;
            target.install_remote_page(peer, &remote_page(&page), checkpoint)?;
        }
        for database in [&first, &second] {
            assert!(
                database
                    .read(document, |reader| reader.get(&Object::root(), "title"))?
                    .is_none()
            );
            assert!(database.is_deleted(document)?);
            let candidates = database
                .conflicts(document, &Object::root(), "title")?
                .ok_or("expected conflicts")?;
            assert!(
                candidates.iter().any(
                    |candidate| candidate.value == Item::Scalar(Scalar::String("rival".into()))
                )
            );
        }
        drop(first);
        drop(second);
        fs::remove_file(first_path)?;
        fs::remove_file(second_path)?;
        Ok(())
    }

    #[test]
    fn restart_between_delete_commit_and_sync_preserves_tombstone()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path();
        let peer_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let replica = ReplicaId::new();
        {
            let database = Database::open(&db_path, scope, replica)?;
            database.edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
            )?;
            database.delete(document, OperationId::new(), OperationDigest::new([2; 32]))?;
        }
        let database = Database::open(&db_path, scope, replica)?;
        assert!(database.is_deleted(document)?);
        assert!(
            database
                .read(document, |reader| reader.get(&Object::root(), "title"))?
                .is_none()
        );
        let pending = database.pending_changes(0, 10)?;
        assert_eq!(pending.len(), 2);
        let target = Database::open(&peer_path, scope, ReplicaId::new())?;
        target.install_remote_page(peer, &remote_page(&pending), b"checkpoint-restart")?;
        assert!(
            target
                .read(document, |reader| reader.get(&Object::root(), "title"))?
                .is_none()
        );
        assert!(target.is_deleted(document)?);
        drop(database);
        drop(target);
        fs::remove_file(db_path)?;
        fs::remove_file(peer_path)?;
        Ok(())
    }

    #[test]
    fn synchronization_carries_tombstones_through_push_and_checkpoint()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let document = DocumentId::new();
        let database = Database::open(&db_path, scope, ReplicaId::new())?;
        database.edit(
            document,
            OperationId::new(),
            OperationDigest::new([1; 32]),
            |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
        )?;
        database.delete(document, OperationId::new(), OperationDigest::new([2; 32]))?;
        let report = lgwks_std::task::block_on(database.synchronize(
            peer,
            &mut crate::test_helpers::AcceptingTransport,
            SyncLimits::default(),
            || false,
        ))?;
        assert_eq!(report.pushed, 2);
        assert_eq!(report.pulled, 0);
        assert!(database.pending_changes(0, 10)?.is_empty());
        assert_eq!(ReplicaPort::checkpoint(&database, peer)?, Some(vec![9]));
        assert!(
            database
                .read(document, |reader| reader.get(&Object::root(), "title"))?
                .is_none()
        );
        assert!(database.is_deleted(document)?);
        drop(database);
        fs::remove_file(db_path)?;
        Ok(())
    }
}
