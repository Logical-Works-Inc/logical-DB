use std::fs;
use std::path::PathBuf;

use logical_db_crdt::{Item, Object, Scalar};
use logical_db_model::{
    AccountId, DocumentId, IndexId, IndexValue, OperationDigest, OperationId, PeerId, ReplicaId,
    Scope, SpaceId,
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
        first.install_remote_page(peer, &remote_page(&second_changes), b"checkpoint-a")?;
        second.install_remote_page(peer, &remote_page(&first_changes), b"checkpoint-b")?;

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
    let second =
        Database::open_with_projections(&second_path, scope, ReplicaId::new(), vec![projection])?;
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
                |candidate| candidate.value() == &Item::Scalar(Scalar::String("rival".into()))
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
