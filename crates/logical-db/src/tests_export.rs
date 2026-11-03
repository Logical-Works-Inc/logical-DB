//! Export and restore tests; split from the facade export module.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::fs;
use std::path::{Path, PathBuf};

use lgwks_std::hash::blake3;
use logical_db_crdt::{Item, Object, Scalar};
use logical_db_model::{
    AccountId, DocumentId, IndexId, IndexValue, OperationDigest, OperationId, PeerId, ReplicaId,
    Scope, SpaceId,
};
use logical_db_store::{ExportEntry, MAX_EXPORT_BYTES, StoreError};

use super::{Database, DatabaseError, DeleteOutcome, RestoreReport};
use crate::test_helpers::{
    RestorePrefix, kind_projection, path, pending_identity, read_kind, remote_page,
    replay_restore_prefix,
};
use crate::tests_schema::TestError;

/// Opens one export/restore fixture pair with a fresh scope.
fn export_fixture() -> (PathBuf, PathBuf, Scope) {
    (path(), path(), Scope::new(AccountId::new(), SpaceId::new()))
}

/// Opens one projected restore target over an existing scope and index.
fn open_projected_target(
    target_path: &Path,
    scope: Scope,
    index: IndexId,
) -> Result<Database, TestError> {
    Ok(Database::open_with_projections(
        target_path,
        scope,
        ReplicaId::new(),
        vec![kind_projection(index)],
    )?)
}

/// Seeds one `title` row and exports the replica.
fn seed_title_export(source: &Database, document: DocumentId) -> Result<Vec<u8>, TestError> {
    source.edit(
        document,
        OperationId::new(),
        OperationDigest::new([1; 32]),
        |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
    )?;
    Ok(source.export()?)
}

/// Opens a live replica seeded with one `live` title row.
fn seed_live_title(target_path: &Path, scope: Scope) -> Result<(Database, DocumentId), TestError> {
    let live = Database::open(target_path, scope, ReplicaId::new())?;
    let live_document = DocumentId::new();
    live.edit(
        live_document,
        OperationId::new(),
        OperationDigest::new([9; 32]),
        |editor| editor.put(&Object::root(), "title", Scalar::String("live".into())),
    )?;
    Ok((live, live_document))
}

#[test]
fn export_then_restore_reproduces_reads_indexes_and_cursors_exactly() -> Result<(), TestError> {
    let (source_path, target_path, scope) = export_fixture();
    let index = IndexId::new();
    let first = DocumentId::new();
    let second = DocumentId::new();
    let third = DocumentId::new();
    let source = open_projected_target(&source_path, scope, index)?;
    for (document, request, kind) in [
        (first, [1; 32], "note"),
        (second, [2; 32], "note"),
        (third, [3; 32], "task"),
        (first, [4; 32], "task"),
    ] {
        source.edit(
            document,
            OperationId::new(),
            OperationDigest::new(request),
            |editor| editor.put(&Object::root(), "kind", Scalar::String(kind.into())),
        )?;
    }
    source.delete(second, OperationId::new(), OperationDigest::new([5; 32]))?;
    let source_events = source.changes_since(0, 256)?;
    assert_eq!(source_events.len(), 5);
    assert!(
        source_events
            .windows(2)
            .all(|pair| pair[0].sequence() + 1 == pair[1].sequence())
    );
    let source_pending = source.pending_changes(0, 256)?;
    assert_eq!(source_pending.len(), 5);
    let payload = source.export()?;
    assert!(!payload.is_empty());
    assert!(payload.len() <= MAX_EXPORT_BYTES);
    drop(source);

    let target = open_projected_target(&target_path, scope, index)?;
    let report = target.restore(&payload)?;
    assert_eq!(
        report,
        RestoreReport {
            documents: 3,
            entries: 5,
            applied: 5,
            resumed: false,
        }
    );
    assert_eq!(read_kind(&target, first).as_deref(), Some("task"));
    assert_eq!(read_kind(&target, third).as_deref(), Some("task"));
    assert_eq!(read_kind(&target, second), None);
    assert!(target.is_deleted(second)?);
    assert!(!target.is_deleted(first)?);
    assert!(!target.is_deleted(third)?);
    let mut tasks = target.query_index(index, &IndexValue::Text("task".into()), None, 10)?;
    tasks.sort();
    let mut expected_tasks = vec![first, third];
    expected_tasks.sort();
    assert_eq!(tasks, expected_tasks);
    assert!(
        target
            .query_index(index, &IndexValue::Text("note".into()), None, 10)?
            .is_empty()
    );
    assert_eq!(target.changes_since(0, 256)?, source_events);
    let restored_pending = target.pending_changes(0, 256)?;
    assert_eq!(
        pending_identity(&restored_pending),
        pending_identity(&source_pending)
    );
    assert!(
        restored_pending
            .iter()
            .all(|change| change.replica == target.replica())
    );
    assert_eq!(target.restore_cursor()?, None);
    drop(target);
    fs::remove_file(source_path)?;
    fs::remove_file(target_path)?;
    Ok(())
}

#[test]
fn interrupted_restore_resumes_to_completion_on_retry() -> Result<(), TestError> {
    /// PREFIX
    const PREFIX: usize = 2;
    let (source_path, target_path, scope) = export_fixture();
    let index = IndexId::new();
    let first = DocumentId::new();
    let second = DocumentId::new();
    let source = open_projected_target(&source_path, scope, index)?;
    for (document, request, kind) in [
        (first, [1; 32], "note"),
        (second, [2; 32], "task"),
        (first, [3; 32], "task"),
        (second, [4; 32], "note"),
    ] {
        source.edit(
            document,
            OperationId::new(),
            OperationDigest::new(request),
            |editor| editor.put(&Object::root(), "kind", Scalar::String(kind.into())),
        )?;
    }
    let payload = source.export()?;
    let source_pending = source.pending_changes(0, 256)?;
    let source_events = source.changes_since(0, 256)?;
    drop(source);
    let identity: [u8; 32] = *blake3(&payload).as_bytes();

    let replica = ReplicaId::new();
    drop(Database::open_with_projections(
        &target_path,
        scope,
        replica,
        vec![kind_projection(index)],
    )?);
    let replayed = replay_restore_prefix(&RestorePrefix {
        target_path: &target_path,
        replica,
        scope,
        index,
        payload: &payload,
        identity: &identity,
        prefix: PREFIX,
        first,
    })?;
    assert_eq!(replayed, 4);
    let target = Database::open_with_projections(
        &target_path,
        scope,
        replica,
        vec![kind_projection(index)],
    )?;
    let cursor = target.restore_cursor()?.ok_or("expected restore cursor")?;
    assert_eq!(cursor.next, u64::try_from(PREFIX)?);
    let report = target.restore(&payload)?;
    assert_eq!(
        report,
        RestoreReport {
            documents: 2,
            entries: 4,
            applied: 2,
            resumed: true,
        }
    );
    assert_eq!(read_kind(&target, first).as_deref(), Some("task"));
    assert_eq!(read_kind(&target, second).as_deref(), Some("note"));
    let restored_pending = target.pending_changes(0, 256)?;
    assert_eq!(
        pending_identity(&restored_pending),
        pending_identity(&source_pending)
    );
    assert!(
        restored_pending
            .iter()
            .all(|change| change.replica == target.replica())
    );
    assert_eq!(target.changes_since(0, 256)?, source_events);
    assert_eq!(target.restore_cursor()?, None);
    drop(target);
    fs::remove_file(source_path)?;
    fs::remove_file(target_path)?;
    Ok(())
}

#[test]
fn export_replays_synchronized_changes_with_synthesized_bindings() -> Result<(), TestError> {
    use logical_db_store::restore_operation;

    let peer_path = path();
    let source_path = path();
    let target_path = path();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer_id = PeerId::new();
    let index = IndexId::new();
    let remote_edited = DocumentId::new();
    let remote_deleted = DocumentId::new();
    let local = DocumentId::new();
    let peer = Database::open_with_projections(
        &peer_path,
        scope,
        ReplicaId::new(),
        vec![kind_projection(index)],
    )?;
    peer.edit(
        remote_edited,
        OperationId::new(),
        OperationDigest::new([1; 32]),
        |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
    )?;
    peer.edit(
        remote_deleted,
        OperationId::new(),
        OperationDigest::new([2; 32]),
        |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
    )?;
    peer.delete(
        remote_deleted,
        OperationId::new(),
        OperationDigest::new([3; 32]),
    )?;
    let peer_pending = peer.pending_changes(0, 256)?;
    assert_eq!(peer_pending.len(), 3);
    drop(peer);

    let source = open_projected_target(&source_path, scope, index)?;
    source.edit(
        local,
        OperationId::new(),
        OperationDigest::new([4; 32]),
        |editor| editor.put(&Object::root(), "kind", Scalar::String("task".into())),
    )?;
    source.install_remote_page(peer_id, &remote_page(&peer_pending), b"checkpoint-remote")?;
    assert!(source.is_deleted(remote_deleted)?);
    let source_events = source.changes_since(0, 256)?;
    assert_eq!(source_events.len(), 4);
    let payload = source.export()?;
    let source_local_pending = source.pending_changes(0, 256)?;
    assert_eq!(source_local_pending.len(), 1);
    drop(source);

    let target = open_projected_target(&target_path, scope, index)?;
    let report = target.restore(&payload)?;
    assert_eq!(report.documents, 3);
    assert_eq!(report.entries, 4);
    assert_eq!(report.applied, 4);
    assert!(!report.resumed);
    assert_eq!(read_kind(&target, remote_edited).as_deref(), Some("note"));
    assert_eq!(read_kind(&target, local).as_deref(), Some("task"));
    assert_eq!(read_kind(&target, remote_deleted), None);
    assert!(target.is_deleted(remote_deleted)?);
    assert_eq!(target.changes_since(0, 256)?, source_events);
    let restored_pending = target.pending_changes(0, 256)?;
    assert_eq!(restored_pending.len(), 4);
    assert_eq!(
        pending_identity(&restored_pending[..1]),
        pending_identity(&source_local_pending)
    );
    for change in &restored_pending[1..] {
        let (operation, request) = restore_operation(change.document, change.digest)?;
        assert_eq!(change.operation, operation);
        assert_eq!(change.request, request);
    }
    assert_eq!(target.restore_cursor()?, None);
    drop(target);
    fs::remove_file(peer_path)?;
    fs::remove_file(source_path)?;
    fs::remove_file(target_path)?;
    Ok(())
}

#[test]
fn corrupt_backup_is_refused_with_live_state_untouched() -> Result<(), TestError> {
    use logical_db_store::{decode_export, encode_export};

    let source_path = path();
    let target_path = path();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let source = Database::open(&source_path, scope, ReplicaId::new())?;
    let document = DocumentId::new();
    let payload = seed_title_export(&source, document)?;
    drop(source);

    let (live, live_document) = seed_live_title(&target_path, scope)?;
    let live_pending = live.pending_changes(0, 256)?;
    let live_events = live.changes_since(0, 256)?;
    let live_title = live.read(live_document, |reader| reader.get(&Object::root(), "title"))?;

    let (backup_scope, mut entries, documents) = decode_export(&payload)?;
    let tampered = entries[0].clone();
    entries[0] = ExportEntry::new(
        tampered.document,
        tampered.operation,
        tampered.request,
        tampered.digest,
        vec![0xFF],
    );
    let digest_damaged = encode_export(backup_scope, &entries, &documents)?;
    assert!(matches!(
        live.restore(&digest_damaged),
        Err(DatabaseError::Crdt(_))
    ));

    let mut truncated = payload.clone();
    truncated.pop();
    assert!(matches!(
        live.restore(&truncated),
        Err(DatabaseError::Store(_))
    ));

    let mut magic_damaged = payload;
    magic_damaged[0] ^= 0xff;
    assert!(matches!(
        live.restore(&magic_damaged),
        Err(DatabaseError::Store(_))
    ));

    assert_eq!(
        live.read(live_document, |reader| {
            reader.get(&Object::root(), "title")
        })?,
        live_title
    );
    assert_eq!(live.pending_changes(0, 256)?, live_pending);
    assert_eq!(live.changes_since(0, 256)?, live_events);
    assert_eq!(live.restore_cursor()?, None);
    drop(live);
    fs::remove_file(source_path)?;
    fs::remove_file(target_path)?;
    Ok(())
}

#[test]
fn oversized_and_unknown_version_payloads_refused_before_allocation() -> Result<(), TestError> {
    let source_path = path();
    let target_path = path();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let source = Database::open(&source_path, scope, ReplicaId::new())?;
    let payload = seed_title_export(&source, DocumentId::new())?;
    drop(source);

    let (live, live_document) = seed_live_title(&target_path, scope)?;
    let live_pending = live.pending_changes(0, 256)?;

    let oversized = vec![0xA5_u8; MAX_EXPORT_BYTES + 1];
    assert!(matches!(
        live.restore(&oversized),
        Err(DatabaseError::Store(StoreError::InvalidExportSize(_)))
    ));
    let mut future = payload;
    future[4] = 2;
    assert!(matches!(
        live.restore(&future),
        Err(DatabaseError::Store(StoreError::UnknownExportVersion(2)))
    ));
    assert!(matches!(live.restore(&[]), Err(DatabaseError::Store(_))));

    assert_eq!(live.pending_changes(0, 256)?, live_pending);
    assert!(
        live.read(live_document, |reader| {
            reader.get(&Object::root(), "title")
        })?
        .is_some()
    );
    assert_eq!(live.restore_cursor()?, None);
    drop(live);
    fs::remove_file(source_path)?;
    fs::remove_file(target_path)?;
    Ok(())
}

#[test]
fn export_includes_tombstones_and_restored_state_stays_converged() -> Result<(), TestError> {
    use logical_db_store::decode_export;

    let source_path = path();
    let target_path = path();
    let converge_path = path();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let document = DocumentId::new();
    let source = Database::open(&source_path, scope, ReplicaId::new())?;
    source.edit(
        document,
        OperationId::new(),
        OperationDigest::new([1; 32]),
        |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
    )?;
    let applied = source.delete(document, OperationId::new(), OperationDigest::new([2; 32]))?;
    let DeleteOutcome::Applied(receipt) = applied else {
        return Err(std::io::Error::other("delete must apply").into());
    };
    let payload = source.export()?;
    let (_, entries, documents) = decode_export(&payload)?;
    assert_eq!(entries.len(), 2);
    assert_eq!(documents.len(), 1);
    assert_eq!(documents[0].document, document);
    assert!(documents[0].tombstone);
    assert!(entries.iter().any(|entry| entry.digest == receipt.digest));
    drop(source);

    let target = Database::open(&target_path, scope, ReplicaId::new())?;
    let report = target.restore(&payload)?;
    assert_eq!(report.documents, 1);
    assert_eq!(report.entries, 2);
    assert!(target.is_deleted(document)?);
    assert!(
        target
            .read(document, |reader| reader.get(&Object::root(), "title"))?
            .is_none()
    );
    let candidates = target
        .conflicts(document, &Object::root(), "title")?
        .ok_or("expected conflicts")?;
    assert_eq!(candidates.len(), 1);
    assert_eq!(
        candidates[0].value,
        Item::Scalar(Scalar::String("v1".into()))
    );

    let converged = Database::open(&converge_path, scope, ReplicaId::new())?;
    let pending = target.pending_changes(0, 10)?;
    converged.install_remote_page(peer, &remote_page(&pending), b"checkpoint-converged")?;
    assert!(
        converged
            .read(document, |reader| reader.get(&Object::root(), "title"))?
            .is_none()
    );
    assert!(converged.is_deleted(document)?);
    drop(target);
    drop(converged);
    fs::remove_file(source_path)?;
    fs::remove_file(target_path)?;
    fs::remove_file(converge_path)?;
    Ok(())
}
