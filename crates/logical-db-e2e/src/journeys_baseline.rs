//! Baseline integrity journeys: tombstones, corruption, merges, backlogs.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

use std::{fs, sync::Arc};

use logical_db::{
    AccountId, ChangeEvent, Database, DatabaseError, DatabaseErrorKind, DocumentId, Object,
    OperationDigest, OperationId, PeerId, RemoteChange, ReplicaId, Scalar, Scope, SpaceId,
    SyncLimits,
};
use logical_db_model::SnapshotArtifact;
use logical_db_service::{RetentionStore, ServiceStore};

use super::harness::{
    JourneyError, pair_case, path, seal_consume_and_prune_through, serve_test_router, test_runtime,
};
use crate::support::MemoryServiceStore;

/// Seeds one synced integer state row shared by the merge and backlog bodies.
async fn seed_synced_integer(
    victim: &Database,
    peer: PeerId,
    transport: &mut logical_db_http::HttpTransport<logical_db_http::StaticBearer>,
    document: DocumentId,
) -> Result<(), Box<dyn std::error::Error>> {
    victim.edit(
        document,
        OperationId::new(),
        OperationDigest::new([0x81; 32]),
        |editor| editor.put(&Object::root(), "state", Scalar::Integer(1)),
    )?;
    victim
        .synchronize(peer, transport, SyncLimits::default(), || false)
        .await?;
    Ok(())
}

/// One opened recovery victim with its scope, document, peer, paths and handle.
type OpenedVictim = (
    Scope,
    DocumentId,
    PeerId,
    std::path::PathBuf,
    ReplicaId,
    Database,
);

/// Opens one recovery victim replica with a fresh scope and peer.
fn open_victim(label: &str) -> Result<OpenedVictim, JourneyError> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let peer = PeerId::new();
    let victim_path = path(label);
    let replica = ReplicaId::new();
    let victim = Database::open(&victim_path, scope, replica)?;
    Ok((scope, document, peer, victim_path, replica, victim))
}

/// Opens one recovery victim replica with a fresh scope and peer.

#[test]
fn tombstone_survives_baseline_recovery_and_cannot_resurrect() -> Result<(), JourneyError> {
    test_runtime()?.block_on(tombstone_baseline_recovery_body())
}

async fn tombstone_baseline_recovery_body() -> Result<(), JourneyError> {
    let store = Arc::new(MemoryServiceStore::default());
    let (mut transport, mut server_tasks) = serve_test_router(Arc::clone(&store), [24; 32], 60_000)
        .await
        .map_err(JourneyError::Harness)?;

    let (scope, document, peer, first_path, second_path) = pair_case("tombstone");

    let first = Database::open(&first_path, scope, ReplicaId::new())?;
    first.edit(
        document,
        OperationId::new(),
        OperationDigest::new([0x61; 32]),
        |editor| editor.put(&Object::root(), "state", Scalar::String("v1".into())),
    )?;
    // The edit below the delete is the resurrection candidate: once the
    // recovered baseline carries the tombstone it must never bring the
    // document back, even if the service re-delivers that older change.
    let pre_floor = first.pending_changes(0, 10)?;
    assert_eq!(pre_floor.len(), 1);
    first.delete(
        document,
        OperationId::new(),
        OperationDigest::new([0x62; 32]),
    )?;
    first
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    assert_eq!(store.frontier(scope).await?, 2);

    seal_consume_and_prune_through(&store, scope, 2)
        .await
        .map_err(JourneyError::Harness)?;

    let second = Database::open(&second_path, scope, ReplicaId::new())?;
    let report = second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    assert!(report.recovered);
    assert!(second.is_deleted(document)?);
    assert!(
        second
            .read(document, |reader| reader.get(&Object::root(), "state"))?
            .is_none()
    );

    let resurrection = pre_floor
        .iter()
        .map(|change| RemoteChange::new(change.document, change.digest, change.bytes().to_vec()))
        .collect::<Vec<_>>();
    second.install_remote_page(peer, &resurrection, b"resurrection-attempt")?;
    assert!(second.is_deleted(document)?);
    assert!(
        second
            .read(document, |reader| reader.get(&Object::root(), "state"))?
            .is_none()
    );

    drop((first, second));
    fs::remove_file(first_path)?;
    fs::remove_file(second_path)?;
    server_tasks.abort_all();
    Ok(())
}

#[test]
fn corrupt_or_incomplete_baseline_leaves_state_and_checkpoint_unchanged() -> Result<(), JourneyError>
{
    test_runtime()?.block_on(corrupt_baseline_refusal_body())
}

async fn corrupt_baseline_refusal_body() -> Result<(), JourneyError> {
    let store = Arc::new(MemoryServiceStore::default());
    let (mut transport, mut server_tasks) = serve_test_router(Arc::clone(&store), [25; 32], 60_000)
        .await
        .map_err(JourneyError::Harness)?;

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let peer = PeerId::new();
    let victim_path = path("corrupt-victim");

    let victim = Database::open(&victim_path, scope, ReplicaId::new())?;
    victim.edit(
        document,
        OperationId::new(),
        OperationDigest::new([0x71; 32]),
        |editor| editor.put(&Object::root(), "state", Scalar::String("keep".into())),
    )?;
    victim
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    let checkpoint_before = victim
        .checkpoint(peer)?
        .ok_or("a synchronized replica must hold a checkpoint")?;
    assert!(!victim.is_deleted(document)?);
    assert_eq!(
        victim.read(document, |reader| reader.get(&Object::root(), "state"))?,
        Some(Some(logical_db::Item::Scalar(Scalar::String(
            "keep".into()
        ))))
    );

    let genuine = store.build_snapshot(scope, 1).await?;
    assert_eq!(genuine.documents.len(), 1);

    let mut substituted = genuine.clone();
    substituted.documents[0].snapshot = vec![0xEE; substituted.documents[0].snapshot.len()];

    let mut truncated = genuine.clone();
    truncated.documents.pop();

    let mut mismatched = genuine.clone();
    mismatched.documents[0].tombstone = !mismatched.documents[0].tombstone;
    // Recomputing the descriptor keeps the artifact internally consistent,
    // so only the tombstone-versus-document check can refuse it.
    let mismatched =
        SnapshotArtifact::new(scope, genuine.baseline.position(), mismatched.documents)?;

    for (label, artifact) in [
        ("substituted snapshot payload", substituted),
        ("truncated document list", truncated),
        ("mismatched tombstone flag", mismatched),
    ] {
        let Err(error) = victim.install_baseline(peer, &artifact, b"corrupt-checkpoint") else {
            return Err("a corrupt checkpoint must be refused".into());
        };
        assert!(
            matches!(
                &error,
                DatabaseError::InvalidBaseline | DatabaseError::Model(_)
            ),
            "{label} must be refused with a typed failure, got {error:?}"
        );
        assert_eq!(error.kind(), DatabaseErrorKind::Other, "{label} kind");
        assert_eq!(
            victim.checkpoint(peer)?.as_deref(),
            Some(checkpoint_before.as_slice()),
            "{label} must leave the checkpoint byte-for-byte unchanged"
        );
        assert!(
            !victim.is_deleted(document)?,
            "{label} must not tombstone the existing document"
        );
        assert_eq!(
            victim.read(document, |reader| reader.get(&Object::root(), "state"))?,
            Some(Some(logical_db::Item::Scalar(Scalar::String(
                "keep".into()
            )))),
            "{label} must leave the readable state unchanged"
        );
    }

    drop(victim);
    fs::remove_file(victim_path)?;
    server_tasks.abort_all();
    Ok(())
}

#[test]
fn recovery_merges_a_local_edit_the_baseline_predates() -> Result<(), JourneyError> {
    test_runtime()?.block_on(recovery_merges_a_local_edit_body())
}

async fn recovery_merges_a_local_edit_body() -> Result<(), JourneyError> {
    let store = Arc::new(MemoryServiceStore::default());
    let (mut transport, mut server_tasks) = serve_test_router(Arc::clone(&store), [26; 32], 60_000)
        .await
        .map_err(JourneyError::Harness)?;

    let (scope, document, peer, victim_path, replica, victim) = open_victim("merge-victim")?;

    seed_synced_integer(&victim, peer, &mut transport, document)
        .await
        .map_err(JourneyError::Harness)?;

    let baseline = store.build_snapshot(scope, 1).await?;
    assert_eq!(baseline.documents.len(), 1);

    let local_operation = OperationId::new();
    victim.edit(
        document,
        local_operation,
        OperationDigest::new([0x82; 32]),
        |editor| editor.put(&Object::root(), "state", Scalar::Integer(2)),
    )?;
    assert_eq!(victim.pending_changes(0, 10)?.len(), 1);

    victim.install_baseline(peer, &baseline, b"merge-checkpoint")?;

    assert_eq!(
        victim.read(document, |reader| reader.get(&Object::root(), "state"))?,
        Some(Some(logical_db::Item::Scalar(Scalar::Integer(2)))),
        "the baseline predates the local edit, so the merged value must keep it"
    );
    let pending = victim.pending_changes(0, 10)?;
    assert_eq!(
        pending.len(),
        1,
        "recovery must not consume the local outbox"
    );
    assert_eq!(pending[0].operation, local_operation);
    drop(victim);

    let reopened = Database::open(&victim_path, scope, replica)?;
    assert_eq!(
        reopened.read(document, |reader| reader.get(&Object::root(), "state"))?,
        Some(Some(logical_db::Item::Scalar(Scalar::Integer(2)))),
        "the merged local edit must survive reopen"
    );
    drop(reopened);
    fs::remove_file(victim_path)?;
    server_tasks.abort_all();
    Ok(())
}

#[test]
fn recovery_preserves_a_local_backlog_larger_than_one_upload_batch() -> Result<(), JourneyError> {
    test_runtime()?.block_on(recovery_preserves_a_large_backlog_body())
}

// One linear fill/drain narrative: splitting it would hide the backlog depth
// that the test exists to prove. Kept whole rather than suppressed, because
// `clippy::too_many_lines` no longer fires here and an unfulfilled `expect`
// is itself a defect.
async fn recovery_preserves_a_large_backlog_body() -> Result<(), JourneyError> {
    let store = Arc::new(MemoryServiceStore::default());
    let (mut transport, mut server_tasks) = serve_test_router(Arc::clone(&store), [26; 32], 60_000)
        .await
        .map_err(JourneyError::Harness)?;

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let replica = ReplicaId::new();
    let backlog_path = path("backlog-victim");
    let victim = Database::open(&backlog_path, scope, replica)?;

    let seeded = DocumentId::new();
    seed_synced_integer(&victim, peer, &mut transport, seeded)
        .await
        .map_err(JourneyError::Harness)?;
    let baseline = store.build_snapshot(scope, 1).await?;

    let batch = usize::from(SyncLimits::default().changes_per_page);
    let backlog = batch + 2;
    let mut documents = Vec::with_capacity(backlog);
    let mut operations = Vec::with_capacity(backlog);
    for index in 0..backlog {
        let document = DocumentId::new();
        let operation = OperationId::new();
        let mut digest = [0u8; 32];
        digest[0] = 0x90;
        digest[1] = u8::try_from(index & 0xff)?;
        digest[2] = u8::try_from((index >> 8) & 0xff)?;
        let step = i64::try_from(index)?;
        let sequence_value = step.saturating_add(10);
        victim.edit(
            document,
            operation,
            OperationDigest::new(digest),
            |editor| editor.put(&Object::root(), "state", Scalar::Integer(sequence_value)),
        )?;
        documents.push(document);
        operations.push(operation);
    }

    let before = victim.pending_changes(0, backlog.saturating_add(8))?;
    assert!(
        before.len() > batch,
        "the fixture backlog must exceed one upload batch, saw {} of {batch}",
        before.len()
    );

    victim.install_baseline(peer, &baseline, b"backlog-checkpoint")?;

    let after = victim.pending_changes(0, backlog.saturating_add(8))?;
    assert_eq!(
        after.len(),
        backlog,
        "recovery must keep every unsent change in a multi-batch backlog"
    );
    let kept: Vec<_> = after.iter().map(|change| change.operation).collect();
    assert_eq!(kept, operations, "the backlog must keep its exact order");
    for (index, document) in documents.iter().enumerate() {
        assert_eq!(
            victim.read(*document, |reader| reader.get(&Object::root(), "state"))?,
            Some(Some(logical_db::Item::Scalar(Scalar::Integer(
                i64::try_from(index)?.saturating_add(10)
            )))),
            "local change {index} of a multi-batch backlog must survive recovery"
        );
    }
    assert_eq!(
        victim.read(seeded, |reader| reader.get(&Object::root(), "state"))?,
        Some(Some(logical_db::Item::Scalar(Scalar::Integer(1)))),
        "the acknowledged seed change must not regress"
    );
    drop(victim);
    let reopened = Database::open(&backlog_path, scope, replica)?;
    assert_eq!(
        reopened
            .pending_changes(0, backlog.saturating_add(8))?
            .len(),
        backlog,
        "the preserved backlog must be durable, not in-memory only"
    );
    drop(reopened);
    fs::remove_file(backlog_path)?;
    server_tasks.abort_all();
    Ok(())
}

#[test]
fn recovery_publishes_a_durable_invalidation_for_a_held_cursor() -> Result<(), JourneyError> {
    test_runtime()?.block_on(recovery_publishes_a_durable_invalidation_body())
}

async fn recovery_publishes_a_durable_invalidation_body() -> Result<(), JourneyError> {
    let store = Arc::new(MemoryServiceStore::default());
    let (mut transport, mut server_tasks) = serve_test_router(Arc::clone(&store), [27; 32], 60_000)
        .await
        .map_err(JourneyError::Harness)?;

    let (scope, document, peer, victim_path, replica, victim) = open_victim("invalidate-victim")?;

    victim.edit(
        document,
        OperationId::new(),
        OperationDigest::new([0x91; 32]),
        |editor| editor.put(&Object::root(), "state", Scalar::Integer(1)),
    )?;
    victim
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;

    // A subscriber holds a cursor immediately before recovery.
    let held = victim.changes_since(0, 256)?;
    let cursor = held
        .last()
        .ok_or("the held cursor must observe at least one change")?
        .sequence();
    assert_eq!(victim.baseline_generation()?, 0);

    let baseline = store.build_snapshot(scope, 1).await?;
    victim.install_baseline(peer, &baseline, b"invalidate-checkpoint")?;

    // The install carries no change of its own, so nothing in the
    // pre-recovery stream reveals it; the committed invalidation
    // must still be exposed to the held cursor and be durable.
    assert_eq!(victim.baseline_generation()?, 1);
    let page = victim.changes_since(cursor, 256)?;
    assert_eq!(page.len(), 1, "the baseline install must be visible");
    assert!(matches!(
        page[0],
        ChangeEvent::Baseline { generation: 1, .. }
    ));
    assert!(page[0].digest().is_none());
    drop(victim);

    let reopened = Database::open(&victim_path, scope, replica)?;
    assert_eq!(reopened.baseline_generation()?, 1);
    let replayed = reopened.changes_since(cursor, 256)?;
    assert!(
        matches!(
            replayed.as_slice(),
            [ChangeEvent::Baseline { generation: 1, .. }]
        ),
        "the invalidation must survive reopen"
    );
    // The consumer rebuilds its view from the recovered state.
    assert_eq!(
        reopened.read(document, |reader| reader.get(&Object::root(), "state"))?,
        Some(Some(logical_db::Item::Scalar(Scalar::Integer(1))))
    );
    // A recovered replica's backup must round-trip through restore
    // even though its baseline marker carries no change of its own.
    let exported = reopened.export()?;
    drop(reopened);
    fs::remove_file(victim_path)?;

    let restored_path = path("invalidate-restored");
    let restored = Database::open(&restored_path, scope, ReplicaId::new())?;
    restored.restore(&exported)?;
    assert_eq!(
        restored.read(document, |reader| reader.get(&Object::root(), "state"))?,
        Some(Some(logical_db::Item::Scalar(Scalar::Integer(1))))
    );
    drop(restored);
    fs::remove_file(restored_path)?;
    server_tasks.abort_all();

    Ok(())
}
