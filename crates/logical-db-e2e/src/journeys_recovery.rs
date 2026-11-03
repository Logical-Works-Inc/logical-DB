//! Recovery journeys: pruned documents and multi-page baselines.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

use std::{fs, sync::Arc};

use logical_db::{
    AccountId, BatchEdit, Database, DocumentId, Object, OperationDigest, OperationId, PeerId,
    ReplicaId, Scalar, Scope, SpaceId, SyncLimits,
};
use logical_db_http::{HttpTransport, StaticBearer};
use logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS;
use logical_db_service::ServiceStore;

use super::harness::{
    JourneyError, pair_case, path, seal_consume_and_prune, seal_consume_and_prune_through,
    serve_test_router, test_runtime,
};
use crate::support::MemoryServiceStore;

#[test]
fn fresh_replica_recovers_a_pruned_document_through_real_http() -> Result<(), JourneyError> {
    test_runtime()?.block_on(fresh_replica_recovers_a_pruned_document_body())
}

async fn fresh_replica_recovers_a_pruned_document_body() -> Result<(), JourneyError> {
    let store = Arc::new(MemoryServiceStore::default());
    let (mut transport, mut server_tasks) = serve_test_router(Arc::clone(&store), [23; 32], 60_000)
        .await
        .map_err(JourneyError::Harness)?;

    let (scope, document, peer, first_path, second_path) = pair_case("recovery");

    let first = Database::open(&first_path, scope, ReplicaId::new())?;
    first.edit(
        document,
        OperationId::new(),
        OperationDigest::new([0x51; 32]),
        |editor| editor.put(&Object::root(), "state", Scalar::String("sealed".into())),
    )?;
    first
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    assert_eq!(store.frontier(scope).await?, 1);

    seal_consume_and_prune(&store, scope)
        .await
        .map_err(JourneyError::Harness)?;

    let second = Database::open(&second_path, scope, ReplicaId::new())?;
    let report = second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    assert!(report.recovered);
    assert_eq!(
        second.read(document, |reader| reader.get(&Object::root(), "state"))?,
        Some(Some(logical_db::Item::Scalar(Scalar::String(
            "sealed".into()
        ))))
    );

    drop((first, second));
    fs::remove_file(first_path)?;
    fs::remove_file(second_path)?;
    server_tasks.abort_all();
    Ok(())
}

#[test]
fn a_baseline_larger_than_one_page_recovers_through_real_http() -> Result<(), JourneyError> {
    test_runtime()?.block_on(a_baseline_larger_than_one_page_recovers_body())
}

async fn a_baseline_larger_than_one_page_recovers_body() -> Result<(), JourneyError> {
    let store = Arc::new(MemoryServiceStore::default());
    let (mut transport, mut server_tasks) = serve_test_router(Arc::clone(&store), [25; 32], 60_000)
        .await
        .map_err(JourneyError::Harness)?;

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let first_path = path("paged-recovery-first");
    let second_path = path("paged-recovery-second");
    let total = MAX_BASELINE_PAGE_DOCUMENTS + 44;
    let documents = (0..total).map(|_| DocumentId::new()).collect::<Vec<_>>();
    // The first document of page two is deleted on the seeding replica, so
    // its tombstone crosses the page boundary inside the sealed baseline.
    let tombstoned = documents[MAX_BASELINE_PAGE_DOCUMENTS];

    let first = Database::open(&first_path, scope, ReplicaId::new())?;
    for (batch, chunk) in documents.chunks(150).enumerate() {
        let mut edits = Vec::with_capacity(chunk.len());
        for (offset, document) in chunk.iter().enumerate() {
            let index = batch.saturating_mul(150).saturating_add(offset);
            let value = i64::try_from(index % 1_000)?;
            let digest_byte = u8::try_from(index % 256)?;
            edits.push(BatchEdit::new(
                *document,
                OperationId::new(),
                OperationDigest::new([digest_byte; 32]),
                move |editor| editor.put(&Object::root(), "page", Scalar::Integer(value)),
            ));
        }
        first.edit_many(edits)?;
    }
    first.delete(
        tombstoned,
        OperationId::new(),
        OperationDigest::new([0x7a; 32]),
    )?;
    let seeded = u64::try_from(total + 1)?;
    for _round in 0..8 {
        first
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;
        if store.frontier(scope).await? == seeded {
            break;
        }
    }
    assert_eq!(store.frontier(scope).await?, seeded);

    seal_consume_and_prune_through(&store, scope, seeded)
        .await
        .map_err(JourneyError::Harness)?;

    let second = Database::open(&second_path, scope, ReplicaId::new())?;
    let report = second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    assert!(report.recovered, "a >256-document baseline must recover");
    assert!(
        second.is_deleted(tombstoned)?,
        "a tombstone crossing the page boundary must survive recovery"
    );
    for (index, document) in documents.iter().enumerate() {
        let observed = second.read(*document, |reader| reader.get(&Object::root(), "page"))?;
        if *document == tombstoned {
            assert_eq!(observed, None, "the boundary tombstone must not resurrect");
        } else {
            let expected = i64::try_from(index % 1_000)?;
            assert_eq!(
                observed,
                Some(Some(logical_db::Item::Scalar(Scalar::Integer(expected)))),
                "document {index} must carry its seeded value after paged recovery"
            );
        }
    }

    drop((first, second));
    fs::remove_file(first_path)?;
    fs::remove_file(second_path)?;
    server_tasks.abort_all();
    Ok(())
}

#[test]
fn a_local_edit_landing_between_recovery_pages_survives_the_install() -> Result<(), JourneyError> {
    test_runtime()?.block_on(local_edit_between_pages_body())
}

/// Seeds the recovery fixture: one baseline page plus one document of chunks.
async fn seed_interleave_documents(
    first: &Database,
    total: usize,
) -> Result<Vec<DocumentId>, Box<dyn std::error::Error>> {
    let documents = (0..total).map(|_| DocumentId::new()).collect::<Vec<_>>();
    for (batch, chunk) in documents.chunks(150).enumerate() {
        let mut edits = Vec::with_capacity(chunk.len());
        for (offset, document) in chunk.iter().enumerate() {
            let index = batch.saturating_mul(150).saturating_add(offset);
            let digest_byte = u8::try_from(index % 256)?;
            let step = i64::try_from(index)?;
            edits.push(BatchEdit::new(
                *document,
                OperationId::new(),
                OperationDigest::new([digest_byte; 32]),
                move |editor| editor.put(&Object::root(), "page", Scalar::Integer(step)),
            ));
        }
        first.edit_many(edits)?;
    }
    Ok(documents)
}

/// Uploads the seeded changes until the service frontier covers them.
async fn drain_seeded_uploads(
    first: &Database,
    peer: PeerId,
    transport: &mut HttpTransport<StaticBearer>,
    store: &MemoryServiceStore,
    seeded: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for _round in 0..8 {
        first
            .synchronize(peer, transport, SyncLimits::default(), || false)
            .await?;
        if store.frontier(first.scope()).await? == seeded {
            break;
        }
    }
    assert_eq!(store.frontier(first.scope()).await?, seeded);
    Ok(())
}

async fn local_edit_between_pages_body() -> Result<(), JourneyError> {
    let store = Arc::new(MemoryServiceStore::default());
    let (mut transport, mut server_tasks) = serve_test_router(Arc::clone(&store), [27; 32], 60_000)
        .await
        .map_err(JourneyError::Harness)?;

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let first_path = path("interleave-first");
    let second_path = path("interleave-second");
    let total = MAX_BASELINE_PAGE_DOCUMENTS + 1;

    let first = Database::open(&first_path, scope, ReplicaId::new())?;
    let documents = seed_interleave_documents(&first, total)
        .await
        .map_err(JourneyError::Harness)?;
    let seeded = u64::try_from(total)?;
    drain_seeded_uploads(&first, peer, &mut transport, &store, seeded)
        .await
        .map_err(JourneyError::Harness)?;
    seal_consume_and_prune_through(&store, scope, seeded)
        .await
        .map_err(JourneyError::Harness)?;

    let second = Database::open(&second_path, scope, ReplicaId::new())?;
    let local_document = DocumentId::new();
    let local_operation = OperationId::new();
    let mut calls = 0usize;
    let mut landed = false;
    let mut seam_error: Option<Box<dyn std::error::Error>> = None;
    let report = second
        .synchronize(peer, &mut transport, SyncLimits::default(), || {
            calls = calls.saturating_add(1);
            if calls == 2 {
                match second.read(documents[0], |reader| reader.get(&Object::root(), "page")) {
                    Ok(Some(_)) => {
                        seam_error = Some(
                            "the edit must land after the first page and before any install".into(),
                        );
                    }
                    Ok(None) => {
                        match second.edit(
                            local_document,
                            local_operation,
                            OperationDigest::new([0xb7; 32]),
                            |editor| editor.put(&Object::root(), "local", Scalar::Integer(7)),
                        ) {
                            Ok(_) => {
                                landed = true;
                            }
                            Err(error) => {
                                seam_error = Some(error.into());
                            }
                        }
                    }
                    Err(error) => {
                        seam_error = Some(error.into());
                    }
                }
            }
            false
        })
        .await?;
    if let Some(error) = seam_error {
        return Err(JourneyError::Harness(error));
    }

    assert!(
        landed,
        "the recovery loop must reach its between-pages seam, saw {calls} calls"
    );
    assert!(report.recovered, "the replica must still take the baseline");
    assert_eq!(
        second.read(local_document, |reader| reader
            .get(&Object::root(), "local"))?,
        Some(Some(logical_db::Item::Scalar(Scalar::Integer(7)))),
        "an edit that lands mid-recovery must survive the install"
    );
    for (index, document) in documents.iter().enumerate() {
        assert_eq!(
            second.read(*document, |reader| reader.get(&Object::root(), "page"))?,
            Some(Some(logical_db::Item::Scalar(Scalar::Integer(
                i64::try_from(index)?
            )))),
            "baseline document {index} must still install around a concurrent edit"
        );
    }
    let pending = second.pending_changes(0, 8)?;
    assert!(
        pending
            .iter()
            .any(|change| change.operation == local_operation),
        "the concurrent edit must remain in the outbox for upload"
    );

    drop((first, second));
    fs::remove_file(first_path)?;
    fs::remove_file(second_path)?;
    server_tasks.abort_all();
    Ok(())
}
