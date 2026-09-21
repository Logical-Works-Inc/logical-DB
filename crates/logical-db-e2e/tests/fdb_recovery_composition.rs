//! Retained live recovery: public HTTP `/v1/recovery:baseline` over real FDB.
//!
//! A fresh replica that has lost the log prefix is served a state-bearing
//! baseline through the public recovery route and installs it atomically. The
//! store behind the route is the real `FdbServiceStore`, so the journey proves
//! the transport, service, and `FoundationDB` composition together rather than
//! against an in-process double.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![cfg(feature = "fdb")]
#![expect(
    unsafe_code,
    reason = "the FDB network boot is this journey's FFI seam"
)]

#[path = "support/fdb_harness.rs"]
mod fdb_harness;

use std::{
    error::Error,
    fs, io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use fdb_harness::{FixtureAuthenticator, read_floor, read_frontier, seal_consume_and_prune};
use logical_db::{
    AccountId, BatchEdit, Database, DocumentId, Item, Object, OperationDigest, OperationId, PeerId,
    ReplicaId, Scalar, Scope, SpaceId, SyncLimits,
    http::{HttpTransport, StaticBearer, Url},
    transport::{BaselineRequest, BaselineResult, Checkpoint, PullRequest, PullResult, Transport},
};
use logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS;
use logical_db_server::{Clock, router};
use logical_db_service::CheckpointCodec;
use logical_db_store_fdb::{
    FdbServiceStore,
    foundationdb::{Database as FdbDatabase, api::FdbApiBuilder},
    foundationdb_tuple::Subspace,
};

struct RecoveryClock(Arc<AtomicU64>);

impl Clock for RecoveryClock {
    fn now_unix_ms(&self) -> Option<u64> {
        Some(self.0.load(Ordering::SeqCst))
    }
}

/// The composition entry that drives the FDB recovery journey.
#[test]
fn fdb_recovery_composition() -> Result<(), Box<dyn Error>> {
    lgwks_bot::Runtime::new()?.block_on(journey())
}

/// Drives the expired-checkpoint recovery journey against a real FDB cluster.
#[expect(
    clippy::too_many_lines,
    reason = "one ordered end-to-end journey; splitting hides the ordering"
)]
async fn journey() -> Result<(), Box<dyn Error>> {
    let cluster_file = std::env::var("LOGICAL_DB_FDB_CLUSTER_FILE")?;
    #[expect(
        unsafe_code,
        reason = "the FDB network boot is this journey's FFI seam"
    )]
    // SAFETY: this dedicated process boots exactly one FDB network and retains
    // the auto-stop guard beyond every database, store, and future.
    let network = unsafe { FdbApiBuilder::default().build()?.boot()? };
    let http_database = FdbDatabase::new_compat(Some(&cluster_file)).await?;
    let prune_database = FdbDatabase::new_compat(Some(&cluster_file)).await?;
    let root = Subspace::from_bytes(
        format!("logical-db/recovery-composition/{}/", uuid::Uuid::now_v7()).into_bytes(),
    );
    let service_store = FdbServiceStore::new(http_database, root.clone())?;
    let prune_store = FdbServiceStore::new(prune_database, root)?;
    let now = Arc::new(AtomicU64::new(1_000));
    let application = router(
        service_store,
        FixtureAuthenticator,
        CheckpointCodec::new([0x27; 32], 60_000)?,
        RecoveryClock(Arc::clone(&now)),
    );
    let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server =
        lgwks_bot::rt::task::spawn(async move { axum::serve(listener, application).await });
    let endpoint: Url = format!("http://{address}/").parse()?;
    let mut transport = HttpTransport::new(endpoint, StaticBearer::new("recovery-fixture")?)?;

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let run = uuid::Uuid::now_v7();
    let first_path = std::env::temp_dir().join(format!("logical-db-fdb-recovery-first-{run}.redb"));
    let second_path =
        std::env::temp_dir().join(format!("logical-db-fdb-recovery-second-{run}.redb"));
    let expired_path =
        std::env::temp_dir().join(format!("logical-db-fdb-recovery-expired-{run}.redb"));
    let expired_id = ReplicaId::new();
    let expired = Database::open(&expired_path, scope, expired_id)?;
    expired
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    let old_checkpoint = expired
        .checkpoint(peer)?
        .ok_or("initial HTTP pull issues a checkpoint")?;
    let total = MAX_BASELINE_PAGE_DOCUMENTS.saturating_add(44);
    let documents = (0..total).map(|_| DocumentId::new()).collect::<Vec<_>>();
    // The first document of page two is deleted on the seeding replica, so its
    // tombstone crosses the page boundary inside the sealed baseline.
    let tombstoned = documents[MAX_BASELINE_PAGE_DOCUMENTS];

    let first = Database::open(&first_path, scope, ReplicaId::new())?;
    for (batch, chunk) in documents.chunks(150).enumerate() {
        let edits = chunk
            .iter()
            .enumerate()
            .map(|(offset, document)| {
                let index = batch.saturating_mul(150).saturating_add(offset);
                let value = i64::try_from(
                    index
                        .checked_rem(1_000)
                        .ok_or("seed modulus must be non-zero")?,
                )?;
                Ok(BatchEdit::new(
                    *document,
                    OperationId::new(),
                    logical_db::OperationDigest::new(
                        [u8::try_from(
                            index
                                .checked_rem(256)
                                .ok_or("digest modulus must be non-zero")?,
                        )?; 32],
                    ),
                    move |editor| editor.put(&Object::root(), "state", Scalar::Integer(value)),
                ))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        first.edit_many(edits)?;
    }
    first.delete(
        tombstoned,
        OperationId::new(),
        OperationDigest::new([0x7a; 32]),
    )?;
    let seeded_target = u64::try_from(total.saturating_add(1))?;
    for _round in 0..8 {
        first
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;
        if read_frontier(&prune_store, scope).await? == seeded_target {
            break;
        }
    }
    let seeded = read_frontier(&prune_store, scope).await?;
    if seeded != seeded_target {
        return Err(io::Error::other(format!(
            "first replica did not seed every document (frontier={seeded} target={seeded_target})"
        ))
        .into());
    }

    seal_consume_and_prune(&prune_store, scope, seeded_target).await?;

    // Expire a real server-issued checkpoint without sleeping or changing the
    // production codec. This client last pulled before the pruned log existed.
    now.store(61_001, Ordering::SeqCst);
    let pull = PullRequest {
        protocol: logical_db_protocol::PROTOCOL_VERSION,
        scope,
        replica: expired_id,
        request: OperationDigest::new([0x6e; 32]),
        after: Some(Checkpoint::from_wire(old_checkpoint.clone())?),
        max_changes: 1,
        max_bytes: 1_048_576,
    };
    assert!(
        matches!(
            transport.pull(pull.clone()).await?,
            PullResult::BaselineRequired
        ),
        "an expired checkpoint pull must demand a baseline"
    );
    let recovered = expired
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    assert!(
        recovered.recovered,
        "expired checkpoint must install the baseline"
    );
    let replacement = expired
        .checkpoint(peer)?
        .ok_or("recovery persists a checkpoint")?;
    assert_ne!(
        replacement, old_checkpoint,
        "a recovery must publish a fresh checkpoint id"
    );
    drop(expired);
    let expired = Database::open(&expired_path, scope, expired_id)?;
    assert_eq!(
        expired.checkpoint(peer)?,
        Some(replacement.clone()),
        "the reopened store must serve the recovered checkpoint"
    );
    assert!(
        matches!(
            transport
                .pull(PullRequest {
                    after: Some(Checkpoint::from_wire(replacement)?),
                    ..pull
                })
                .await?,
            PullResult::Page(_)
        ),
        "the replacement checkpoint must serve changes again"
    );

    let second = Database::open(&second_path, scope, ReplicaId::new())?;
    let report = second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    if !report.recovered {
        return Err(io::Error::other(
            "fresh replica did not require recovery (no baseline installed)",
        )
        .into());
    }
    if !second.is_deleted(tombstoned)? {
        return Err(io::Error::other("recovered replica lost a boundary tombstone").into());
    }
    assert!(
        expired.is_deleted(tombstoned)?,
        "expired replica must keep the boundary tombstone"
    );
    for (index, document) in documents.iter().enumerate() {
        let observed = second.read(*document, |reader| reader.get(&Object::root(), "state"))?;
        assert_eq!(
            expired.read(*document, |reader| reader.get(&Object::root(), "state"))?,
            observed,
            "expired and fresh replicas disagree at document {index}"
        );
        if *document == tombstoned {
            if observed.is_some() {
                return Err(
                    io::Error::other("recovered replica resurrected a boundary tombstone").into(),
                );
            }
        } else {
            let expected = i64::try_from(
                index
                    .checked_rem(1_000)
                    .ok_or("observation modulus must be non-zero")?,
            )?;
            if observed != Some(Some(Item::Scalar(Scalar::Integer(expected)))) {
                return Err(io::Error::other(format!(
                    "recovered replica lost a seeded value at index={index} observed={observed:?}"
                ))
                .into());
            }
        }
    }
    // Range-reader boundary: page the public route by cursor and prove the FDB
    // continuation splits at the page limit, advances, and loses or duplicates
    // no document.
    let mut seen = std::collections::HashSet::new();
    let mut cursor: Option<DocumentId> = None;
    let mut pages = 0usize;
    loop {
        let request = BaselineRequest {
            protocol: logical_db_protocol::PROTOCOL_VERSION,
            scope,
            replica: ReplicaId::new(),
            request: OperationDigest::new([0x11; 32]),
            after: None,
            after_document: cursor,
        };
        let BaselineResult::Artifact(page) = transport.baseline(request).await? else {
            return Err(io::Error::other("range walk lost the sealed baseline").into());
        };
        page.validate_page(scope)?;
        if page.documents.len() > MAX_BASELINE_PAGE_DOCUMENTS {
            return Err(io::Error::other(format!(
                "a page exceeded the document limit (len={})",
                page.documents.len()
            ))
            .into());
        }
        pages = pages.saturating_add(1);
        for document in &page.documents {
            assert!(
                seen.insert(document.document),
                "baseline repeated a document"
            );
        }
        let Some(next) = page.next_document else {
            break;
        };
        if !page.more {
            return Err(io::Error::other("a final page carried a continuation cursor").into());
        }
        if cursor.is_some_and(|previous| next <= previous) {
            return Err(io::Error::other("a page cursor did not strictly advance").into());
        }
        cursor = Some(next);
    }
    if pages < 2 {
        return Err(io::Error::other(format!(
            "the range reader did not split the baseline (served {pages} page(s))"
        ))
        .into());
    }
    if seen.len() != total {
        return Err(io::Error::other(format!(
            "range walk lost or duplicated a document (saw {} of {total} across {pages} pages)",
            seen.len()
        ))
        .into());
    }

    let floor = read_floor(&prune_store, scope).await?;
    let frontier = read_frontier(&prune_store, scope).await?;
    if frontier != seeded_target || floor != seeded_target {
        return Err(io::Error::other(format!(
            "prune did not retain the sealed baseline (frontier={frontier} floor={floor})"
        ))
        .into());
    }

    drop((first, second, expired));
    server.abort();
    let _ = server.await;
    drop(prune_store);
    drop(network);
    let _ = fs::remove_file(&first_path);
    let _ = fs::remove_file(&second_path);
    let _ = fs::remove_file(&expired_path);
    Ok(())
}
