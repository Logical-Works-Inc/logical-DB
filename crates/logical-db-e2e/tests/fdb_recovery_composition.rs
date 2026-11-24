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

#[path = "support/fdb_harness.rs"]
mod fdb_harness;

use std::{
    collections::HashSet,
    error::Error,
    fs, io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use fdb_harness::{
    open_stores, read_floor, read_frontier, run_journey, seal_consume_and_prune, seed_documents,
    seeded_value, serve, sync_until_frontier,
};
use logical_db::{
    AccountId, Database, DocumentId, Item, Object, OperationDigest, OperationId, PeerId, ReplicaId,
    Scalar, Scope, SpaceId, SyncLimits,
    http::{HttpTransport, StaticBearer},
    transport::{BaselineRequest, BaselineResult, Checkpoint, PullRequest, PullResult, Transport},
};
use logical_db_daemon::boot_network;
use logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS;
use logical_db_protocol::PROTOCOL_VERSION;
use logical_db_server::Clock;

/// A clock the journey advances by hand, so checkpoint expiry needs no sleep.
#[derive(Debug)]
struct RecoveryClock(Arc<AtomicU64>);

impl Clock for RecoveryClock {
    fn now_unix_ms(&self) -> Option<u64> {
        Some(self.0.load(Ordering::SeqCst))
    }
}

/// One journey's replica paths, removed once the journey passes.
#[derive(Debug)]
struct Paths {
    /// The seeding replica.
    first: PathBuf,
    /// The fresh replica that recovers from the baseline.
    second: PathBuf,
    /// The replica whose checkpoint expires.
    expired: PathBuf,
}

impl Paths {
    /// Fresh temporary paths unique to this run.
    fn new() -> Self {
        let run = ReplicaId::new();
        let path = |role: &str| {
            std::env::temp_dir().join(format!("logical-db-fdb-recovery-{role}-{run}.redb"))
        };
        Self {
            first: path("first"),
            second: path("second"),
            expired: path("expired"),
        }
    }

    /// Removes every replica file.
    fn remove(&self) -> io::Result<()> {
        fs::remove_file(&self.first)?;
        fs::remove_file(&self.second)?;
        fs::remove_file(&self.expired)
    }
}

/// The composition entry that drives the FDB recovery journey.
#[test]
fn fdb_recovery_composition() -> Result<(), Box<dyn Error>> {
    run_journey(journey())
}

/// Drives the expired-checkpoint recovery journey against a real FDB cluster.
async fn journey() -> Result<(), Box<dyn Error>> {
    let cluster_file = std::env::var("LOGICAL_DB_FDB_CLUSTER_FILE")?;
    // The one network boot of this process; dropped after every store below.
    let network = boot_network(Vec::new())?;
    let (service_store, prune_store) = open_stores(&cluster_file, "recovery-composition").await?;
    let now = Arc::new(AtomicU64::new(1_000));
    let (_endpoint, mut transport, mut server) =
        serve(service_store, [0x27; 32], RecoveryClock(Arc::clone(&now))).await?;

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let paths = Paths::new();
    let expired_id = ReplicaId::new();
    let expired = Database::open(&paths.expired, scope, expired_id)?;
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
    let tombstoned = *documents
        .get(MAX_BASELINE_PAGE_DOCUMENTS)
        .ok_or("the seed spans two baseline pages")?;

    let first = Database::open(&paths.first, scope, ReplicaId::new())?;
    seed_documents(&first, &documents)?;
    first.delete(
        tombstoned,
        OperationId::new(),
        OperationDigest::new([0x7a; 32]),
    )?;
    let seeded_target = u64::try_from(total.saturating_add(1))?;
    sync_until_frontier(
        &first,
        peer,
        &mut transport,
        &prune_store,
        scope,
        seeded_target,
    )
    .await?;
    seal_consume_and_prune(&prune_store, scope, seeded_target).await?;

    now.store(61_001, Ordering::SeqCst);
    let identity = (scope, peer, expired_id);
    let expired =
        recover_expired(expired, &paths, identity, old_checkpoint, &mut transport).await?;
    let second = Database::open(&paths.second, scope, ReplicaId::new())?;
    let report = second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    if !report.recovered {
        return Err(io::Error::other(
            "fresh replica did not require recovery (no baseline installed)",
        )
        .into());
    }
    verify_replicas_agree(&second, &expired, &documents, tombstoned)?;
    verify_range_walk(&mut transport, scope, total).await?;

    let floor = read_floor(&prune_store, scope).await?;
    let frontier = read_frontier(&prune_store, scope).await?;
    if frontier != seeded_target || floor != seeded_target {
        return Err(io::Error::other(format!(
            "prune did not retain the sealed baseline (frontier={frontier} floor={floor})"
        ))
        .into());
    }

    drop((first, second, expired));
    server.abort_all();
    while server.join_next().await.is_some() {}
    drop(prune_store);
    drop(network);
    paths.remove()?;
    Ok(())
}

/// Expires a real server-issued checkpoint (the clock moved past its idle
/// timeout; nothing sleeps) and proves the replica recovers from the baseline,
/// persists a fresh checkpoint, and is served changes with it again.
async fn recover_expired(
    expired: Database,
    paths: &Paths,
    (scope, peer, expired_id): (Scope, PeerId, ReplicaId),
    old_checkpoint: Vec<u8>,
    transport: &mut HttpTransport<StaticBearer>,
) -> Result<Database, Box<dyn Error>> {
    let pull = PullRequest::new(
        PROTOCOL_VERSION,
        scope,
        expired_id,
        OperationDigest::new([0x6e; 32]),
        Some(Checkpoint::from_wire(old_checkpoint.clone())?),
        1,
        1_048_576,
    );
    assert!(
        matches!(
            transport.pull(pull.clone()).await?,
            PullResult::BaselineRequired
        ),
        "an expired checkpoint pull must demand a baseline"
    );
    let recovered = expired
        .synchronize(peer, transport, SyncLimits::default(), || false)
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
    let expired = Database::open(&paths.expired, scope, expired_id)?;
    assert_eq!(
        expired.checkpoint(peer)?,
        Some(replacement.clone()),
        "the reopened store must serve the recovered checkpoint"
    );
    let resumed = PullRequest::new(
        pull.protocol,
        pull.scope,
        pull.replica,
        pull.request,
        Some(Checkpoint::from_wire(replacement)?),
        pull.max_changes,
        pull.max_bytes,
    );
    assert!(
        matches!(transport.pull(resumed).await?, PullResult::Page(_)),
        "the replacement checkpoint must serve changes again"
    );
    Ok(expired)
}

/// The fresh and the expired replica hold the same seeded state and both keep
/// the boundary tombstone.
fn verify_replicas_agree(
    second: &Database,
    expired: &Database,
    documents: &[DocumentId],
    tombstoned: DocumentId,
) -> Result<(), Box<dyn Error>> {
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
        } else if observed != Some(Some(Item::Scalar(Scalar::Integer(seeded_value(index)?)))) {
            return Err(io::Error::other(format!(
                "recovered replica lost a seeded value at index={index} observed={observed:?}"
            ))
            .into());
        }
    }
    Ok(())
}

/// Range-reader boundary: pages the public route by cursor and proves the FDB
/// continuation splits at the page limit, advances, and loses or duplicates no
/// document.
async fn verify_range_walk(
    transport: &mut HttpTransport<StaticBearer>,
    scope: Scope,
    total: usize,
) -> Result<(), Box<dyn Error>> {
    let mut seen = HashSet::new();
    let mut cursor: Option<DocumentId> = None;
    let mut pages = 0_usize;
    loop {
        let request = BaselineRequest::new(
            PROTOCOL_VERSION,
            scope,
            ReplicaId::new(),
            OperationDigest::new([0x11; 32]),
            None,
            cursor,
        );
        let BaselineResult::Artifact(page) = transport.baseline(request).await? else {
            return Err(io::Error::other("range walk lost the sealed baseline").into());
        };
        page.validate_page(scope)?;
        if page.documents().len() > MAX_BASELINE_PAGE_DOCUMENTS {
            return Err(io::Error::other(format!(
                "a page exceeded the document limit (len={})",
                page.documents().len()
            ))
            .into());
        }
        pages = pages.saturating_add(1);
        for document in page.documents() {
            assert!(
                seen.insert(document.document()),
                "baseline repeated a document"
            );
        }
        let Some(next) = page.next_document() else {
            break;
        };
        if !page.more() {
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
    Ok(())
}
