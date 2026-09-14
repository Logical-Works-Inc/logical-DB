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
#![allow(unsafe_code)]

#[path = "support/fdb_harness.rs"]
mod fdb_harness;

use std::{error::Error, fs, io};

use fdb_harness::{FixtureAuthenticator, read_floor, read_frontier, seal_consume_and_prune};
use logical_db::{
    AccountId, BatchEdit, Database, DocumentId, Item, Object, OperationDigest, OperationId, PeerId,
    ReplicaId, Scalar, Scope, SpaceId, SyncLimits,
    http::{HttpTransport, StaticBearer, Url},
    transport::{BaselineRequest, BaselineResult, Transport},
};
use logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS;
use logical_db_server::{SystemClock, router};
use logical_db_service::CheckpointCodec;
use logical_db_store_fdb::{
    FdbServiceStore,
    foundationdb::{Database as FdbDatabase, api::FdbApiBuilder},
    foundationdb_tuple::Subspace,
};

#[test]
fn fdb_recovery_composition() -> Result<(), Box<dyn Error>> {
    lgwks_bot::Runtime::new()?.block_on(journey())
}

#[allow(clippy::too_many_lines)]
async fn journey() -> Result<(), Box<dyn Error>> {
    let cluster_file = std::env::var("LOGICAL_DB_FDB_CLUSTER_FILE")?;
    #[allow(unsafe_code)]
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
    let application = router(
        service_store,
        FixtureAuthenticator,
        CheckpointCodec::new([0x27; 32], 60_000)?,
        SystemClock,
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
    let total = MAX_BASELINE_PAGE_DOCUMENTS + 44;
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
                let index = batch * 150 + offset;
                let value = i64::try_from(index % 1_000)?;
                Ok(BatchEdit::new(
                    *document,
                    OperationId::new(),
                    logical_db::OperationDigest::new([u8::try_from(index % 256)?; 32]),
                    move |editor| editor.put(&Object::root(), "state", Scalar::Integer(value)),
                ))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        first.edit_many(edits)?;
    }
    first
        .delete(
            tombstoned,
            OperationId::new(),
            OperationDigest::new([0x7a; 32]),
        )
        .unwrap();
    let seeded_target = u64::try_from(total + 1)?;
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
        eprintln!("fdb-recovery-composition: first replica seeded frontier={seeded}");
        return Err(io::Error::other("first replica did not seed every document").into());
    }

    seal_consume_and_prune(&prune_store, scope, seeded_target).await?;

    let second = Database::open(&second_path, scope, ReplicaId::new())?;
    let report = second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    if !report.recovered {
        eprintln!("fdb-recovery-composition: fresh replica did not install a baseline");
        return Err(io::Error::other("fresh replica did not require recovery").into());
    }
    if !second.is_deleted(tombstoned)? {
        eprintln!("fdb-recovery-composition: boundary tombstone did not survive recovery");
        return Err(io::Error::other("recovered replica lost a boundary tombstone").into());
    }
    for (index, document) in documents.iter().enumerate() {
        let observed = second.read(*document, |reader| reader.get(&Object::root(), "state"))?;
        if *document == tombstoned {
            if observed.is_some() {
                eprintln!("fdb-recovery-composition: boundary tombstone resurrected");
                return Err(io::Error::other("recovered replica resurrected a tombstone").into());
            }
        } else {
            let expected = i64::try_from(index % 1_000)?;
            if observed != Some(Some(Item::Scalar(Scalar::Integer(expected)))) {
                eprintln!(
                    "fdb-recovery-composition: replica state diverged at index={index} observed={observed:?}"
                );
                return Err(io::Error::other("recovered replica lost a seeded value").into());
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
            eprintln!("fdb-recovery-composition: range walk lost the sealed baseline");
            return Err(io::Error::other("range walk lost the sealed baseline").into());
        };
        page.validate_page(scope)?;
        if page.documents.len() > MAX_BASELINE_PAGE_DOCUMENTS {
            eprintln!(
                "fdb-recovery-composition: page len={} exceeded the limit",
                page.documents.len()
            );
            return Err(io::Error::other("a page exceeded the document limit").into());
        }
        pages += 1;
        for document in &page.documents {
            seen.insert(document.document);
        }
        let Some(next) = page.next_document else {
            break;
        };
        if !page.more {
            eprintln!("fdb-recovery-composition: a final page carried a continuation cursor");
            return Err(io::Error::other("a final page carried a continuation cursor").into());
        }
        if cursor.is_some_and(|previous| next <= previous) {
            eprintln!("fdb-recovery-composition: a page cursor did not strictly advance");
            return Err(io::Error::other("a page cursor did not strictly advance").into());
        }
        cursor = Some(next);
    }
    if pages < 2 {
        eprintln!(
            "fdb-recovery-composition: the range reader served the baseline in {pages} page(s)"
        );
        return Err(io::Error::other("the range reader did not split the baseline").into());
    }
    if seen.len() != total {
        eprintln!(
            "fdb-recovery-composition: range walk saw {} of {total} documents across {pages} pages",
            seen.len()
        );
        return Err(io::Error::other("range walk lost or duplicated a document").into());
    }

    let floor = read_floor(&prune_store, scope).await?;
    let frontier = read_frontier(&prune_store, scope).await?;
    if frontier != seeded_target || floor != seeded_target {
        eprintln!("fdb-recovery-composition: durable retention frontier={frontier} floor={floor}");
        return Err(io::Error::other("prune did not retain the sealed baseline").into());
    }

    println!(
        "PASS fdb-http-recovery route=/v1/recovery:baseline recovered=true documents={total} frontier={frontier} floor={floor} store=real-fdb scope=isolated"
    );

    drop((first, second));
    server.abort();
    let _ = server.await;
    drop(prune_store);
    drop(network);
    let _ = fs::remove_file(&first_path);
    let _ = fs::remove_file(&second_path);
    Ok(())
}
