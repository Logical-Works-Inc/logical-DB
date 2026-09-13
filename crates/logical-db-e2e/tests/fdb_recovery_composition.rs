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

use std::{error::Error, fs, io};

use foundationdb::{Database as FdbDatabase, api::FdbApiBuilder};
use foundationdb_tuple::Subspace;
use logical_db::{
    AccountId, Database, DocumentId, Item, Object, OperationDigest, OperationId, PeerId, ReplicaId,
    Scalar, Scope, SpaceId, SyncLimits,
    http::{HttpTransport, StaticBearer, Url},
};
use logical_db_model::{ActionDigest, Effect};
use logical_db_protocol::{mark_consumed_action, prune_action, seal_snapshot_action};
use logical_db_server::{AuthenticationError, Authenticator, SystemClock, router};
use logical_db_service::{
    Authority, CheckpointCodec, ConsumedResult, ExecutionGrant, PruneReceipt, PruneResult,
    RetentionStore, SealResult, ServiceStore,
};
use logical_db_store_fdb::FdbServiceStore;

const BEARER: &[u8] = b"Bearer recovery-fixture";

struct Allow;

struct Grant {
    scope: Scope,
    effect: Effect,
    action: ActionDigest,
    frontier: u64,
}

impl ExecutionGrant for Grant {
    fn scope(&self) -> Scope {
        self.scope
    }

    fn effect(&self) -> Effect {
        self.effect
    }

    fn action(&self) -> ActionDigest {
        self.action
    }

    fn frontier(&self) -> u64 {
        self.frontier
    }

    fn invocation_id(&self) -> [u8; 32] {
        self.action.into_bytes()
    }
}

impl Authority for Allow {
    type Grant = Grant;

    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        _now_unix_ms: u64,
    ) -> Option<Self::Grant> {
        Some(Grant {
            scope,
            effect,
            action,
            frontier,
        })
    }
}

struct FixtureAuthenticator;

impl Authenticator for FixtureAuthenticator {
    type Policy = Allow;

    fn authenticate(
        &self,
        authorization: &axum::http::HeaderValue,
        _now_unix_ms: u64,
    ) -> Result<Self::Policy, AuthenticationError> {
        if authorization.as_bytes() == BEARER {
            Ok(Allow)
        } else {
            Err(AuthenticationError::Unauthorized)
        }
    }
}

fn grant(scope: Scope, effect: Effect, action: ActionDigest, frontier: u64) -> Grant {
    Grant {
        scope,
        effect,
        action,
        frontier,
    }
}

#[test]
fn fdb_recovery_composition() -> Result<(), Box<dyn Error>> {
    lgwks_bot::Runtime::new()?.block_on(journey())
}

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
    let document = DocumentId::new();
    let peer = PeerId::new();
    let run = uuid::Uuid::now_v7();
    let first_path = std::env::temp_dir().join(format!("logical-db-fdb-recovery-first-{run}.redb"));
    let second_path =
        std::env::temp_dir().join(format!("logical-db-fdb-recovery-second-{run}.redb"));

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
    let seeded = read_frontier(&prune_store, scope).await?;
    if seeded != 1 {
        eprintln!("fdb-recovery-composition: first replica seeded frontier={seeded}");
        return Err(io::Error::other("first replica did not seed exactly one change").into());
    }

    seal_consume_and_prune(&prune_store, scope).await?;

    let second = Database::open(&second_path, scope, ReplicaId::new())?;
    let report = second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    if !report.recovered {
        eprintln!("fdb-recovery-composition: fresh replica did not install a baseline");
        return Err(io::Error::other("fresh replica did not require recovery").into());
    }
    let observed = second.read(document, |reader| reader.get(&Object::root(), "state"))?;
    if observed != Some(Some(Item::Scalar(Scalar::String("sealed".into())))) {
        eprintln!("fdb-recovery-composition: replica state diverged after recovery: {observed:?}");
        return Err(io::Error::other("recovered replica lost its document state").into());
    }
    let frontier = read_frontier(&prune_store, scope).await?;
    let floor = read_floor(&prune_store, scope).await?;
    if frontier != 1 || floor != 1 {
        eprintln!("fdb-recovery-composition: durable retention frontier={frontier} floor={floor}");
        return Err(io::Error::other("prune did not retain the sealed baseline").into());
    }

    println!(
        "PASS fdb-http-recovery route=/v1/recovery:baseline recovered=true documents=1 frontier={frontier} floor={floor} store=real-fdb scope=isolated"
    );

    drop((first, second));
    server.abort();
    drop(network);
    let _ = fs::remove_file(&first_path);
    let _ = fs::remove_file(&second_path);
    Ok(())
}

async fn seal_consume_and_prune(
    store: &FdbServiceStore,
    scope: Scope,
) -> Result<(), Box<dyn Error>> {
    let artifact = store.build_snapshot(scope, 1).await?;
    let frontier = store.frontier(scope).await?;
    let seal_request = OperationDigest::new([0x52; 32]);
    if store
        .seal_snapshot(
            scope,
            &artifact,
            seal_request,
            &grant(
                scope,
                Effect::Sync,
                seal_snapshot_action(scope, seal_request, &artifact.baseline),
                frontier,
            ),
        )
        .await?
        != (SealResult::Sealed { position: 1 })
    {
        eprintln!("fdb-recovery-composition: baseline did not seal");
        return Err(io::Error::other("baseline did not seal").into());
    }
    let consumed_request = OperationDigest::new([0x53; 32]);
    if store
        .mark_consumed(
            scope,
            1,
            consumed_request,
            &grant(
                scope,
                Effect::Sync,
                mark_consumed_action(scope, consumed_request, 1),
                frontier,
            ),
        )
        .await?
        != (ConsumedResult::Advanced {
            consumed_through: 1,
        })
    {
        eprintln!("fdb-recovery-composition: consumption did not advance");
        return Err(io::Error::other("consumption did not advance").into());
    }
    let prune_request = OperationDigest::new([0x54; 32]);
    if store
        .prune(
            scope,
            1,
            None,
            prune_request,
            &grant(
                scope,
                Effect::Sync,
                prune_action(scope, prune_request, 1, None),
                frontier,
            ),
        )
        .await?
        != (PruneResult::Pruned(PruneReceipt {
            pruned_through: 1,
            floor: 1,
        }))
    {
        eprintln!("fdb-recovery-composition: prune did not collect the log prefix");
        return Err(io::Error::other("prune did not collect the log prefix").into());
    }
    Ok(())
}

async fn read_frontier(store: &FdbServiceStore, scope: Scope) -> Result<u64, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..128 {
        match store.frontier(scope).await {
            Ok(frontier) => return Ok(frontier),
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!("frontier retry budget exhausted: {last_error:?}")).into())
}

async fn read_floor(store: &FdbServiceStore, scope: Scope) -> Result<u64, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..128 {
        match store.retention_floor(scope).await {
            Ok(floor) => return Ok(floor),
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!(
        "retention-floor retry budget exhausted: {last_error:?}"
    ))
    .into())
}
