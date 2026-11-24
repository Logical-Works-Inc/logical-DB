//! Shared live-`FoundationDB` recovery fixture for the composition journeys.
//!
//! Included by name (never a test target itself) so the successful recovery
//! journey and the process-kill journey open, serve, seed, seal, and prune
//! through one implementation. Grants and bearer authentication are the shared
//! journey composition in `logical_db_e2e::support`.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32 S33
//! S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

use std::{
    error::Error,
    io,
    net::{Ipv4Addr, SocketAddr},
};

use lgwks_bot::rt::task::JoinSet;
use logical_db::{
    BatchEdit, Database, DocumentId, Object, OperationDigest, OperationId, PeerId, ReplicaId,
    Scalar, Scope, SyncLimits,
    http::{Endpoint, HttpTransport, StaticBearer},
};
use logical_db_e2e::support::{Allow, BLACK_BOX_TOKEN, Grant, TestAuthentication};
use logical_db_model::Effect;
use logical_db_protocol::{mark_consumed_action, prune_action, seal_snapshot_action};
use logical_db_server::{Clock, router};
use logical_db_service::{
    CheckpointCodec, ConsumedResult, PruneReceipt, PruneResult, RetentionStore, SealResult,
    ServiceStore,
};
use logical_db_store_fdb::{
    FdbServiceStore, foundationdb::Database as FdbDatabase, foundationdb_tuple::Subspace,
};

/// Documents seeded per local batch edit.
const SEED_BATCH: usize = 150;

/// Runs one live journey on the two-worker runtime every FDB journey uses.
pub(crate) fn run_journey(
    journey: impl std::future::Future<Output = Result<(), Box<dyn Error>>>,
) -> Result<(), Box<dyn Error>> {
    lgwks_bot::rt::runtime::Builder::new()
        .worker_threads(std::num::NonZeroUsize::new(2))
        .build()?
        .block_on(journey)
}

/// Opens the serving and pruning stores over one fresh subspace of the cluster.
/// The caller has already booted the FDB network and keeps it alive longer.
pub(crate) async fn open_stores(
    cluster_file: &str,
    journey: &str,
) -> Result<(FdbServiceStore, FdbServiceStore), Box<dyn Error>> {
    let root =
        Subspace::from_bytes(format!("logical-db/{journey}/{}/", ReplicaId::new()).into_bytes());
    let service = FdbServiceStore::new(
        FdbDatabase::new_compat(Some(cluster_file)).await?,
        root.clone(),
    )?;
    let prune = FdbServiceStore::new(FdbDatabase::new_compat(Some(cluster_file)).await?, root)?;
    Ok((service, prune))
}

/// Serves `store` on an ephemeral loopback port behind the black-box bearer and
/// returns its endpoint, a transport to it, and the set that owns the server.
pub(crate) async fn serve(
    store: FdbServiceStore,
    codec_key: [u8; 32],
    clock: impl Clock + 'static,
) -> Result<
    (
        Endpoint,
        HttpTransport<StaticBearer>,
        JoinSet<io::Result<()>>,
    ),
    Box<dyn Error>,
> {
    let application = router(
        store,
        TestAuthentication::new(
            BLACK_BOX_TOKEN,
            Allow::when(|_scope, _effect, _action| true),
        ),
        CheckpointCodec::new(codec_key, 60_000)?,
        clock,
    );
    // An ephemeral loopback port: only this journey's clients connect to it.
    let listener =
        lgwks_bot::rt::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
    let address = listener.local_addr()?;
    let mut server = JoinSet::new();
    server.spawn(async move { axum::serve(listener, application).await });
    let endpoint = Endpoint::parse(&format!("http://{address}/"))?;
    let bearer = StaticBearer::new(BLACK_BOX_TOKEN)?;
    let transport = HttpTransport::new(endpoint.clone(), bearer)?;
    Ok((endpoint, transport, server))
}

/// The value seeded into, and expected back from, document `index`.
pub(crate) fn seeded_value(index: usize) -> Result<i64, Box<dyn Error>> {
    let value = index
        .checked_rem(1_000)
        .ok_or_else(|| io::Error::other("seed modulus must be non-zero"))?;
    Ok(i64::try_from(value)?)
}

/// Writes one `state` value per document into `replica`, in local batches.
pub(crate) fn seed_documents(
    replica: &Database,
    documents: &[DocumentId],
) -> Result<(), Box<dyn Error>> {
    for (batch, chunk) in documents.chunks(SEED_BATCH).enumerate() {
        let edits = chunk
            .iter()
            .enumerate()
            .map(|(offset, document)| {
                let index = batch.saturating_mul(SEED_BATCH).saturating_add(offset);
                let value = seeded_value(index)?;
                let [.., request_byte] = index.to_be_bytes();
                Ok(BatchEdit::new(
                    *document,
                    OperationId::new(),
                    OperationDigest::new([request_byte; 32]),
                    move |editor| editor.put(&Object::root(), "state", Scalar::Integer(value)),
                ))
            })
            .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
        replica.edit_many(edits)?;
    }
    Ok(())
}

/// Synchronizes `replica` until the service frontier reaches `target`.
pub(crate) async fn sync_until_frontier(
    replica: &Database,
    peer: PeerId,
    transport: &mut HttpTransport<StaticBearer>,
    prune_store: &FdbServiceStore,
    scope: Scope,
    target: u64,
) -> Result<(), Box<dyn Error>> {
    for _round in 0..8 {
        replica
            .synchronize(peer, transport, SyncLimits::default(), || false)
            .await?;
        if read_frontier(prune_store, scope).await? == target {
            return Ok(());
        }
    }
    let seeded = read_frontier(prune_store, scope).await?;
    Err(io::Error::other(format!(
        "the service did not retain every document (frontier={seeded} target={target})"
    ))
    .into())
}

/// Seals the covering baseline at `through`, consumes it, and prunes the prefix.
pub(crate) async fn seal_consume_and_prune(
    store: &FdbServiceStore,
    scope: Scope,
    through: u64,
) -> Result<(), Box<dyn Error>> {
    let artifact = store.build_snapshot(scope, through).await?;
    let frontier = store.frontier(scope).await?;
    let seal_request = OperationDigest::new([0x52; 32]);
    let seal_action = seal_snapshot_action(scope, seal_request, artifact.baseline());
    let seal_grant = Grant::new(scope, Effect::Sync, seal_action, frontier);
    if store
        .seal_snapshot(scope, &artifact, seal_request, &seal_grant)
        .await?
        != (SealResult::Sealed { position: through })
    {
        return Err(io::Error::other("baseline did not seal").into());
    }
    let consumed_request = OperationDigest::new([0x53; 32]);
    let consumed_action = mark_consumed_action(scope, consumed_request, through);
    let consumed_grant = Grant::new(scope, Effect::Sync, consumed_action, frontier);
    if store
        .mark_consumed(scope, through, consumed_request, &consumed_grant)
        .await?
        != (ConsumedResult::Advanced {
            consumed_through: through,
        })
    {
        return Err(io::Error::other("consumption did not advance").into());
    }
    let prune_request = OperationDigest::new([0x54; 32]);
    let prune_grant = Grant::new(
        scope,
        Effect::Sync,
        prune_action(scope, prune_request, through, None),
        frontier,
    );
    if store
        .prune(scope, through, None, prune_request, &prune_grant)
        .await?
        != (PruneResult::Pruned(PruneReceipt::new(through, through)))
    {
        return Err(io::Error::other("prune did not collect the log prefix").into());
    }
    Ok(())
}

/// Reads the durable frontier with a bounded retry budget for transient FDB errors.
pub(crate) async fn read_frontier(
    store: &FdbServiceStore,
    scope: Scope,
) -> Result<u64, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..128 {
        match store.frontier(scope).await {
            Ok(frontier) => return Ok(frontier),
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!("frontier retry budget exhausted: {last_error:?}")).into())
}

/// Reads the durable retention floor with a bounded retry budget.
pub(crate) async fn read_floor(
    store: &FdbServiceStore,
    scope: Scope,
) -> Result<u64, Box<dyn Error>> {
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
