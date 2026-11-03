//! Shared async journey harness helpers for the black-box suites.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

use std::sync::Arc;

use logical_db::{
    AccountId, Database, DocumentId, IndexId, IndexValue, Item, Object, OperationDigest, PeerId,
    Scope, SpaceId,
};
use logical_db_http::{HttpTransport, StaticBearer};
use logical_db_model::Effect;
use logical_db_protocol::{mark_consumed_action, prune_action, seal_snapshot_action};
use logical_db_server::router;
use logical_db_service::{
    CheckpointCodec, ConsumedResult, PruneReceipt, PruneResult, RetentionStore, SealResult,
    ServiceStore,
};

use crate::support::{FixedClock, Grant, MemoryServiceStore, TestAuthentication};

static HARNESS_PATH_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Typed failure for every black-box journey step.
///
/// Each fallible operation names its error instead of erasing it behind a
/// box, so a journey failure identifies the layer that refused.
#[derive(Debug)]
pub(crate) enum JourneyError {
    /// The embedded replica refused or failed the operation.
    Db(logical_db::DatabaseError),
    /// The in-memory service refused or failed the operation.
    Service(logical_db_model::ModelError),
    /// Fixture file or socket I/O failed.
    Io(std::io::Error),
    /// An integer conversion exceeded its addressable range.
    Int(std::num::TryFromIntError),
    /// A fixture invariant message with no typed source.
    Fixture(String),
    /// A synchronization round failed.
    Sync(logical_db::SyncError<logical_db::DatabaseError, logical_db_http::HttpError>),
    /// A shared helper erased its failure before this layer existed.
    Harness(Box<dyn std::error::Error>),
}

impl From<logical_db::DatabaseError> for JourneyError {
    /// Adapts a replica refusal without discarding its variant.
    fn from(error: logical_db::DatabaseError) -> Self {
        Self::Db(error)
    }
}

impl From<logical_db_model::ModelError> for JourneyError {
    /// Adapts a service refusal without discarding its variant.
    fn from(error: logical_db_model::ModelError) -> Self {
        Self::Service(error)
    }
}

impl From<std::io::Error> for JourneyError {
    /// Adapts a fixture I/O failure without discarding its kind.
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<std::num::TryFromIntError> for JourneyError {
    /// Adapts an out-of-range integer conversion without discarding it.
    fn from(error: std::num::TryFromIntError) -> Self {
        Self::Int(error)
    }
}

impl From<&str> for JourneyError {
    /// Adapts a fixture invariant message with no typed source.
    fn from(message: &str) -> Self {
        Self::Fixture(message.to_owned())
    }
}

impl From<logical_db::SyncError<logical_db::DatabaseError, logical_db_http::HttpError>>
    for JourneyError
{
    /// Adapts a synchronization refusal without discarding its variant.
    fn from(
        error: logical_db::SyncError<logical_db::DatabaseError, logical_db_http::HttpError>,
    ) -> Self {
        Self::Sync(error)
    }
}

impl std::fmt::Display for JourneyError {
    /// Renders the failing journey step for the test runner.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Db(ref error) => write!(formatter, "journey database failed: {error}"),
            Self::Service(ref error) => write!(formatter, "journey service failed: {error}"),
            Self::Io(ref error) => write!(formatter, "journey I/O failed: {error}"),
            Self::Int(ref error) => write!(formatter, "journey integer conversion failed: {error}"),
            Self::Sync(ref error) => write!(formatter, "journey synchronization failed: {error}"),
            Self::Fixture(ref message) => write!(formatter, "journey fixture failed: {message}"),
            Self::Harness(ref error) => write!(formatter, "journey harness failed: {error}"),
        }
    }
}

impl std::error::Error for JourneyError {}

pub(crate) fn path(label: &str) -> std::path::PathBuf {
    let sequence = HARNESS_PATH_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "logical-db-network-{label}-{}-{sequence}.redb",
        std::process::id()
    ))
}

/// Opens one two-replica journey case with a fresh scope, document and peer.
pub(crate) fn pair_case(
    label: &str,
) -> (
    Scope,
    DocumentId,
    PeerId,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    (
        Scope::new(AccountId::new(), SpaceId::new()),
        DocumentId::new(),
        PeerId::new(),
        path(&format!("{label}-first")),
        path(&format!("{label}-second")),
    )
}

/// Builds the two-thread runtime every async journey runs on.
pub(crate) fn test_runtime() -> Result<lgwks_bot::Runtime, JourneyError> {
    Ok(lgwks_bot::Builder::new()
        .worker_threads(std::num::NonZeroUsize::new(2))
        .build()?)
}

/// Starts one black-box HTTP server over an in-memory service store and
/// returns the transport plus the owned server task set.
///
/// Every journey test boots the same stack (router, listener, server task,
/// transport); the only per-journey inputs are the checkpoint codec seed
/// and its idle timeout. Callers keep the returned set alive for the
/// journey and abort it when the journey ends: dropping the set would abort
/// the server mid-journey.
pub(crate) async fn serve_test_router(
    store: Arc<MemoryServiceStore>,
    codec_seed: [u8; 32],
    idle_timeout_ms: u64,
) -> Result<
    (
        HttpTransport<StaticBearer>,
        lgwks_bot::rt::task::JoinSet<()>,
    ),
    Box<dyn std::error::Error>,
> {
    let app = router(
        Arc::clone(&store),
        TestAuthentication,
        CheckpointCodec::new(codec_seed, idle_timeout_ms)?,
        FixedClock,
    );
    let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let mut server_tasks = lgwks_bot::rt::task::JoinSet::new();
    server_tasks.spawn(async move {
        let serve_result = axum::serve(listener, app).await;
        drop(serve_result);
    });
    let endpoint = format!("http://{address}/").parse()?;
    let transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;
    Ok((transport, server_tasks))
}

pub(crate) fn projection(index: IndexId) -> logical_db::Projection {
    logical_db::Projection::new(index, logical_db::ProjectionSemantics::V1, |reader| {
        Ok(match reader.get(&Object::root(), "kind")? {
            Some(Item::Scalar(logical_db::Scalar::String(value))) => Some(IndexValue::Text(value)),
            _ => None,
        })
    })
}

pub(crate) fn assert_converged(
    database: &Database,
    document: DocumentId,
    index: IndexId,
) -> Result<(), Box<dyn std::error::Error>> {
    let values = database.read(document, |reader| {
        Ok((
            reader.get(&Object::root(), "left")?,
            reader.get(&Object::root(), "right")?,
        ))
    })?;
    assert_eq!(
        values,
        Some((
            Some(Item::Scalar(logical_db::Scalar::Integer(11))),
            Some(Item::Scalar(logical_db::Scalar::Integer(22))),
        ))
    );
    assert_eq!(
        database.query_index(index, &IndexValue::Text("record".into()), None, 10)?,
        vec![document]
    );
    let events = database.changes_since(0, 10)?;
    assert_eq!(events.len(), 3);
    assert!(
        events
            .iter()
            .all(|event| event.document() == Some(document))
    );
    assert!(
        database
            .changes_since(
                events
                    .last()
                    .ok_or("the fixture must record at least one event")?
                    .sequence(),
                10
            )?
            .is_empty()
    );
    Ok(())
}

pub(crate) async fn seal_consume_and_prune(
    store: &MemoryServiceStore,
    scope: Scope,
) -> Result<(), Box<dyn std::error::Error>> {
    seal_consume_and_prune_through(store, scope, 1).await
}

pub(crate) async fn seal_consume_and_prune_through(
    store: &MemoryServiceStore,
    scope: Scope,
    through: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let artifact = store.build_snapshot(scope, through).await?;
    let frontier = store.frontier(scope).await?;
    let seal_request = OperationDigest::new([0x52; 32]);
    assert!(matches!(
        store
            .seal_snapshot(
                scope,
                &artifact,
                seal_request,
                &Grant {
                    scope,
                    effect: Effect::Sync,
                    action: seal_snapshot_action(scope, seal_request, &artifact.baseline),
                    frontier,
                },
            )
            .await
            ?,
        SealResult::Sealed { position } if position == through
    ));
    let consumed_request = OperationDigest::new([0x53; 32]);
    assert!(matches!(
        store
            .mark_consumed(
                scope,
                through,
                consumed_request,
                &Grant {
                    scope,
                    effect: Effect::Sync,
                    action: mark_consumed_action(scope, consumed_request, through),
                    frontier,
                },
            )
            .await
            ?,
        ConsumedResult::Advanced { consumed_through } if consumed_through == through
    ));
    let prune_request = OperationDigest::new([0x54; 32]);
    assert!(matches!(
        store
            .prune(
                scope,
                through,
                None,
                prune_request,
                &Grant {
                    scope,
                    effect: Effect::Sync,
                    action: prune_action(scope, prune_request, through, None),
                    frontier,
                },
            )
            .await
            ?,
        PruneResult::Pruned(PruneReceipt { pruned_through, floor, .. })
            if pruned_through == through && floor == through
    ));
    Ok(())
}
