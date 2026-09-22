//! Direct process-kill-during-restore and external-consumer recovery evidence.
//!
//! The recovery consumer is a real child process speaking the public HTTP
//! recovery route to the real `FdbServiceStore`. The parent terminates it with
//! `SIGKILL` after it has fetched a continued baseline page but before it can
//! commit, then reopens the replica's durable store. Nothing from the baseline
//! may be observable: an in-memory accumulator is not durable state. The same
//! consumer then reruns to completion and installs the whole baseline, which is
//! the external-consumer recovery receipt.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32 S33
//! S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![cfg(feature = "fdb")]
#![expect(
    unsafe_code,
    reason = "the FDB network boot is this journey's FFI seam"
)]

#[path = "support/fdb_harness.rs"]
mod fdb_harness;

use std::{error::Error, fs, io, path::Path, process::Command, thread, time::Duration};

use fdb_harness::{FixtureAuthenticator, read_floor, read_frontier, seal_consume_and_prune};
use logical_db::{
    AccountId, BatchEdit, Database, DocumentId, Item, Object, OperationDigest, OperationId, PeerId,
    ReplicaId, Scalar, Scope, SpaceId, SyncLimits,
    http::{HttpTransport, StaticBearer, Url},
};
use logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS;
use logical_db_server::{SystemClock, router};
use logical_db_service::CheckpointCodec;
use logical_db_store_fdb::{
    FdbServiceStore,
    foundationdb::{Database as FdbDatabase, api::FdbApiBuilder},
    foundationdb_tuple::Subspace,
};

/// How long the killed consumer stalls once a continued page has arrived.
const KILL_PAUSE_MS: u64 = 30_000;

/// Documents seeded into the covering baseline.
const BASELINE_DOCUMENTS: usize = MAX_BASELINE_PAGE_DOCUMENTS + 44;

/// A kill during restore commits nothing; a clean rerun recovers the baseline.
#[test]
fn killed_restore_commits_nothing_and_a_clean_rerun_recovers() -> Result<(), Box<dyn Error>> {
    lgwks_bot::Builder::new()
        .worker_threads(std::num::NonZeroUsize::new(2))
        .build()?
        .block_on(journey())
}

/// Drives the process-kill recovery journey against a real FDB cluster.
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
    // the auto-stop guard beyond every database, store, future, and child.
    let network = unsafe { FdbApiBuilder::default().build()?.boot()? };
    let http_database = FdbDatabase::new_compat(Some(&cluster_file)).await?;
    let prune_database = FdbDatabase::new_compat(Some(&cluster_file)).await?;
    let root = Subspace::from_bytes(
        format!("logical-db/process-kill-restore/{}/", uuid::Uuid::now_v7()).into_bytes(),
    );
    let service_store = FdbServiceStore::new(http_database, root.clone())?;
    let prune_store = FdbServiceStore::new(prune_database, root)?;
    let application = router(
        service_store,
        FixtureAuthenticator,
        CheckpointCodec::new([0x28; 32], 60_000)?,
        SystemClock,
    );
    let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server =
        lgwks_bot::rt::task::spawn(async move { axum::serve(listener, application).await });
    let endpoint: Url = format!("http://{address}/").parse()?;
    let mut transport =
        HttpTransport::new(endpoint.clone(), StaticBearer::new("recovery-fixture")?)?;

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let run = uuid::Uuid::now_v7();
    let first_path = std::env::temp_dir().join(format!("logical-db-kill-first-{run}.redb"));
    let consumer_path = std::env::temp_dir().join(format!("logical-db-kill-consumer-{run}.redb"));
    let ready = std::env::temp_dir().join(format!("logical-db-kill-ready-{run}"));
    let committed_ready =
        std::env::temp_dir().join(format!("logical-db-kill-committed-ready-{run}"));
    let committed_path = std::env::temp_dir().join(format!("logical-db-kill-committed-{run}.redb"));
    let consumer_replica = ReplicaId::new();
    let consumer_peer = PeerId::new();
    let total = u64::try_from(BASELINE_DOCUMENTS)?;
    let documents = (0..BASELINE_DOCUMENTS)
        .map(|_| DocumentId::new())
        .collect::<Vec<_>>();

    let first = seed_and_prune(
        &first_path,
        &prune_store,
        scope,
        peer,
        &mut transport,
        &documents,
        total,
    )
    .await?;

    let mut killed = spawn_consumer(
        &consumer_path,
        scope,
        consumer_replica,
        consumer_peer,
        &endpoint,
        &ready,
        KILL_PAUSE_MS,
        &committed_ready,
        0,
    )?;
    await_ready(&mut killed, &ready, "page two")?;
    killed.kill()?;
    if killed.wait()?.success() {
        return Err(
            io::Error::other("the consumer exited cleanly before it could be killed").into(),
        );
    }
    assert_nothing_committed(
        &consumer_path,
        scope,
        consumer_replica,
        consumer_peer,
        &documents,
    )?;

    let mut rerun = spawn_consumer(
        &consumer_path,
        scope,
        consumer_replica,
        consumer_peer,
        &endpoint,
        &ready,
        0,
        &committed_ready,
        0,
    )?;
    let status = rerun.wait()?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "the recovery rerun did not succeed (status={status:?})"
        ))
        .into());
    }
    assert_recovered(
        &consumer_path,
        scope,
        consumer_replica,
        consumer_peer,
        &documents,
    )?;

    let committed_replica = ReplicaId::new();
    let committed_peer = PeerId::new();
    let mut post_commit = spawn_consumer(
        &committed_path,
        scope,
        committed_replica,
        committed_peer,
        &endpoint,
        &ready,
        0,
        &committed_ready,
        KILL_PAUSE_MS,
    )?;
    await_ready(&mut post_commit, &committed_ready, "the durable commit")?;
    post_commit.kill()?;
    if post_commit.wait()?.success() {
        return Err(io::Error::other(
            "the consumer exited after committing before it could be killed",
        )
        .into());
    }
    assert_recovered(
        &committed_path,
        scope,
        committed_replica,
        committed_peer,
        &documents,
    )
    .map_err(|error| {
        io::Error::other(format!(
            "state after a post-commit kill was not durable: {error}"
        ))
    })?;

    drop((first, transport));
    server.abort();
    let _ = server.await;
    drop(prune_store);
    drop(network);
    let _ = fs::remove_file(&first_path);
    let _ = fs::remove_file(&consumer_path);
    let _ = fs::remove_file(&ready);
    let _ = fs::remove_file(&committed_ready);
    let _ = fs::remove_file(&committed_path);
    Ok(())
}

/// Seeds every document through the public service and prunes to the baseline.
async fn seed_and_prune(
    first_path: &Path,
    prune_store: &FdbServiceStore,
    scope: Scope,
    peer: PeerId,
    transport: &mut HttpTransport<StaticBearer>,
    documents: &[DocumentId],
    total: u64,
) -> Result<Database, Box<dyn Error>> {
    let first = Database::open(first_path, scope, ReplicaId::new())?;
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
                    OperationDigest::new(
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
    for _round in 0..8 {
        first
            .synchronize(peer, transport, SyncLimits::default(), || false)
            .await?;
        if read_frontier(prune_store, scope).await? == total {
            break;
        }
    }
    let seeded = read_frontier(prune_store, scope).await?;
    if seeded != total {
        return Err(io::Error::other(format!(
            "the service did not retain every document (frontier={seeded} target={total})"
        ))
        .into());
    }
    seal_consume_and_prune(prune_store, scope, total).await?;
    let floor = read_floor(prune_store, scope).await?;
    if floor != total {
        return Err(io::Error::other(format!(
            "the baseline was not retained at the floor (floor={floor})"
        ))
        .into());
    }
    Ok(first)
}

/// Asserts the interrupted consumer's durable store carries no baseline state.
fn assert_nothing_committed(
    consumer_path: &Path,
    scope: Scope,
    replica: ReplicaId,
    peer: PeerId,
    documents: &[DocumentId],
) -> Result<(), Box<dyn Error>> {
    let interrupted = Database::open(consumer_path, scope, replica)?;
    if interrupted.checkpoint(peer)?.is_some() {
        return Err(io::Error::other("a killed restore committed a checkpoint").into());
    }
    if !interrupted.pending_changes(0, 256)?.is_empty()
        || !interrupted.changes_since(0, 256)?.is_empty()
    {
        return Err(io::Error::other("a killed restore wrote local state").into());
    }
    for document in documents {
        if interrupted
            .read(*document, |reader| reader.get(&Object::root(), "state"))?
            .is_some()
        {
            return Err(io::Error::other("a killed restore materialized a document").into());
        }
    }
    Ok(())
}

/// Asserts the clean rerun installed the whole baseline and its checkpoint.
fn assert_recovered(
    consumer_path: &Path,
    scope: Scope,
    replica: ReplicaId,
    peer: PeerId,
    documents: &[DocumentId],
) -> Result<(), Box<dyn Error>> {
    let recovered = Database::open(consumer_path, scope, replica)?;
    if recovered.checkpoint(peer)?.is_none() {
        return Err(io::Error::other("the recovered consumer has no checkpoint").into());
    }
    for (index, document) in documents.iter().enumerate() {
        let expected = Some(Some(Item::Scalar(Scalar::Integer(i64::try_from(
            index
                .checked_rem(1_000)
                .ok_or("observation modulus must be non-zero")?,
        )?))));
        if recovered.read(*document, |reader| reader.get(&Object::root(), "state"))? != expected {
            return Err(io::Error::other(format!(
                "a recovered document diverged at index={index}"
            ))
            .into());
        }
    }
    if !recovered.pending_changes(0, 256)?.is_empty() {
        return Err(io::Error::other("recovery invented local changes").into());
    }
    Ok(())
}

/// Owns a spawned recovery consumer and terminates and reaps it on every path,
/// so an early return or panic cannot leak a live child.
struct ConsumerChild {
    child: std::process::Child,
}

impl ConsumerChild {
    /// Signals the consumer child to terminate.
    fn kill(&mut self) -> io::Result<()> {
        self.child.kill()
    }

    /// Waits for the consumer child's exit status.
    fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        self.child.wait()
    }

    /// Polls whether the consumer child has exited.
    fn try_wait(&mut self) -> io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }
}

impl Drop for ConsumerChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawns one recovery-consumer child wired to the public HTTP fixture.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors the consumer binary's positional CLI one-for-one"
)]
fn spawn_consumer(
    path: &Path,
    scope: Scope,
    replica: ReplicaId,
    peer: PeerId,
    endpoint: &Url,
    ready: &Path,
    pause_ms: u64,
    committed: &Path,
    post_pause_ms: u64,
) -> Result<ConsumerChild, Box<dyn Error>> {
    let child = Command::new(env!("CARGO_BIN_EXE_recovery_consumer"))
        .arg(path)
        .arg(scope.account().to_string())
        .arg(scope.space().to_string())
        .arg(replica.to_string())
        .arg(peer.to_string())
        .arg(endpoint.as_str())
        .arg("recovery-fixture")
        .arg(ready)
        .arg(pause_ms.to_string())
        .arg(committed)
        .arg(post_pause_ms.to_string())
        .spawn()?;
    Ok(ConsumerChild { child })
}

/// Waits until the consumer writes its `ready` file, failing if it dies first.
fn await_ready(child: &mut ConsumerChild, ready: &Path, stage: &str) -> Result<(), Box<dyn Error>> {
    for _poll in 0..2_000 {
        if ready.exists() {
            return Ok(());
        }
        if child.try_wait()?.is_some() {
            return Err(io::Error::other(format!(
                "the consumer exited before {stage} (kill seam never reached)"
            ))
            .into());
        }
        thread::park_timeout(Duration::from_millis(10));
    }
    Err(io::Error::other(format!(
        "the consumer never signalled the kill seam before {stage}"
    ))
    .into())
}
