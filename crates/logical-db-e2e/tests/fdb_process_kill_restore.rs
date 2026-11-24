//! Direct process-kill-during-restore and external-consumer recovery evidence.
//!
//! The recovery consumer is a real child process speaking the public HTTP
//! recovery route to the real `FdbServiceStore`. The parent terminates it with
//! `SIGKILL` after it has fetched a continued baseline page but before it can
//! commit, then reopens the replica's durable store. Nothing from the baseline
//! may be observable: an in-memory accumulator is not durable state. The same
//! consumer then reruns to completion and installs the whole baseline, which is
//! the external-consumer recovery receipt. A third child is killed after its
//! commit, and everything it committed must survive.
//!
//! The child reports each seam on stdout and holds there on stdin, so every kill
//! lands at an exact point with no sleep, poll, or timer.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32 S33
//! S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![cfg(feature = "fdb")]

#[path = "support/fdb_harness.rs"]
mod fdb_harness;

use std::{
    error::Error,
    fs,
    io::{self, BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
};

use fdb_harness::{
    open_stores, read_floor, run_journey, seal_consume_and_prune, seed_documents, seeded_value,
    serve, sync_until_frontier,
};
use logical_db::{
    AccountId, Database, DocumentId, Item, Object, PeerId, ReplicaId, Scalar, Scope, SpaceId,
    http::Endpoint,
};
use logical_db_daemon::boot_network;
use logical_db_e2e::support::BLACK_BOX_TOKEN;
use logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS;
use logical_db_server::SystemClock;

/// Documents seeded into the covering baseline.
const BASELINE_DOCUMENTS: usize = MAX_BASELINE_PAGE_DOCUMENTS + 44;

/// One consumer replica: where it lives and who it is.
#[derive(Clone, Debug)]
struct Consumer {
    /// The consumer's durable redb file.
    path: PathBuf,
    /// The scope it recovers.
    scope: Scope,
    /// Its replica identity.
    replica: ReplicaId,
    /// The peer it synchronizes with.
    peer: PeerId,
}

impl Consumer {
    /// A fresh consumer of `scope` stored at a run-unique temporary path.
    fn new(scope: Scope, role: &str, run: ReplicaId) -> Self {
        Self {
            path: std::env::temp_dir().join(format!("logical-db-kill-{role}-{run}.redb")),
            scope,
            replica: ReplicaId::new(),
            peer: PeerId::new(),
        }
    }

    /// Spawns the consumer binary against `endpoint`, holding at `hold`.
    fn spawn(&self, endpoint: &Endpoint, hold: &str) -> Result<ConsumerChild, Box<dyn Error>> {
        let child = Command::new(env!("CARGO_BIN_EXE_recovery_consumer"))
            .arg(&self.path)
            .arg(self.scope.account().to_string())
            .arg(self.scope.space().to_string())
            .arg(self.replica.to_string())
            .arg(self.peer.to_string())
            .arg(endpoint.to_string())
            .arg(BLACK_BOX_TOKEN)
            .arg(hold)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        Ok(ConsumerChild { child })
    }

    /// Opens the consumer's durable store as it is on disk now.
    fn reopen(&self) -> Result<Database, Box<dyn Error>> {
        Ok(Database::open(&self.path, self.scope, self.replica)?)
    }
}

/// A kill during restore commits nothing; a clean rerun recovers the baseline.
#[test]
fn killed_restore_commits_nothing_and_a_clean_rerun_recovers() -> Result<(), Box<dyn Error>> {
    run_journey(journey())
}

/// Drives the process-kill recovery journey against a real FDB cluster.
async fn journey() -> Result<(), Box<dyn Error>> {
    let cluster_file = std::env::var("LOGICAL_DB_FDB_CLUSTER_FILE")?;
    // The one network boot of this process; dropped after every store below.
    let network = boot_network(Vec::new())?;
    let (service_store, prune_store) = open_stores(&cluster_file, "process-kill-restore").await?;
    let (endpoint, mut transport, mut server) =
        serve(service_store, [0x28; 32], SystemClock::new()).await?;

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let peer = PeerId::new();
    let run = ReplicaId::new();
    let first_path = std::env::temp_dir().join(format!("logical-db-kill-first-{run}.redb"));
    let total = u64::try_from(BASELINE_DOCUMENTS)?;
    let documents = (0..BASELINE_DOCUMENTS)
        .map(|_| DocumentId::new())
        .collect::<Vec<_>>();

    let first = Database::open(&first_path, scope, ReplicaId::new())?;
    seed_documents(&first, &documents)?;
    sync_until_frontier(&first, peer, &mut transport, &prune_store, scope, total).await?;
    seal_consume_and_prune(&prune_store, scope, total).await?;
    let floor = read_floor(&prune_store, scope).await?;
    if floor != total {
        return Err(io::Error::other(format!(
            "the baseline was not retained at the floor (floor={floor})"
        ))
        .into());
    }

    let consumer = Consumer::new(scope, "consumer", run);
    kill_mid_restore(&consumer, &endpoint, &documents).await?;
    let committed = Consumer::new(scope, "committed", run);
    kill_after_commit(&committed, &endpoint, &documents).await?;

    drop((first, transport));
    server.abort_all();
    while server.join_next().await.is_some() {}
    drop(prune_store);
    drop(network);
    fs::remove_file(&first_path)?;
    fs::remove_file(&consumer.path)?;
    fs::remove_file(&committed.path)?;
    Ok(())
}

/// Kills the consumer once a continued page has arrived, proves nothing was
/// committed, then reruns it to completion and proves the whole baseline.
async fn kill_mid_restore(
    consumer: &Consumer,
    endpoint: &Endpoint,
    documents: &[DocumentId],
) -> Result<(), Box<dyn Error>> {
    let mut killed = consumer.spawn(endpoint, "page-two")?;
    killed.await_seam("page-two").await?;
    killed.kill()?;
    if killed.wait()?.success() {
        return Err(
            io::Error::other("the consumer exited cleanly before it could be killed").into(),
        );
    }
    assert_nothing_committed(consumer, documents)?;

    let mut rerun = consumer.spawn(endpoint, "none")?;
    let status = rerun.wait()?;
    if !status.success() {
        return Err(io::Error::other(format!(
            "the recovery rerun did not succeed (status={status:?})"
        ))
        .into());
    }
    assert_recovered(consumer, documents)
}

/// Kills a consumer after its baseline commit and proves the commit survived.
async fn kill_after_commit(
    consumer: &Consumer,
    endpoint: &Endpoint,
    documents: &[DocumentId],
) -> Result<(), Box<dyn Error>> {
    let mut post_commit = consumer.spawn(endpoint, "committed")?;
    post_commit.await_seam("committed").await?;
    post_commit.kill()?;
    if post_commit.wait()?.success() {
        return Err(io::Error::other(
            "the consumer exited after committing before it could be killed",
        )
        .into());
    }
    assert_recovered(consumer, documents).map_err(|error| {
        io::Error::other(format!(
            "state after a post-commit kill was not durable: {error}"
        ))
        .into()
    })
}

/// Asserts the interrupted consumer's durable store carries no baseline state.
fn assert_nothing_committed(
    consumer: &Consumer,
    documents: &[DocumentId],
) -> Result<(), Box<dyn Error>> {
    let interrupted = consumer.reopen()?;
    if interrupted.checkpoint(consumer.peer)?.is_some() {
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

/// Asserts the consumer installed the whole baseline and its checkpoint.
fn assert_recovered(consumer: &Consumer, documents: &[DocumentId]) -> Result<(), Box<dyn Error>> {
    let recovered = consumer.reopen()?;
    if recovered.checkpoint(consumer.peer)?.is_none() {
        return Err(io::Error::other("the recovered consumer has no checkpoint").into());
    }
    for (index, document) in documents.iter().enumerate() {
        let expected = Some(Some(Item::Scalar(Scalar::Integer(seeded_value(index)?))));
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

/// Owns a spawned recovery consumer. A held child blocks only on its stdin
/// pipe, which closes when this handle (or the whole test process) goes away,
/// so an early return or panic releases it to finish and exit on its own.
#[derive(Debug)]
struct ConsumerChild {
    /// The consumer process.
    child: Child,
}

impl ConsumerChild {
    /// Blocks (off the async workers) until the child reports `seam` on
    /// stdout, failing if it exits first.
    async fn await_seam(&mut self, seam: &'static str) -> Result<(), Box<dyn Error>> {
        let stdout = self
            .child
            .stdout
            .take()
            .ok_or_else(|| io::Error::other("the consumer stdout is not piped"))?;
        let reader = lgwks_std::task::try_spawn_blocking(move || -> io::Result<bool> {
            let wanted = format!("seam {seam}");
            for line in BufReader::new(stdout).lines() {
                if line? == wanted {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;
        let reached = reader.await?;
        if reached {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "the consumer exited before {seam} (kill seam never reached)"
            ))
            .into())
        }
    }

    /// Signals the consumer child to terminate.
    fn kill(&mut self) -> io::Result<()> {
        self.child.kill()
    }

    /// Waits for the consumer child's exit status.
    fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
        self.child.wait()
    }
}
