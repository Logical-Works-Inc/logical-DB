//! External recovery consumer: install a paged baseline over real HTTP.
//!
//! Spawned as a real child process by the process-kill recovery journey. On the
//! first *continued* baseline page it writes a ready file and pauses, so the
//! parent can terminate it while the accumulator already holds earlier pages but
//! before the replica has committed anything. A second seam signals *after*
//! `synchronize` returns, so the parent can terminate a child whose baseline
//! commit is already durable and check what survives it. A run with both pauses
//! at zero recovers the whole baseline and reports it, standing in for the
//! external consumer.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32 S33
//! S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]
#![expect(
    clippy::print_stderr,
    reason = "src/bin/ is the PRINTS-exempt binary surface per the execution contract"
)]
#![expect(
    clippy::print_stdout,
    reason = "src/bin/ is the PRINTS-exempt binary surface per the execution contract"
)]

use std::{error::Error, io, path::PathBuf, time::Duration};

use logical_db::{
    AccountId, Database, PeerId, ReplicaId, Scope, SpaceId, SyncLimits, Transport,
    http::{HttpError, HttpTransport, StaticBearer, Url},
    transport::{
        BaselineRequest, BaselineResult, PullRequest, PullResult, PushRequest, PushResponse,
    },
};
use uuid::Uuid;

/// HTTP transport wrapper that signals the recovery seam and can stall there.
struct PausingTransport {
    /// Real HTTP transport every call delegates to.
    inner: HttpTransport<StaticBearer>,
    /// File the orchestrator watches for the recovery-seam signal.
    ready: PathBuf,
    /// How long to stall once the seam has been signalled.
    pause: Duration,
}

impl Transport for PausingTransport {
    type Error = HttpError;

    async fn push(&mut self, request: PushRequest) -> Result<PushResponse, Self::Error> {
        self.inner.push(request).await
    }

    async fn pull(&mut self, request: PullRequest) -> Result<PullResult, Self::Error> {
        self.inner.pull(request).await
    }

    async fn baseline(&mut self, request: BaselineRequest) -> Result<BaselineResult, Self::Error> {
        let continued = request.after_document.is_some();
        let result = self.inner.baseline(request).await?;
        if continued && matches!(result, BaselineResult::Artifact(_)) && !self.pause.is_zero() {
            if let Err(error) = std::fs::write(&self.ready, b"accumulated\n") {
                eprintln!("recovery-consumer: ready signal: {error}");
            }
            std::thread::park_timeout(self.pause);
        }
        Ok(result)
    }
}

/// Fetches one positional CLI argument, naming it when the slot is missing.
fn required(arguments: &[String], index: usize, name: &str) -> Result<String, io::Error> {
    arguments.get(index).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("recovery-consumer: missing argument {index} ({name})"),
        )
    })
}

/// Boots the async runtime and runs the recovery-consumer journey.
fn main() -> Result<(), Box<dyn Error>> {
    lgwks_bot::Runtime::new()?.block_on(recover())
}

/// Recovers the baseline over HTTP, pauses at the seam, then commits.
async fn recover() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().collect::<Vec<_>>();
    let path = PathBuf::from(required(&arguments, 1, "redb path")?);
    let scope = Scope::new(
        AccountId::from_uuid(Uuid::parse_str(&required(&arguments, 2, "account")?)?),
        SpaceId::from_uuid(Uuid::parse_str(&required(&arguments, 3, "space")?)?),
    );
    let replica = ReplicaId::from_uuid(Uuid::parse_str(&required(&arguments, 4, "replica")?)?);
    let peer = PeerId::from_uuid(Uuid::parse_str(&required(&arguments, 5, "peer")?)?);
    let endpoint: Url = required(&arguments, 6, "endpoint")?.parse()?;
    let bearer = StaticBearer::new(&required(&arguments, 7, "bearer")?)?;
    let ready = PathBuf::from(required(&arguments, 8, "ready path")?);
    let pause = Duration::from_millis(required(&arguments, 9, "pause ms")?.parse()?);
    let committed = arguments.get(10).map(PathBuf::from);
    let post_pause = arguments
        .get(11)
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_millis)
        .unwrap_or_default();

    let database = Database::open(&path, scope, replica)?;
    let mut transport = PausingTransport {
        inner: HttpTransport::new(endpoint, bearer)?,
        ready,
        pause,
    };
    let report = database
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    if !report.recovered {
        eprintln!("recovery-consumer: no baseline was installed");
        return Err(io::Error::other("expected a state-bearing baseline").into());
    }
    if let Some(path) = committed.filter(|_| !post_pause.is_zero()) {
        if let Err(error) = std::fs::write(&path, b"committed\n") {
            eprintln!("recovery-consumer: committed signal: {error}");
        }
        std::thread::park_timeout(post_pause);
    }
    println!(
        "PASS recovery-consumer recovered={} pushed={} pulled={}",
        report.recovered, report.pushed, report.pulled
    );
    Ok(())
}
