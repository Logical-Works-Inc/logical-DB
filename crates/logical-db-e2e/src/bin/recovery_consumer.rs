//! External recovery consumer: install a paged baseline over real HTTP.
//!
//! Spawned as a real child process by the process-kill recovery journey. It
//! reports each seam it reaches as one line on stdout. Told to hold at a seam,
//! it then blocks on stdin until the parent kills it or closes the pipe, so the
//! parent terminates it at an exact point with no sleep or timer:
//!
//! - `page-two`: the first *continued* baseline page has arrived; the
//!   accumulator holds earlier pages and the replica has committed nothing.
//! - `committed`: `synchronize` has returned, so the baseline is durable.
//!
//! A run with `hold=none` recovers the whole baseline and reports it, standing
//! in for the external consumer.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32 S33
//! S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{
    io::{self, Read, Write},
    path::PathBuf,
};

use logical_db::{
    Database, PeerId, ReplicaId, Scope, SyncLimits, Transport,
    http::{Endpoint, HttpError, HttpTransport, StaticBearer},
    transport::{
        BaselineRequest, BaselineResult, PullRequest, PullResult, PushRequest, PushResponse,
    },
};
use logical_db_e2e::support::{ClientJourneyError, parse_identifier};

/// The seam this run holds at, if any.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Hold {
    /// Run to completion.
    Never,
    /// Hold once the first continued baseline page has arrived.
    PageTwo,
    /// Hold once the baseline commit is durable.
    Committed,
}

impl Hold {
    /// Parses the `hold` argument.
    fn parse(text: &str) -> Result<Self, io::Error> {
        match text {
            "none" => Ok(Self::Never),
            "page-two" => Ok(Self::PageTwo),
            "committed" => Ok(Self::Committed),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("recovery-consumer: unknown hold {other:?}"),
            )),
        }
    }
}

/// Reports `seam` on stdout and, when this run holds there, blocks on stdin
/// until the parent kills this process or closes the pipe.
fn reach(seam: &str, hold: bool) -> Result<(), io::Error> {
    let line = format!("seam {seam}\n");
    io::stdout().lock().write_all(line.as_bytes())?;
    io::stdout().lock().flush()?;
    if hold {
        let mut released = Vec::new();
        io::stdin().lock().read_to_end(&mut released)?;
    }
    Ok(())
}

/// HTTP transport wrapper that reports the continued-page seam and can hold there.
#[derive(Debug)]
struct SeamTransport {
    /// Real HTTP transport every call delegates to.
    inner: HttpTransport<StaticBearer>,
    /// Whether to hold at the continued-page seam.
    hold: bool,
    /// The seam report outcome, once the continued page has arrived. A failed
    /// report is returned after synchronization; `HttpError` has no I/O case.
    reported: Option<Result<(), io::Error>>,
}

impl Transport for SeamTransport {
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
        if continued && self.reported.is_none() && matches!(result, BaselineResult::Artifact(_)) {
            self.reported = Some(reach("page-two", self.hold));
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
fn main() -> Result<(), ClientJourneyError> {
    lgwks_bot::rt::runtime::Builder::new()
        .worker_threads(std::num::NonZeroUsize::new(2))
        .build()?
        .block_on(recover())
}

/// Recovers the baseline over HTTP, holding at the requested seam.
///
/// Arguments: redb path, account, space, replica, peer, endpoint, bearer, hold.
async fn recover() -> Result<(), ClientJourneyError> {
    let arguments = std::env::args().collect::<Vec<_>>();
    let path = PathBuf::from(required(&arguments, 1, "redb path")?);
    let scope = Scope::new(
        parse_identifier(&required(&arguments, 2, "account")?, "account")?,
        parse_identifier(&required(&arguments, 3, "space")?, "space")?,
    );
    let replica: ReplicaId = parse_identifier(&required(&arguments, 4, "replica")?, "replica")?;
    let peer: PeerId = parse_identifier(&required(&arguments, 5, "peer")?, "peer")?;
    let endpoint = Endpoint::parse(&required(&arguments, 6, "endpoint")?)?;
    let bearer = StaticBearer::new(&required(&arguments, 7, "bearer")?)?;
    let hold = Hold::parse(&required(&arguments, 8, "hold")?)?;

    let database = Database::open(&path, scope, replica)?;
    let mut transport = SeamTransport {
        inner: HttpTransport::new(endpoint, bearer)?,
        hold: hold == Hold::PageTwo,
        reported: None,
    };
    let report = database
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    if let Some(seam) = transport.reported {
        seam?;
    }
    if !report.recovered {
        return Err(io::Error::other("recovery-consumer: no baseline was installed").into());
    }
    reach("committed", hold == Hold::Committed)?;
    let verdict = format!(
        "PASS recovery-consumer recovered={} pushed={} pulled={}\n",
        report.recovered, report.pushed, report.pulled
    );
    io::stdout().lock().write_all(verdict.as_bytes())?;
    Ok(())
}
