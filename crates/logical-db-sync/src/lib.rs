//! Bounded, transport-neutral push/pull orchestration.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::expect_used, clippy::unwrap_used))]

use std::{error::Error, fmt, future::Future};

use lgwks_std::hash::Hasher;
use logical_db_model::{
    DocumentId, MAX_BASELINE_BYTES, OperationDigest, OperationId, PeerId, ReplicaId, Scope,
    SnapshotArtifact,
};
use logical_db_protocol::{
    BaselineAccumulator, BaselinePage, BaselineRequest, ChangeEnvelope, Checkpoint,
    PROTOCOL_VERSION, ProtocolError, PullPage, PullRequest, PushOutcome, PushRequest, PushResponse,
};

/// Whether `count` exceeds a wire budget; a budget wider than `usize` is never exceeded.
fn exceeds(count: usize, budget: u64) -> bool {
    matches!(usize::try_from(budget), Ok(budget) if count > budget)
}

/// Maximum pull pages processed by one foreground synchronization cycle.
pub const MAX_PAGES_PER_CYCLE: usize = 32;

/// Maximum baseline pages one recovery may request: the whole aggregate
/// artifact at the smallest legal page (`MAX_BASELINE_DOCUMENTS` divided by
/// `MAX_BASELINE_PAGE_DOCUMENTS`).
pub const MAX_RECOVERY_PAGES: u16 = 256;

mod change;
pub use change::{InboundChange, OutboundChange};

/// Embedded-replica surface required by synchronization.
pub trait ReplicaPort {
    /// Replica-specific failure.
    type Error;

    /// Fixed logical ownership scope.
    fn scope(&self) -> Scope;
    /// Durable installation identity.
    fn replica(&self) -> ReplicaId;
    /// Durably binds the globally acknowledged outbox to one upstream service.
    ///
    /// # Errors
    ///
    /// Returns a topology error if this replica is already bound elsewhere.
    fn bind_upstream(&self, peer: PeerId) -> Result<(), Self::Error>;
    /// Bounded unacknowledged local changes.
    ///
    /// # Errors
    ///
    /// Returns the replica's durable read or validation failure.
    fn pending(&self, limit: usize) -> Result<Vec<OutboundChange>, Self::Error>;
    /// Atomically advances the durable upload acknowledgement for `peer`.
    ///
    /// # Errors
    ///
    /// Returns the replica's durable commit or validation failure.
    fn acknowledge(&self, peer: PeerId, through: u64) -> Result<(), Self::Error>;
    /// Last atomically installed opaque peer checkpoint.
    ///
    /// # Errors
    ///
    /// Returns the replica's durable read or corruption failure.
    fn checkpoint(&self, peer: PeerId) -> Result<Option<Vec<u8>>, Self::Error>;
    /// Atomically installs changes and the replacement peer checkpoint.
    ///
    /// # Errors
    ///
    /// Returns the replica's validation, merge, or durable commit failure.
    fn install(
        &self,
        peer: PeerId,
        changes: &[InboundChange],
        checkpoint: &[u8],
    ) -> Result<(), Self::Error>;
    /// Atomically installs a verified state-bearing baseline and its checkpoint.
    ///
    /// The artifact has already reproduced its sealed descriptor digest. A
    /// replica must commit the document states and the replacement checkpoint
    /// in one durable transition, leaving both unchanged on any failure.
    ///
    /// # Errors
    ///
    /// Returns the replica's validation, merge, or durable commit failure.
    fn install_baseline(
        &self,
        peer: PeerId,
        artifact: &SnapshotArtifact,
        checkpoint: &[u8],
    ) -> Result<(), Self::Error>;
}

/// Result of requesting one pull page.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PullResult {
    /// A normal bounded page.
    Page(PullPage),
    /// The service no longer recognizes the supplied checkpoint.
    CheckpointExpired,
    /// The retained prefix was collected; the replica must install a baseline.
    BaselineRequired,
}

/// Result of requesting the state-bearing recovery baseline.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BaselineResult {
    /// A bounded state-bearing artifact and its replacement checkpoint.
    Artifact(BaselinePage),
    /// No baseline is sealed, so the durable log still covers every position.
    Unavailable,
}

/// Network or in-process protocol transport.
pub trait Transport {
    /// Transport-specific failure.
    type Error;

    /// Uploads a bounded immutable page.
    ///
    /// # Errors
    ///
    /// Returns a transport-specific failure without implying commit status.
    fn push(
        &mut self,
        request: PushRequest,
    ) -> impl Future<Output = Result<PushResponse, Self::Error>>;
    /// Downloads one bounded page or reports explicit checkpoint expiry.
    ///
    /// # Errors
    ///
    /// Returns a transport-specific request or response failure.
    fn pull(
        &mut self,
        request: PullRequest,
    ) -> impl Future<Output = Result<PullResult, Self::Error>>;
    /// Downloads the state-bearing baseline a recovering replica must install.
    ///
    /// # Errors
    ///
    /// Returns a transport-specific request or response failure.
    fn baseline(
        &mut self,
        request: BaselineRequest,
    ) -> impl Future<Output = Result<BaselineResult, Self::Error>>;
}

/// Where one recovery attempt has gotten to, and whether it has already started.
///
/// `after` and `restarted` only make sense together: a recovery that has already
/// started may not start again on the same cycle, and the cursor it advances is
/// the one it must carry. Keeping them in one value keeps that coupling in the
/// type instead of in the call site's argument list.
#[derive(Clone, Debug, Default)]
struct RecoveryCursor {
    /// The durable position already covered by a page, if any.
    after: Option<Checkpoint>,
    /// Whether a recovery attempt has already begun on this cycle.
    restarted: bool,
}

/// Resource ceilings for one synchronization cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SyncLimits {
    /// Maximum changes in each page.
    pub changes_per_page: u16,
    /// Maximum encoded change bytes in each upload or pull page.
    pub bytes_per_page: u32,
    /// Maximum pull pages before yielding to the host.
    pub pages_per_cycle: u8,
    /// Maximum baseline pages one recovery may request before refusing.
    pub recovery_pages: u16,
    /// Maximum encoded baseline bytes one recovery may request before refusing.
    pub recovery_bytes: usize,
}

impl Default for SyncLimits {
    fn default() -> Self {
        Self {
            changes_per_page: 128,
            bytes_per_page: 4 * 1_048_576,
            pages_per_cycle: 16,
            recovery_pages: MAX_RECOVERY_PAGES,
            recovery_bytes: usize::from(MAX_RECOVERY_PAGES).saturating_mul(MAX_BASELINE_BYTES),
        }
    }
}

impl SyncLimits {
    /// Rebuilds the limits through the protocol validator so cycle ceilings stay wire-legal.
    fn validate(self) -> Result<Self, ProtocolError> {
        let request = PullRequest::new(
            PROTOCOL_VERSION,
            Scope::new(
                logical_db_model::AccountId::new(),
                logical_db_model::SpaceId::new(),
            ),
            ReplicaId::new(),
            OperationDigest::new([0; 32]),
            None,
            self.changes_per_page,
            self.bytes_per_page,
        );
        request.validate()?;
        if self.pages_per_cycle == 0 || usize::from(self.pages_per_cycle) > MAX_PAGES_PER_CYCLE {
            return Err(ProtocolError::PageLimit);
        }
        if self.recovery_pages == 0 || self.recovery_pages > MAX_RECOVERY_PAGES {
            return Err(ProtocolError::PageLimit);
        }
        if self.recovery_bytes < MAX_BASELINE_BYTES {
            return Err(ProtocolError::PageLimit);
        }
        Ok(self)
    }
}

/// Observable result of one bounded cycle.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct SyncReport {
    /// Local changes accepted or already present remotely.
    pub pushed: usize,
    /// Remote changes atomically installed locally.
    pub pulled: usize,
    /// Pull pages installed.
    pub pages: usize,
    /// Whether an expired checkpoint forced a full pull.
    pub full_resync: bool,
    /// Whether a state-bearing baseline was installed during this cycle.
    pub recovered: bool,
    /// Whether unsent local changes or remote pages remain after this bounded cycle.
    pub more: bool,
}

/// Typed synchronization failure preserving both boundary error types.
#[derive(Debug)]
#[non_exhaustive]
pub enum SyncError<ReplicaError, TransportError> {
    /// Embedded replica failed before or during a durable transition.
    Replica(ReplicaError),
    /// Transport failed; no acknowledgement is inferred.
    Transport(TransportError),
    /// Wire version, bounds, scope, or correlation was invalid.
    Protocol(ProtocolError),
    /// Service explicitly rejected one uploaded operation.
    Rejected {
        /// Operation rejected by the service.
        operation: OperationId,
        /// Stable reason needed for policy-aware recovery.
        code: logical_db_protocol::RejectionCode,
    },
    /// Host requested cancellation between atomic operations.
    Cancelled,
    /// Recovery exceeded its declared request or byte budget before completing.
    RecoveryBudgetExhausted,
}

impl<R: fmt::Display, T: fmt::Display> fmt::Display for SyncError<R, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Replica(ref error) => {
                write!(formatter, "replica synchronization failed: {error}")
            }
            Self::Transport(ref error) => {
                write!(formatter, "transport synchronization failed: {error}")
            }
            Self::Protocol(ref error) => {
                write!(formatter, "protocol synchronization failed: {error}")
            }
            Self::Rejected {
                ref operation,
                ref code,
            } => {
                write!(
                    formatter,
                    "service rejected operation {operation}: {code:?}"
                )
            }
            Self::Cancelled => formatter.write_str("synchronization was cancelled"),
            Self::RecoveryBudgetExhausted => {
                formatter.write_str("recovery exceeded its bounded request or byte budget")
            }
        }
    }
}

impl<R: Error + 'static, T: Error + 'static> Error for SyncError<R, T> {}

/// Runs one bounded push-then-pull cycle.
///
/// Cancellation is observed only between atomic replica/transport operations.
/// A transport error never advances the local upload acknowledgement.
///
/// # Errors
///
/// Returns the exact replica, transport, protocol, rejection, or cancellation
/// boundary that stopped progress.
pub async fn synchronize<R, T>(
    replica: &R,
    peer: PeerId,
    transport: &mut T,
    limits: SyncLimits,
    mut cancelled: impl FnMut() -> bool,
) -> Result<SyncReport, SyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
{
    let limits = limits.validate().map_err(SyncError::Protocol)?;
    if cancelled() {
        return Err(SyncError::Cancelled);
    }
    replica.bind_upstream(peer).map_err(SyncError::Replica)?;
    let (pushed, upload_more) = push_pending(
        replica,
        peer,
        transport,
        limits.changes_per_page,
        limits.bytes_per_page,
    )
    .await?;
    let mut report = SyncReport {
        pushed,
        ..SyncReport::default()
    };

    if cancelled() {
        return Err(SyncError::Cancelled);
    }
    let durable_checkpoint = replica.checkpoint(peer).map_err(SyncError::Replica)?;
    // One cursor for the whole cycle: `recovery.after` is what `pull_and_install`
    // resumes from and what a baseline install advances. Splitting them leaves
    // the pull loop on the pre-recovery position, which re-enters recovery and
    // trips `restarted` on the same cycle.
    let mut recovery = RecoveryCursor {
        after: durable_checkpoint
            .map(Checkpoint::from_wire)
            .transpose()
            .map_err(SyncError::Protocol)?,
        restarted: false,
    };
    while report.pages < usize::from(limits.pages_per_cycle) {
        let Some(page) =
            pull_and_install(replica, peer, transport, limits, &mut recovery.after).await?
        else {
            if install_recovery(
                replica,
                peer,
                transport,
                &mut recovery,
                limits,
                &mut cancelled,
                &mut report,
            )
            .await?
            {
                report.recovered = true;
            } else {
                report.full_resync = true;
            }
            continue;
        };
        let inbound = page.changes().len();
        report.pulled = report.pulled.saturating_add(inbound);
        report.pages = report.pages.saturating_add(1);
        let download_more = page.more();
        report.more = upload_more || download_more;
        recovery.after = Some(page.into_parts().1);
        if !download_more {
            return Ok(report);
        }
        if cancelled() {
            return Err(SyncError::Cancelled);
        }
    }
    Ok(report)
}

/// Drains at most one pending page within the push byte and count budgets.
async fn push_pending<R, T>(
    replica: &R,
    peer: PeerId,
    transport: &mut T,
    page_limit: u16,
    byte_limit: u32,
) -> Result<(usize, bool), SyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
{
    let available = replica
        .pending(usize::from(page_limit))
        .map_err(SyncError::Replica)?;
    if available.is_empty() {
        return Ok((0, false));
    }
    let mut aggregate_bytes = 0usize;
    let mut pending = Vec::with_capacity(available.len());
    for change in available {
        let next_bytes = aggregate_bytes
            .checked_add(change.bytes.len())
            .ok_or(SyncError::Protocol(ProtocolError::PageLimit))?;
        if exceeds(next_bytes, u64::from(byte_limit)) {
            break;
        }
        aggregate_bytes = next_bytes;
        pending.push(change);
    }
    let Some(through) = pending.last().map(|change| change.sequence) else {
        return Err(SyncError::Protocol(ProtocolError::PageLimit));
    };
    let request = PushRequest::new(
        PROTOCOL_VERSION,
        replica.replica(),
        pending
            .iter()
            .map(|change| {
                ChangeEnvelope::new(
                    replica.scope(),
                    change.document,
                    replica.replica(),
                    change.operation,
                    change.request,
                    change.digest,
                    change.bytes.clone(),
                )
            })
            .collect(),
    );
    request.validate().map_err(SyncError::Protocol)?;
    let response = transport
        .push(request.clone())
        .await
        .map_err(SyncError::Transport)?;
    response.validate(&request).map_err(SyncError::Protocol)?;
    for outcome in response.outcomes() {
        if let PushOutcome::Rejected { operation, code } = *outcome {
            return Err(SyncError::Rejected { operation, code });
        }
    }
    replica
        .acknowledge(peer, through)
        .map_err(SyncError::Replica)?;
    let more = !replica.pending(1).map_err(SyncError::Replica)?.is_empty();
    Ok((pending.len(), more))
}

/// Builds the pull request digest binding scope, replica, cursor, and limits.
fn pull_request_digest(
    scope: Scope,
    replica: ReplicaId,
    after: Option<&Checkpoint>,
    limits: SyncLimits,
) -> OperationDigest {
    let mut digest = request_prefix(b"logical-db/pull/v1", scope, replica, after);
    digest.update(&limits.changes_per_page.to_be_bytes());
    digest.update(&limits.bytes_per_page.to_be_bytes());
    OperationDigest::new(*digest.finalize().as_bytes())
}

/// Hashes the domain tag, scope, replica, and optional checkpoint shared by
/// every request digest; each request then appends its own cursor or limits.
fn request_prefix(
    domain: &[u8],
    scope: Scope,
    replica: ReplicaId,
    after: Option<&Checkpoint>,
) -> Hasher {
    let mut digest = Hasher::new();
    digest.update(domain);
    digest.update(scope.account().as_uuid().as_bytes());
    digest.update(scope.space().as_uuid().as_bytes());
    digest.update(replica.as_uuid().as_bytes());
    match after {
        Some(checkpoint) => digest.update(&[1]).update(checkpoint.as_wire()),
        None => digest.update(&[0]),
    };
    digest
}

/// Fetches exactly one pull page from the transport.
async fn fetch_page<T: Transport>(
    transport: &mut T,
    request: PullRequest,
) -> Result<PullResult, T::Error> {
    transport.pull(request).await
}

/// Fetches and installs pull pages for one bounded synchronize cycle.
async fn pull_and_install<R, T>(
    replica: &R,
    peer: PeerId,
    transport: &mut T,
    limits: SyncLimits,
    after: &mut Option<Checkpoint>,
) -> Result<Option<PullPage>, SyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
{
    let request = PullRequest::new(
        PROTOCOL_VERSION,
        replica.scope(),
        replica.replica(),
        pull_request_digest(replica.scope(), replica.replica(), after.as_ref(), limits),
        after.clone(),
        limits.changes_per_page,
        limits.bytes_per_page,
    );
    request.validate().map_err(SyncError::Protocol)?;
    let pulled = fetch_page(transport, request)
        .await
        .map_err(SyncError::Transport)?;
    let page = match pulled {
        PullResult::Page(page) => page,
        PullResult::CheckpointExpired if after.is_none() => {
            return Err(SyncError::Protocol(ProtocolError::InvalidCheckpoint));
        }
        PullResult::CheckpointExpired | PullResult::BaselineRequired => return Ok(None),
    };
    page.validate(replica.scope())
        .map_err(SyncError::Protocol)?;
    let inbound = page
        .changes()
        .iter()
        .map(|change| InboundChange {
            document: change.document(),
            digest: change.digest(),
            bytes: change.bytes().to_vec(),
        })
        .collect::<Vec<_>>();
    replica
        .install(peer, &inbound, page.checkpoint().as_wire())
        .map_err(SyncError::Replica)?;
    Ok(Some(page))
}

/// Installs the state-bearing baseline after the service collected the prefix.
///
/// Fetches one bounded page at a time, folds them into the descriptor identity,
/// and installs the assembled document state plus its checkpoint in one durable
/// replica transition. A page that repeats or skips a cursor, changes the
/// descriptor, or reports no baseline mid-sequence is a loud protocol failure,
/// never a partial install. `false` reports that no baseline is sealed for the
/// scope, so the durable log still covers every position and the caller restarts
/// from the floor.
async fn install_recovery<R, T>(
    replica: &R,
    peer: PeerId,
    transport: &mut T,
    recovery: &mut RecoveryCursor,
    limits: SyncLimits,
    cancelled: &mut impl FnMut() -> bool,
    report: &mut SyncReport,
) -> Result<bool, SyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
{
    if recovery.restarted {
        return Err(SyncError::Protocol(ProtocolError::InvalidCheckpoint));
    }
    recovery.restarted = true;
    if cancelled() {
        return Err(SyncError::Cancelled);
    }
    let BaselineResult::Artifact(page) =
        request_baseline_page(transport, replica, recovery.after.as_ref(), None).await?
    else {
        recovery.after = None;
        return Ok(false);
    };
    let descriptor = page
        .validate_page(replica.scope())
        .map_err(SyncError::Protocol)?;
    let declared = u64::from(descriptor.document_count());
    let mut accumulator = BaselineAccumulator::new(replica.scope(), descriptor);
    let mut checkpoint = page.checkpoint().clone();
    let mut fetched = 1usize;
    let mut bytes = page_snapshot_bytes(&page);
    let mut accumulated = page.documents().len();
    if exceeds(accumulated, declared) || bytes > limits.recovery_bytes {
        return Err(SyncError::RecoveryBudgetExhausted);
    }
    let mut complete = accumulator.push(&page).map_err(SyncError::Protocol)?;
    let mut cursor = page.next_document();
    while !complete {
        let Some(next) = cursor else {
            return Err(SyncError::Protocol(ProtocolError::NonProgressPage));
        };
        if cancelled() {
            return Err(SyncError::Cancelled);
        }
        if fetched >= usize::from(limits.recovery_pages) {
            return Err(SyncError::RecoveryBudgetExhausted);
        }
        let BaselineResult::Artifact(page) =
            request_baseline_page(transport, replica, recovery.after.as_ref(), Some(next)).await?
        else {
            return Err(SyncError::Protocol(ProtocolError::BaselineMismatch));
        };
        fetched = fetched.saturating_add(1);
        bytes = bytes
            .checked_add(page_snapshot_bytes(&page))
            .ok_or(SyncError::RecoveryBudgetExhausted)?;
        accumulated = accumulated
            .checked_add(page.documents().len())
            .ok_or(SyncError::Protocol(ProtocolError::BaselineMismatch))?;
        if exceeds(accumulated, declared) || bytes > limits.recovery_bytes {
            return Err(SyncError::RecoveryBudgetExhausted);
        }
        checkpoint = page.checkpoint().clone();
        complete = accumulator.push(&page).map_err(SyncError::Protocol)?;
        cursor = page.next_document();
    }
    if cancelled() {
        return Err(SyncError::Cancelled);
    }
    let artifact = accumulator.finish().map_err(SyncError::Protocol)?;
    replica
        .install_baseline(peer, &artifact, checkpoint.as_wire())
        .map_err(SyncError::Replica)?;
    recovery.after = Some(checkpoint);
    report.pages = report.pages.saturating_add(fetched);
    Ok(true)
}

/// Encoded snapshot bytes carried by one baseline page.
fn page_snapshot_bytes(page: &BaselinePage) -> usize {
    page.documents()
        .iter()
        .map(|document| document.snapshot().len())
        .fold(0, usize::saturating_add)
}

/// Requests exactly one bounded baseline page at one exclusive cursor.
async fn request_baseline_page<R, T>(
    transport: &mut T,
    replica: &R,
    after: Option<&Checkpoint>,
    cursor: Option<DocumentId>,
) -> Result<BaselineResult, SyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
{
    let request = BaselineRequest::new(
        PROTOCOL_VERSION,
        replica.scope(),
        replica.replica(),
        baseline_request_digest(replica.scope(), replica.replica(), after, cursor),
        after.cloned(),
        cursor,
    );
    request.validate().map_err(SyncError::Protocol)?;
    fetch_baseline(transport, request)
        .await
        .map_err(SyncError::Transport)
}

/// Fetches exactly one baseline page from the transport.
async fn fetch_baseline<T: Transport>(
    transport: &mut T,
    request: BaselineRequest,
) -> Result<BaselineResult, T::Error> {
    transport.baseline(request).await
}

/// Builds the baseline request digest binding scope, replica, and cursors.
fn baseline_request_digest(
    scope: Scope,
    replica: ReplicaId,
    after: Option<&Checkpoint>,
    after_document: Option<DocumentId>,
) -> OperationDigest {
    let mut digest = request_prefix(b"logical-db/baseline/v1", scope, replica, after);
    match after_document {
        Some(cursor) => {
            digest.update(&[1]);
            digest.update(cursor.as_uuid().as_bytes());
        }
        None => {
            digest.update(&[0]);
        }
    }
    OperationDigest::new(*digest.finalize().as_bytes())
}

#[cfg(test)]
mod tests;
