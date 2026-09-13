//! Bounded, transport-neutral push/pull orchestration.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{error::Error, fmt, time::Duration};

use logical_db_model::{
    ChangeDigest, DocumentId, ModelError, NAMESPACE_CHECKPOINT_TTL_MS, NamespaceCheckpoint,
    NamespaceCheckpoints, NamespacePolicy, NetworkCondition, OperationDigest, OperationId, PeerId,
    ReplicaId, Scope, SnapshotArtifact, is_namespace_checkpoint_expired, validate_namespace,
};
use logical_db_protocol::{
    BaselinePage, BaselineRequest, ChangeEnvelope, Checkpoint, PROTOCOL_VERSION, ProtocolError,
    PullPage, PullRequest, PushOutcome, PushRequest, PushResponse, RejectionCode,
};
use sha2::{Digest, Sha256};

/// Maximum pull pages processed by one foreground synchronization cycle.
pub const MAX_PAGES_PER_CYCLE: usize = 32;

/// One durable local change with its private acknowledgement position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundChange {
    /// Local sequence acknowledged only after the whole push response validates.
    pub sequence: u64,
    /// Changed document.
    pub document: DocumentId,
    /// Retry identity.
    pub operation: OperationId,
    /// Canonical caller request identity.
    pub request: OperationDigest,
    /// Native CRDT change identity.
    pub digest: ChangeDigest,
    /// Verified canonical CRDT bytes.
    pub bytes: Vec<u8>,
}

/// One verified remote change handed to a replica for atomic installation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InboundChange {
    /// Changed document.
    pub document: DocumentId,
    /// Native CRDT change identity.
    pub digest: ChangeDigest,
    /// Canonical CRDT bytes.
    pub bytes: Vec<u8>,
}

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
pub enum BaselineResult {
    /// A bounded state-bearing artifact and its replacement checkpoint.
    Artifact(BaselinePage),
    /// No baseline is sealed, so the durable log still covers every position.
    Unavailable,
}

/// Network or in-process protocol transport.
#[allow(async_fn_in_trait)]
pub trait Transport {
    /// Transport-specific failure.
    type Error;

    /// Uploads a bounded immutable page.
    ///
    /// # Errors
    ///
    /// Returns a transport-specific failure without implying commit status.
    async fn push(&mut self, request: PushRequest) -> Result<PushResponse, Self::Error>;
    /// Downloads one bounded page or reports explicit checkpoint expiry.
    ///
    /// # Errors
    ///
    /// Returns a transport-specific request or response failure.
    async fn pull(&mut self, request: PullRequest) -> Result<PullResult, Self::Error>;
    /// Downloads the state-bearing baseline a recovering replica must install.
    ///
    /// # Errors
    ///
    /// Returns a transport-specific request or response failure.
    async fn baseline(&mut self, request: BaselineRequest) -> Result<BaselineResult, Self::Error>;
}

/// Resource ceilings for one synchronization cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SyncLimits {
    /// Maximum changes in each page.
    pub changes_per_page: u16,
    /// Maximum encoded change bytes in each upload or pull page.
    pub bytes_per_page: u32,
    /// Maximum pull pages before yielding to the host.
    pub pages_per_cycle: u8,
}

impl Default for SyncLimits {
    fn default() -> Self {
        Self {
            changes_per_page: 128,
            bytes_per_page: 4 * 1_048_576,
            pages_per_cycle: 16,
        }
    }
}

impl SyncLimits {
    fn validate(self) -> Result<Self, ProtocolError> {
        let request = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope: Scope::new(
                logical_db_model::AccountId::new(),
                logical_db_model::SpaceId::new(),
            ),
            replica: ReplicaId::new(),
            request: OperationDigest::new([0; 32]),
            after: None,
            max_changes: self.changes_per_page,
            max_bytes: self.bytes_per_page,
        };
        request.validate()?;
        if self.pages_per_cycle == 0 || usize::from(self.pages_per_cycle) > MAX_PAGES_PER_CYCLE {
            eprintln!("logical-db-sync: pages per cycle exceeds the configured bound");
            return Err(ProtocolError::PageLimit);
        }
        Ok(self)
    }
}

/// Observable result of one bounded cycle.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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
}

impl<R: fmt::Display, T: fmt::Display> fmt::Display for SyncError<R, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Replica(error) => write!(formatter, "replica synchronization failed: {error}"),
            Self::Transport(error) => {
                write!(formatter, "transport synchronization failed: {error}")
            }
            Self::Protocol(error) => write!(formatter, "protocol synchronization failed: {error}"),
            Self::Rejected { operation, code } => {
                write!(
                    formatter,
                    "service rejected operation {operation}: {code:?}"
                )
            }
            Self::Cancelled => formatter.write_str("synchronization was cancelled"),
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
        eprintln!("logical-db-sync: synchronization cancelled before binding");
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
        eprintln!("logical-db-sync: synchronization cancelled before pull");
        return Err(SyncError::Cancelled);
    }
    let durable_checkpoint = replica.checkpoint(peer).map_err(SyncError::Replica)?;
    let mut after = durable_checkpoint
        .map(Checkpoint::from_wire)
        .transpose()
        .map_err(SyncError::Protocol)?;
    let mut restarted_after_expiry = false;
    while report.pages < usize::from(limits.pages_per_cycle) {
        let Some(page) = pull_and_install(replica, peer, transport, limits, &mut after).await?
        else {
            if install_recovery(
                replica,
                peer,
                transport,
                &mut after,
                &mut restarted_after_expiry,
            )
            .await?
            {
                report.recovered = true;
            } else {
                report.full_resync = true;
            }
            continue;
        };
        let inbound = page.changes.len();
        report.pulled += inbound;
        report.pages += 1;
        let download_more = page.more;
        report.more = upload_more || download_more;
        after = Some(page.checkpoint);
        if !download_more {
            return Ok(report);
        }
        if cancelled() {
            eprintln!("logical-db-sync: synchronization cancelled between pull pages");
            return Err(SyncError::Cancelled);
        }
    }
    Ok(report)
}

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
        if next_bytes > byte_limit as usize {
            break;
        }
        aggregate_bytes = next_bytes;
        pending.push(change);
    }
    if pending.is_empty() {
        eprintln!("logical-db-sync: pending upload cannot fit within byte limit");
        return Err(SyncError::Protocol(ProtocolError::PageLimit));
    }
    let request = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica: replica.replica(),
        changes: pending
            .iter()
            .map(|change| ChangeEnvelope {
                scope: replica.scope(),
                document: change.document,
                replica: replica.replica(),
                operation: change.operation,
                request: change.request,
                digest: change.digest,
                bytes: change.bytes.clone(),
            })
            .collect(),
    };
    request.validate().map_err(SyncError::Protocol)?;
    let response = transport
        .push(request.clone())
        .await
        .map_err(SyncError::Transport)?;
    response.validate(&request).map_err(SyncError::Protocol)?;
    for outcome in &response.outcomes {
        if let PushOutcome::Rejected { operation, code } = outcome {
            eprintln!("logical-db-sync: upload rejected operation {operation}: {code:?}");
            return Err(SyncError::Rejected {
                operation: *operation,
                code: *code,
            });
        }
    }
    let through = pending.last().map_or(0, |change| change.sequence);
    replica
        .acknowledge(peer, through)
        .map_err(SyncError::Replica)?;
    let more = !replica.pending(1).map_err(SyncError::Replica)?.is_empty();
    Ok((pending.len(), more))
}

fn pull_request_digest(
    scope: Scope,
    replica: ReplicaId,
    after: Option<&Checkpoint>,
    limits: SyncLimits,
) -> OperationDigest {
    let mut digest = Sha256::new();
    digest.update(b"logical-db/pull/v1");
    digest.update(scope.account().as_uuid().as_bytes());
    digest.update(scope.space().as_uuid().as_bytes());
    digest.update(replica.as_uuid().as_bytes());
    match after {
        Some(checkpoint) => {
            digest.update([1]);
            digest.update(checkpoint.as_wire());
        }
        None => digest.update([0]),
    }
    digest.update(limits.changes_per_page.to_be_bytes());
    digest.update(limits.bytes_per_page.to_be_bytes());
    OperationDigest::new(digest.finalize().into())
}

async fn fetch_page<T: Transport>(
    transport: &mut T,
    request: PullRequest,
) -> Result<PullResult, T::Error> {
    transport.pull(request).await
}

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
    let request = PullRequest {
        protocol: PROTOCOL_VERSION,
        scope: replica.scope(),
        replica: replica.replica(),
        request: pull_request_digest(replica.scope(), replica.replica(), after.as_ref(), limits),
        after: after.clone(),
        max_changes: limits.changes_per_page,
        max_bytes: limits.bytes_per_page,
    };
    request.validate().map_err(SyncError::Protocol)?;
    let pulled = fetch_page(transport, request)
        .await
        .map_err(SyncError::Transport)?;
    let page = match pulled {
        PullResult::Page(page) => page,
        PullResult::CheckpointExpired if after.is_none() => {
            eprintln!("logical-db-sync: checkpoint expired without a durable checkpoint");
            return Err(SyncError::Protocol(ProtocolError::InvalidCheckpoint));
        }
        PullResult::CheckpointExpired | PullResult::BaselineRequired => return Ok(None),
    };
    page.validate(replica.scope())
        .map_err(SyncError::Protocol)?;
    let inbound = page
        .changes
        .iter()
        .map(|change| InboundChange {
            document: change.document,
            digest: change.digest,
            bytes: change.bytes.clone(),
        })
        .collect::<Vec<_>>();
    replica
        .install(peer, &inbound, page.checkpoint.as_wire())
        .map_err(SyncError::Replica)?;
    Ok(Some(page))
}

/// Installs the state-bearing baseline after the service collected the prefix.
///
/// Returns whether a baseline was installed. `false` reports that no baseline is
/// sealed for the scope, so the durable log still covers every position and the
/// caller restarts from the floor. A second recovery request in one cycle is a
/// loud protocol failure, never a silent loop.
async fn install_recovery<R, T>(
    replica: &R,
    peer: PeerId,
    transport: &mut T,
    after: &mut Option<Checkpoint>,
    restarted: &mut bool,
) -> Result<bool, SyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
{
    if *restarted {
        eprintln!("logical-db-sync: recovery requested twice in one cycle");
        return Err(SyncError::Protocol(ProtocolError::InvalidCheckpoint));
    }
    *restarted = true;
    let request = BaselineRequest {
        protocol: PROTOCOL_VERSION,
        scope: replica.scope(),
        replica: replica.replica(),
        request: baseline_request_digest(replica.scope(), replica.replica(), after.as_ref()),
        after: after.clone(),
    };
    request.validate().map_err(SyncError::Protocol)?;
    match fetch_baseline(transport, request)
        .await
        .map_err(SyncError::Transport)?
    {
        BaselineResult::Artifact(page) => {
            let artifact = page
                .validate(replica.scope())
                .map_err(SyncError::Protocol)?;
            replica
                .install_baseline(peer, &artifact, page.checkpoint.as_wire())
                .map_err(SyncError::Replica)?;
            *after = Some(page.checkpoint);
            Ok(true)
        }
        BaselineResult::Unavailable => {
            *after = None;
            Ok(false)
        }
    }
}

async fn fetch_baseline<T: Transport>(
    transport: &mut T,
    request: BaselineRequest,
) -> Result<BaselineResult, T::Error> {
    transport.baseline(request).await
}

fn baseline_request_digest(
    scope: Scope,
    replica: ReplicaId,
    after: Option<&Checkpoint>,
) -> OperationDigest {
    let mut digest = Sha256::new();
    digest.update(b"logical-db/baseline/v1");
    digest.update(scope.account().as_uuid().as_bytes());
    digest.update(scope.space().as_uuid().as_bytes());
    digest.update(replica.as_uuid().as_bytes());
    match after {
        Some(checkpoint) => {
            digest.update([1]);
            digest.update(checkpoint.as_wire());
        }
        None => digest.update([0]),
    }
    OperationDigest::new(digest.finalize().into())
}

struct NamespacePullContext<'a, F> {
    namespace: &'a str,
    is_mine: &'a F,
    checkpoints: &'a mut NamespaceCheckpoints,
    now_unix_ms: u64,
    policy: NamespacePolicy,
    after: &'a mut Option<Checkpoint>,
}

async fn pull_namespace_page<R, T, F>(
    replica: &R,
    peer: PeerId,
    transport: &mut T,
    context: &mut NamespacePullContext<'_, F>,
) -> Result<Option<(usize, bool)>, NamespaceSyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
    F: Fn(DocumentId) -> bool,
{
    let request = PullRequest {
        protocol: PROTOCOL_VERSION,
        scope: replica.scope(),
        replica: replica.replica(),
        request: namespace_pull_digest(
            replica.scope(),
            replica.replica(),
            context.namespace,
            context.after.as_ref(),
            context.policy,
        ),
        after: context.after.clone(),
        max_changes: context.policy.changes_per_page(),
        max_bytes: context.policy.bytes_per_page(),
    };
    request.validate().map_err(NamespaceSyncError::Protocol)?;
    let pulled = fetch_page(transport, request)
        .await
        .map_err(NamespaceSyncError::Transport)?;
    let page = match pulled {
        PullResult::Page(page) => page,
        PullResult::CheckpointExpired | PullResult::BaselineRequired => {
            eprintln!("logical-db-sync: namespace checkpoint requires baseline recovery");
            return Err(NamespaceSyncError::Protocol(
                ProtocolError::InvalidCheckpoint,
            ));
        }
    };
    page.validate(replica.scope())
        .map_err(NamespaceSyncError::Protocol)?;
    let installed = install_namespace_page(
        replica,
        peer,
        context.namespace,
        &page,
        context.is_mine,
        context.checkpoints,
        context.now_unix_ms,
    )?;
    *context.after = Some(page.checkpoint);
    Ok(Some((installed, page.more)))
}

/// Layout version of one encoded orchestrator state blob.
pub const ORCHESTRATOR_STATE_VERSION: u16 = 1;
/// Exact byte length of one encoded orchestrator state blob.
pub const ORCHESTRATOR_STATE_BYTES: usize = 22;
/// Magic prefix rejecting foreign slot bytes before version checks.
const ORCHESTRATOR_STATE_MAGIC: [u8; 8] = *b"LDBSYNC1";
/// Default ceiling on synchronize cycles drained by one orchestrator run.
const DEFAULT_MAX_CYCLES: usize = 8;
/// Default first retryable-failure delay in milliseconds.
const DEFAULT_BASE_BACKOFF_MS: u64 = 1_000;
/// Default backoff ceiling in milliseconds.
const DEFAULT_MAX_BACKOFF_MS: u64 = 60_000;
/// Doubling steps folded into the exponential delay before the ceiling binds.
const MAX_BACKOFF_DOUBLINGS: u32 = 16;

/// One namespace sync domain: name, policy, and observation time.
///
/// The domain bundles everything one [`synchronize_namespace`] cycle needs
/// beyond the replica and transport handles. The namespace validates once at
/// construction; the timestamp decides checkpoint expiry for the cycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NamespaceDomain {
    namespace: String,
    policy: NamespacePolicy,
    now_unix_ms: u64,
}

impl NamespaceDomain {
    /// Binds a namespace to its policy at one observation time.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidIdentifierLength`] for an empty or
    /// oversized namespace.
    pub fn new(
        namespace: &str,
        policy: NamespacePolicy,
        now_unix_ms: u64,
    ) -> Result<Self, ModelError> {
        validate_namespace(namespace)?;
        Ok(Self {
            namespace: namespace.to_owned(),
            policy,
            now_unix_ms,
        })
    }

    /// Returns the namespace this domain drives.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the page budget and network condition for this domain.
    #[must_use]
    pub const fn policy(&self) -> NamespacePolicy {
        self.policy
    }

    /// Returns the observation time deciding checkpoint expiry.
    #[must_use]
    pub const fn now_unix_ms(&self) -> u64 {
        self.now_unix_ms
    }
}

/// Observable result of one bounded namespace cycle.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NamespaceSyncReport {
    /// Namespace this cycle drove.
    pub namespace: String,
    /// Remote changes for this namespace installed locally.
    pub pulled: usize,
    /// Pull pages installed, including pages carrying only foreign changes.
    pub pages: usize,
    /// Whether an expired cursor forced a full pull.
    pub full_resync: bool,
    /// Whether remote pages remain after this bounded cycle.
    pub more: bool,
    /// Whether the network condition deferred all work.
    pub deferred: bool,
}

/// Observable result of one namespace upload page.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NamespaceSendReport {
    /// Namespace whose pending changes were offered.
    pub namespace: String,
    /// Changes for this namespace accepted or already present remotely.
    pub uploaded: usize,
    /// Whether unacknowledged local changes remain after this page.
    pub more: bool,
    /// Whether the network condition deferred all work.
    pub deferred: bool,
}

/// Typed namespace synchronization failure preserving every boundary.
#[derive(Debug)]
pub enum NamespaceSyncError<ReplicaError, TransportError> {
    /// Embedded replica failed before or during a durable transition.
    Replica(ReplicaError),
    /// Transport failed; no checkpoint is inferred.
    Transport(TransportError),
    /// Wire version, bounds, scope, or correlation was invalid.
    Protocol(ProtocolError),
    /// Namespace name, policy, or checkpoint map was invalid.
    Model(ModelError),
    /// Service explicitly rejected one uploaded operation.
    Rejected {
        /// Operation rejected by the service.
        operation: OperationId,
        /// Stable reason needed for policy-aware recovery.
        code: RejectionCode,
    },
    /// Host requested cancellation between atomic operations.
    Cancelled,
}

impl<R: fmt::Display, T: fmt::Display> fmt::Display for NamespaceSyncError<R, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Replica(error) => {
                write!(formatter, "namespace synchronization failed: {error}")
            }
            Self::Transport(error) => {
                write!(formatter, "namespace transport failed: {error}")
            }
            Self::Protocol(error) => {
                write!(formatter, "namespace protocol failed: {error}")
            }
            Self::Model(error) => {
                write!(formatter, "namespace sync state failed: {error}")
            }
            Self::Rejected { operation, code } => {
                write!(
                    formatter,
                    "service rejected operation {operation}: {code:?}"
                )
            }
            Self::Cancelled => formatter.write_str("namespace synchronization was cancelled"),
        }
    }
}

impl<R: Error + 'static, T: Error + 'static> Error for NamespaceSyncError<R, T> {}

/// Runs one bounded pull cycle for a single namespace.
///
/// The cycle partitions pull state within the one upstream binding: the
/// single durable checkpoint slot holds a multiplexed map, and this domain
/// advances only its own entry while installing every page it pulls — pages
/// carrying only foreign changes install an empty change set so the cursor
/// still moves past other namespaces' traffic. A hot namespace therefore
/// never spends a cold namespace's pages or moves its cursor.
///
/// Cancellation is observed only between atomic replica/transport operations.
/// Mixing this driver with the global [`synchronize`] cycle on the same peer
/// fails closed at map decode instead of merging two cursor shapes.
///
/// # Errors
///
/// Returns the exact replica, transport, protocol, checkpoint-map, or
/// cancellation boundary that stopped progress.
pub async fn synchronize_namespace<R, T>(
    replica: &R,
    peer: PeerId,
    transport: &mut T,
    domain: &NamespaceDomain,
    is_mine: impl Fn(DocumentId) -> bool,
    mut cancelled: impl FnMut() -> bool,
) -> Result<NamespaceSyncReport, NamespaceSyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
{
    let namespace = domain.namespace();
    let policy = domain.policy();
    let now_unix_ms = domain.now_unix_ms();
    let mut report = NamespaceSyncReport {
        namespace: namespace.to_owned(),
        ..NamespaceSyncReport::default()
    };
    if policy.network() == NetworkCondition::Offline {
        report.deferred = true;
        return Ok(report);
    }
    if cancelled() {
        eprintln!("logical-db-sync: namespace synchronization cancelled before binding");
        return Err(NamespaceSyncError::Cancelled);
    }
    replica
        .bind_upstream(peer)
        .map_err(NamespaceSyncError::Replica)?;
    let start = namespace_start(replica, peer, namespace, now_unix_ms)?;
    let mut checkpoints = start.checkpoints;
    let mut after = start.after;
    if start.recovered {
        report.full_resync = true;
    }
    while report.pages < usize::from(effective_pages(policy)) {
        let mut context = NamespacePullContext {
            namespace,
            is_mine: &is_mine,
            checkpoints: &mut checkpoints,
            now_unix_ms,
            policy,
            after: &mut after,
        };
        let Some(page) = pull_namespace_page(replica, peer, transport, &mut context).await? else {
            report.full_resync = true;
            continue;
        };
        report.pulled += page.0;
        report.pages += 1;
        report.more = page.1;
        if !page.1 {
            return Ok(report);
        }
        if cancelled() {
            eprintln!("logical-db-sync: namespace synchronization cancelled between pull pages");
            return Err(NamespaceSyncError::Cancelled);
        }
    }
    Ok(report)
}

/// Uploads one bounded page of a single namespace's pending changes.
///
/// Delivery only: the shared upload acknowledgement is never advanced here,
/// so a quiet namespace escapes a chatty backlog without moving the global
/// watermark. The regular drain still owns acknowledgement, and the service
/// still deduplicates by operation identity when pages overlap.
///
/// # Errors
///
/// Returns the exact replica, transport, protocol, rejection, or
/// cancellation boundary that stopped progress.
pub async fn send_namespace_now<R, T>(
    replica: &R,
    peer: PeerId,
    transport: &mut T,
    domain: &NamespaceDomain,
    is_mine: impl Fn(DocumentId) -> bool,
    mut cancelled: impl FnMut() -> bool,
) -> Result<NamespaceSendReport, NamespaceSyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
{
    let namespace = domain.namespace();
    let policy = domain.policy();
    let mut report = NamespaceSendReport {
        namespace: namespace.to_owned(),
        ..NamespaceSendReport::default()
    };
    if policy.network() == NetworkCondition::Offline {
        report.deferred = true;
        return Ok(report);
    }
    if cancelled() {
        eprintln!("logical-db-sync: namespace upload cancelled before binding");
        return Err(NamespaceSyncError::Cancelled);
    }
    replica
        .bind_upstream(peer)
        .map_err(NamespaceSyncError::Replica)?;
    let available = replica
        .pending(usize::from(policy.changes_per_page()))
        .map_err(NamespaceSyncError::Replica)?;
    let mine = available
        .into_iter()
        .filter(|change| is_mine(change.document))
        .collect::<Vec<_>>();
    if mine.is_empty() {
        report.more = !replica
            .pending(1)
            .map_err(NamespaceSyncError::Replica)?
            .is_empty();
        return Ok(report);
    }
    let mut aggregate_bytes = 0usize;
    let mut pending = Vec::with_capacity(mine.len());
    for change in mine {
        let next_bytes = aggregate_bytes
            .checked_add(change.bytes.len())
            .ok_or(NamespaceSyncError::Protocol(ProtocolError::PageLimit))?;
        if next_bytes > policy.bytes_per_page() as usize {
            break;
        }
        aggregate_bytes = next_bytes;
        pending.push(change);
    }
    if pending.is_empty() {
        eprintln!("logical-db-sync: namespace upload cannot fit within byte limit");
        return Err(NamespaceSyncError::Protocol(ProtocolError::PageLimit));
    }
    let request = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica: replica.replica(),
        changes: pending
            .iter()
            .map(|change| ChangeEnvelope {
                scope: replica.scope(),
                document: change.document,
                replica: replica.replica(),
                operation: change.operation,
                request: change.request,
                digest: change.digest,
                bytes: change.bytes.clone(),
            })
            .collect(),
    };
    request.validate().map_err(NamespaceSyncError::Protocol)?;
    let response = transport
        .push(request.clone())
        .await
        .map_err(NamespaceSyncError::Transport)?;
    response
        .validate(&request)
        .map_err(NamespaceSyncError::Protocol)?;
    for outcome in &response.outcomes {
        if let PushOutcome::Rejected { operation, code } = outcome {
            eprintln!("logical-db-sync: namespace upload rejected operation {operation}: {code:?}");
            return Err(NamespaceSyncError::Rejected {
                operation: *operation,
                code: *code,
            });
        }
    }
    report.uploaded = pending.len();
    report.more = !replica
        .pending(1)
        .map_err(NamespaceSyncError::Replica)?
        .is_empty();
    Ok(report)
}

/// Loaded pull state for one namespace cycle.
struct NamespaceStart {
    checkpoints: NamespaceCheckpoints,
    after: Option<Checkpoint>,
    recovered: bool,
}

fn namespace_start<R, TransportError>(
    replica: &R,
    peer: PeerId,
    namespace: &str,
    now_unix_ms: u64,
) -> Result<NamespaceStart, NamespaceSyncError<R::Error, TransportError>>
where
    R: ReplicaPort,
{
    let checkpoints = match replica
        .checkpoint(peer)
        .map_err(NamespaceSyncError::Replica)?
    {
        None => NamespaceCheckpoints::new(),
        Some(bytes) => NamespaceCheckpoints::decode(&bytes).map_err(NamespaceSyncError::Model)?,
    };
    let mut after = None;
    let mut recovered = false;
    if let Some(stored) = checkpoints.get(namespace) {
        if is_namespace_checkpoint_expired(stored, now_unix_ms) {
            recovered = true;
        } else {
            after = Some(
                Checkpoint::from_wire(stored.cursor().to_vec())
                    .map_err(NamespaceSyncError::Protocol)?,
            );
        }
    }
    Ok(NamespaceStart {
        checkpoints,
        after,
        recovered,
    })
}

fn install_namespace_page<R, TransportError>(
    replica: &R,
    peer: PeerId,
    namespace: &str,
    page: &PullPage,
    is_mine: impl Fn(DocumentId) -> bool,
    checkpoints: &mut NamespaceCheckpoints,
    now_unix_ms: u64,
) -> Result<usize, NamespaceSyncError<R::Error, TransportError>>
where
    R: ReplicaPort,
{
    let inbound = page
        .changes
        .iter()
        .filter(|change| is_mine(change.document))
        .map(|change| InboundChange {
            document: change.document,
            digest: change.digest,
            bytes: change.bytes.clone(),
        })
        .collect::<Vec<_>>();
    let refreshed = NamespaceCheckpoint::new(
        page.checkpoint.as_wire().to_vec(),
        now_unix_ms.saturating_add(NAMESPACE_CHECKPOINT_TTL_MS),
    )
    .map_err(NamespaceSyncError::Model)?;
    checkpoints
        .insert(namespace, refreshed)
        .map_err(NamespaceSyncError::Model)?;
    let stored = checkpoints.encode().map_err(NamespaceSyncError::Model)?;
    replica
        .install(peer, &inbound, &stored)
        .map_err(NamespaceSyncError::Replica)?;
    Ok(inbound.len())
}

fn effective_pages(policy: NamespacePolicy) -> u8 {
    match policy.network() {
        NetworkCondition::Online => policy.pages_per_cycle(),
        NetworkCondition::Constrained => policy.pages_per_cycle().min(1),
        NetworkCondition::Offline => 0,
    }
}

fn namespace_pull_digest(
    scope: Scope,
    replica: ReplicaId,
    namespace: &str,
    after: Option<&Checkpoint>,
    policy: NamespacePolicy,
) -> OperationDigest {
    let mut digest = Sha256::new();
    digest.update(b"logical-db/pull-ns/v1");
    digest.update(scope.account().as_uuid().as_bytes());
    digest.update(scope.space().as_uuid().as_bytes());
    digest.update(replica.as_uuid().as_bytes());
    digest.update(namespace.as_bytes());
    digest.update([0]);
    match after {
        Some(checkpoint) => {
            digest.update([1]);
            digest.update(checkpoint.as_wire());
        }
        None => digest.update([0]),
    }
    digest.update(policy.changes_per_page().to_be_bytes());
    digest.update(policy.bytes_per_page().to_be_bytes());
    OperationDigest::new(digest.finalize().into())
}

/// Drain and backoff budgets for one [`SyncOrchestrator`].
///
/// [`max_cycles`](Self::max_cycles) bounds how many [`synchronize`] cycles one
/// run issues before yielding with [`more`](RunReport::more) still set; the
/// host reschedules on the next hint. The backoff bounds shape the delay
/// recorded after a retryable transport failure: the first failure waits
/// [`base_backoff_ms`](Self::base_backoff_ms), each consecutive failure
/// doubles, and [`max_backoff_ms`](Self::max_backoff_ms) always caps the wait,
/// including server-supplied retry hints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrchestratorPolicy {
    max_cycles: usize,
    base_backoff_ms: u64,
    max_backoff_ms: u64,
}

impl OrchestratorPolicy {
    /// Builds validated orchestrator budgets.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::PageLimit`] for an empty cycle budget or an
    /// empty backoff bound.
    pub fn new(
        max_cycles: usize,
        base_backoff_ms: u64,
        max_backoff_ms: u64,
    ) -> Result<Self, ProtocolError> {
        if max_cycles == 0 || base_backoff_ms == 0 || max_backoff_ms == 0 {
            eprintln!("logical-db-sync: orchestrator policy contains an empty budget");
            return Err(ProtocolError::PageLimit);
        }
        Ok(Self {
            max_cycles,
            base_backoff_ms,
            max_backoff_ms,
        })
    }

    /// Returns the maximum [`synchronize`] cycles drained by one run.
    #[must_use]
    pub const fn max_cycles(self) -> usize {
        self.max_cycles
    }

    /// Returns the first retryable-failure delay in milliseconds.
    #[must_use]
    pub const fn base_backoff_ms(self) -> u64 {
        self.base_backoff_ms
    }

    /// Returns the backoff ceiling in milliseconds, applied to hints as well.
    #[must_use]
    pub const fn max_backoff_ms(self) -> u64 {
        self.max_backoff_ms
    }
}

impl Default for OrchestratorPolicy {
    fn default() -> Self {
        Self {
            max_cycles: DEFAULT_MAX_CYCLES,
            base_backoff_ms: DEFAULT_BASE_BACKOFF_MS,
            max_backoff_ms: DEFAULT_MAX_BACKOFF_MS,
        }
    }
}

/// Opaque scheduling state the application persists across launches.
///
/// The blob carries only scheduling state — consecutive retryable failures
/// and the backoff deadline. Checkpoints and upload acknowledgements stay in
/// the replica's durable slots, so restoring this blob after process death
/// resumes timing without replaying acknowledged uploads. Encode with
/// [`encode`](Self::encode), persist the bytes in the application's own
/// sidecar, and restore with [`SyncOrchestrator::restore`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrchestratorState {
    consecutive_failures: u32,
    not_ready_until_ms: u64,
}

impl OrchestratorState {
    /// Builds idle scheduling state with no backoff outstanding.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            consecutive_failures: 0,
            not_ready_until_ms: 0,
        }
    }

    /// Returns the consecutive retryable transport failures behind the deadline.
    #[must_use]
    pub const fn consecutive_failures(self) -> u32 {
        self.consecutive_failures
    }

    /// Returns the millisecond timestamp before which runs defer without work.
    #[must_use]
    pub const fn not_ready_until_ms(self) -> u64 {
        self.not_ready_until_ms
    }

    /// Returns whether a run at `now_unix_ms` must defer without work.
    #[must_use]
    pub const fn is_backing_off(self, now_unix_ms: u64) -> bool {
        self.not_ready_until_ms > now_unix_ms
    }

    /// Encodes the state as magic plus version plus two fixed fields.
    #[must_use]
    pub fn encode(self) -> [u8; ORCHESTRATOR_STATE_BYTES] {
        let mut output = [0_u8; ORCHESTRATOR_STATE_BYTES];
        output[0..ORCHESTRATOR_STATE_MAGIC.len()].copy_from_slice(&ORCHESTRATOR_STATE_MAGIC);
        output[8..10].copy_from_slice(&ORCHESTRATOR_STATE_VERSION.to_be_bytes());
        output[10..14].copy_from_slice(&self.consecutive_failures.to_be_bytes());
        output[14..22].copy_from_slice(&self.not_ready_until_ms.to_be_bytes());
        output
    }

    /// Decodes state bytes, refusing foreign, truncated, or padded input.
    ///
    /// # Errors
    ///
    /// Returns [`OrchestratorStateError::Malformed`] for a bad structure and
    /// [`OrchestratorStateError::UnsupportedVersion`] for a foreign version.
    pub fn decode(bytes: &[u8]) -> Result<Self, OrchestratorStateError> {
        if bytes.len() != ORCHESTRATOR_STATE_BYTES {
            eprintln!("logical-db-sync: orchestrator state has an invalid length");
            return Err(OrchestratorStateError::Malformed);
        }
        if bytes[0..ORCHESTRATOR_STATE_MAGIC.len()] != ORCHESTRATOR_STATE_MAGIC {
            eprintln!("logical-db-sync: orchestrator state magic is invalid");
            return Err(OrchestratorStateError::Malformed);
        }
        let version = u16::from_be_bytes([bytes[8], bytes[9]]);
        if version != ORCHESTRATOR_STATE_VERSION {
            eprintln!("logical-db-sync: unsupported orchestrator state version {version}");
            return Err(OrchestratorStateError::UnsupportedVersion { actual: version });
        }
        Ok(Self {
            consecutive_failures: u32::from_be_bytes([bytes[10], bytes[11], bytes[12], bytes[13]]),
            not_ready_until_ms: u64::from_be_bytes([
                bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19], bytes[20],
                bytes[21],
            ]),
        })
    }

    fn note_success(&mut self) {
        self.consecutive_failures = 0;
    }

    fn note_retryable(&mut self, now_unix_ms: u64, delay_ms: u64) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.not_ready_until_ms = now_unix_ms.saturating_add(delay_ms);
    }
}

impl Default for OrchestratorState {
    fn default() -> Self {
        Self::new()
    }
}

/// Stable orchestrator state-codec failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrchestratorStateError {
    /// The blob violated its fixed magic-plus-version-plus-fields encoding.
    Malformed,
    /// The blob used a foreign layout version.
    UnsupportedVersion {
        /// Observed state layout version.
        actual: u16,
    },
    /// A decoding failure with the source boundary retained for diagnostics.
    Context {
        /// Decoder boundary that rejected the state.
        at: &'static str,
    },
}

impl OrchestratorStateError {
    /// Creates a contextual state error for a caller-owned decoder boundary.
    #[must_use]
    pub const fn at(at: &'static str) -> Self {
        Self::Context { at }
    }
}

impl fmt::Display for OrchestratorStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => formatter.write_str("orchestrator state encoding is malformed"),
            Self::UnsupportedVersion { actual } => {
                write!(
                    formatter,
                    "orchestrator state version {actual} is unsupported"
                )
            }
            Self::Context { at } => write!(formatter, "orchestrator state error at {at}"),
        }
    }
}

impl Error for OrchestratorStateError {}

/// Transport failure classification supplied by the application.
///
/// The sync crate stays transport-neutral, so the application names
/// retryability the way its own transport does: the HTTP transport passes its
/// `is_retryable` predicate and bounded `retry_after` hint directly. A
/// retryable failure becomes a deferred report with a recorded backoff
/// deadline; any other failure surfaces immediately with no state recorded.
pub struct RetrySignal<'signal, TransportError> {
    /// Classifies a transport failure as worth backing off for.
    pub is_retryable: &'signal dyn Fn(&TransportError) -> bool,
    /// Server-supplied delta delay, when syntactically valid.
    pub retry_after: &'signal dyn Fn(&TransportError) -> Option<Duration>,
}

fn never_retryable<TransportError>(error: &TransportError) -> bool {
    std::hint::black_box(error);
    false
}

fn no_retry_hint<TransportError>(error: &TransportError) -> Option<Duration> {
    std::hint::black_box(error);
    None
}

impl<TransportError> RetrySignal<'static, TransportError> {
    /// Builds a classifier that surfaces every transport failure immediately.
    #[must_use]
    pub fn never() -> Self {
        Self {
            is_retryable: &never_retryable,
            retry_after: &no_retry_hint,
        }
    }
}

/// Observable result of one orchestrator run over [`synchronize`] cycles.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunReport {
    /// Local changes accepted or already present remotely across cycles.
    pub pushed: usize,
    /// Remote changes atomically installed locally across cycles.
    pub pulled: usize,
    /// Pull pages installed across cycles.
    pub pages: usize,
    /// [`synchronize`] cycles issued.
    pub cycles: usize,
    /// Whether any cycle forced a full pull after checkpoint expiry.
    pub full_resync: bool,
    /// Whether unsent local changes or remote pages remain after this run.
    pub more: bool,
    /// Whether a backoff window is now active and the host must reschedule.
    pub deferred: bool,
}

/// Observable result of one orchestrator run over one namespace domain.
///
/// The run is one upload page plus a pull drain, so the report embeds the
/// phase reports instead of duplicating their fields: [`send`](Self::send)
/// is the single delivery-only upload page, and the remaining counters cover
/// the pull drain. [`send.more`](NamespaceSendReport::more) stays set while
/// the namespace has unacknowledged local changes — delivery never advances
/// the shared acknowledgement, the global drain still owns it — even after
/// the pull side reports [`more`](Self::more) cleared.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NamespaceRunReport {
    /// Namespace this run drove.
    pub namespace: String,
    /// Pull cycles issued (the upload page is offered once per run).
    pub cycles: usize,
    /// The single delivery-only upload page offered before the pull drain.
    pub send: NamespaceSendReport,
    /// Remote changes for this namespace installed locally across cycles.
    pub pulled: usize,
    /// Pull pages installed across cycles.
    pub pages: usize,
    /// Whether any cycle forced a full pull after cursor expiry.
    pub full_resync: bool,
    /// Whether remote pages remain after this bounded run.
    pub more: bool,
    /// Whether a backoff window or an offline policy deferred the run.
    pub deferred: bool,
}

/// Application-facing sync engine wrapping the sync functions.
///
/// The orchestrator owns batching (it loops [`synchronize`] on the [`more`](SyncReport::more)
/// signal until the backlog drains or the policy budget binds), backoff (it
/// converts retryable transport failures into deferred reports honoring retry
/// hints instead of surfacing them), and opaque persisted state (see
/// [`OrchestratorState`]). It never forks the push-then-pull loop: checkpoint
/// expiry still recovers exactly once inside [`synchronize`] and surfaces a
/// second expiry as a protocol error, and namespace runs drive
/// [`send_namespace_now`] plus [`synchronize_namespace`] without duplicating
/// their per-namespace cursors.
///
/// One orchestrator binds one upstream peer for its lifetime, matching the
/// replica's single-upstream binding.
#[derive(Clone, Debug)]
pub struct SyncOrchestrator {
    peer: PeerId,
    policy: OrchestratorPolicy,
    state: OrchestratorState,
}

impl SyncOrchestrator {
    /// Binds an orchestrator to one upstream peer with idle scheduling state.
    #[must_use]
    pub const fn new(peer: PeerId, policy: OrchestratorPolicy) -> Self {
        Self {
            peer,
            policy,
            state: OrchestratorState::new(),
        }
    }

    /// Rebuilds an orchestrator from application-persisted state bytes.
    ///
    /// # Errors
    ///
    /// Returns [`OrchestratorStateError::Malformed`] for a bad structure and
    /// [`OrchestratorStateError::UnsupportedVersion`] for a foreign version.
    pub fn restore(
        peer: PeerId,
        policy: OrchestratorPolicy,
        bytes: &[u8],
    ) -> Result<Self, OrchestratorStateError> {
        Ok(Self {
            peer,
            policy,
            state: OrchestratorState::decode(bytes)?,
        })
    }

    /// Returns the bound upstream peer.
    #[must_use]
    pub const fn peer(&self) -> PeerId {
        self.peer
    }

    /// Returns the drain and backoff budgets.
    #[must_use]
    pub const fn policy(&self) -> OrchestratorPolicy {
        self.policy
    }

    /// Returns the current scheduling state for inspection.
    #[must_use]
    pub const fn state(&self) -> OrchestratorState {
        self.state
    }

    /// Exports the opaque blob the application persists across launches.
    #[must_use]
    pub fn export_state(&self) -> [u8; ORCHESTRATOR_STATE_BYTES] {
        self.state.encode()
    }

    /// Drains the backlog through the push-then-pull loop until it clears.
    ///
    /// The run issues [`synchronize`] cycles until a cycle reports no more
    /// work or the policy cycle budget binds. A retryable transport failure
    /// records a backoff deadline and returns a deferred report holding the
    /// progress so far; call again after
    /// [`not_ready_until_ms`](OrchestratorState::not_ready_until_ms).
    ///
    /// # Errors
    ///
    /// Returns the exact replica, permanent-transport, protocol, rejection,
    /// or cancellation boundary that stopped progress. Retryable transport
    /// failures never surface here; they become deferred reports instead.
    pub async fn run<R, T>(
        &mut self,
        replica: &R,
        transport: &mut T,
        limits: SyncLimits,
        now_unix_ms: u64,
        retry: &RetrySignal<'_, T::Error>,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<RunReport, SyncError<R::Error, T::Error>>
    where
        R: ReplicaPort,
        T: Transport,
    {
        if cancelled() {
            eprintln!("logical-db-sync: orchestrator run cancelled before cycle");
            return Err(SyncError::Cancelled);
        }
        let mut report = RunReport::default();
        if self.state.is_backing_off(now_unix_ms) {
            report.deferred = true;
            return Ok(report);
        }
        loop {
            if cancelled() {
                eprintln!("logical-db-sync: orchestrator run cancelled between cycles");
                return Err(SyncError::Cancelled);
            }
            match synchronize(replica, self.peer, transport, limits, &mut cancelled).await {
                Ok(cycle) => {
                    report.pushed += cycle.pushed;
                    report.pulled += cycle.pulled;
                    report.pages += cycle.pages;
                    report.cycles += 1;
                    report.full_resync = report.full_resync || cycle.full_resync;
                    report.more = cycle.more;
                    self.state.note_success();
                    if !cycle.more || report.cycles >= self.policy.max_cycles() {
                        return Ok(report);
                    }
                }
                Err(SyncError::Transport(error)) if (retry.is_retryable)(&error) => {
                    defer_transport(&mut self.state, self.policy, now_unix_ms, retry, &error);
                    report.deferred = true;
                    report.more = true;
                    return Ok(report);
                }
                Err(error) => {
                    eprintln!(
                        "logical-db-sync: namespace run failed (replica={}, transport={})",
                        std::any::type_name::<R::Error>(),
                        std::any::type_name::<T::Error>(),
                    );
                    return Err(error);
                }
            }
        }
    }

    /// Sends enqueued changes now, for backup-now immediacy after local edits.
    ///
    /// This drives the same push-then-pull loop as [`run`](Self::run): the
    /// protocol admits no push-only global cycle, so immediacy is caller
    /// intent rather than wire behavior. The application never drives pages
    /// itself; one call drains every pending page the budget allows.
    ///
    /// # Errors
    ///
    /// Returns the exact replica, permanent-transport, protocol, rejection,
    /// or cancellation boundary that stopped progress.
    pub async fn send_now<R, T>(
        &mut self,
        replica: &R,
        transport: &mut T,
        limits: SyncLimits,
        now_unix_ms: u64,
        retry: &RetrySignal<'_, T::Error>,
        cancelled: impl FnMut() -> bool,
    ) -> Result<RunReport, SyncError<R::Error, T::Error>>
    where
        R: ReplicaPort,
        T: Transport,
    {
        self.run(replica, transport, limits, now_unix_ms, retry, cancelled)
            .await
    }

    /// Pulls remote changes now, for pull-to-refresh immediacy on push hints.
    ///
    /// This drives the same push-then-pull loop as [`run`](Self::run): the
    /// protocol admits no pull-only global cycle, so immediacy is caller
    /// intent rather than wire behavior. Multi-page backlogs drain across the
    /// run's cycles until the [`more`](SyncReport::more) signal clears.
    ///
    /// # Errors
    ///
    /// Returns the exact replica, permanent-transport, protocol, rejection,
    /// or cancellation boundary that stopped progress.
    pub async fn fetch_now<R, T>(
        &mut self,
        replica: &R,
        transport: &mut T,
        limits: SyncLimits,
        now_unix_ms: u64,
        retry: &RetrySignal<'_, T::Error>,
        cancelled: impl FnMut() -> bool,
    ) -> Result<RunReport, SyncError<R::Error, T::Error>>
    where
        R: ReplicaPort,
        T: Transport,
    {
        self.run(replica, transport, limits, now_unix_ms, retry, cancelled)
            .await
    }

    /// Drives one namespace domain: one upload page, then a pull drain.
    ///
    /// The upload page is delivery-only by [`send_namespace_now`] design — it
    /// never advances the shared acknowledgement — so exactly one is offered
    /// per run and the loop continues on the pull [`more`](NamespaceSyncReport::more)
    /// signal alone. Per-namespace cursors stay owned by
    /// [`synchronize_namespace`]; this run only schedules its cycles. The
    /// domain's observation time doubles as the backoff clock. An offline
    /// domain defers without touching the replica binding or the transport.
    ///
    /// # Errors
    ///
    /// Returns the exact replica, permanent-transport, protocol,
    /// checkpoint-map, rejection, or cancellation boundary that stopped
    /// progress. Retryable transport failures never surface here; they become
    /// deferred reports instead.
    pub async fn run_namespace<R, T, F>(
        &mut self,
        replica: &R,
        transport: &mut T,
        domain: &NamespaceDomain,
        is_mine: &F,
        retry: &RetrySignal<'_, T::Error>,
        mut cancelled: impl FnMut() -> bool,
    ) -> Result<NamespaceRunReport, NamespaceSyncError<R::Error, T::Error>>
    where
        R: ReplicaPort,
        T: Transport,
        F: Fn(DocumentId) -> bool,
    {
        let mut report = NamespaceRunReport {
            namespace: domain.namespace().to_owned(),
            send: NamespaceSendReport {
                namespace: domain.namespace().to_owned(),
                ..NamespaceSendReport::default()
            },
            ..NamespaceRunReport::default()
        };
        if cancelled() {
            eprintln!("logical-db-sync: namespace orchestrator run cancelled before send");
            return Err(NamespaceSyncError::Cancelled);
        }
        let now_unix_ms = domain.now_unix_ms();
        if self.state.is_backing_off(now_unix_ms) {
            report.deferred = true;
            report.send.deferred = true;
            return Ok(report);
        }
        if domain.policy().network() == NetworkCondition::Offline {
            report.deferred = true;
            report.send.deferred = true;
            return Ok(report);
        }
        let send = match send_namespace_now(
            replica,
            self.peer,
            transport,
            domain,
            is_mine,
            &mut cancelled,
        )
        .await
        {
            Ok(send) => send,
            Err(NamespaceSyncError::Transport(error)) if (retry.is_retryable)(&error) => {
                defer_transport(&mut self.state, self.policy, now_unix_ms, retry, &error);
                report.deferred = true;
                report.more = true;
                return Ok(report);
            }
            Err(error) => {
                eprintln!(
                    "logical-db-sync: namespace upload failed (replica={}, transport={})",
                    std::any::type_name::<R::Error>(),
                    std::any::type_name::<T::Error>()
                );
                return Err(error);
            }
        };
        report.send = send;
        loop {
            if cancelled() {
                eprintln!("logical-db-sync: namespace run cancelled between cycles");
                return Err(NamespaceSyncError::Cancelled);
            }
            match synchronize_namespace(
                replica,
                self.peer,
                transport,
                domain,
                is_mine,
                &mut cancelled,
            )
            .await
            {
                Ok(cycle) => {
                    report.pulled += cycle.pulled;
                    report.pages += cycle.pages;
                    report.cycles += 1;
                    report.full_resync = report.full_resync || cycle.full_resync;
                    report.more = cycle.more;
                    self.state.note_success();
                    if !cycle.more || report.cycles >= self.policy.max_cycles() {
                        return Ok(report);
                    }
                }
                Err(NamespaceSyncError::Transport(error)) if (retry.is_retryable)(&error) => {
                    defer_transport(&mut self.state, self.policy, now_unix_ms, retry, &error);
                    report.deferred = true;
                    report.more = true;
                    return Ok(report);
                }
                Err(error) => {
                    eprintln!(
                        "logical-db-sync: namespace cycle failed (replica={}, transport={})",
                        std::any::type_name::<R::Error>(),
                        std::any::type_name::<T::Error>(),
                    );
                    return Err(error);
                }
            }
        }
    }
}

fn backoff_delay_ms(policy: OrchestratorPolicy, consecutive_failures: u32) -> u64 {
    let mut delay = policy.base_backoff_ms();
    for _ in 0..consecutive_failures.min(MAX_BACKOFF_DOUBLINGS) {
        delay = delay.saturating_mul(2).min(policy.max_backoff_ms());
    }
    delay.min(policy.max_backoff_ms()).max(1)
}

fn hint_delay_ms(hint: Duration, max_backoff_ms: u64) -> u64 {
    u64::try_from(hint.as_millis())
        .unwrap_or(u64::MAX)
        .min(max_backoff_ms)
        .max(1)
}

fn defer_transport<TransportError>(
    state: &mut OrchestratorState,
    policy: OrchestratorPolicy,
    now_unix_ms: u64,
    retry: &RetrySignal<'_, TransportError>,
    error: &TransportError,
) {
    let delay = match (retry.retry_after)(error) {
        Some(hint) => hint_delay_ms(hint, policy.max_backoff_ms()),
        None => backoff_delay_ms(policy, state.consecutive_failures()),
    };
    state.note_retryable(now_unix_ms, delay);
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, future::Future};

    use logical_db_model::{AccountId, SpaceId};
    use logical_db_protocol::RejectionCode;

    use super::*;

    struct Replica {
        scope: Scope,
        installation: ReplicaId,
        pending: Vec<OutboundChange>,
        checkpoint: std::cell::RefCell<Option<Vec<u8>>>,
        acknowledged: std::cell::Cell<u64>,
        installed: std::cell::RefCell<Vec<InboundChange>>,
        baselines: std::cell::RefCell<Vec<(Vec<u8>, Vec<u8>)>>,
    }

    impl ReplicaPort for Replica {
        type Error = Infallible;
        fn scope(&self) -> Scope {
            self.scope
        }
        fn replica(&self) -> ReplicaId {
            self.installation
        }
        fn bind_upstream(&self, _peer: PeerId) -> Result<(), Self::Error> {
            Ok(())
        }
        fn pending(&self, limit: usize) -> Result<Vec<OutboundChange>, Self::Error> {
            Ok(self
                .pending
                .iter()
                .filter(|change| change.sequence > self.acknowledged.get())
                .take(limit)
                .cloned()
                .collect())
        }
        fn acknowledge(&self, _peer: PeerId, through: u64) -> Result<(), Self::Error> {
            self.acknowledged.set(through);
            Ok(())
        }
        fn checkpoint(&self, _peer: PeerId) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.checkpoint.borrow().clone())
        }
        fn install(
            &self,
            _peer: PeerId,
            changes: &[InboundChange],
            checkpoint: &[u8],
        ) -> Result<(), Self::Error> {
            self.installed.borrow_mut().extend_from_slice(changes);
            *self.checkpoint.borrow_mut() = Some(checkpoint.to_vec());
            Ok(())
        }
        fn install_baseline(
            &self,
            _peer: PeerId,
            artifact: &SnapshotArtifact,
            checkpoint: &[u8],
        ) -> Result<(), Self::Error> {
            self.baselines
                .borrow_mut()
                .push((artifact.baseline.encode().to_vec(), checkpoint.to_vec()));
            *self.checkpoint.borrow_mut() = Some(checkpoint.to_vec());
            Ok(())
        }
    }

    struct Wire {
        push: Option<PushResponse>,
        pulls: std::collections::VecDeque<PullResult>,
    }

    impl Transport for Wire {
        type Error = Infallible;
        fn push(
            &mut self,
            _request: PushRequest,
        ) -> impl Future<Output = Result<PushResponse, Self::Error>> {
            std::future::ready(Ok(self.push.take().unwrap()))
        }
        fn pull(
            &mut self,
            _request: PullRequest,
        ) -> impl Future<Output = Result<PullResult, Self::Error>> {
            std::future::ready(Ok(self.pulls.pop_front().unwrap()))
        }
        fn baseline(
            &mut self,
            _request: BaselineRequest,
        ) -> impl Future<Output = Result<BaselineResult, Self::Error>> {
            std::future::ready(Ok(BaselineResult::Unavailable))
        }
    }

    #[test]
    fn expired_checkpoint_restarts_pull_without_guessing_continuity() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica_id = ReplicaId::new();
        let operation = OperationId::new();
        let replica = Replica {
            scope,
            installation: replica_id,
            pending: vec![OutboundChange {
                sequence: 4,
                document: DocumentId::new(),
                operation,
                request: OperationDigest::new([7; 32]),
                digest: ChangeDigest::new([1; 32]),
                bytes: vec![1],
            }],
            checkpoint: std::cell::RefCell::new(Some(vec![9])),
            acknowledged: std::cell::Cell::new(0),
            installed: std::cell::RefCell::new(Vec::new()),
            baselines: std::cell::RefCell::new(Vec::new()),
        };
        let new_checkpoint = Checkpoint::from_wire(vec![10]).unwrap();
        let mut wire = Wire {
            push: Some(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes: vec![PushOutcome::Accepted { operation }],
            }),
            pulls: [
                PullResult::CheckpointExpired,
                PullResult::Page(PullPage {
                    protocol: PROTOCOL_VERSION,
                    changes: Vec::new(),
                    checkpoint: new_checkpoint,
                    more: false,
                }),
            ]
            .into(),
        };
        let report = lgwks_std::task::block_on(synchronize(
            &replica,
            PeerId::new(),
            &mut wire,
            SyncLimits {
                pages_per_cycle: 1,
                ..SyncLimits::default()
            },
            || false,
        ))
        .unwrap();
        assert_eq!(
            report,
            SyncReport {
                pushed: 1,
                pulled: 0,
                pages: 1,
                full_resync: true,
                recovered: false,
                more: false
            }
        );
        assert_eq!(replica.acknowledged.get(), 4);
        assert_eq!(*replica.checkpoint.borrow(), Some(vec![10]));
    }

    #[test]
    fn rejected_push_never_advances_acknowledgement() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let operation = OperationId::new();
        let replica = Replica {
            scope,
            installation: ReplicaId::new(),
            pending: vec![OutboundChange {
                sequence: 1,
                document: DocumentId::new(),
                operation,
                request: OperationDigest::new([8; 32]),
                digest: ChangeDigest::new([2; 32]),
                bytes: vec![2],
            }],
            checkpoint: std::cell::RefCell::new(None),
            acknowledged: std::cell::Cell::new(0),
            installed: std::cell::RefCell::new(Vec::new()),
            baselines: std::cell::RefCell::new(Vec::new()),
        };
        let mut wire = Wire {
            push: Some(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes: vec![PushOutcome::Rejected {
                    operation,
                    code: RejectionCode::Forbidden,
                }],
            }),
            pulls: std::collections::VecDeque::default(),
        };
        assert!(
            matches!(lgwks_std::task::block_on(synchronize(&replica, PeerId::new(), &mut wire, SyncLimits::default(), || false)), Err(SyncError::Rejected { operation: value, code: RejectionCode::Forbidden }) if value == operation)
        );
        assert_eq!(replica.acknowledged.get(), 0);
    }

    #[test]
    fn upload_page_stops_at_byte_budget_and_advances_only_sent_prefix() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let first = OperationId::new();
        let replica = Replica {
            scope,
            installation: ReplicaId::new(),
            pending: [first, OperationId::new()]
                .into_iter()
                .enumerate()
                .map(|(offset, operation)| OutboundChange {
                    sequence: u64::try_from(offset + 1).unwrap(),
                    document: DocumentId::new(),
                    operation,
                    request: OperationDigest::new([u8::try_from(offset + 1).unwrap(); 32]),
                    digest: ChangeDigest::new([u8::try_from(offset + 3).unwrap(); 32]),
                    bytes: vec![1],
                })
                .collect(),
            checkpoint: std::cell::RefCell::new(None),
            acknowledged: std::cell::Cell::new(0),
            installed: std::cell::RefCell::new(Vec::new()),
            baselines: std::cell::RefCell::new(Vec::new()),
        };
        let mut wire = Wire {
            push: Some(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes: vec![PushOutcome::Accepted { operation: first }],
            }),
            pulls: std::collections::VecDeque::new(),
        };
        assert_eq!(
            lgwks_std::task::block_on(push_pending(&replica, PeerId::new(), &mut wire, 2, 1,))
                .unwrap(),
            (1, true)
        );
        assert_eq!(replica.acknowledged.get(), 1);
    }

    #[test]
    fn report_keeps_unsent_upload_backlog_visible() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let first = OperationId::new();
        let replica = Replica {
            scope,
            installation: ReplicaId::new(),
            pending: [first, OperationId::new()]
                .into_iter()
                .enumerate()
                .map(|(offset, operation)| OutboundChange {
                    sequence: u64::try_from(offset + 1).unwrap(),
                    document: DocumentId::new(),
                    operation,
                    request: OperationDigest::new([u8::try_from(offset + 1).unwrap(); 32]),
                    digest: ChangeDigest::new([u8::try_from(offset + 1).unwrap(); 32]),
                    bytes: vec![1],
                })
                .collect(),
            checkpoint: std::cell::RefCell::new(None),
            acknowledged: std::cell::Cell::new(0),
            installed: std::cell::RefCell::new(Vec::new()),
            baselines: std::cell::RefCell::new(Vec::new()),
        };
        let mut wire = Wire {
            push: Some(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes: vec![PushOutcome::Accepted { operation: first }],
            }),
            pulls: [PullResult::Page(PullPage {
                protocol: PROTOCOL_VERSION,
                changes: Vec::new(),
                checkpoint: Checkpoint::from_wire(vec![1]).unwrap(),
                more: false,
            })]
            .into(),
        };
        let report = lgwks_std::task::block_on(synchronize(
            &replica,
            PeerId::new(),
            &mut wire,
            SyncLimits {
                changes_per_page: 1,
                ..SyncLimits::default()
            },
            || false,
        ))
        .unwrap();
        assert_eq!(report.pushed, 1);
        assert!(report.more);
        assert_eq!(replica.acknowledged.get(), 1);
    }

    fn empty_replica(scope: Scope) -> Replica {
        Replica {
            scope,
            installation: ReplicaId::new(),
            pending: Vec::new(),
            checkpoint: std::cell::RefCell::new(None),
            acknowledged: std::cell::Cell::new(0),
            installed: std::cell::RefCell::new(Vec::new()),
            baselines: std::cell::RefCell::new(Vec::new()),
        }
    }

    #[test]
    fn sync_limits_accept_exact_edges_and_reject_every_zero_or_excess() {
        let valid = SyncLimits {
            changes_per_page: logical_db_model::MAX_CHANGES_PER_PAGE.try_into().unwrap(),
            bytes_per_page: logical_db_model::MAX_PAGE_BYTES.try_into().unwrap(),
            pages_per_cycle: MAX_PAGES_PER_CYCLE.try_into().unwrap(),
        };
        assert_eq!(valid.validate(), Ok(valid));
        for invalid in [
            SyncLimits {
                changes_per_page: 0,
                ..valid
            },
            SyncLimits {
                changes_per_page: valid.changes_per_page + 1,
                ..valid
            },
            SyncLimits {
                bytes_per_page: 0,
                ..valid
            },
            SyncLimits {
                bytes_per_page: valid.bytes_per_page + 1,
                ..valid
            },
            SyncLimits {
                pages_per_cycle: 0,
                ..valid
            },
            SyncLimits {
                pages_per_cycle: valid.pages_per_cycle + 1,
                ..valid
            },
        ] {
            assert_eq!(invalid.validate(), Err(ProtocolError::PageLimit));
        }
    }

    #[test]
    fn expiry_without_a_durable_checkpoint_is_a_protocol_failure() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = empty_replica(scope);
        let mut wire = Wire {
            push: None,
            pulls: [PullResult::CheckpointExpired].into(),
        };
        assert!(matches!(
            lgwks_std::task::block_on(synchronize(
                &replica,
                PeerId::new(),
                &mut wire,
                SyncLimits::default(),
                || false,
            )),
            Err(SyncError::Protocol(ProtocolError::InvalidCheckpoint))
        ));
        assert!(replica.checkpoint.borrow().is_none());
    }

    struct RecoveryWire {
        pulls: std::collections::VecDeque<PullResult>,
        baseline: Option<BaselinePage>,
    }

    impl Transport for RecoveryWire {
        type Error = Infallible;
        fn push(
            &mut self,
            _request: PushRequest,
        ) -> impl Future<Output = Result<PushResponse, Self::Error>> {
            std::future::ready(Ok(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes: Vec::new(),
            }))
        }
        fn pull(
            &mut self,
            _request: PullRequest,
        ) -> impl Future<Output = Result<PullResult, Self::Error>> {
            std::future::ready(Ok(self.pulls.pop_front().unwrap()))
        }
        fn baseline(
            &mut self,
            _request: BaselineRequest,
        ) -> impl Future<Output = Result<BaselineResult, Self::Error>> {
            let result = match self.baseline.take() {
                Some(page) => BaselineResult::Artifact(page),
                None => BaselineResult::Unavailable,
            };
            std::future::ready(Ok(result))
        }
    }

    fn recovery_page(scope: Scope, position: u64) -> BaselinePage {
        let artifact = SnapshotArtifact::new(
            scope,
            position,
            vec![logical_db_model::SnapshotDocument {
                document: DocumentId::new(),
                snapshot: vec![1, 2, 3],
                tombstone: false,
            }],
        )
        .unwrap();
        BaselinePage {
            protocol: PROTOCOL_VERSION,
            descriptor: artifact.baseline.encode().to_vec(),
            documents: artifact
                .documents
                .iter()
                .map(|document| logical_db_protocol::BaselineDocument {
                    document: document.document,
                    tombstone: document.tombstone,
                    snapshot: document.snapshot.clone(),
                })
                .collect(),
            checkpoint: Checkpoint::from_wire(vec![5]).unwrap(),
        }
    }

    #[test]
    fn baseline_required_installs_state_before_the_suffix_commit() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = empty_replica(scope);
        *replica.checkpoint.borrow_mut() = Some(vec![9]);
        let baseline = recovery_page(scope, 2);
        let baseline_checkpoint = baseline.checkpoint.as_wire().to_vec();
        let suffix_checkpoint = Checkpoint::from_wire(vec![10]).unwrap();
        let mut wire = RecoveryWire {
            pulls: [
                PullResult::BaselineRequired,
                PullResult::Page(PullPage {
                    protocol: PROTOCOL_VERSION,
                    changes: Vec::new(),
                    checkpoint: suffix_checkpoint.clone(),
                    more: false,
                }),
            ]
            .into(),
            baseline: Some(baseline),
        };
        let report = lgwks_std::task::block_on(synchronize(
            &replica,
            PeerId::new(),
            &mut wire,
            SyncLimits::default(),
            || false,
        ))
        .unwrap();
        assert!(report.recovered);
        assert!(!report.full_resync);
        assert_eq!(report.pages, 1);
        let baselines = replica.baselines.borrow();
        assert_eq!(baselines.len(), 1);
        assert_eq!(baselines[0].1, baseline_checkpoint);
        assert!(matches!(
            logical_db_model::SnapshotBaseline::decode(&baselines[0].0),
            Ok(logical_db_model::SnapshotRecord::State(baseline)) if baseline.position() == 2
        ));
        assert_eq!(
            *replica.checkpoint.borrow(),
            Some(suffix_checkpoint.as_wire().to_vec())
        );
    }

    #[test]
    fn report_counts_every_installed_change_and_page() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = empty_replica(scope);
        let remote_replica = ReplicaId::new();
        let change = ChangeEnvelope {
            scope,
            document: DocumentId::new(),
            replica: remote_replica,
            operation: OperationId::new(),
            request: OperationDigest::new([5; 32]),
            digest: ChangeDigest::new([6; 32]),
            bytes: vec![1],
        };
        let mut wire = Wire {
            push: None,
            pulls: [PullResult::Page(PullPage {
                protocol: PROTOCOL_VERSION,
                changes: vec![change.clone()],
                checkpoint: Checkpoint::from_wire(vec![4]).unwrap(),
                more: false,
            })]
            .into(),
        };
        let report = lgwks_std::task::block_on(synchronize(
            &replica,
            PeerId::new(),
            &mut wire,
            SyncLimits::default(),
            || false,
        ))
        .unwrap();
        assert_eq!(report.pulled, 1);
        assert_eq!(report.pages, 1);
        assert_eq!(replica.installed.borrow().len(), 1);
        assert_eq!(replica.installed.borrow()[0].document, change.document);
    }

    #[test]
    fn exact_pull_page_budget_never_fetches_one_page_too_many() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = empty_replica(scope);
        let change = ChangeEnvelope {
            scope,
            document: DocumentId::new(),
            replica: ReplicaId::new(),
            operation: OperationId::new(),
            request: OperationDigest::new([7; 32]),
            digest: ChangeDigest::new([8; 32]),
            bytes: vec![1],
        };
        let mut wire = Wire {
            push: None,
            pulls: [PullResult::Page(PullPage {
                protocol: PROTOCOL_VERSION,
                changes: vec![change],
                checkpoint: Checkpoint::from_wire(vec![2]).unwrap(),
                more: true,
            })]
            .into(),
        };
        let report = lgwks_std::task::block_on(synchronize(
            &replica,
            PeerId::new(),
            &mut wire,
            SyncLimits {
                pages_per_cycle: 1,
                ..SyncLimits::default()
            },
            || false,
        ))
        .unwrap();
        assert_eq!(report.pages, 1);
        assert!(report.more);
        assert!(wire.pulls.is_empty());
    }

    #[test]
    fn sync_error_display_keeps_each_recovery_category_visible() {
        let operation = OperationId::new();
        assert_eq!(
            SyncError::<&str, &str>::Replica("disk").to_string(),
            "replica synchronization failed: disk"
        );
        assert_eq!(
            SyncError::<&str, &str>::Transport("offline").to_string(),
            "transport synchronization failed: offline"
        );
        assert!(
            SyncError::<&str, &str>::Protocol(ProtocolError::PageLimit)
                .to_string()
                .contains("protocol synchronization failed")
        );
        assert!(
            SyncError::<&str, &str>::Rejected {
                operation,
                code: RejectionCode::Forbidden,
            }
            .to_string()
            .contains(&operation.to_string())
        );
        assert_eq!(
            SyncError::<&str, &str>::Cancelled.to_string(),
            "synchronization was cancelled"
        );
    }

    use logical_db_model::{
        NamespaceCheckpoint, NamespaceCheckpoints, NamespacePolicy, NetworkCondition,
    };
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;

    struct NsReplica {
        scope: Scope,
        installation: ReplicaId,
        slot: RefCell<Option<Vec<u8>>>,
        stored: RefCell<Vec<(Vec<u8>, Vec<InboundChange>)>>,
        pending: Vec<OutboundChange>,
        acknowledged: Cell<u64>,
    }

    impl NsReplica {
        fn fresh(scope: Scope, pending: Vec<OutboundChange>) -> Self {
            Self {
                scope,
                installation: ReplicaId::new(),
                slot: RefCell::new(None),
                stored: RefCell::new(Vec::new()),
                pending,
                acknowledged: Cell::new(0),
            }
        }

        fn stored_map(&self) -> NamespaceCheckpoints {
            match self.slot.borrow().clone() {
                None => NamespaceCheckpoints::new(),
                Some(bytes) => NamespaceCheckpoints::decode(&bytes).unwrap(),
            }
        }

        fn seed_map(&self, map: &NamespaceCheckpoints) {
            *self.slot.borrow_mut() = Some(map.encode().unwrap());
        }
    }

    impl ReplicaPort for NsReplica {
        type Error = Infallible;
        fn scope(&self) -> Scope {
            self.scope
        }
        fn replica(&self) -> ReplicaId {
            self.installation
        }
        fn bind_upstream(&self, _peer: PeerId) -> Result<(), Self::Error> {
            Ok(())
        }
        fn pending(&self, limit: usize) -> Result<Vec<OutboundChange>, Self::Error> {
            Ok(self
                .pending
                .iter()
                .filter(|change| change.sequence > self.acknowledged.get())
                .take(limit)
                .cloned()
                .collect())
        }
        fn acknowledge(&self, _peer: PeerId, through: u64) -> Result<(), Self::Error> {
            self.acknowledged.set(through);
            Ok(())
        }
        fn checkpoint(&self, _peer: PeerId) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.slot.borrow().clone())
        }
        fn install(
            &self,
            _peer: PeerId,
            changes: &[InboundChange],
            checkpoint: &[u8],
        ) -> Result<(), Self::Error> {
            self.stored
                .borrow_mut()
                .push((checkpoint.to_vec(), changes.to_vec()));
            *self.slot.borrow_mut() = Some(checkpoint.to_vec());
            Ok(())
        }
        fn install_baseline(
            &self,
            _peer: PeerId,
            _artifact: &SnapshotArtifact,
            _checkpoint: &[u8],
        ) -> Result<(), Self::Error> {
            unreachable!("namespace synchronization fails closed before baseline installation")
        }
    }

    struct LogChange {
        document: DocumentId,
        bytes: Vec<u8>,
    }

    struct PositionalLog {
        scope: Scope,
        origin: ReplicaId,
        entries: RefCell<Vec<LogChange>>,
        floor: u64,
        pulls: Cell<usize>,
        pull_afters: RefCell<Vec<Option<Vec<u8>>>>,
        pushed_operations: RefCell<Vec<OperationId>>,
        grow_per_pull: usize,
        reject_push: bool,
    }

    impl PositionalLog {
        fn position(checkpoint: &Checkpoint) -> u64 {
            let wire = checkpoint.as_wire();
            u64::from_be_bytes(wire.try_into().unwrap())
        }

        fn position_bytes(position: u64) -> Vec<u8> {
            position.to_be_bytes().to_vec()
        }
    }

    impl Transport for PositionalLog {
        type Error = Infallible;
        fn push(
            &mut self,
            request: PushRequest,
        ) -> impl Future<Output = Result<PushResponse, Self::Error>> {
            let mut outcomes = Vec::with_capacity(request.changes.len());
            for change in &request.changes {
                self.pushed_operations.borrow_mut().push(change.operation);
                if self.reject_push {
                    outcomes.push(PushOutcome::Rejected {
                        operation: change.operation,
                        code: RejectionCode::Forbidden,
                    });
                } else {
                    outcomes.push(PushOutcome::Accepted {
                        operation: change.operation,
                    });
                }
            }
            std::future::ready(Ok(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes,
            }))
        }
        fn pull(
            &mut self,
            request: PullRequest,
        ) -> impl Future<Output = Result<PullResult, Self::Error>> {
            self.pulls.set(self.pulls.get() + 1);
            self.pull_afters
                .borrow_mut()
                .push(request.after.clone().map(|after| after.as_wire().to_vec()));
            if self.grow_per_pull > 0 {
                let mut entries = self.entries.borrow_mut();
                for _ in 0..self.grow_per_pull {
                    entries.push(LogChange {
                        document: DocumentId::new(),
                        bytes: vec![9],
                    });
                }
            }
            let position = match &request.after {
                None => 0_u64,
                Some(checkpoint) => Self::position(checkpoint),
            };
            if position < self.floor {
                return std::future::ready(Ok(PullResult::CheckpointExpired));
            }
            let start = usize::try_from(position).unwrap();
            let entries = self.entries.borrow();
            let mut changes = Vec::new();
            let mut used_bytes = 0_usize;
            for entry in entries
                .iter()
                .skip(start)
                .take(usize::from(request.max_changes))
            {
                if used_bytes + entry.bytes.len() > request.max_bytes as usize {
                    break;
                }
                used_bytes += entry.bytes.len();
                changes.push(ChangeEnvelope {
                    scope: self.scope,
                    document: entry.document,
                    replica: self.origin,
                    operation: OperationId::new(),
                    request: OperationDigest::new([3; 32]),
                    digest: ChangeDigest::new([4; 32]),
                    bytes: entry.bytes.clone(),
                });
            }
            let next = start + changes.len();
            let more = next < entries.len();
            std::future::ready(Ok(PullResult::Page(PullPage {
                protocol: PROTOCOL_VERSION,
                changes,
                checkpoint: Checkpoint::from_wire(Self::position_bytes(next as u64)).unwrap(),
                more,
            })))
        }
        fn baseline(
            &mut self,
            _request: BaselineRequest,
        ) -> impl Future<Output = Result<BaselineResult, Self::Error>> {
            std::future::ready(Ok(BaselineResult::Unavailable))
        }
    }

    fn outbound(sequence: u64, document: DocumentId) -> OutboundChange {
        OutboundChange {
            sequence,
            document,
            operation: OperationId::new(),
            request: OperationDigest::new([5; 32]),
            digest: ChangeDigest::new([6; 32]),
            bytes: vec![1],
        }
    }

    fn log_scope() -> Scope {
        Scope::new(AccountId::new(), SpaceId::new())
    }

    fn log_with(
        scope: Scope,
        namespaces: &[&'static str],
    ) -> (PositionalLog, HashMap<DocumentId, &'static str>) {
        let mut ownership = HashMap::new();
        let mut entries = Vec::new();
        for namespace in namespaces {
            let document = DocumentId::new();
            ownership.insert(document, *namespace);
            entries.push(LogChange {
                document,
                bytes: vec![1],
            });
        }
        (
            PositionalLog {
                scope,
                origin: ReplicaId::new(),
                entries: RefCell::new(entries),
                floor: 0,
                pulls: Cell::new(0),
                pull_afters: RefCell::new(Vec::new()),
                pushed_operations: RefCell::new(Vec::new()),
                grow_per_pull: 0,
                reject_push: false,
            },
            ownership,
        )
    }

    fn online_policy(changes_per_page: u16, pages_per_cycle: u8) -> NamespacePolicy {
        NamespacePolicy::new(
            changes_per_page,
            1_048_576,
            pages_per_cycle,
            NetworkCondition::Online,
        )
        .unwrap()
    }

    fn domain(namespace: &str, policy: NamespacePolicy, now_unix_ms: u64) -> NamespaceDomain {
        NamespaceDomain::new(namespace, policy, now_unix_ms).unwrap()
    }

    #[test]
    fn namespace_sync_advances_only_its_own_checkpoint() {
        let scope = log_scope();
        let (mut log, ownership) = log_with(scope, &["quiet", "chatty"]);
        let replica = NsReplica::fresh(scope, Vec::new());
        let peer = PeerId::new();
        let quiet = ownership
            .iter()
            .find_map(|(document, namespace)| (*namespace == "quiet").then_some(*document))
            .unwrap();
        let report = lgwks_std::task::block_on(synchronize_namespace(
            &replica,
            peer,
            &mut log,
            &domain("quiet", online_policy(16, 8), 1_000),
            |document| ownership.get(&document) == Some(&"quiet"),
            || false,
        ))
        .unwrap();
        assert_eq!(report.namespace, "quiet");
        assert_eq!(report.pulled, 1);
        assert_eq!(report.pages, 1);
        assert!(!report.full_resync);
        assert!(!report.more);
        assert!(!report.deferred);
        let installed: Vec<DocumentId> = replica
            .stored
            .borrow()
            .iter()
            .flat_map(|(_, changes)| changes.iter().map(|change| change.document))
            .collect();
        assert_eq!(installed, vec![quiet]);
        let map = replica.stored_map();
        assert_eq!(
            map.get("quiet").unwrap().cursor(),
            PositionalLog::position_bytes(2)
        );
        assert!(map.get("chatty").is_none());
        let rerun = lgwks_std::task::block_on(synchronize_namespace(
            &replica,
            peer,
            &mut log,
            &domain("quiet", online_policy(16, 8), 2_000),
            |document| ownership.get(&document) == Some(&"quiet"),
            || false,
        ))
        .unwrap();
        assert_eq!(rerun.pulled, 0);
        assert!(!rerun.more);
    }

    #[test]
    fn chatty_flood_never_blocks_quiet_convergence_or_upload() {
        let scope = log_scope();
        let (mut log, ownership) = log_with(scope, &["quiet"]);
        log.grow_per_pull = 20;
        let replica = NsReplica::fresh(scope, Vec::new());
        let report = lgwks_std::task::block_on(synchronize_namespace(
            &replica,
            PeerId::new(),
            &mut log,
            &domain("quiet", online_policy(4, 8), 1_000),
            |document| ownership.get(&document) == Some(&"quiet"),
            || false,
        ))
        .unwrap();
        assert_eq!(report.pulled, 1);
        assert_eq!(report.pages, 8);
        assert!(report.more);
        assert!(log.entries.borrow().len() >= 161);

        let quiet_document = DocumentId::new();
        let mut pending = Vec::new();
        for sequence in 1..=50_u64 {
            pending.push(outbound(sequence, DocumentId::new()));
        }
        let quiet_change = outbound(51, quiet_document);
        let quiet_operation = quiet_change.operation;
        pending.push(quiet_change);
        let sender = NsReplica::fresh(scope, pending);
        let mut uplink = log_with(scope, &[]).0;
        let send = lgwks_std::task::block_on(send_namespace_now(
            &sender,
            PeerId::new(),
            &mut uplink,
            &domain("quiet", online_policy(64, 8), 1_000),
            |document| document == quiet_document,
            || false,
        ))
        .unwrap();
        assert_eq!(send.namespace, "quiet");
        assert_eq!(send.uploaded, 1);
        assert!(!send.deferred);
        assert_eq!(*uplink.pushed_operations.borrow(), vec![quiet_operation]);
        assert_eq!(sender.acknowledged.get(), 0);
    }

    #[test]
    fn expired_namespace_checkpoint_takes_the_explicit_recovery_transition() {
        let scope = log_scope();
        let (mut log, ownership) = log_with(scope, &["quiet", "quiet"]);
        let replica = NsReplica::fresh(scope, Vec::new());
        let mut stale = NamespaceCheckpoints::new();
        stale
            .insert(
                "quiet",
                NamespaceCheckpoint::new(PositionalLog::position_bytes(1), 100).unwrap(),
            )
            .unwrap();
        replica.seed_map(&stale);
        let peer = PeerId::new();
        let report = lgwks_std::task::block_on(synchronize_namespace(
            &replica,
            peer,
            &mut log,
            &domain("quiet", online_policy(16, 8), 200),
            |document| ownership.get(&document) == Some(&"quiet"),
            || false,
        ))
        .unwrap();
        assert!(report.full_resync);
        assert_eq!(report.pulled, 2);
        assert_eq!(log.pull_afters.borrow().first(), Some(&None));
        assert_eq!(
            replica.stored_map().get("quiet").unwrap().cursor(),
            PositionalLog::position_bytes(2)
        );

        let mut expired_service = log_with(scope, &[]).0;
        expired_service.floor = u64::MAX;
        let fresh = NsReplica::fresh(scope, Vec::new());
        assert!(matches!(
            lgwks_std::task::block_on(synchronize_namespace(
                &fresh,
                PeerId::new(),
                &mut expired_service,
                &domain("quiet", online_policy(16, 8), 300),
                |_| true,
                || false,
            )),
            Err(NamespaceSyncError::Protocol(
                logical_db_protocol::ProtocolError::InvalidCheckpoint
            ))
        ));
        assert!(fresh.slot.borrow().is_none());
    }

    #[test]
    fn network_condition_gates_namespace_work() {
        let scope = log_scope();
        let (mut log, ownership) = log_with(scope, &["quiet"; 6]);
        let replica = NsReplica::fresh(scope, Vec::new());
        let offline = NamespacePolicy::new(16, 1_048_576, 8, NetworkCondition::Offline).unwrap();
        let deferred = lgwks_std::task::block_on(synchronize_namespace(
            &replica,
            PeerId::new(),
            &mut log,
            &domain("quiet", offline, 1_000),
            |document| ownership.get(&document) == Some(&"quiet"),
            || false,
        ))
        .unwrap();
        assert!(deferred.deferred);
        assert_eq!(deferred.pulled, 0);
        assert_eq!(deferred.pages, 0);
        assert_eq!(log.pulls.get(), 0);
        assert!(replica.slot.borrow().is_none());

        let constrained =
            NamespacePolicy::new(2, 1_048_576, 8, NetworkCondition::Constrained).unwrap();
        let partial = lgwks_std::task::block_on(synchronize_namespace(
            &replica,
            PeerId::new(),
            &mut log,
            &domain("quiet", constrained, 1_000),
            |document| ownership.get(&document) == Some(&"quiet"),
            || false,
        ))
        .unwrap();
        assert!(!partial.deferred);
        assert_eq!(partial.pages, 1);
        assert_eq!(partial.pulled, 2);
        assert!(partial.more);
    }

    #[test]
    fn foreign_slot_bytes_fail_closed_before_any_pull() {
        let scope = log_scope();
        let (mut log, _) = log_with(scope, &["quiet"]);
        let replica = NsReplica::fresh(scope, Vec::new());
        *replica.slot.borrow_mut() = Some(b"legacy-global-cursor".to_vec());
        assert!(matches!(
            lgwks_std::task::block_on(synchronize_namespace(
                &replica,
                PeerId::new(),
                &mut log,
                &domain("quiet", online_policy(16, 8), 1_000),
                |_| true,
                || false,
            )),
            Err(NamespaceSyncError::Model(_))
        ));
        assert_eq!(log.pulls.get(), 0);
        assert_eq!(
            replica.slot.borrow().clone(),
            Some(b"legacy-global-cursor".to_vec())
        );
    }

    #[test]
    fn two_namespaces_converge_independently_over_one_binding() {
        let scope = log_scope();
        let (mut log, ownership) = log_with(scope, &["alpha", "beta", "alpha", "beta"]);
        let replica = NsReplica::fresh(scope, Vec::new());
        let peer = PeerId::new();
        let alpha_report = lgwks_std::task::block_on(synchronize_namespace(
            &replica,
            peer,
            &mut log,
            &domain("alpha", online_policy(1, 8), 1_000),
            |document| ownership.get(&document) == Some(&"alpha"),
            || false,
        ))
        .unwrap();
        assert_eq!(alpha_report.pulled, 2);
        assert_eq!(alpha_report.pages, 4);
        assert!(!alpha_report.more);
        assert!(replica.stored_map().get("beta").is_none());
        let beta_report = lgwks_std::task::block_on(synchronize_namespace(
            &replica,
            peer,
            &mut log,
            &domain("beta", online_policy(1, 8), 1_000),
            |document| ownership.get(&document) == Some(&"beta"),
            || false,
        ))
        .unwrap();
        assert_eq!(beta_report.pulled, 2);
        assert!(!beta_report.more);
        let map = replica.stored_map();
        assert_eq!(
            map.get("alpha").unwrap().cursor(),
            PositionalLog::position_bytes(4)
        );
        assert_eq!(
            map.get("beta").unwrap().cursor(),
            PositionalLog::position_bytes(4)
        );
        let quiet_rerun = lgwks_std::task::block_on(synchronize_namespace(
            &replica,
            peer,
            &mut log,
            &domain("alpha", online_policy(1, 8), 2_000),
            |document| ownership.get(&document) == Some(&"alpha"),
            || false,
        ))
        .unwrap();
        assert_eq!(quiet_rerun.pulled, 0);
        assert!(!quiet_rerun.more);
    }

    #[test]
    fn rejected_namespace_upload_surfaces_without_acknowledgement() {
        let scope = log_scope();
        let document = DocumentId::new();
        let change = outbound(1, document);
        let operation = change.operation;
        let replica = NsReplica::fresh(scope, vec![change]);
        let mut uplink = log_with(scope, &[]).0;
        uplink.reject_push = true;
        assert!(matches!(
            lgwks_std::task::block_on(send_namespace_now(
                &replica,
                PeerId::new(),
                &mut uplink,
                &domain("quiet", online_policy(16, 8), 1_000),
                |candidate| candidate == document,
                || false,
            )),
            Err(NamespaceSyncError::Rejected {
                operation: value,
                code: RejectionCode::Forbidden,
            }) if value == operation
        ));
        assert_eq!(replica.acknowledged.get(), 0);
    }

    struct ScriptWire {
        pulls: std::collections::VecDeque<PullResult>,
        pushed: RefCell<Vec<OperationId>>,
        pulls_seen: Cell<usize>,
    }

    impl ScriptWire {
        fn with_pulls(pulls: Vec<PullResult>) -> Self {
            Self {
                pulls: pulls.into(),
                pushed: RefCell::new(Vec::new()),
                pulls_seen: Cell::new(0),
            }
        }
    }

    impl Transport for ScriptWire {
        type Error = Infallible;
        fn push(
            &mut self,
            request: PushRequest,
        ) -> impl Future<Output = Result<PushResponse, Self::Error>> {
            for change in &request.changes {
                self.pushed.borrow_mut().push(change.operation);
            }
            std::future::ready(Ok(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes: request
                    .changes
                    .iter()
                    .map(|change| PushOutcome::Accepted {
                        operation: change.operation,
                    })
                    .collect(),
            }))
        }
        fn pull(
            &mut self,
            _request: PullRequest,
        ) -> impl Future<Output = Result<PullResult, Self::Error>> {
            self.pulls_seen.set(self.pulls_seen.get() + 1);
            std::future::ready(Ok(self
                .pulls
                .pop_front()
                .expect("orchestrator pulled past the scripted pages")))
        }
        fn baseline(
            &mut self,
            _request: BaselineRequest,
        ) -> impl Future<Output = Result<BaselineResult, Self::Error>> {
            std::future::ready(Ok(BaselineResult::Unavailable))
        }
    }

    fn scripted_page(
        scope: Scope,
        checkpoint: u8,
        more: bool,
        documents: &[DocumentId],
    ) -> PullResult {
        PullResult::Page(PullPage {
            protocol: PROTOCOL_VERSION,
            changes: documents
                .iter()
                .map(|document| ChangeEnvelope {
                    scope,
                    document: *document,
                    replica: ReplicaId::new(),
                    operation: OperationId::new(),
                    request: OperationDigest::new([3; 32]),
                    digest: ChangeDigest::new([4; 32]),
                    bytes: vec![1],
                })
                .collect(),
            checkpoint: Checkpoint::from_wire(vec![checkpoint]).unwrap(),
            more,
        })
    }

    fn scripted_empty_page(checkpoint: u8) -> PullResult {
        PullResult::Page(PullPage {
            protocol: PROTOCOL_VERSION,
            changes: Vec::new(),
            checkpoint: Checkpoint::from_wire(vec![checkpoint]).unwrap(),
            more: false,
        })
    }

    fn pending_replica(scope: Scope, count: u64) -> (Replica, Vec<OperationId>) {
        let mut pending = Vec::new();
        let mut operations = Vec::new();
        for sequence in 1..=count {
            let change = outbound(sequence, DocumentId::new());
            operations.push(change.operation);
            pending.push(change);
        }
        (
            Replica {
                scope,
                installation: ReplicaId::new(),
                pending,
                checkpoint: RefCell::new(None),
                acknowledged: Cell::new(0),
                installed: RefCell::new(Vec::new()),
                baselines: RefCell::new(Vec::new()),
            },
            operations,
        )
    }

    fn orchestrator() -> SyncOrchestrator {
        SyncOrchestrator::new(PeerId::new(), OrchestratorPolicy::default())
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum WireFault {
        Retryable,
        Permanent,
    }

    impl std::fmt::Display for WireFault {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Retryable => formatter.write_str("retryable wire fault"),
                Self::Permanent => formatter.write_str("permanent wire fault"),
            }
        }
    }

    impl std::error::Error for WireFault {}

    struct FlakyTransport {
        faults_left: usize,
        fault: WireFault,
        pushes: RefCell<Vec<OperationId>>,
        pulls_seen: Cell<usize>,
    }

    impl Transport for FlakyTransport {
        type Error = WireFault;
        fn push(
            &mut self,
            request: PushRequest,
        ) -> impl Future<Output = Result<PushResponse, Self::Error>> {
            if self.faults_left > 0 {
                self.faults_left -= 1;
                eprintln!("logical-db-sync test transport: scripted push fault");
                return std::future::ready(Err(self.fault));
            }
            for change in &request.changes {
                self.pushes.borrow_mut().push(change.operation);
            }
            std::future::ready(Ok(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes: request
                    .changes
                    .iter()
                    .map(|change| PushOutcome::Accepted {
                        operation: change.operation,
                    })
                    .collect(),
            }))
        }
        fn pull(
            &mut self,
            _request: PullRequest,
        ) -> impl Future<Output = Result<PullResult, Self::Error>> {
            if self.faults_left > 0 {
                self.faults_left -= 1;
                eprintln!("logical-db-sync test transport: scripted pull fault");
                return std::future::ready(Err(self.fault));
            }
            self.pulls_seen.set(self.pulls_seen.get() + 1);
            std::future::ready(Ok(PullResult::Page(PullPage {
                protocol: PROTOCOL_VERSION,
                changes: Vec::new(),
                checkpoint: Checkpoint::from_wire(vec![7]).unwrap(),
                more: false,
            })))
        }
        fn baseline(
            &mut self,
            _request: BaselineRequest,
        ) -> impl Future<Output = Result<BaselineResult, Self::Error>> {
            std::future::ready(Ok(BaselineResult::Unavailable))
        }
    }

    struct ServiceLog {
        entries: RefCell<Vec<LogChange>>,
        pushes: RefCell<Vec<OperationId>>,
    }

    impl Transport for ServiceLog {
        type Error = Infallible;
        fn push(
            &mut self,
            request: PushRequest,
        ) -> impl Future<Output = Result<PushResponse, Self::Error>> {
            let mut entries = self.entries.borrow_mut();
            let mut outcomes = Vec::with_capacity(request.changes.len());
            for change in &request.changes {
                self.pushes.borrow_mut().push(change.operation);
                let duplicate = entries
                    .iter()
                    .any(|entry| entry.document == change.document && entry.bytes == change.bytes);
                if duplicate {
                    outcomes.push(PushOutcome::Duplicate {
                        operation: change.operation,
                    });
                } else {
                    entries.push(LogChange {
                        document: change.document,
                        bytes: change.bytes.clone(),
                    });
                    outcomes.push(PushOutcome::Accepted {
                        operation: change.operation,
                    });
                }
            }
            std::future::ready(Ok(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes,
            }))
        }
        fn pull(
            &mut self,
            request: PullRequest,
        ) -> impl Future<Output = Result<PullResult, Self::Error>> {
            let position = match &request.after {
                None => 0_u64,
                Some(checkpoint) => PositionalLog::position(checkpoint),
            };
            let start = usize::try_from(position).unwrap();
            let entries = self.entries.borrow();
            let changes = entries
                .iter()
                .skip(start)
                .take(usize::from(request.max_changes))
                .map(|entry| ChangeEnvelope {
                    scope: request.scope,
                    document: entry.document,
                    replica: ReplicaId::new(),
                    operation: OperationId::new(),
                    request: OperationDigest::new([3; 32]),
                    digest: ChangeDigest::new([4; 32]),
                    bytes: entry.bytes.clone(),
                })
                .collect::<Vec<_>>();
            let next = start + changes.len();
            let more = next < entries.len();
            std::future::ready(Ok(PullResult::Page(PullPage {
                protocol: PROTOCOL_VERSION,
                changes,
                checkpoint: Checkpoint::from_wire(PositionalLog::position_bytes(next as u64))
                    .unwrap(),
                more,
            })))
        }
        fn baseline(
            &mut self,
            _request: BaselineRequest,
        ) -> impl Future<Output = Result<BaselineResult, Self::Error>> {
            std::future::ready(Ok(BaselineResult::Unavailable))
        }
    }

    #[test]
    fn orchestrator_policy_rejects_empty_budgets_fail_closed() {
        assert_eq!(
            OrchestratorPolicy::new(0, 1_000, 60_000),
            Err(ProtocolError::PageLimit)
        );
        assert_eq!(
            OrchestratorPolicy::new(8, 0, 60_000),
            Err(ProtocolError::PageLimit)
        );
        assert_eq!(
            OrchestratorPolicy::new(8, 1_000, 0),
            Err(ProtocolError::PageLimit)
        );
        assert_eq!(
            OrchestratorPolicy::new(8, 1_000, 60_000).unwrap(),
            OrchestratorPolicy::default()
        );
    }

    #[test]
    fn orchestrator_state_blob_is_versioned_and_fail_closed() {
        let state = OrchestratorState::new();
        assert_eq!(state.encode().len(), ORCHESTRATOR_STATE_BYTES);
        assert_eq!(OrchestratorState::decode(&state.encode()).unwrap(), state);
        assert!(matches!(
            OrchestratorState::decode(&[0_u8; 4]),
            Err(OrchestratorStateError::Malformed)
        ));
        let mut bad_magic = state.encode();
        bad_magic[0] ^= 0xFF;
        assert!(matches!(
            OrchestratorState::decode(&bad_magic),
            Err(OrchestratorStateError::Malformed)
        ));
        let mut bad_version = state.encode();
        bad_version[8..10].copy_from_slice(&(ORCHESTRATOR_STATE_VERSION + 1).to_be_bytes());
        assert!(matches!(
            OrchestratorState::decode(&bad_version),
            Err(OrchestratorStateError::UnsupportedVersion { actual: 2 })
        ));
    }

    #[test]
    fn orchestrator_send_now_drains_upload_pages_without_app_paging() {
        let scope = log_scope();
        let (replica, operations) = pending_replica(scope, 3);
        let mut wire = ScriptWire::with_pulls(vec![scripted_empty_page(1), scripted_empty_page(2)]);
        let mut engine = orchestrator();
        let retry = RetrySignal::never();
        let report = lgwks_std::task::block_on(engine.send_now(
            &replica,
            &mut wire,
            SyncLimits {
                changes_per_page: 2,
                ..SyncLimits::default()
            },
            1_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(report.pushed, 3);
        assert_eq!(report.pulled, 0);
        assert_eq!(report.pages, 2);
        assert_eq!(report.cycles, 2);
        assert!(!report.more);
        assert!(!report.deferred);
        assert!(!report.full_resync);
        assert_eq!(*wire.pushed.borrow(), operations);
        assert_eq!(replica.acknowledged.get(), 3);
    }

    #[test]
    fn orchestrator_fetch_now_drains_pull_backlog_until_more_clears() {
        let scope = log_scope();
        let replica = empty_replica(scope);
        let documents = [
            DocumentId::new(),
            DocumentId::new(),
            DocumentId::new(),
            DocumentId::new(),
            DocumentId::new(),
        ];
        let mut wire = ScriptWire::with_pulls(vec![
            scripted_page(scope, 1, true, &documents[0..2]),
            scripted_page(scope, 2, true, &documents[2..4]),
            scripted_page(scope, 3, false, &documents[4..5]),
        ]);
        let mut engine = orchestrator();
        let retry = RetrySignal::never();
        let report = lgwks_std::task::block_on(engine.fetch_now(
            &replica,
            &mut wire,
            SyncLimits {
                pages_per_cycle: 1,
                ..SyncLimits::default()
            },
            1_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(report.pulled, 5);
        assert_eq!(report.pages, 3);
        assert_eq!(report.cycles, 3);
        assert!(!report.more);
        assert!(!report.deferred);
        let installed = replica
            .installed
            .borrow()
            .iter()
            .map(|change| change.document)
            .collect::<Vec<_>>();
        assert_eq!(installed, documents);
        assert_eq!(*replica.checkpoint.borrow(), Some(vec![3]));
    }

    #[test]
    fn orchestrator_surfaces_single_recovery_and_second_expiry_as_protocol_error() {
        let scope = log_scope();
        let replica = Replica {
            scope,
            installation: ReplicaId::new(),
            pending: Vec::new(),
            checkpoint: RefCell::new(Some(vec![9])),
            acknowledged: Cell::new(0),
            installed: RefCell::new(Vec::new()),
            baselines: RefCell::new(Vec::new()),
        };
        let mut wire =
            ScriptWire::with_pulls(vec![PullResult::CheckpointExpired, scripted_empty_page(10)]);
        let mut engine = orchestrator();
        let retry = RetrySignal::never();
        let report = lgwks_std::task::block_on(engine.fetch_now(
            &replica,
            &mut wire,
            SyncLimits::default(),
            1_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert!(report.full_resync);
        assert_eq!(report.pages, 1);
        assert!(!report.more);
        assert!(!report.deferred);
        assert_eq!(engine.state().consecutive_failures(), 0);

        let fresh = empty_replica(scope);
        let mut expired = ScriptWire::with_pulls(vec![
            PullResult::CheckpointExpired,
            PullResult::CheckpointExpired,
        ]);
        *fresh.checkpoint.borrow_mut() = Some(vec![9]);
        let mut retrying = orchestrator();
        assert!(matches!(
            lgwks_std::task::block_on(retrying.fetch_now(
                &fresh,
                &mut expired,
                SyncLimits::default(),
                1_000,
                &retry,
                || false,
            )),
            Err(SyncError::Protocol(ProtocolError::InvalidCheckpoint))
        ));
        assert_eq!(retrying.state().consecutive_failures(), 0);
        assert_eq!(retrying.state().not_ready_until_ms(), 0);
    }

    #[test]
    fn orchestrator_backs_off_retryable_then_succeeds_and_surfaces_permanent() {
        let scope = log_scope();
        let (replica, _) = pending_replica(scope, 1);
        let mut wire = FlakyTransport {
            faults_left: 1,
            fault: WireFault::Retryable,
            pushes: RefCell::new(Vec::new()),
            pulls_seen: Cell::new(0),
        };
        let is_retryable = |error: &WireFault| *error == WireFault::Retryable;
        let retry_after = |_: &WireFault| Some(std::time::Duration::from_millis(50));
        let retry = RetrySignal {
            is_retryable: &is_retryable,
            retry_after: &retry_after,
        };
        let mut engine = orchestrator();
        let deferred = lgwks_std::task::block_on(engine.send_now(
            &replica,
            &mut wire,
            SyncLimits::default(),
            0,
            &retry,
            || false,
        ))
        .unwrap();
        assert!(deferred.deferred);
        assert!(deferred.more);
        assert_eq!(deferred.cycles, 0);
        assert_eq!(engine.state().consecutive_failures(), 1);
        assert_eq!(engine.state().not_ready_until_ms(), 50);
        assert!(wire.pushes.borrow().is_empty());

        let gated = lgwks_std::task::block_on(engine.send_now(
            &replica,
            &mut wire,
            SyncLimits::default(),
            10,
            &retry,
            || false,
        ))
        .unwrap();
        assert!(gated.deferred);
        assert_eq!(gated.cycles, 0);
        assert!(wire.pushes.borrow().is_empty());
        assert_eq!(wire.pulls_seen.get(), 0);

        let recovered = lgwks_std::task::block_on(engine.send_now(
            &replica,
            &mut wire,
            SyncLimits::default(),
            60,
            &retry,
            || false,
        ))
        .unwrap();
        assert!(!recovered.deferred);
        assert_eq!(recovered.pushed, 1);
        assert!(!recovered.more);
        assert_eq!(engine.state().consecutive_failures(), 0);
        assert_eq!(wire.pushes.borrow().len(), 1);
        assert_eq!(wire.pulls_seen.get(), 1);

        let (stuck, _) = pending_replica(scope, 1);
        let mut fatal = FlakyTransport {
            faults_left: 1,
            fault: WireFault::Permanent,
            pushes: RefCell::new(Vec::new()),
            pulls_seen: Cell::new(0),
        };
        let mut failing = orchestrator();
        assert!(matches!(
            lgwks_std::task::block_on(failing.send_now(
                &stuck,
                &mut fatal,
                SyncLimits::default(),
                0,
                &retry,
                || false,
            )),
            Err(SyncError::Transport(WireFault::Permanent))
        ));
        assert_eq!(failing.state().consecutive_failures(), 0);
        assert_eq!(failing.state().not_ready_until_ms(), 0);
        assert!(fatal.pushes.borrow().is_empty());
    }

    #[test]
    fn orchestrator_state_restored_after_restart_resumes_without_duplicate_delivery() {
        let scope = log_scope();
        let (replica, operations) = pending_replica(scope, 3);
        let policy = OrchestratorPolicy::new(1, 1_000, 60_000).unwrap();
        let peer = PeerId::new();
        let mut wire = ScriptWire::with_pulls(vec![
            scripted_empty_page(1),
            scripted_empty_page(2),
            scripted_empty_page(3),
        ]);
        let mut engine = SyncOrchestrator::new(peer, policy);
        let retry = RetrySignal::never();
        let limits = SyncLimits {
            changes_per_page: 1,
            ..SyncLimits::default()
        };
        let partial = lgwks_std::task::block_on(engine.send_now(
            &replica,
            &mut wire,
            limits,
            1_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(partial.pushed, 1);
        assert!(partial.more);

        let blob = engine.export_state();
        assert_eq!(OrchestratorState::decode(&blob).unwrap(), engine.state());
        let mut revived = SyncOrchestrator::restore(peer, policy, &blob).unwrap();
        let resumed = lgwks_std::task::block_on(revived.send_now(
            &replica,
            &mut wire,
            limits,
            2_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(resumed.pushed, 1);
        assert!(resumed.more);
        let finished = lgwks_std::task::block_on(revived.send_now(
            &replica,
            &mut wire,
            limits,
            3_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(finished.pushed, 1);
        assert!(!finished.more);
        assert_eq!(*wire.pushed.borrow(), operations);
        assert_eq!(replica.acknowledged.get(), 3);

        assert!(matches!(
            SyncOrchestrator::restore(peer, policy, &[7_u8; 3]),
            Err(OrchestratorStateError::Malformed)
        ));
    }

    #[test]
    fn orchestrator_drives_namespace_domains_without_duplicating_cursors() {
        let scope = log_scope();
        let document = DocumentId::new();
        let change = outbound(1, document);
        let operation = change.operation;
        let replica = NsReplica::fresh(scope, vec![change]);
        let (mut log, mut ownership) = log_with(scope, &["quiet"]);
        ownership.insert(document, "quiet");
        let is_mine = |candidate| ownership.get(&candidate) == Some(&"quiet");
        let mut engine = orchestrator();
        let retry = RetrySignal::never();
        let report = lgwks_std::task::block_on(engine.run_namespace(
            &replica,
            &mut log,
            &domain("quiet", online_policy(16, 8), 1_000),
            &is_mine,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(report.namespace, "quiet");
        assert_eq!(report.send.namespace, "quiet");
        assert_eq!(report.send.uploaded, 1);
        assert!(report.send.more);
        assert_eq!(report.pulled, 1);
        assert_eq!(report.pages, 1);
        assert_eq!(report.cycles, 1);
        assert!(!report.more);
        assert!(!report.deferred);
        assert_eq!(*log.pushed_operations.borrow(), vec![operation]);
        assert_eq!(replica.acknowledged.get(), 0);
        assert_eq!(
            replica.stored_map().get("quiet").unwrap().cursor(),
            PositionalLog::position_bytes(1)
        );

        let pulls_before = log.pulls.get();
        let offline = NamespacePolicy::new(16, 1_048_576, 8, NetworkCondition::Offline).unwrap();
        let parked = lgwks_std::task::block_on(engine.run_namespace(
            &replica,
            &mut log,
            &domain("quiet", offline, 2_000),
            &is_mine,
            &retry,
            || false,
        ))
        .unwrap();
        assert!(parked.deferred);
        assert!(parked.send.deferred);
        assert_eq!(parked.cycles, 0);
        assert_eq!(parked.send.uploaded, 0);
        assert_eq!(parked.pulled, 0);
        assert_eq!(log.pulls.get(), pulls_before);
    }

    #[test]
    fn two_replicas_converge_through_orchestrator_runs_without_duplicate_delivery() {
        let scope = log_scope();
        let first_document = DocumentId::new();
        let second_document = DocumentId::new();
        let first = outbound(1, first_document);
        let second = outbound(2, second_document);
        let operations = vec![first.operation, second.operation];
        let sender = Replica {
            scope,
            installation: ReplicaId::new(),
            pending: vec![first, second],
            checkpoint: RefCell::new(None),
            acknowledged: Cell::new(0),
            installed: RefCell::new(Vec::new()),
            baselines: RefCell::new(Vec::new()),
        };
        let receiver = empty_replica(scope);
        let mut log = ServiceLog {
            entries: RefCell::new(Vec::new()),
            pushes: RefCell::new(Vec::new()),
        };
        let retry = RetrySignal::never();
        let limits = SyncLimits {
            changes_per_page: 8,
            ..SyncLimits::default()
        };
        let mut uplink = SyncOrchestrator::new(PeerId::new(), OrchestratorPolicy::default());
        let sent = lgwks_std::task::block_on(uplink.send_now(
            &sender,
            &mut log,
            limits,
            1_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(sent.pushed, 2);
        assert!(!sent.more);

        let mut downlink = SyncOrchestrator::new(PeerId::new(), OrchestratorPolicy::default());
        let fetched = lgwks_std::task::block_on(downlink.fetch_now(
            &receiver,
            &mut log,
            limits,
            2_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(fetched.pulled, 2);
        assert!(!fetched.more);
        assert!(!fetched.full_resync);
        let installed_docs = receiver
            .installed
            .borrow()
            .iter()
            .map(|change| change.document)
            .collect::<Vec<_>>();
        assert_eq!(installed_docs, vec![first_document, second_document]);
        assert_eq!(*log.pushes.borrow(), operations);

        let quiet = lgwks_std::task::block_on(downlink.send_now(
            &receiver,
            &mut log,
            limits,
            3_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(quiet.pushed, 0);
        assert_eq!(quiet.pulled, 0);
        assert!(!quiet.more);
        let settled = lgwks_std::task::block_on(uplink.fetch_now(
            &sender,
            &mut log,
            limits,
            4_000,
            &retry,
            || false,
        ))
        .unwrap();
        assert_eq!(settled.pulled, 0);
        assert!(!settled.more);
        assert_eq!(*log.pushes.borrow(), operations);
    }
}
