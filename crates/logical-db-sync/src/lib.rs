//! Bounded, transport-neutral push/pull orchestration.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{error::Error, fmt};

use logical_db_model::{
    ChangeDigest, DocumentId, ModelError, NAMESPACE_CHECKPOINT_TTL_MS, NamespaceCheckpoint,
    NamespaceCheckpoints, NamespacePolicy, NetworkCondition, OperationDigest, OperationId, PeerId,
    ReplicaId, Scope, is_namespace_checkpoint_expired, validate_namespace,
};
use logical_db_protocol::{
    ChangeEnvelope, Checkpoint, PROTOCOL_VERSION, ProtocolError, PullPage, PullRequest,
    PushOutcome, PushRequest, PushResponse, RejectionCode,
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
}

/// Result of requesting one pull page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PullResult {
    /// A normal bounded page.
    Page(PullPage),
    /// The service no longer recognizes the supplied checkpoint.
    CheckpointExpired,
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
    let mut after = durable_checkpoint
        .map(Checkpoint::from_wire)
        .transpose()
        .map_err(SyncError::Protocol)?;
    let mut restarted_after_expiry = false;
    while report.pages < usize::from(limits.pages_per_cycle) {
        let request = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope: replica.scope(),
            replica: replica.replica(),
            request: pull_request_digest(
                replica.scope(),
                replica.replica(),
                after.as_ref(),
                limits,
            ),
            after: after.clone(),
            max_changes: limits.changes_per_page,
            max_bytes: limits.bytes_per_page,
        };
        request.validate().map_err(SyncError::Protocol)?;
        let page = match transport
            .pull(request)
            .await
            .map_err(SyncError::Transport)?
        {
            PullResult::Page(page) => page,
            PullResult::CheckpointExpired if after.is_some() && !restarted_after_expiry => {
                after = None;
                report.full_resync = true;
                restarted_after_expiry = true;
                continue;
            }
            PullResult::CheckpointExpired => {
                return Err(SyncError::Protocol(ProtocolError::InvalidCheckpoint));
            }
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
        report.pulled += inbound.len();
        report.pages += 1;
        let download_more = page.more;
        report.more = upload_more || download_more;
        after = Some(page.checkpoint);
        if !download_more {
            return Ok(report);
        }
        if cancelled() {
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
        return Err(NamespaceSyncError::Cancelled);
    }
    replica
        .bind_upstream(peer)
        .map_err(NamespaceSyncError::Replica)?;
    let start = namespace_start(replica, peer, namespace, now_unix_ms)?;
    let mut checkpoints = start.checkpoints;
    let mut after = start.after;
    let mut restarted_after_expiry = start.restarted_after_expiry;
    if start.recovered {
        report.full_resync = true;
    }
    while report.pages < usize::from(effective_pages(policy)) {
        let request = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope: replica.scope(),
            replica: replica.replica(),
            request: namespace_pull_digest(
                replica.scope(),
                replica.replica(),
                namespace,
                after.as_ref(),
                policy,
            ),
            after: after.clone(),
            max_changes: policy.changes_per_page(),
            max_bytes: policy.bytes_per_page(),
        };
        request.validate().map_err(NamespaceSyncError::Protocol)?;
        let page = match transport
            .pull(request)
            .await
            .map_err(NamespaceSyncError::Transport)?
        {
            PullResult::Page(page) => page,
            PullResult::CheckpointExpired if after.is_some() && !restarted_after_expiry => {
                after = None;
                report.full_resync = true;
                restarted_after_expiry = true;
                continue;
            }
            PullResult::CheckpointExpired => {
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
            namespace,
            &page,
            &is_mine,
            &mut checkpoints,
            now_unix_ms,
        )?;
        report.pulled += installed;
        report.pages += 1;
        report.more = page.more;
        after = Some(page.checkpoint);
        if !page.more {
            return Ok(report);
        }
        if cancelled() {
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
    restarted_after_expiry: bool,
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
    let mut restarted_after_expiry = false;
    let mut recovered = false;
    if let Some(stored) = checkpoints.get(namespace) {
        if is_namespace_checkpoint_expired(stored, now_unix_ms) {
            recovered = true;
            restarted_after_expiry = true;
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
        restarted_after_expiry,
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

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

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
    }

    struct Wire {
        push: Option<PushResponse>,
        pulls: std::collections::VecDeque<PullResult>,
    }

    #[allow(clippy::unused_async_trait_impl)]
    impl Transport for Wire {
        type Error = Infallible;
        async fn push(&mut self, _request: PushRequest) -> Result<PushResponse, Self::Error> {
            Ok(self.push.take().unwrap())
        }
        async fn pull(&mut self, _request: PullRequest) -> Result<PullResult, Self::Error> {
            Ok(self.pulls.pop_front().unwrap())
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
        let report = futures::executor::block_on(synchronize(
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
            matches!(futures::executor::block_on(synchronize(&replica, PeerId::new(), &mut wire, SyncLimits::default(), || false)), Err(SyncError::Rejected { operation: value, code: RejectionCode::Forbidden }) if value == operation)
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
        };
        let mut wire = Wire {
            push: Some(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes: vec![PushOutcome::Accepted { operation: first }],
            }),
            pulls: std::collections::VecDeque::new(),
        };
        assert_eq!(
            futures::executor::block_on(push_pending(&replica, PeerId::new(), &mut wire, 2, 1,))
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
        let report = futures::executor::block_on(synchronize(
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
            futures::executor::block_on(synchronize(
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
        let report = futures::executor::block_on(synchronize(
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
        let report = futures::executor::block_on(synchronize(
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

    #[allow(clippy::unused_async_trait_impl)]
    impl Transport for PositionalLog {
        type Error = Infallible;
        async fn push(&mut self, request: PushRequest) -> Result<PushResponse, Self::Error> {
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
            Ok(PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes,
            })
        }
        async fn pull(&mut self, request: PullRequest) -> Result<PullResult, Self::Error> {
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
                return Ok(PullResult::CheckpointExpired);
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
            Ok(PullResult::Page(PullPage {
                protocol: PROTOCOL_VERSION,
                changes,
                checkpoint: Checkpoint::from_wire(Self::position_bytes(next as u64)).unwrap(),
                more,
            }))
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
        let report = futures::executor::block_on(synchronize_namespace(
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
        let rerun = futures::executor::block_on(synchronize_namespace(
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
        let report = futures::executor::block_on(synchronize_namespace(
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
        let send = futures::executor::block_on(send_namespace_now(
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
        let report = futures::executor::block_on(synchronize_namespace(
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
            futures::executor::block_on(synchronize_namespace(
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
        let deferred = futures::executor::block_on(synchronize_namespace(
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
        let partial = futures::executor::block_on(synchronize_namespace(
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
            futures::executor::block_on(synchronize_namespace(
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
        let alpha_report = futures::executor::block_on(synchronize_namespace(
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
        let beta_report = futures::executor::block_on(synchronize_namespace(
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
        let quiet_rerun = futures::executor::block_on(synchronize_namespace(
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
            futures::executor::block_on(send_namespace_now(
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
}
