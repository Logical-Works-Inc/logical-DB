//! Bounded, transport-neutral push/pull orchestration.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{error::Error, fmt};

use logical_db_model::{
    ChangeDigest, DocumentId, OperationDigest, OperationId, PeerId, ReplicaId, Scope,
};
use logical_db_protocol::{
    ChangeEnvelope, Checkpoint, PROTOCOL_VERSION, ProtocolError, PullPage, PullRequest,
    PushOutcome, PushRequest, PushResponse,
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
    /// Bounded unacknowledged local changes.
    ///
    /// # Errors
    ///
    /// Returns the replica's durable read or validation failure.
    fn pending(&self, limit: usize) -> Result<Vec<OutboundChange>, Self::Error>;
    /// Atomically advances the durable upload acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns the replica's durable commit or validation failure.
    fn acknowledge(&self, through: u64) -> Result<(), Self::Error>;
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
    /// Whether more remote pages remain after the host budget was consumed.
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
    Rejected(OperationId),
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
            Self::Rejected(operation) => {
                write!(formatter, "service rejected operation {operation}")
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
    let pushed = push_pending(
        replica,
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
    for _ in 0..limits.pages_per_cycle {
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
            PullResult::CheckpointExpired if after.is_some() => {
                after = None;
                report.full_resync = true;
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
        report.more = page.more;
        after = Some(page.checkpoint);
        if !report.more {
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
    transport: &mut T,
    page_limit: u16,
    byte_limit: u32,
) -> Result<usize, SyncError<R::Error, T::Error>>
where
    R: ReplicaPort,
    T: Transport,
{
    let available = replica
        .pending(usize::from(page_limit))
        .map_err(SyncError::Replica)?;
    if available.is_empty() {
        return Ok(0);
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
        if let PushOutcome::Rejected { operation, .. } = outcome {
            return Err(SyncError::Rejected(*operation));
        }
    }
    let through = pending.last().map_or(0, |change| change.sequence);
    replica.acknowledge(through).map_err(SyncError::Replica)?;
    Ok(pending.len())
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
        fn pending(&self, limit: usize) -> Result<Vec<OutboundChange>, Self::Error> {
            Ok(self.pending.iter().take(limit).cloned().collect())
        }
        fn acknowledge(&self, through: u64) -> Result<(), Self::Error> {
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
            SyncLimits::default(),
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
            matches!(futures::executor::block_on(synchronize(&replica, PeerId::new(), &mut wire, SyncLimits::default(), || false)), Err(SyncError::Rejected(value)) if value == operation)
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
            futures::executor::block_on(push_pending(&replica, &mut wire, 2, 1)).unwrap(),
            1
        );
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
            SyncError::<&str, &str>::Rejected(operation)
                .to_string()
                .contains(&operation.to_string())
        );
        assert_eq!(
            SyncError::<&str, &str>::Cancelled.to_string(),
            "synchronization was cancelled"
        );
    }
}
