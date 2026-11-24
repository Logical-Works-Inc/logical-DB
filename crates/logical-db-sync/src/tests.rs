//! Push, pull and limit tests over scripted replicas and wires.

use std::{convert::Infallible, future::Future};

use logical_db_model::{AccountId, ChangeDigest, MAX_BASELINE_PAGE_DOCUMENTS, SpaceId};
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

/// A fresh replica in `scope` holding `pending` and no checkpoint.
fn replica_with(scope: Scope, pending: Vec<OutboundChange>) -> Replica {
    Replica {
        scope,
        installation: ReplicaId::new(),
        pending,
        checkpoint: std::cell::RefCell::default(),
        acknowledged: std::cell::Cell::default(),
        installed: std::cell::RefCell::default(),
        baselines: std::cell::RefCell::default(),
    }
}

/// Two one-byte pending changes, the first carrying `first`; `digest_seed`
/// offsets their change digests.
fn two_pending(
    first: OperationId,
    digest_seed: usize,
) -> Result<Vec<OutboundChange>, std::num::TryFromIntError> {
    [first, OperationId::new()]
        .into_iter()
        .zip(1_u8..)
        .map(|(operation, sequence)| {
            let digest = u8::try_from(
                usize::from(sequence)
                    .saturating_add(digest_seed)
                    .saturating_sub(1),
            )?;
            Ok(OutboundChange {
                sequence: u64::from(sequence),
                document: DocumentId::new(),
                operation,
                request: OperationDigest::new([sequence; 32]),
                digest: ChangeDigest::new([digest; 32]),
                bytes: vec![1],
            })
        })
        .collect()
}

impl Replica {
    /// Replaces the stored checkpoint, as every install does last.
    fn store_checkpoint(&self, checkpoint: &[u8]) {
        self.checkpoint.replace(Some(checkpoint.to_vec()));
    }
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
        let floor = self.acknowledged.get();
        let unsent = self.pending.iter().filter(|change| change.sequence > floor);
        Ok(unsent.take(limit).cloned().collect())
    }
    fn acknowledge(&self, _peer: PeerId, through: u64) -> Result<(), Self::Error> {
        self.acknowledged.replace(through);
        Ok(())
    }
    fn checkpoint(&self, _peer: PeerId) -> Result<Option<Vec<u8>>, Self::Error> {
        Ok(self.checkpoint.borrow().clone())
    }
    fn install(&self, _: PeerId, changes: &[InboundChange], at: &[u8]) -> Result<(), Self::Error> {
        self.installed.borrow_mut().extend_from_slice(changes);
        self.store_checkpoint(at);
        Ok(())
    }
    fn install_baseline(
        &self,
        _: PeerId,
        artifact: &SnapshotArtifact,
        at: &[u8],
    ) -> Result<(), Self::Error> {
        let descriptor = artifact.baseline().encode().to_vec();
        self.baselines.borrow_mut().push((descriptor, at.to_vec()));
        self.store_checkpoint(at);
        Ok(())
    }
}

struct Wire {
    push: Option<PushResponse>,
    pulls: std::collections::VecDeque<PullResult>,
}

/// Fixture failure when a scripted transport response is missing.
#[derive(Debug)]
struct MissingScriptedResponse;

impl fmt::Display for MissingScriptedResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("scripted transport response is missing")
    }
}

impl Error for MissingScriptedResponse {}

impl Transport for Wire {
    type Error = MissingScriptedResponse;
    fn push(
        &mut self,
        _request: PushRequest,
    ) -> impl Future<Output = Result<PushResponse, Self::Error>> {
        match self.push.take() {
            Some(response) => std::future::ready(Ok(response)),
            None => std::future::ready(Err(MissingScriptedResponse)),
        }
    }
    fn pull(
        &mut self,
        _request: PullRequest,
    ) -> impl Future<Output = Result<PullResult, Self::Error>> {
        match self.pulls.pop_front() {
            Some(page) => std::future::ready(Ok(page)),
            None => std::future::ready(Err(MissingScriptedResponse)),
        }
    }
    fn baseline(
        &mut self,
        _request: BaselineRequest,
    ) -> impl Future<Output = Result<BaselineResult, Self::Error>> {
        std::future::ready(Ok(BaselineResult::Unavailable))
    }
}

#[test]
fn expired_checkpoint_restarts_pull_without_guessing_continuity() -> Result<(), Box<dyn Error>> {
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
    let new_checkpoint = Checkpoint::from_wire(vec![10])?;
    let mut wire = Wire {
        push: Some(PushResponse::new(
            PROTOCOL_VERSION,
            vec![PushOutcome::Accepted { operation }],
        )),
        pulls: [
            PullResult::CheckpointExpired,
            PullResult::Page(PullPage::new(
                PROTOCOL_VERSION,
                Vec::new(),
                new_checkpoint,
                false,
            )),
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
    ))?;
    assert_eq!(
        report,
        SyncReport {
            pushed: 1,
            pulled: 0,
            pages: 1,
            full_resync: true,
            recovered: false,
            more: false
        },
        "expiry forces one full page without guessing continuity"
    );
    assert_eq!(
        replica.acknowledged.get(),
        4,
        "the accepted push must acknowledge through sequence four"
    );
    assert_eq!(
        *replica.checkpoint.borrow(),
        Some(vec![10]),
        "the expiry restart must install the fresh checkpoint"
    );
    Ok(())
}

#[test]
fn rejected_push_never_advances_acknowledgement() {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let operation = OperationId::new();
    let replica = replica_with(
        scope,
        vec![OutboundChange {
            sequence: 1,
            document: DocumentId::new(),
            operation,
            request: OperationDigest::new([8; 32]),
            digest: ChangeDigest::new([2; 32]),
            bytes: vec![2],
        }],
    );
    let mut wire = Wire {
        push: Some(PushResponse::new(
            PROTOCOL_VERSION,
            vec![PushOutcome::Rejected {
                operation,
                code: RejectionCode::Forbidden,
            }],
        )),
        pulls: std::collections::VecDeque::default(),
    };
    assert!(
        matches!(lgwks_std::task::block_on(synchronize(&replica, PeerId::new(), &mut wire, SyncLimits::default(), || false)), Err(SyncError::Rejected { operation: value, code: RejectionCode::Forbidden }) if value == operation),
        "a forbidden push must surface the rejection without acknowledging"
    );
    assert_eq!(
        replica.acknowledged.get(),
        0,
        "a rejected push must never advance the acknowledgement"
    );
}

#[test]
fn upload_page_stops_at_byte_budget_and_advances_only_sent_prefix() -> Result<(), Box<dyn Error>> {
    let first = OperationId::new();
    let replica = replica_with(
        Scope::new(AccountId::new(), SpaceId::new()),
        two_pending(first, 3)?,
    );
    let mut wire = Wire {
        push: Some(PushResponse::new(
            PROTOCOL_VERSION,
            vec![PushOutcome::Accepted { operation: first }],
        )),
        pulls: std::collections::VecDeque::new(),
    };
    assert_eq!(
        lgwks_std::task::block_on(push_pending(&replica, PeerId::new(), &mut wire, 2, 1,))?,
        (1, true),
        "the byte budget must stop after one one-byte change"
    );
    assert_eq!(
        replica.acknowledged.get(),
        1,
        "only the sent prefix may advance the acknowledgement"
    );
    Ok(())
}

#[test]
fn report_keeps_unsent_upload_backlog_visible() -> Result<(), Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let first = OperationId::new();
    let replica = replica_with(scope, two_pending(first, 1)?);
    let mut wire = Wire {
        push: Some(PushResponse::new(
            PROTOCOL_VERSION,
            vec![PushOutcome::Accepted { operation: first }],
        )),
        pulls: [PullResult::Page(PullPage::new(
            PROTOCOL_VERSION,
            Vec::new(),
            Checkpoint::from_wire(vec![1])?,
            false,
        ))]
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
    ))?;
    assert_eq!(
        report.pushed, 1,
        "the accepted first change must report one push"
    );
    assert!(
        report.more,
        "the unsent second change must keep the backlog visible"
    );
    assert_eq!(
        replica.acknowledged.get(),
        1,
        "acknowledgement advances only through the accepted change"
    );
    Ok(())
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
fn sync_limits_accept_exact_edges_and_reject_every_zero_or_excess() -> Result<(), Box<dyn Error>> {
    let valid = SyncLimits {
        changes_per_page: u16::try_from(logical_db_model::MAX_CHANGES_PER_PAGE)?,
        bytes_per_page: u32::try_from(logical_db_model::MAX_PAGE_BYTES)?,
        pages_per_cycle: u8::try_from(MAX_PAGES_PER_CYCLE)?,
        recovery_pages: MAX_RECOVERY_PAGES,
        recovery_bytes: usize::MAX,
    };
    assert_eq!(
        valid.validate(),
        Ok(valid),
        "the exact protocol edges must validate"
    );
    for invalid in [
        SyncLimits {
            changes_per_page: 0,
            ..valid
        },
        SyncLimits {
            changes_per_page: valid.changes_per_page.saturating_add(1),
            ..valid
        },
        SyncLimits {
            bytes_per_page: 0,
            ..valid
        },
        SyncLimits {
            bytes_per_page: valid.bytes_per_page.saturating_add(1),
            ..valid
        },
        SyncLimits {
            pages_per_cycle: 0,
            ..valid
        },
        SyncLimits {
            pages_per_cycle: valid.pages_per_cycle.saturating_add(1),
            ..valid
        },
        SyncLimits {
            recovery_pages: 0,
            ..valid
        },
        SyncLimits {
            recovery_pages: valid.recovery_pages.saturating_add(1),
            ..valid
        },
        SyncLimits {
            recovery_bytes: MAX_BASELINE_BYTES.saturating_sub(1),
            ..valid
        },
    ] {
        assert_eq!(
            invalid.validate(),
            Err(ProtocolError::PageLimit),
            "zero or excess budget edges must fail closed"
        );
    }
    Ok(())
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

mod recovery;
