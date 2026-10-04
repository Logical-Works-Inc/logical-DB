//! Shared in-memory service composition for black-box loopback journeys.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! This is the reviewed journey composition used by the in-process e2e tests
//! and by the loopback host binary that an *external* consumer syncs against.
//! It exists once so the store, authority, authentication, and clock the
//! offline tests exercise are byte-for-byte the ones the external tape
//! exercises. It is a test composition: it never ships in a product binary and
//! makes no durability claim.

use std::{
    collections::HashMap,
    future::{Future, ready},
    sync::Mutex,
};

use logical_db_model::{
    ActionDigest, ChangeDigest, Effect, MAX_BASELINE_PAGE_DOCUMENTS, OperationDigest, OperationId,
    Scope, SnapshotArtifact, SnapshotBaseline, SnapshotRecord,
};
use logical_db_protocol::{
    ChangeEnvelope, PageAction, RecoveryAction, mark_consumed_action, prune_action,
    seal_snapshot_action, write_action,
};
use logical_db_server::{AuthenticationError, Authenticator, Clock};
use logical_db_service::{
    AcceptResult, Authority, BaselinePageResult, BaselinePageSlice, ConsumedResult, ExecutionGrant,
    PageResult, PruneReceipt, PruneRefusal, PruneResult, RetentionStore, SealResult, ServiceStore,
    StoredPage, materialize_snapshot, prune_decision,
};

/// A grant that authorizes exactly the scope, effect, action, and frontier it
/// was minted for.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Grant {
    /// Authorized scope.
    pub scope: Scope,
    /// Authorized effect.
    pub effect: Effect,
    /// Authorized canonical action digest.
    pub action: ActionDigest,
    /// Durability frontier the authorization was justified against.
    pub frontier: u64,
}

impl ExecutionGrant for Grant {
    fn scope(&self) -> Scope {
        self.scope
    }

    fn effect(&self) -> Effect {
        self.effect
    }

    fn action(&self) -> ActionDigest {
        self.action
    }

    fn frontier(&self) -> u64 {
        self.frontier
    }

    fn invocation_id(&self) -> [u8; 32] {
        self.action.into_bytes()
    }
}

/// Authority that mints a grant for any justified request.
#[non_exhaustive]
pub struct Allow;

impl Authority for Allow {
    type Grant = Grant;

    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        _now_unix_ms: u64,
    ) -> Option<Self::Grant> {
        Some(Grant {
            scope,
            effect,
            action,
            frontier,
        })
    }
}

/// Bearer authentication for the loopback journey host.
#[non_exhaustive]
pub struct TestAuthentication;

impl Authenticator for TestAuthentication {
    type Policy = Allow;

    fn authenticate(
        &self,
        authorization: &axum::http::HeaderValue,
        _now_unix_ms: u64,
    ) -> Result<Self::Policy, AuthenticationError> {
        if authorization.as_bytes() == b"Bearer black-box" {
            Ok(Allow)
        } else {
            Err(AuthenticationError::Unauthorized)
        }
    }
}

/// Clock pinned to one instant so checkpoint expiry is deterministic.
#[non_exhaustive]
pub struct FixedClock;

impl Clock for FixedClock {
    fn now_unix_ms(&self) -> Option<u64> {
        Some(10_000)
    }
}

/// Retention bookkeeping for one scope.
#[derive(Clone, Default)]
#[non_exhaustive]
pub struct RetentionState {
    /// Retention floor: the first position that is still addressable.
    pub floor: u64,
    /// Highest position marked consumed.
    pub consumed: u64,
    /// Sealed state-bearing baseline, when one exists.
    pub baseline: Option<SnapshotArtifact>,
}

/// In-memory service store implementing the full retention contract.
#[derive(Default)]
#[non_exhaustive]
pub struct MemoryServiceStore {
    /// Durable change log for every scope served by this store.
    changes: Mutex<Vec<ChangeEnvelope>>,
    /// Idempotency map binding each operation to its request and change.
    operations: Mutex<HashMap<OperationId, (OperationDigest, ChangeDigest)>>,
    /// Per-scope retention floor, consumed position, and sealed baseline.
    retention: Mutex<HashMap<Scope, RetentionState>>,
}

impl MemoryServiceStore {
    /// Returns the durable frontier for one scope: floor plus retained changes.
    fn scope_frontier(&self, scope: Scope) -> u64 {
        let changes = self
            .changes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.retention_state(scope)
            .floor
            .saturating_add(Self::retained_count(&changes, scope))
    }

    /// Counts the retained changes belonging to one scope.
    fn retained_count(changes: &[ChangeEnvelope], scope: Scope) -> u64 {
        u64::try_from(
            changes
                .iter()
                .filter(|change| change.scope == scope)
                .count(),
        )
        .unwrap_or(u64::MAX)
    }

    /// Returns a copy of one scope's retention state, or its default.
    fn retention_state(&self, scope: Scope) -> RetentionState {
        self.retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&scope)
            .cloned()
            .unwrap_or_default()
    }

    /// Validates one retention grant against the observed frontier.
    fn check_retention_grant(
        &self,
        scope: Scope,
        action: ActionDigest,
        grant: &impl ExecutionGrant,
    ) -> Option<u64> {
        let frontier = self.scope_frontier(scope);
        if grant.scope() != scope
            || grant.effect() != Effect::Sync
            || grant.action() != action
            || grant.frontier() != frontier
        {
            return None;
        }
        Some(frontier)
    }
}

impl ServiceStore for MemoryServiceStore {
    type Error = logical_db_model::ModelError;

    fn accept(
        &self,
        change: &ChangeEnvelope,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        if grant.scope() != change.scope
            || grant.effect() != Effect::Write
            || grant.action() != write_action(change)
        {
            return ready(Ok(AcceptResult::StaleAuthority));
        }
        let mut operations = self
            .operations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outcome = match operations.get(&change.operation) {
            Some(existing) if *existing == (change.request, change.digest) => {
                AcceptResult::Duplicate
            }
            Some(_) => AcceptResult::Conflict,
            None => {
                operations.insert(change.operation, (change.request, change.digest));
                self.changes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(change.clone());
                AcceptResult::Accepted
            }
        };
        ready(Ok(outcome))
    }

    fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(Ok(self.scope_frontier(scope)))
    }

    fn retention_floor(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(Ok(self.retention_state(scope).floor))
    }

    fn page(
        &self,
        action: PageAction,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
        let PageAction {
            scope,
            after,
            max_changes,
            max_bytes,
            ..
        } = action;
        let floor = self.retention_state(scope).floor;
        if after < floor {
            return ready(Ok(PageResult::BehindFloor));
        }
        let changes = self
            .changes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let scoped = changes
            .iter()
            .filter(|change| change.scope == scope)
            .collect::<Vec<_>>();
        if grant.scope() != scope
            || grant.effect() != Effect::Sync
            || grant.action() != action.digest()
            || grant.frontier()
                != floor.saturating_add(u64::try_from(scoped.len()).unwrap_or(u64::MAX))
        {
            return ready(Ok(PageResult::StaleAuthority));
        }
        let mut bytes = 0usize;
        let page = scoped
            .iter()
            .skip(usize::try_from(after.saturating_sub(floor)).unwrap_or(usize::MAX))
            .take(usize::from(max_changes))
            .take_while(|change| {
                bytes = bytes.saturating_add(change.as_bytes().len());
                bytes <= usize::try_from(max_bytes).unwrap_or(usize::MAX)
            })
            .map(|change| (*change).clone())
            .collect::<Vec<_>>();
        let position = after.saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
        ready(Ok(PageResult::Page(StoredPage::new(
            page,
            position,
            floor.saturating_add(u64::try_from(scoped.len()).unwrap_or(u64::MAX)) > position,
        ))))
    }
}

impl RetentionStore for MemoryServiceStore {
    fn consumed_through(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(Ok(self.retention_state(scope).consumed))
    }

    fn snapshot_baseline(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, Self::Error>> + Send {
        ready(Ok(self
            .retention_state(scope)
            .baseline
            .map(|artifact| SnapshotRecord::State(artifact.baseline))))
    }

    fn build_snapshot(
        &self,
        scope: Scope,
        through: u64,
    ) -> impl Future<Output = Result<SnapshotArtifact, Self::Error>> + Send {
        let state = self.retention_state(scope);
        let base = state
            .baseline
            .filter(|artifact| artifact.baseline.position() <= through);
        let base_position = base
            .as_ref()
            .map_or(state.floor, |artifact| artifact.baseline.position());
        let skip = usize::try_from(base_position.saturating_sub(state.floor)).unwrap_or(usize::MAX);
        let retained = usize::try_from(through.saturating_sub(base_position)).unwrap_or(usize::MAX);
        let changes = self
            .changes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let retained_changes = changes
            .iter()
            .filter(|change| change.scope == scope)
            .skip(skip)
            .take(retained)
            .cloned()
            .collect::<Vec<_>>();
        ready(
            materialize_snapshot(scope, through, base.as_ref(), &retained_changes).map_err(
                |error| {
                    let _ = error;
                    logical_db_model::ModelError::PageTooLarge
                },
            ),
        )
    }

    fn read_baseline_page(
        &self,
        action: &RecoveryAction,
        descriptor: &SnapshotBaseline,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<BaselinePageResult, Self::Error>> + Send {
        if self
            .check_retention_grant(action.scope, action.digest(), grant)
            .is_none()
        {
            return ready(Ok(BaselinePageResult::StaleAuthority));
        }
        let Some(artifact) = self.retention_state(action.scope).baseline else {
            return ready(Ok(BaselinePageResult::Page(BaselinePageSlice::new(
                Vec::new(),
                false,
            ))));
        };
        assert_eq!(
            &artifact.baseline, descriptor,
            "the journey store baseline descriptor agrees with its artifact"
        );
        let mut documents = artifact
            .documents
            .iter()
            .filter(|document| {
                action
                    .after_document
                    .is_none_or(|cursor| document.document > cursor)
            })
            .take(MAX_BASELINE_PAGE_DOCUMENTS + 1)
            .cloned()
            .collect::<Vec<_>>();
        let more = documents.len() > MAX_BASELINE_PAGE_DOCUMENTS;
        documents.truncate(MAX_BASELINE_PAGE_DOCUMENTS);
        ready(Ok(BaselinePageResult::Page(BaselinePageSlice::new(
            documents, more,
        ))))
    }

    fn seal_snapshot(
        &self,
        scope: Scope,
        artifact: &SnapshotArtifact,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<SealResult, Self::Error>> + Send {
        let baseline = &artifact.baseline;
        let Some(frontier) = self.check_retention_grant(
            scope,
            seal_snapshot_action(scope, request, baseline),
            grant,
        ) else {
            return ready(Ok(SealResult::StaleAuthority));
        };
        if baseline.position() > frontier {
            return ready(Ok(SealResult::BeyondFrontier { frontier }));
        }
        self.retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry(scope)
            .or_default()
            .baseline = Some(artifact.clone());
        ready(Ok(SealResult::Sealed {
            position: baseline.position(),
        }))
    }

    fn mark_consumed(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<ConsumedResult, Self::Error>> + Send {
        let Some(frontier) =
            self.check_retention_grant(scope, mark_consumed_action(scope, request, through), grant)
        else {
            return ready(Ok(ConsumedResult::StaleAuthority));
        };
        if through > frontier {
            return ready(Ok(ConsumedResult::BeyondFrontier { frontier }));
        }
        let mut retention = self
            .retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = retention.entry(scope).or_default();
        if through <= state.consumed {
            return ready(Ok(ConsumedResult::Unchanged {
                consumed_through: state.consumed,
            }));
        }
        state.consumed = through;
        ready(Ok(ConsumedResult::Advanced {
            consumed_through: through,
        }))
    }

    fn prune(
        &self,
        scope: Scope,
        up_to: u64,
        min_live: Option<u64>,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PruneResult, Self::Error>> + Send {
        let Some(frontier) =
            self.check_retention_grant(scope, prune_action(scope, request, up_to, min_live), grant)
        else {
            return ready(Ok(PruneResult::StaleAuthority));
        };
        let state = self.retention_state(scope);
        let through = match prune_decision(frontier, state.floor, up_to, min_live, state.consumed) {
            Ok(through) => through,
            Err(refusal) => return ready(Ok(PruneResult::Refused(refusal))),
        };
        if through > state.floor {
            let Some(artifact) = state.baseline.as_ref() else {
                return ready(Ok(PruneResult::Refused(PruneRefusal::MissingBaseline)));
            };
            if artifact.baseline.position() < through {
                return ready(Ok(PruneResult::Refused(PruneRefusal::BaselineBehind {
                    position: artifact.baseline.position(),
                })));
            }
            let floor = state.floor;
            let mut position = floor;
            self.changes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retain(|change| {
                    if change.scope != scope {
                        return true;
                    }
                    position = position.saturating_add(1);
                    position > through
                });
            self.retention
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(scope)
                .or_default()
                .floor = through;
        }
        ready(Ok(PruneResult::Pruned(PruneReceipt::new(
            through,
            through.max(state.floor),
        ))))
    }
}

/// Binds a loopback listener on an ephemeral port and serves the journey
/// composition, returning the address and the server task handle.
///
/// # Errors
///
/// Returns the bind failure when no loopback port is available.
///
/// # Panics
///
/// Panics only if the served task itself fails, which would mean the loopback
/// listener was torn down outside this call. That is a test-composition
/// invariant, not a recoverable input condition.
pub async fn serve_journey(
    store: std::sync::Arc<MemoryServiceStore>,
) -> Result<(std::net::SocketAddr, lgwks_bot::rt::task::JoinHandle<()>), std::io::Error> {
    use logical_db_server::router;
    use logical_db_service::CheckpointCodec;

    let app = router(
        std::sync::Arc::clone(&store),
        TestAuthentication,
        CheckpointCodec::new([23; 32], 60_000)
            .map_err(|error| std::io::Error::other(error.to_string()))?,
        FixedClock,
    );
    let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = lgwks_bot::rt::task::spawn(async move {
        let serve_result = axum::serve(listener, app).await;
        drop(serve_result);
    });
    Ok((address, server))
}
