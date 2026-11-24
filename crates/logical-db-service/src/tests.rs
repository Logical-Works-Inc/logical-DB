//! Service fixtures: authorities, an in-memory store and change builders.

use std::{collections::HashMap, error::Error, future::Future, sync::Arc, sync::Mutex};

use logical_db_crdt::{Document, Object, Scalar};
use logical_db_model::{
    AccountId, ActionDigest, ChangeDigest, DocumentId, Effect, OperationDigest, OperationId,
    ReplicaId, SnapshotBaseline, SnapshotRecord, SpaceId,
};
use logical_db_protocol::{
    BaselineRequest, Checkpoint, PROTOCOL_VERSION, PageAction, PullRequest, PushOutcome,
    PushRequest, PushResponse, RecoveryAction, RejectionCode, mark_consumed_action, prune_action,
    recover_action, seal_snapshot_action,
};

use super::*;
use crate::{
    checkpoint::CHECKPOINT_MAGIC,
    service::{push_outcome, valid_stored_page},
};

/// Fixture authority: each variant is one grant rule.
#[derive(Clone, Copy)]
enum Fixture {
    /// Issues a fresh grant for every request.
    Allow,
    /// Issues every grant against a moved frontier.
    Stale,
    /// Certifies exactly one captured sync invocation, like a host authority
    /// that binds a grant to the action digest it justified.
    Exact {
        /// Scope the captured invocation must match.
        scope: Scope,
        /// Action digest the captured invocation must match.
        action: ActionDigest,
        /// Frontier the captured invocation must match.
        frontier: u64,
    },
}

use Fixture::{Allow, Stale};

/// Concrete execution grant captured by the fixture authorities: the proven
/// scope, strongest effect, bound action digest and justified frontier.
struct Grant(Scope, Effect, ActionDigest, u64);

impl ExecutionGrant for Grant {
    fn scope(&self) -> Scope {
        self.0
    }
    fn effect(&self) -> Effect {
        self.1
    }
    fn action(&self) -> ActionDigest {
        self.2
    }
    fn frontier(&self) -> u64 {
        self.3
    }
    fn invocation_id(&self) -> [u8; 32] {
        self.2.into_bytes()
    }
}

impl Authority for Fixture {
    type Grant = Grant;
    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        observed: u64,
        _clock_ms: u64,
    ) -> Option<Grant> {
        let justified = match *self {
            Self::Allow => observed,
            // XOR always yields a frontier the store will not observe.
            Self::Stale => observed ^ 1,
            Self::Exact {
                scope: bound_scope,
                action: bound_action,
                frontier: bound_frontier,
            } => {
                let exact = scope == bound_scope
                    && effect == Effect::Sync
                    && action == bound_action
                    && observed == bound_frontier;
                if !exact {
                    return None;
                }
                observed
            }
        };
        Some(Grant(scope, effect, action, justified))
    }
}

/// Mutable retention fixture state for one scope.
#[derive(Clone, Default)]
struct RetentionState {
    /// Durable retention floor after collection.
    floor: u64,
    /// Highest position fully consumed by every live peer.
    consumed: u64,
    /// Sealed state-bearing recovery artifact, when present.
    baseline: Option<SnapshotArtifact>,
}

/// In-memory service store used by the fixture tests.
#[derive(Default)]
struct Memory {
    /// Ordered durable change log shared across scopes.
    changes: Mutex<Vec<ChangeEnvelope>>,
    /// Idempotent operation registry keyed by operation identity.
    operations: Mutex<HashMap<OperationId, (OperationDigest, ChangeDigest)>>,
    /// Per-scope retention fixture state.
    retention: Mutex<HashMap<Scope, RetentionState>>,
}

/// Fixture failures raised while materializing or validating retained state.
#[derive(Debug, Error)]
enum MemoryError {
    /// Durable fixture materialization refused the requested prefix.
    #[error(transparent)]
    Materialization(#[from] SnapshotMaterializationError),
    /// A retained fixture artifact failed its model validation.
    #[error("retained fixture artifact failed validation: {0}")]
    Artifact(#[from] logical_db_model::ModelError),
}

/// Locks a fixture mutex; a poisoned fixture lock still holds whole values.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

impl Memory {
    /// Reads the scoped frontier as floor plus retained change count.
    fn scope_frontier(&self, scope: Scope) -> u64 {
        let changes = lock(&self.changes);
        let floor = self.retention_state(scope).floor;
        floor.saturating_add(Self::retained_count(&changes, scope))
    }

    /// Counts retained changes belonging to one scope, without a cast.
    fn retained_count(changes: &[ChangeEnvelope], scope: Scope) -> u64 {
        let scoped = changes.iter().filter(|change| change.scope() == scope);
        scoped.fold(0, |count, _| count.saturating_add(1))
    }

    /// Reads the per-scope retention state; an unseen scope is a fresh one.
    fn retention_state(&self, scope: Scope) -> RetentionState {
        match lock(&self.retention).get(&scope) {
            Some(state) => state.clone(),
            None => RetentionState::default(),
        }
    }

    /// Rechecks a retention grant against the observed durable frontier.
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

impl ServiceStore for Memory {
    type Error = MemoryError;
    fn accept(
        &self,
        change: &ChangeEnvelope,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        std::future::ready(Ok(self.accept_result(change)))
    }
    fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        std::future::ready(Ok(self.frontier_result(scope)))
    }
    fn retention_floor(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        std::future::ready(Ok(self.retention_floor_result(scope)))
    }
    fn page(
        &self,
        action: PageAction,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
        std::future::ready(Ok(self.page_result(action, grant)))
    }
}

impl RetentionStore for Memory {
    fn consumed_through(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        std::future::ready(Ok(self.consumed_through_result(scope)))
    }
    fn snapshot_baseline(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, Self::Error>> + Send {
        std::future::ready(Ok(self.snapshot_baseline_result(scope)))
    }
    fn build_snapshot(
        &self,
        scope: Scope,
        through: u64,
    ) -> impl Future<Output = Result<SnapshotArtifact, Self::Error>> + Send {
        std::future::ready(self.build_snapshot_result(scope, through))
    }
    fn read_baseline_page(
        &self,
        action: &RecoveryAction,
        descriptor: &SnapshotBaseline,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<BaselinePageResult, Self::Error>> + Send {
        std::future::ready(Ok(self.read_baseline_page_result(action, descriptor, grant)))
    }
    fn seal_snapshot(
        &self,
        scope: Scope,
        artifact: &SnapshotArtifact,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<SealResult, Self::Error>> + Send {
        std::future::ready(Ok(
            self.seal_snapshot_result(scope, artifact, request, grant)
        ))
    }
    fn mark_consumed(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<ConsumedResult, Self::Error>> + Send {
        std::future::ready(Ok(self.mark_consumed_result(scope, through, request, grant)))
    }
    fn prune(
        &self,
        scope: Scope,
        up_to: u64,
        min_live: Option<u64>,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PruneResult, Self::Error>> + Send {
        std::future::ready(self.prune_result(scope, up_to, min_live, request, grant))
    }
}

impl Memory {
    /// Applies one accepted change to the durable operation and change logs.
    fn accept_result(&self, change: &ChangeEnvelope) -> AcceptResult {
        let mut operations = lock(&self.operations);
        if let Some(existing) = operations.get(&change.operation()) {
            return if *existing == (change.request(), change.digest()) {
                AcceptResult::Duplicate
            } else {
                AcceptResult::Conflict
            };
        }
        operations.insert(change.operation(), (change.request(), change.digest()));
        lock(&self.changes).push(change.clone());
        AcceptResult::Accepted
    }
    /// Reads the scoped durable frontier.
    fn frontier_result(&self, scope: Scope) -> u64 {
        self.scope_frontier(scope)
    }
    /// Reads the per-scope retention floor.
    fn retention_floor_result(&self, scope: Scope) -> u64 {
        self.retention_state(scope).floor
    }
    /// Serves one bounded ordered page after a durable position.
    fn page_result(&self, action: PageAction, grant: &impl ExecutionGrant) -> PageResult {
        if grant.scope() != action.scope
            || grant.effect() != Effect::Sync
            || grant.action() != action.digest()
        {
            return PageResult::StaleAuthority;
        }
        let scope = action.scope;
        let after = action.after;
        let max_changes = action.max_changes;
        let max_bytes = action.max_bytes;
        let floor = self.retention_state(scope).floor;
        if after < floor {
            return PageResult::BehindFloor;
        }
        let changes = lock(&self.changes);
        let frontier = floor.saturating_add(Self::retained_count(&changes, scope));
        if grant.frontier() != frontier {
            return PageResult::StaleAuthority;
        }
        let limit = u64::from(max_bytes);
        let mut bytes = 0_usize;
        let page = changes
            .iter()
            .filter(|change| change.scope() == scope)
            .zip(floor..)
            .filter_map(|(change, at)| (at >= after).then_some(change))
            .take(usize::from(max_changes))
            .take_while(|change| {
                bytes = bytes.saturating_add(change.bytes().len());
                matches!(u64::try_from(bytes), Ok(total) if total <= limit)
            })
            .cloned()
            .collect::<Vec<_>>();
        let position = after.saturating_add(Self::retained_count(&page, scope));
        let more = frontier > position;
        PageResult::Page(StoredPage {
            changes: page,
            position,
            more,
        })
    }
    /// Reads the peer-consumption watermark for one scope.
    fn consumed_through_result(&self, scope: Scope) -> u64 {
        self.retention_state(scope).consumed
    }
    /// Reads the sealed state-bearing recovery artifact for one scope.
    fn snapshot_baseline_result(&self, scope: Scope) -> Option<SnapshotRecord> {
        self.retention_state(scope)
            .baseline
            .map(|artifact| SnapshotRecord::State(artifact.baseline().clone()))
    }
    /// Materializes a bounded state artifact through one position.
    fn build_snapshot_result(
        &self,
        scope: Scope,
        through: u64,
    ) -> Result<SnapshotArtifact, MemoryError> {
        let state = self.retention_state(scope);
        let base = state
            .baseline
            .filter(|artifact| artifact.baseline().position() <= through);
        let base_position = match base.as_ref() {
            Some(artifact) => artifact.baseline().position(),
            None => state.floor,
        };
        let changes = lock(&self.changes)
            .iter()
            .filter(|change| change.scope() == scope)
            .zip(state.floor..)
            .filter(|&(_, at)| at >= base_position && at < through)
            .map(|(change, _)| change.clone())
            .collect::<Vec<_>>();
        Ok(materialize_snapshot(
            scope,
            through,
            base.as_ref(),
            &changes,
        )?)
    }
    /// Reads one bounded ordered page of the sealed baseline.
    fn read_baseline_page_result(
        &self,
        action: &RecoveryAction,
        descriptor: &SnapshotBaseline,
        grant: &impl ExecutionGrant,
    ) -> BaselinePageResult {
        if self
            .check_retention_grant(action.scope, action.digest(), grant)
            .is_none()
        {
            return BaselinePageResult::StaleAuthority;
        }
        let Some(artifact) = self.retention_state(action.scope).baseline else {
            return BaselinePageResult::Page(BaselinePageSlice {
                documents: Vec::new(),
                more: false,
            });
        };
        assert!(
            artifact.baseline() == descriptor,
            "memory store baseline descriptor disagrees with its artifact"
        );
        let mut documents = artifact
            .documents()
            .iter()
            .filter(|document| {
                action
                    .after_document
                    .is_none_or(|cursor| document.document() > cursor)
            })
            .take(logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS + 1)
            .cloned()
            .collect::<Vec<_>>();
        let more = documents.len() > logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS;
        documents.truncate(logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS);
        BaselinePageResult::Page(BaselinePageSlice { documents, more })
    }
    /// Durably records one sealed state-bearing baseline.
    fn seal_snapshot_result(
        &self,
        scope: Scope,
        artifact: &SnapshotArtifact,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> SealResult {
        let baseline = artifact.baseline();
        let Some(frontier) = self.check_retention_grant(
            scope,
            seal_snapshot_action(scope, request, baseline),
            grant,
        ) else {
            return SealResult::StaleAuthority;
        };
        if baseline.position() > frontier {
            return SealResult::BeyondFrontier { frontier };
        }
        lock(&self.retention).entry(scope).or_default().baseline = Some(artifact.clone());
        SealResult::Sealed {
            position: baseline.position(),
        }
    }
    /// Advances the peer-consumption watermark for one scope.
    fn mark_consumed_result(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> ConsumedResult {
        let Some(frontier) =
            self.check_retention_grant(scope, mark_consumed_action(scope, request, through), grant)
        else {
            return ConsumedResult::StaleAuthority;
        };
        if through > frontier {
            return ConsumedResult::BeyondFrontier { frontier };
        }
        let mut retention = lock(&self.retention);
        let state = retention.entry(scope).or_default();
        if through <= state.consumed {
            return ConsumedResult::Unchanged {
                consumed_through: state.consumed,
            };
        }
        state.consumed = through;
        ConsumedResult::Advanced {
            consumed_through: through,
        }
    }
    /// Collects the log prefix the peer-aware prune decision allows.
    fn prune_result(
        &self,
        scope: Scope,
        up_to: u64,
        min_live: Option<u64>,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> Result<PruneResult, MemoryError> {
        let Some(frontier) =
            self.check_retention_grant(scope, prune_action(scope, request, up_to, min_live), grant)
        else {
            return Ok(PruneResult::StaleAuthority);
        };
        let state = self.retention_state(scope);
        let through = match prune_decision(frontier, state.floor, up_to, min_live, state.consumed) {
            Ok(through) => through,
            Err(refusal) => return Ok(PruneResult::Refused(refusal)),
        };
        if through > state.floor {
            let Some(artifact) = state.baseline else {
                return Ok(PruneResult::Refused(PruneRefusal::MissingBaseline));
            };
            if artifact.baseline().position() < through {
                return Ok(PruneResult::Refused(PruneRefusal::BaselineBehind {
                    position: artifact.baseline().position(),
                }));
            }
            artifact.validate(scope)?;
            let mut position = state.floor;
            lock(&self.changes).retain(|change| {
                if change.scope() != scope {
                    return true;
                }
                position = position.saturating_add(1);
                position > through
            });
            lock(&self.retention).entry(scope).or_default().floor = through;
        }
        Ok(PruneResult::Pruned(PruneReceipt {
            pruned_through: through,
            floor: through.max(state.floor),
        }))
    }
}

/// Builds one fixture change envelope with a single integer put.
fn change(
    scope: Scope,
    replica: ReplicaId,
    operation: OperationId,
) -> Result<ChangeEnvelope, Box<dyn Error>> {
    let mut document = Document::new();
    let ((), mut changes) =
        document.edit(|editor| editor.put(&Object::root(), "x", Scalar::Integer(1)))?;
    let encoded = changes.pop().ok_or("edit must stage exactly one change")?;
    Ok(ChangeEnvelope::new(
        scope,
        DocumentId::new(),
        replica,
        operation,
        OperationDigest::new([4; 32]),
        encoded.digest(),
        encoded.into_bytes(),
    ))
}

mod protocol;
mod retention;
