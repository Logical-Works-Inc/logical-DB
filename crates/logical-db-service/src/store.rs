//! Durable store contracts: change log, pages, retention and recovery.

use std::{future::Future, sync::Arc};

use logical_db_model::{
    OperationDigest, Scope, SnapshotArtifact, SnapshotBaseline, SnapshotDocument, SnapshotRecord,
};
use logical_db_protocol::{ChangeEnvelope, PageAction, RecoveryAction};

use crate::authority::ExecutionGrant;

/// Durable outcome of accepting one immutable change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AcceptResult {
    /// Newly appended to the scoped change log.
    Accepted,
    /// Exact operation and bytes were already appended.
    Duplicate,
    /// A prior attempt may have committed and exact durable identity proved it did.
    RecoveredUnknownCommit,
    /// Operation identity exists with different request or change bytes.
    Conflict,
    /// Durable frontier changed after the invocation was justified.
    StaleAuthority,
    /// The invocation identity was atomically consumed by another operation.
    InvocationConsumed,
}

/// Bounded page returned by a service storage adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StoredPage {
    /// Changes strictly after the requested position; adapters build pages
    /// through [`StoredPage::new`].
    pub(crate) changes: Vec<ChangeEnvelope>,
    /// Durable position covering every returned change.
    pub(crate) position: u64,
    /// Whether another page is immediately available.
    pub(crate) more: bool,
}

impl StoredPage {
    /// Builds one durable page returned by a service storage adapter.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose stored pages through here.
    #[must_use]
    pub fn new(changes: Vec<ChangeEnvelope>, position: u64, more: bool) -> Self {
        Self {
            changes,
            position,
            more,
        }
    }

    /// Changes strictly after the requested position.
    #[must_use]
    pub fn changes(&self) -> &[ChangeEnvelope] {
        &self.changes
    }

    /// Durable position covering every returned change.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Whether another page is immediately available.
    #[must_use]
    pub const fn more(&self) -> bool {
        self.more
    }
}

/// Durable outcome of executing one frontier-bound page invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PageResult {
    /// The transaction observed the justified frontier and returned a page.
    Page(StoredPage),
    /// The requested position fell behind the durable retention floor.
    ///
    /// The caller must take the explicit full-recovery transition instead of
    /// applying a partial prefix.
    BehindFloor,
    /// The scope frontier changed before the read transaction executed.
    StaleAuthority,
}

/// Stable retry classification for failures crossing the service boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StoreFailureClass {
    /// Retrying the exact idempotent request after backoff may make progress.
    Transient,
    /// Retry cannot repair the failure and risks an unbounded request storm.
    Permanent,
}

/// Service-side durable storage contract implemented by `FoundationDB`.
pub trait ServiceStore: Send + Sync {
    /// Adapter-specific failure.
    type Error;

    /// Classifies one adapter failure for transport retry semantics.
    ///
    /// The fail-closed default is permanent. Adapters must explicitly opt
    /// individual failure categories into automatic retry.
    fn classify_error(_error: &Self::Error) -> StoreFailureClass {
        StoreFailureClass::Permanent
    }

    /// Idempotently accepts one validated immutable change.
    ///
    /// # Errors
    ///
    /// Returns a typed backend transaction or corruption failure.
    fn accept(
        &self,
        change: &ChangeEnvelope,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send;

    /// Reads the current monotonic scoped frontier used for authority justification.
    fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Reads the durable retention floor below which log history is pruned.
    ///
    /// The default reports no floor, preserving the pre-retention page shape
    /// for adapters without peer-aware collection. Adapters that prune must
    /// override this with their durable per-scope floor.
    fn retention_floor(
        &self,
        _scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        async { Ok(0) }
    }

    /// Reads a bounded ordered page after a durable scoped position.
    ///
    /// # Errors
    ///
    /// Returns a typed backend read, decode, or corruption failure.
    fn page(
        &self,
        action: PageAction,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send;
}

impl<Store> ServiceStore for Arc<Store>
where
    Store: ServiceStore + ?Sized,
{
    type Error = Store::Error;

    fn classify_error(error: &Self::Error) -> StoreFailureClass {
        Store::classify_error(error)
    }

    fn accept(
        &self,
        change: &ChangeEnvelope,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        (**self).accept(change, grant)
    }

    fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        (**self).frontier(scope)
    }

    fn retention_floor(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        (**self).retention_floor(scope)
    }

    fn page(
        &self,
        action: PageAction,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
        (**self).page(action, grant)
    }
}

/// Stable refusal reasons for one peer-aware prune decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PruneRefusal {
    /// No state-bearing recovery artifact covers the requested prefix.
    MissingBaseline,
    /// The newest state-bearing recovery artifact stops before the requested prefix.
    BaselineBehind {
        /// Highest position covered by the recovery artifact.
        position: u64,
    },
    /// A live unexpired checkpoint still addresses the requested prefix.
    LiveCheckpoint {
        /// Minimum position across live checkpoints that blocks collection.
        min_live: u64,
    },
    /// Unconsumed operation records still reference the requested prefix.
    PendingOperations {
        /// Highest position fully consumed by every live peer.
        consumed_through: u64,
    },
    /// The requested prefix reaches past the durable frontier.
    BeyondFrontier {
        /// Current monotonic scoped frontier.
        frontier: u64,
    },
}

/// Decides how far the prunable log prefix may advance.
///
/// Collection never passes the frontier, never removes history a live
/// checkpoint still addresses, and never drops operation records peers have
/// not consumed. A request at or below the floor is an idempotent no-op that
/// reports the current floor.
///
/// # Errors
///
/// Returns the [`PruneRefusal`] that blocks collection.
pub fn prune_decision(
    frontier: u64,
    floor: u64,
    requested_up_to: u64,
    min_live: Option<u64>,
    consumed_through: u64,
) -> Result<u64, PruneRefusal> {
    if requested_up_to <= floor {
        return Ok(floor);
    }
    if requested_up_to > frontier {
        return Err(PruneRefusal::BeyondFrontier { frontier });
    }
    if let Some(min_live) = min_live
        && requested_up_to > min_live
    {
        return Err(PruneRefusal::LiveCheckpoint { min_live });
    }
    if requested_up_to > consumed_through {
        return Err(PruneRefusal::PendingOperations { consumed_through });
    }
    Ok(requested_up_to)
}

/// Durable receipt for one collected log prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PruneReceipt {
    /// Highest log position collected by this decision, inclusive.
    pub pruned_through: u64,
    /// Durable retention floor after collection.
    pub floor: u64,
}

impl PruneReceipt {
    /// Builds one durable collection receipt from its inclusive prefix and floor.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose prune receipts through here.
    #[must_use]
    pub const fn new(pruned_through: u64, floor: u64) -> Self {
        Self {
            pruned_through,
            floor,
        }
    }
}

/// Durable outcome of executing one frontier-bound prune invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PruneResult {
    /// The prefix decision committed and the floor advanced (or held).
    Pruned(PruneReceipt),
    /// A live peer still needs part of the requested prefix.
    Refused(PruneRefusal),
    /// The durable frontier changed after the invocation was justified.
    StaleAuthority,
}

/// Durable outcome of executing one frontier-bound snapshot seal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SealResult {
    /// The versioned baseline committed durably.
    Sealed {
        /// Baseline position now available for full recovery.
        position: u64,
    },
    /// The baseline addresses positions past the durable frontier.
    BeyondFrontier {
        /// Current monotonic scoped frontier.
        frontier: u64,
    },
    /// The durable frontier changed after the invocation was justified.
    StaleAuthority,
}

/// Durable outcome of executing one frontier-bound consumption mark.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ConsumedResult {
    /// The consumption watermark advanced monotonically.
    Advanced {
        /// Highest position fully consumed by every live peer.
        consumed_through: u64,
    },
    /// The watermark already covered the request; nothing changed.
    Unchanged {
        /// Highest position fully consumed by every live peer.
        consumed_through: u64,
    },
    /// The request addresses positions past the durable frontier.
    BeyondFrontier {
        /// Current monotonic scoped frontier.
        frontier: u64,
    },
    /// The durable frontier changed after the invocation was justified.
    StaleAuthority,
}

/// Durable retention contract for peer-aware log collection.
///
/// One ordered slice of a sealed baseline after an exclusive document cursor.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BaselinePageSlice {
    /// Strictly ascending documents after the requested cursor; adapters build
    /// slices through [`BaselinePageSlice::new`].
    pub(crate) documents: Vec<SnapshotDocument>,
    /// Whether further documents follow this slice for the same descriptor.
    pub(crate) more: bool,
}

impl BaselinePageSlice {
    /// Builds one ordered sealed-baseline slice after an exclusive cursor.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose baseline slices through here.
    #[must_use]
    pub fn new(documents: Vec<SnapshotDocument>, more: bool) -> Self {
        Self { documents, more }
    }

    /// Strictly ascending documents after the requested cursor.
    #[must_use]
    pub fn documents(&self) -> &[SnapshotDocument] {
        &self.documents
    }

    /// Whether further documents follow this slice for the same descriptor.
    #[must_use]
    pub const fn more(&self) -> bool {
        self.more
    }
}

/// Durable outcome of one grant-bound recovery page read.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BaselinePageResult {
    /// The read observed the justified frontier and returned a page slice.
    Page(BaselinePageSlice),
    /// The scope frontier moved after the invocation was justified.
    StaleAuthority,
}

/// Reads report the versioned snapshot baseline, the retention floor, and the
/// peer-consumption watermark. Writes are frontier-bound like page reads: a
/// grant justified against a moved frontier reports stale authority instead of
/// collecting under a prefix a live peer still needs.
pub trait RetentionStore: ServiceStore {
    /// Reads the highest position fully consumed by every live peer.
    fn consumed_through(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    /// Reads the versioned snapshot baseline available for full recovery.
    ///
    /// Legacy position-only records remain observable for migration, but do
    /// not authorize collection.
    fn snapshot_baseline(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, Self::Error>> + Send;

    /// Reconstructs a bounded state-bearing artifact through one position.
    fn build_snapshot(
        &self,
        scope: Scope,
        through: u64,
    ) -> impl Future<Output = Result<SnapshotArtifact, Self::Error>> + Send;

    /// Reads one bounded ordered page of a sealed baseline for one cursor.
    ///
    /// The slice carries at most `MAX_BASELINE_PAGE_DOCUMENTS` documents,
    /// strictly ascending after `action.after_document`. `more` reports that the
    /// same descriptor has further documents. The store recomputes the
    /// normalized action digest from the actual parameters and refuses a grant
    /// issued for any other checkpoint or cursor; it validates each stored
    /// document's own integrity, and the aggregate digest is the recovering
    /// client's terminal check.
    fn read_baseline_page(
        &self,
        action: &RecoveryAction,
        descriptor: &SnapshotBaseline,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<BaselinePageResult, Self::Error>> + Send;

    /// Durably records a versioned snapshot baseline for the scope.
    ///
    /// # Errors
    ///
    /// Returns a typed backend transaction or corruption failure.
    fn seal_snapshot(
        &self,
        scope: Scope,
        artifact: &SnapshotArtifact,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<SealResult, Self::Error>> + Send;

    /// Monotonically advances the peer-consumption watermark.
    ///
    /// # Errors
    ///
    /// Returns a typed backend transaction or corruption failure.
    fn mark_consumed(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<ConsumedResult, Self::Error>> + Send;

    /// Collects the log prefix through the peer-aware prune decision.
    ///
    /// Operation and invocation records survive collection so exact retries
    /// stay idempotent; only log manifests and their chunks are dropped.
    ///
    /// # Errors
    ///
    /// Returns a typed backend transaction or corruption failure.
    fn prune(
        &self,
        scope: Scope,
        up_to: u64,
        min_live: Option<u64>,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PruneResult, Self::Error>> + Send;
}

impl<Store> RetentionStore for Arc<Store>
where
    Store: RetentionStore + ?Sized,
{
    fn consumed_through(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        (**self).consumed_through(scope)
    }

    fn snapshot_baseline(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, Self::Error>> + Send {
        (**self).snapshot_baseline(scope)
    }

    fn build_snapshot(
        &self,
        scope: Scope,
        through: u64,
    ) -> impl Future<Output = Result<SnapshotArtifact, Self::Error>> + Send {
        (**self).build_snapshot(scope, through)
    }

    fn read_baseline_page(
        &self,
        action: &RecoveryAction,
        descriptor: &SnapshotBaseline,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<BaselinePageResult, Self::Error>> + Send {
        (**self).read_baseline_page(action, descriptor, grant)
    }

    fn seal_snapshot(
        &self,
        scope: Scope,
        artifact: &SnapshotArtifact,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<SealResult, Self::Error>> + Send {
        (**self).seal_snapshot(scope, artifact, request, grant)
    }

    fn mark_consumed(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<ConsumedResult, Self::Error>> + Send {
        (**self).mark_consumed(scope, through, request, grant)
    }

    fn prune(
        &self,
        scope: Scope,
        up_to: u64,
        min_live: Option<u64>,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PruneResult, Self::Error>> + Send {
        (**self).prune(scope, up_to, min_live, request, grant)
    }
}
