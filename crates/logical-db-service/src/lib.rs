//! Multi-tenant protocol service with authenticated opaque checkpoints.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{collections::BTreeMap, future::Future, sync::Arc};

use logical_db_crdt::{CrdtError, Document, EncodedChange};
use logical_db_model::OperationId;
use logical_db_model::{
    ActionDigest, Capability, CapabilityProfile, Effect, OperationDigest, Scope, SnapshotArtifact,
    SnapshotBaseline, SnapshotDocument, SnapshotRecord, is_behind_retention,
};
use logical_db_protocol::{
    BaselineDocument, BaselinePage, BaselineRequest, ChangeEnvelope, Checkpoint, PROTOCOL_VERSION,
    PageAction, ProtocolError, PullPage, PullRequest, PushOutcome, PushRequest, PushResponse,
    RecoveryAction, RejectionCode, mark_consumed_action, prune_action, seal_snapshot_action,
    write_action,
};
use thiserror::Error;

/// Magic prefix identifying a versioned checkpoint token body.
const CHECKPOINT_MAGIC: &[u8; 4] = b"LCP\x01";
/// Byte length of the authenticated checkpoint body before the MAC tag.
const CHECKPOINT_BODY_BYTES: usize = 52;
/// Total checkpoint token length: body plus a 32-byte authentication tag.
const CHECKPOINT_BYTES: usize = CHECKPOINT_BODY_BYTES + 32;

/// Compact, typed failures from state-bearing baseline materialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
#[non_exhaustive]
pub enum SnapshotMaterializationError {
    /// The supplied base artifact or one of its tombstone markers is invalid.
    #[error("snapshot base artifact is invalid at {context}")]
    InvalidBase {
        /// Materialization operation that rejected the base artifact.
        context: &'static str,
    },
    /// The ordered change slice does not cover exactly the requested prefix.
    #[error("snapshot changes do not exactly cover the requested prefix")]
    Coverage,
    /// A change belongs to another logical scope.
    #[error("snapshot change belongs to another scope")]
    ScopeMismatch,
    /// A change failed protocol, native decode, digest, dependency, or apply validation.
    #[error("snapshot change is invalid")]
    InvalidChange,
    /// The materialized artifact exceeded one of its bounded model limits.
    #[error("snapshot materialization exceeds its bounded resource limits")]
    ResourceLimit,
}

/// Materializes one deterministic, bounded state artifact from a validated log prefix.
///
/// `base` is an optional state-bearing artifact whose position precedes `changes`.
/// The caller owns sequence verification; this function enforces the exact count,
/// scope, native CRDT validation, tombstone consistency, and final model bounds.
///
/// # Errors
///
/// Returns [`SnapshotMaterializationError`] when the base, coverage, scope, native
/// changes, or bounded artifact is invalid.
pub fn materialize_snapshot(
    scope: Scope,
    through: u64,
    base: Option<&SnapshotArtifact>,
    changes: &[ChangeEnvelope],
) -> Result<SnapshotArtifact, SnapshotMaterializationError> {
    let base_position = match base {
        Some(artifact) => {
            artifact.validate(scope).map_err(|_error| {
                SnapshotMaterializationError::InvalidBase {
                    context: "base validation",
                }
            })?;
            artifact.baseline.position()
        }
        None => 0,
    };
    let expected = through
        .checked_sub(base_position)
        .ok_or(SnapshotMaterializationError::Coverage)?;
    let change_count =
        u64::try_from(changes.len()).map_err(|_error| SnapshotMaterializationError::Coverage)?;
    if expected != change_count {
        return Err(SnapshotMaterializationError::Coverage);
    }

    let mut documents = BTreeMap::<logical_db_model::DocumentId, Document>::new();
    if let Some(artifact) = base {
        for state in &artifact.documents {
            let mut document = Document::load(&state.snapshot).map_err(|_error| {
                SnapshotMaterializationError::InvalidBase {
                    context: "document load",
                }
            })?;
            let tombstone = document.is_deleted().map_err(|_error| {
                SnapshotMaterializationError::InvalidBase {
                    context: "tombstone read",
                }
            })?;
            if tombstone != state.tombstone {
                return Err(SnapshotMaterializationError::InvalidBase {
                    context: "tombstone mismatch",
                });
            }
            documents.insert(state.document, document);
        }
    }

    for change in changes {
        if change.scope != scope {
            return Err(SnapshotMaterializationError::ScopeMismatch);
        }
        change
            .validate()
            .map_err(|_error| SnapshotMaterializationError::InvalidChange)?;
        let encoded = EncodedChange::new(change.digest, change.bytes.clone());
        encoded
            .validate()
            .map_err(|_error| SnapshotMaterializationError::InvalidChange)?;
        documents
            .entry(change.document)
            .or_default()
            .apply(&[encoded])
            .map_err(|_error| SnapshotMaterializationError::InvalidChange)?;
    }

    let states = documents
        .into_iter()
        .map(|(document, mut state)| {
            let tombstone = state
                .is_deleted()
                .map_err(|_error| SnapshotMaterializationError::InvalidChange)?;
            let snapshot = state
                .save()
                .map_err(|_error| SnapshotMaterializationError::ResourceLimit)?;
            Ok(logical_db_model::SnapshotDocument::new(
                document, snapshot, tombstone,
            ))
        })
        .collect::<Result<Vec<_>, SnapshotMaterializationError>>()?;
    SnapshotArtifact::new(scope, through, states)
        .map_err(|_error| SnapshotMaterializationError::ResourceLimit)
}

/// Verifier-produced execution proof consumed by a storage transaction.
pub trait ExecutionGrant: Send + Sync {
    /// Scope proven by the invocation.
    fn scope(&self) -> Scope;
    /// Strongest database effect proven by the invocation.
    fn effect(&self) -> Effect;
    /// Canonical action digest bound into the invocation snapshot.
    fn action(&self) -> ActionDigest;
    /// Exact durable scope frontier justified before transaction execution.
    fn frontier(&self) -> u64;
    /// Stable content identity atomically consumed with a new write.
    fn invocation_id(&self) -> [u8; 32];
}

/// Host authority boundary. Production composition supplies an [`ExecutionGrant`]
/// through this trait; no verifier runtime type crosses it.
pub trait Authority {
    /// Concrete opaque proof forwarded into the durable transaction.
    type Grant: ExecutionGrant;

    /// Produces a proof for the exact request and observed durable frontier.
    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        now_unix_ms: u64,
    ) -> Option<Self::Grant>;
}

/// Host-local execution proof for one authenticated read or scan.
///
/// The invocation identity is the keyed digest of scope, effect, action,
/// frontier, and expiry. Any change to those facts changes the identity and
/// therefore the idempotency key of a write that consumes it.
#[derive(Clone, Debug)]
pub struct LocalGrant {
    /// Scope the grant authorizes.
    scope: Scope,
    /// Strongest effect the grant authorizes.
    effect: Effect,
    /// Canonical action digest bound into the grant.
    action: ActionDigest,
    /// Durable scope frontier observed when the grant was issued.
    frontier: u64,
    /// Stable content identity of the bound facts.
    invocation_id: [u8; 32],
}

impl LocalGrant {
    /// Binds one grant to the exact request facts and observed frontier.
    #[must_use]
    pub fn new(
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        expires_at_unix_ms: u64,
    ) -> Self {
        let mut digest = lgwks_std::hash::Hasher::new();
        digest.update(b"logical-db/local-grant/v1\0");
        digest.update(scope.account().as_uuid().as_bytes());
        digest.update(scope.space().as_uuid().as_bytes());
        digest.update(&[match effect {
            Effect::Read => 1,
            Effect::Scan => 2,
            Effect::Write => 3,
            Effect::Sync => 4,
            // `Effect` is non-exhaustive at the model boundary; an unknown
            // variant must still get a distinct invocation identity.
            _ => 0,
        }]);
        digest.update(&action.into_bytes());
        digest.update(&frontier.to_be_bytes());
        digest.update(&expires_at_unix_ms.to_be_bytes());
        Self {
            scope,
            effect,
            action,
            frontier,
            invocation_id: *digest.finalize().as_bytes(),
        }
    }
}

impl ExecutionGrant for LocalGrant {
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
        self.invocation_id
    }
}

/// Host-local authority for non-egress operations inside one authenticated scope.
///
/// Refuses [`Effect::Write`] and [`Effect::Sync`] and every request outside the
/// bound scope or past the exclusive expiry. Hosts that must write or
/// synchronize supply an externally prepared [`Authority`] instead; this type
/// is the one local implementation and never admits those effects.
#[derive(Clone, Copy, Debug)]
pub struct LocalScopeAuthority {
    /// Exact account and application space this authority may authorize.
    scope: Scope,
    /// Exclusive expiry in Unix milliseconds.
    expires_at_unix_ms: u64,
}

impl LocalScopeAuthority {
    /// Limits locally generated grants to one authenticated scope and expiry.
    #[must_use]
    pub const fn new(scope: Scope, expires_at_unix_ms: u64) -> Self {
        Self {
            scope,
            expires_at_unix_ms,
        }
    }
}

impl Authority for LocalScopeAuthority {
    type Grant = LocalGrant;

    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        now_unix_ms: u64,
    ) -> Option<Self::Grant> {
        if scope != self.scope
            || !matches!(effect, Effect::Read | Effect::Scan)
            || now_unix_ms >= self.expires_at_unix_ms
        {
            return None;
        }
        Some(LocalGrant::new(
            scope,
            effect,
            action,
            frontier,
            self.expires_at_unix_ms,
        ))
    }
}

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
    /// Changes strictly after the requested position.
    pub changes: Vec<ChangeEnvelope>,
    /// Durable position covering every returned change.
    pub position: u64,
    /// Whether another page is immediately available.
    pub more: bool,
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
    /// Strictly ascending documents after the requested cursor.
    pub documents: Vec<SnapshotDocument>,
    /// Whether further documents follow this slice for the same descriptor.
    pub more: bool,
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

/// Constant-time authenticated codec for opaque, expiring scoped positions.
#[derive(Clone)]
pub struct CheckpointCodec {
    /// Host-managed 256-bit authentication key.
    key: [u8; 32],
    /// Token lifetime in milliseconds.
    ttl_ms: u64,
}

impl CheckpointCodec {
    /// Constructs a codec from host-managed key material and a nonzero TTL.
    ///
    /// # Errors
    ///
    /// Refuses an all-zero key or zero TTL.
    pub fn new(key: [u8; 32], ttl_ms: u64) -> Result<Self, CheckpointError> {
        if key == [0; 32] {
            return Err(CheckpointError::WeakKey);
        }
        if ttl_ms == 0 {
            return Err(CheckpointError::InvalidTtl);
        }
        Ok(Self { key, ttl_ms })
    }

    /// Mints an authenticated checkpoint for one scoped position.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::TimeOverflow`] when expiry arithmetic overflows,
    /// or [`CheckpointError::Protocol`] when the encoded token is rejected.
    fn issue(
        &self,
        scope: Scope,
        position: u64,
        now_unix_ms: u64,
    ) -> Result<Checkpoint, CheckpointError> {
        let expires = now_unix_ms
            .checked_add(self.ttl_ms)
            .ok_or(CheckpointError::TimeOverflow)?;
        let mut body = [0; CHECKPOINT_BODY_BYTES];
        body[..4].copy_from_slice(CHECKPOINT_MAGIC);
        body[4..20].copy_from_slice(scope.account().as_uuid().as_bytes());
        body[20..36].copy_from_slice(scope.space().as_uuid().as_bytes());
        body[36..44].copy_from_slice(&position.to_be_bytes());
        body[44..52].copy_from_slice(&expires.to_be_bytes());
        let tag = self.tag(&body);
        let mut token = Vec::with_capacity(CHECKPOINT_BYTES);
        token.extend_from_slice(&body);
        token.extend_from_slice(&tag);
        Checkpoint::from_wire(token).map_err(CheckpointError::Protocol)
    }

    /// Verifies an opaque checkpoint and returns its exclusive position.
    ///
    /// # Errors
    ///
    /// Returns the typed authentication, scope, expiry, or encoding failure.
    fn open(
        &self,
        scope: Scope,
        token: &Checkpoint,
        now_unix_ms: u64,
    ) -> Result<u64, CheckpointError> {
        let bytes = token.as_wire();
        if bytes.len() != CHECKPOINT_BYTES || &bytes[..4] != CHECKPOINT_MAGIC {
            return Err(CheckpointError::Malformed);
        }
        let expected = lgwks_std::hash::keyed(&self.key, &bytes[..CHECKPOINT_BODY_BYTES]);
        let Ok(supplied) = <[u8; 32]>::try_from(&bytes[CHECKPOINT_BODY_BYTES..]) else {
            return Err(CheckpointError::Malformed);
        };
        if !tags_equal(expected.as_bytes(), &supplied) {
            return Err(CheckpointError::InvalidMac);
        }
        if bytes[4..20] != *scope.account().as_uuid().as_bytes()
            || bytes[20..36] != *scope.space().as_uuid().as_bytes()
        {
            return Err(CheckpointError::WrongScope);
        }
        let position = decode_u64(&bytes[36..44])?;
        let expires = decode_u64(&bytes[44..52])?;
        if now_unix_ms >= expires {
            return Err(CheckpointError::Expired);
        }
        Ok(position)
    }

    /// Computes the keyed authentication tag for one checkpoint body.
    fn tag(&self, body: &[u8]) -> [u8; 32] {
        *lgwks_std::hash::keyed(&self.key, body).as_bytes()
    }
}

/// Compares two authentication tags in constant time.
///
/// The folded-xor walk exposes no timing dependence on where the bytes first
/// differ, matching the constant-time equality the estate digest type
/// provides internally.
#[must_use]
pub fn tags_equal(computed: &[u8; 32], expected: &[u8; 32]) -> bool {
    let mut difference = 0_u8;
    for (left, right) in computed.iter().zip(expected.iter()) {
        difference |= left ^ right;
    }
    difference == 0
}

/// Opaque checkpoint validation failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum CheckpointError {
    /// Key material is an obvious unconfigured sentinel.
    #[error("checkpoint key must not be all zero")]
    WeakKey,
    /// Checkpoint TTL must be nonzero.
    #[error("checkpoint TTL must be nonzero")]
    InvalidTtl,
    /// Expiry arithmetic overflowed.
    #[error("checkpoint expiry overflowed")]
    TimeOverflow,
    /// Token length, magic, or integer encoding is malformed.
    #[error("checkpoint is malformed")]
    Malformed,
    /// Authentication tag did not verify.
    #[error("checkpoint authentication failed")]
    InvalidMac,
    /// Token belongs to another account or application space.
    #[error("checkpoint belongs to another scope")]
    WrongScope,
    /// Token's exclusive expiration has passed.
    #[error("checkpoint expired")]
    Expired,
    /// Protocol checkpoint constructor rejected the encoded token.
    #[error("checkpoint protocol encoding failed: {0}")]
    Protocol(ProtocolError),
}

/// Pull response distinguishes expiry from invalid or unauthorized input.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServicePull {
    /// Valid bounded page.
    Page(PullPage),
    /// Valid authenticated checkpoint reached its expiry.
    CheckpointExpired,
    /// The client cannot resume from the log prefix alone.
    ///
    /// The retained prefix below `floor` was collected into a state-bearing
    /// baseline, so a fresh or expired replica must install that artifact
    /// before any suffix pull. A position is not a recoverable snapshot.
    BaselineRequired {
        /// Durable retention floor the client must recover through.
        floor: u64,
    },
}

/// Response to one recovery request: the sealed artifact or its absence.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ServiceBaseline {
    /// A sealed state-bearing artifact covers the retained prefix.
    Artifact(Box<BaselinePage>),
    /// No state-bearing baseline exists, so the durable log still covers every
    /// position and a full pull from the floor is complete.
    Unavailable,
}

/// Stateless service orchestration over a durable storage adapter.
pub struct ProtocolService<Store, Policy> {
    /// Durable storage adapter.
    store: Store,
    /// Host authority boundary used to mint execution grants.
    policy: Policy,
    /// Authenticated opaque checkpoint codec.
    checkpoints: CheckpointCodec,
    /// Backend capability profile published by this service.
    profile: CapabilityProfile,
}

impl<Store, Policy> ProtocolService<Store, Policy>
where
    Store: ServiceStore,
    Policy: Authority,
{
    /// Constructs a service from explicit durable, authority, and token ports.
    ///
    /// The constructed service publishes the
    /// [`CapabilityProfile::distributed_transactional`] class: it holds
    /// distributed transaction authority over a FoundationDB-backed store,
    /// which is exactly the class that carries serializable transactions,
    /// replicated availability, watches/versionstamps, and cluster recovery.
    #[must_use]
    pub const fn new(store: Store, policy: Policy, checkpoints: CheckpointCodec) -> Self {
        Self {
            store,
            policy,
            checkpoints,
            profile: CapabilityProfile::distributed_transactional(),
        }
    }

    /// Returns the backend capability profile this service is bound to.
    #[must_use]
    pub const fn capability_profile(&self) -> CapabilityProfile {
        self.profile
    }

    /// Requires every listed backend guarantee, failing closed on the unmet set.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceError::Model`] wrapping
    /// [`logical_db_model::ModelError::UnsupportedGuarantees`] listing every
    /// required guarantee the bound profile does not provide.
    pub fn require_guarantees(
        &self,
        required: &[Capability],
    ) -> Result<(), ServiceError<Store::Error>> {
        self.profile.require(required).map_err(ServiceError::Model)
    }

    /// Validates and idempotently accepts a bounded upload page.
    ///
    /// # Errors
    ///
    /// Returns a page-level protocol or backend failure. Per-change validation,
    /// authority, and conflict failures are stable outcomes in request order.
    pub async fn push(
        &self,
        request: &PushRequest,
        now_unix_ms: u64,
    ) -> Result<PushResponse, ServiceError<Store::Error>> {
        request.validate().map_err(ServiceError::Protocol)?;
        let mut outcomes = Vec::with_capacity(request.changes.len());
        for change in &request.changes {
            let operation = change.operation;
            let validation = EncodedChange::new(change.digest, change.bytes.clone()).validate();
            if let Err(error) = validation {
                outcomes.push(PushOutcome::Rejected {
                    operation,
                    code: if matches!(error, CrdtError::DigestMismatch) {
                        RejectionCode::DigestMismatch
                    } else {
                        RejectionCode::InvalidChange
                    },
                });
                continue;
            }
            let frontier = self
                .store
                .frontier(change.scope)
                .await
                .map_err(ServiceError::Store)?;
            let Some(grant) = self.policy.authorize(
                change.scope,
                Effect::Write,
                write_action(change),
                frontier,
                now_unix_ms,
            ) else {
                outcomes.push(PushOutcome::Rejected {
                    operation,
                    code: RejectionCode::Forbidden,
                });
                continue;
            };
            let accepted = self
                .store
                .accept(change, &grant)
                .await
                .map_err(ServiceError::Store)?;
            let outcome = push_outcome(operation, accepted);
            outcomes.push(outcome);
        }
        Ok(PushResponse::new(PROTOCOL_VERSION, outcomes))
    }

    /// Reads one authorized page after an authenticated opaque checkpoint.
    ///
    /// A checkpoint behind the durable retention floor, an expired checkpoint
    /// once collection has advanced the floor, and a fresh replica facing a
    /// nonzero floor all take the explicit [`ServicePull::BaselineRequired`]
    /// transition: the collected prefix exists only as a state-bearing
    /// baseline, so no partial log prefix may be applied.
    ///
    /// # Errors
    ///
    /// Returns protocol, authority, checkpoint-authentication, backend, or
    /// cross-scope corruption failures. Expiry and required recovery are typed
    /// non-error responses.
    pub async fn pull(
        &self,
        request: &PullRequest,
        now_unix_ms: u64,
    ) -> Result<ServicePull, ServiceError<Store::Error>> {
        request.validate().map_err(ServiceError::Protocol)?;
        let floor = self
            .store
            .retention_floor(request.scope)
            .await
            .map_err(ServiceError::Store)?;
        let checkpoint_position = match request.after.as_ref() {
            Some(token) => match self.checkpoints.open(request.scope, token, now_unix_ms) {
                Ok(position) if is_behind_retention(position, floor) => {
                    return Ok(ServicePull::BaselineRequired { floor });
                }
                Ok(position) => Some(position),
                Err(CheckpointError::Expired) if floor > 0 => {
                    return Ok(ServicePull::BaselineRequired { floor });
                }
                Err(CheckpointError::Expired) => return Ok(ServicePull::CheckpointExpired),
                Err(error) => return Err(ServiceError::Checkpoint(error)),
            },
            None if floor > 0 => {
                return Ok(ServicePull::BaselineRequired { floor });
            }
            None => None,
        };
        let frontier = self
            .store
            .frontier(request.scope)
            .await
            .map_err(ServiceError::Store)?;
        let after = checkpoint_position.unwrap_or(0).max(floor);
        self.page_after(request, after, frontier, now_unix_ms).await
    }

    /// Serves one authorized page and issues the replacement checkpoint.
    async fn page_after(
        &self,
        request: &PullRequest,
        after: u64,
        frontier: u64,
        now_unix_ms: u64,
    ) -> Result<ServicePull, ServiceError<Store::Error>> {
        let action = PageAction::new(
            request.scope,
            request.replica,
            request.request,
            after,
            request.max_changes,
            request.max_bytes,
        );
        let Some(grant) = self.policy.authorize(
            request.scope,
            Effect::Sync,
            action.digest(),
            frontier,
            now_unix_ms,
        ) else {
            return Err(ServiceError::Forbidden);
        };
        let result = self
            .store
            .page(action, &grant)
            .await
            .map_err(ServiceError::Store)?;
        let stored = match result {
            PageResult::Page(stored) => stored,
            PageResult::BehindFloor => return Ok(ServicePull::CheckpointExpired),
            PageResult::StaleAuthority => return Err(ServiceError::StaleAuthority),
        };
        if !valid_stored_page(after, frontier, &stored) {
            return Err(ServiceError::CorruptStoreData);
        }
        if stored
            .changes
            .iter()
            .any(|change| change.scope != request.scope)
        {
            return Err(ServiceError::CrossScopeStoreData);
        }
        for change in &stored.changes {
            EncodedChange::new(change.digest, change.bytes.clone())
                .validate()
                .map_err(|_error| ServiceError::CorruptStoreData)?;
        }
        let checkpoint = self
            .checkpoints
            .issue(request.scope, stored.position, now_unix_ms)
            .map_err(ServiceError::Checkpoint)?;
        let page = PullPage::new(PROTOCOL_VERSION, stored.changes, checkpoint, stored.more);
        page.validate(request.scope)
            .map_err(ServiceError::Protocol)?;
        Ok(ServicePull::Page(page))
    }
}

impl<Store, Policy> ProtocolService<Store, Policy>
where
    Store: RetentionStore,
    Policy: Authority,
{
    /// Durably records a versioned snapshot baseline for full recovery.
    ///
    /// # Errors
    ///
    /// Returns authority or backend failures. A baseline past the frontier is
    /// a [`SealResult::BeyondFrontier`] refusal, never a silent commit.
    pub async fn seal_snapshot(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        now_unix_ms: u64,
    ) -> Result<SealResult, ServiceError<Store::Error>> {
        let frontier = self
            .store
            .frontier(scope)
            .await
            .map_err(ServiceError::Store)?;
        if through > frontier {
            return Ok(SealResult::BeyondFrontier { frontier });
        }
        let artifact = self
            .store
            .build_snapshot(scope, through)
            .await
            .map_err(ServiceError::Store)?;
        let Some(grant) = self.policy.authorize(
            scope,
            Effect::Sync,
            seal_snapshot_action(scope, request, &artifact.baseline),
            frontier,
            now_unix_ms,
        ) else {
            return Err(ServiceError::Forbidden);
        };
        self.store
            .seal_snapshot(scope, &artifact, request, &grant)
            .await
            .map_err(ServiceError::Store)
    }

    /// Returns one bounded page of the sealed state-bearing baseline a
    /// recovering client must install.
    ///
    /// The durable descriptor is read once per request and the ordered document
    /// slice is read after `request.after_document`, so a baseline larger than
    /// one page is delivered without materializing the whole artifact. The
    /// client verifies the aggregate descriptor digest across the pages.
    ///
    /// # Errors
    ///
    /// Returns protocol, authority, backend, or cross-scope corruption
    /// failures. A scope with no sealed state-bearing baseline reports
    /// [`ServiceBaseline::Unavailable`] instead of an empty artifact.
    pub async fn recover(
        &self,
        request: &BaselineRequest,
        now_unix_ms: u64,
    ) -> Result<ServiceBaseline, ServiceError<Store::Error>> {
        request.validate().map_err(ServiceError::Protocol)?;
        let frontier = self
            .store
            .frontier(request.scope)
            .await
            .map_err(ServiceError::Store)?;
        let action = RecoveryAction::new(
            request.scope,
            request.replica,
            request.request,
            request.after.clone(),
            request.after_document,
        );
        let digest = action.digest();
        let Some(grant) =
            self.policy
                .authorize(request.scope, Effect::Sync, digest, frontier, now_unix_ms)
        else {
            return Err(ServiceError::Forbidden);
        };
        if grant.scope() != request.scope
            || grant.effect() != Effect::Sync
            || grant.action() != digest
            || grant.frontier() != frontier
        {
            return Err(ServiceError::Forbidden);
        }
        let sealed = self
            .store
            .snapshot_baseline(request.scope)
            .await
            .map_err(ServiceError::Store)?;
        let Some(SnapshotRecord::State(descriptor)) = sealed else {
            return Ok(ServiceBaseline::Unavailable);
        };
        let slice = match self
            .store
            .read_baseline_page(&action, &descriptor, &grant)
            .await
            .map_err(ServiceError::Store)?
        {
            BaselinePageResult::Page(slice) => slice,
            BaselinePageResult::StaleAuthority => return Err(ServiceError::StaleAuthority),
        };
        if slice.more && slice.documents.is_empty() {
            return Err(ServiceError::CorruptStoreData);
        }
        let checkpoint = self
            .checkpoints
            .issue(request.scope, descriptor.position(), now_unix_ms)
            .map_err(ServiceError::Checkpoint)?;
        let next_document = if slice.more {
            slice.documents.last().map(|document| document.document)
        } else {
            None
        };
        let page = BaselinePage::new(
            PROTOCOL_VERSION,
            descriptor.encode().to_vec(),
            slice
                .documents
                .iter()
                .map(|document| {
                    BaselineDocument::new(
                        document.document,
                        document.tombstone,
                        document.snapshot.clone(),
                    )
                })
                .collect(),
            checkpoint,
            slice.more,
            next_document,
        );
        page.validate_page(request.scope)
            .map_err(ServiceError::Protocol)?;
        Ok(ServiceBaseline::Artifact(Box::new(page)))
    }

    /// Advances the peer-consumption watermark guarding collection.
    ///
    /// # Errors
    ///
    /// Returns authority or backend failures. A watermark past the frontier is
    /// a [`ConsumedResult::BeyondFrontier`] refusal, never a silent advance.
    pub async fn mark_consumed(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        now_unix_ms: u64,
    ) -> Result<ConsumedResult, ServiceError<Store::Error>> {
        let frontier = self
            .store
            .frontier(scope)
            .await
            .map_err(ServiceError::Store)?;
        let Some(grant) = self.policy.authorize(
            scope,
            Effect::Sync,
            mark_consumed_action(scope, request, through),
            frontier,
            now_unix_ms,
        ) else {
            return Err(ServiceError::Forbidden);
        };
        self.store
            .mark_consumed(scope, through, request, &grant)
            .await
            .map_err(ServiceError::Store)
    }

    /// Collects the log prefix the peer-aware prune decision allows.
    ///
    /// `min_live` is the minimum position across live unexpired checkpoints;
    /// `None` asserts no live peer holds history the floor does not cover.
    ///
    /// # Errors
    ///
    /// Returns authority or backend failures. A prefix a live peer still needs
    /// is a [`PruneResult::Refused`] refusal, never a partial collection.
    pub async fn prune(
        &self,
        scope: Scope,
        up_to: u64,
        min_live: Option<u64>,
        request: OperationDigest,
        now_unix_ms: u64,
    ) -> Result<PruneResult, ServiceError<Store::Error>> {
        let frontier = self
            .store
            .frontier(scope)
            .await
            .map_err(ServiceError::Store)?;
        let Some(grant) = self.policy.authorize(
            scope,
            Effect::Sync,
            prune_action(scope, request, up_to, min_live),
            frontier,
            now_unix_ms,
        ) else {
            return Err(ServiceError::Forbidden);
        };
        self.store
            .prune(scope, up_to, min_live, request, &grant)
            .await
            .map_err(ServiceError::Store)
    }
}

/// Maps one durable accept outcome onto its stable wire representation.
fn push_outcome(operation: OperationId, accepted: AcceptResult) -> PushOutcome {
    match accepted {
        AcceptResult::Accepted => PushOutcome::Accepted { operation },
        AcceptResult::Duplicate | AcceptResult::RecoveredUnknownCommit => {
            PushOutcome::Duplicate { operation }
        }
        AcceptResult::Conflict => PushOutcome::Rejected {
            operation,
            code: RejectionCode::OperationConflict,
        },
        AcceptResult::StaleAuthority => PushOutcome::Rejected {
            operation,
            code: RejectionCode::StaleAuthority,
        },
        AcceptResult::InvocationConsumed => PushOutcome::Rejected {
            operation,
            code: RejectionCode::InvocationConsumed,
        },
    }
}

/// Checks page shape against the justified prefix and durable frontier.
fn valid_stored_page(after: u64, frontier: u64, page: &StoredPage) -> bool {
    let Ok(count) = u64::try_from(page.changes.len()) else {
        return false;
    };
    after
        .checked_add(count)
        .is_some_and(|position| position == page.position)
        && page.position <= frontier
        && page.more == (page.position < frontier)
        && (!page.more || !page.changes.is_empty())
}

/// Service boundary failures.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ServiceError<StoreError> {
    /// Protocol validation failed.
    #[error("service protocol rejected input: {0}")]
    Protocol(ProtocolError),
    /// Pull authority was denied.
    #[error("service authority denied the pull request")]
    Forbidden,
    /// Durable state changed after pull authority was justified.
    #[error("service authority became stale before the pull transaction")]
    StaleAuthority,
    /// Opaque checkpoint was malformed, forged, or cross-scoped.
    #[error("service checkpoint rejected input: {0}")]
    Checkpoint(CheckpointError),
    /// Durable adapter failed.
    #[error("service storage failed")]
    Store(StoreError),
    /// Durable adapter violated account/space isolation.
    #[error("service storage returned cross-scope material")]
    CrossScopeStoreData,
    /// Durable bytes failed CRDT-native decoding or digest verification.
    #[error("service storage returned corrupt change material")]
    CorruptStoreData,
    /// A required backend guarantee is not provided by this service profile.
    #[error("service capability refused: {0}")]
    Model(logical_db_model::ModelError),
}

/// Decodes one big-endian `u64` from an eight-byte checkpoint slice.
fn decode_u64(bytes: &[u8]) -> Result<u64, CheckpointError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(|_error| CheckpointError::Malformed)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        error::Error,
        sync::{Mutex, PoisonError},
    };

    use logical_db_crdt::{Document, Object, Scalar};
    use logical_db_model::{AccountId, ChangeDigest, DocumentId, OperationId, ReplicaId, SpaceId};

    use super::*;
    use logical_db_protocol::recover_action;

    /// Authority fixture that always issues a fresh grant.
    struct Allow;
    /// Authority fixture that always issues a grant against a moved frontier.
    struct Stale;
    /// Concrete execution grant captured by the fixture authorities.
    struct Grant {
        /// Scope proven by the invocation.
        scope: Scope,
        /// Strongest database effect proven by the invocation.
        effect: Effect,
        /// Canonical action digest bound into the invocation snapshot.
        action: ActionDigest,
        /// Exact durable scope frontier justified before execution.
        frontier: u64,
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

    impl Authority for Allow {
        type Grant = Grant;

        fn authorize(
            &self,
            scope: Scope,
            effect: Effect,
            action: ActionDigest,
            frontier: u64,
            _now: u64,
        ) -> Option<Self::Grant> {
            Some(Grant {
                scope,
                effect,
                action,
                frontier,
            })
        }
    }

    impl Authority for Stale {
        type Grant = Grant;

        fn authorize(
            &self,
            scope: Scope,
            effect: Effect,
            action: ActionDigest,
            frontier: u64,
            _now: u64,
        ) -> Option<Self::Grant> {
            Some(Grant {
                scope,
                effect,
                action,
                frontier: frontier.checked_sub(1).unwrap_or(1),
            })
        }
    }

    /// Certifies exactly one captured invocation, like a host authority that
    /// binds a grant to the action digest it justified.
    struct ExactRecovery {
        /// Scope the captured invocation must match.
        scope: Scope,
        /// Action digest the captured invocation must match.
        action: ActionDigest,
        /// Frontier the captured invocation must match.
        frontier: u64,
    }

    impl Authority for ExactRecovery {
        type Grant = Grant;

        fn authorize(
            &self,
            scope: Scope,
            effect: Effect,
            action: ActionDigest,
            frontier: u64,
            _now: u64,
        ) -> Option<Self::Grant> {
            if scope != self.scope
                || effect != Effect::Sync
                || action != self.action
                || frontier != self.frontier
            {
                return None;
            }
            Some(Grant {
                scope,
                effect,
                action,
                frontier,
            })
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

    impl Memory {
        /// Reads the scoped frontier as floor plus retained change count.
        fn scope_frontier(&self, scope: Scope) -> u64 {
            let changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
            let state = self
                .retention
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&scope)
                .cloned()
                .unwrap_or_default();
            state
                .floor
                .saturating_add(Self::retained_count(&changes, scope))
        }

        /// Counts retained changes belonging to one scope.
        fn retained_count(changes: &[ChangeEnvelope], scope: Scope) -> u64 {
            let count = changes
                .iter()
                .filter(|change| change.scope == scope)
                .count();
            u64::try_from(count).unwrap_or(u64::MAX)
        }

        /// Reads the per-scope retention fixture state, defaulting when absent.
        fn retention_state(&self, scope: Scope) -> RetentionState {
            self.retention
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&scope)
                .cloned()
                .unwrap_or_default()
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
            let mut operations = self
                .operations
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(existing) = operations.get(&change.operation) {
                return if *existing == (change.request, change.digest) {
                    AcceptResult::Duplicate
                } else {
                    AcceptResult::Conflict
                };
            }
            operations.insert(change.operation, (change.request, change.digest));
            self.changes
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(change.clone());
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
            let changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
            let frontier = floor.saturating_add(Self::retained_count(&changes, scope));
            if grant.frontier() != frontier {
                return PageResult::StaleAuthority;
            }
            let skip = usize::try_from(after.saturating_sub(floor)).unwrap_or(usize::MAX);
            let limit = usize::try_from(max_bytes).unwrap_or(usize::MAX);
            let mut bytes = 0_usize;
            let page = changes
                .iter()
                .filter(|change| change.scope == scope)
                .skip(skip)
                .take(usize::from(max_changes))
                .take_while(|change| {
                    bytes = bytes.saturating_add(change.bytes.len());
                    bytes <= limit
                })
                .cloned()
                .collect::<Vec<_>>();
            let page_len = u64::try_from(page.len()).unwrap_or(u64::MAX);
            let position = after.saturating_add(page_len);
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
                .map(|artifact| SnapshotRecord::State(artifact.baseline))
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
                .filter(|artifact| artifact.baseline.position() <= through);
            let base_position = base
                .as_ref()
                .map_or(state.floor, |artifact| artifact.baseline.position());
            let skip =
                usize::try_from(base_position.saturating_sub(state.floor)).unwrap_or(usize::MAX);
            let retained =
                usize::try_from(through.saturating_sub(base_position)).unwrap_or(usize::MAX);
            let changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
            let changes = changes
                .iter()
                .filter(|change| change.scope == scope)
                .skip(skip)
                .take(retained)
                .cloned()
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
                &artifact.baseline == descriptor,
                "memory store baseline descriptor disagrees with its artifact"
            );
            let mut documents = artifact
                .documents
                .iter()
                .filter(|document| {
                    action
                        .after_document
                        .is_none_or(|cursor| document.document > cursor)
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
            let baseline = &artifact.baseline;
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
            self.retention
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .entry(scope)
                .or_default()
                .baseline = Some(artifact.clone());
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
            let Some(frontier) = self.check_retention_grant(
                scope,
                mark_consumed_action(scope, request, through),
                grant,
            ) else {
                return ConsumedResult::StaleAuthority;
            };
            if through > frontier {
                return ConsumedResult::BeyondFrontier { frontier };
            }
            let mut retention = self
                .retention
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
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
            let Some(frontier) = self.check_retention_grant(
                scope,
                prune_action(scope, request, up_to, min_live),
                grant,
            ) else {
                return Ok(PruneResult::StaleAuthority);
            };
            let state = self.retention_state(scope);
            let through =
                match prune_decision(frontier, state.floor, up_to, min_live, state.consumed) {
                    Ok(through) => through,
                    Err(refusal) => return Ok(PruneResult::Refused(refusal)),
                };
            if through > state.floor {
                let Some(artifact) = state.baseline else {
                    return Ok(PruneResult::Refused(PruneRefusal::MissingBaseline));
                };
                if artifact.baseline.position() < through {
                    return Ok(PruneResult::Refused(PruneRefusal::BaselineBehind {
                        position: artifact.baseline.position(),
                    }));
                }
                artifact.validate(scope)?;
                let floor = state.floor;
                let mut position = floor;
                self.changes
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .retain(|change| {
                        if change.scope != scope {
                            return true;
                        }
                        position = position.saturating_add(1);
                        position > through
                    });
                self.retention
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .entry(scope)
                    .or_default()
                    .floor = through;
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
            encoded.digest,
            encoded.bytes,
        ))
    }

    #[test]
    fn recovered_and_refused_admissions_keep_their_wire_meaning() {
        let operation = OperationId::new();
        let recovered = AcceptResult::RecoveredUnknownCommit;
        let stale = AcceptResult::StaleAuthority;
        let consumed = AcceptResult::InvocationConsumed;
        assert_eq!(
            push_outcome(operation, recovered),
            PushOutcome::Duplicate { operation },
            "recovered unknown commits must map to a duplicate wire outcome"
        );
        assert_eq!(
            push_outcome(operation, stale),
            PushOutcome::Rejected {
                operation,
                code: RejectionCode::StaleAuthority
            },
            "stale authority must map to the stale-authority rejection code"
        );
        assert_eq!(
            push_outcome(operation, consumed),
            PushOutcome::Rejected {
                operation,
                code: RejectionCode::InvocationConsumed
            },
            "consumed invocation must map to the invocation-consumed rejection code"
        );
    }

    #[test]
    fn service_publishes_distributed_profile_and_refuses_embedded_only_guarantees()
    -> Result<(), Box<dyn Error>> {
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([7; 32], 100)?,
        );
        assert_eq!(
            service.capability_profile(),
            CapabilityProfile::distributed_transactional(),
            "service must publish the distributed transactional profile"
        );
        assert!(
            service
                .require_guarantees(&[
                    Capability::DistributedSerializableTransactions,
                    Capability::ClusterRecovery,
                ])
                .is_ok(),
            "distributed guarantees must be satisfied by the distributed profile"
        );
        let Err(refusal) = service.require_guarantees(&[
            Capability::DistributedSerializableTransactions,
            Capability::ExportRestore,
        ]) else {
            return Err("distributed class must not claim embedded-only guarantees".into());
        };
        match refusal {
            ServiceError::Model(logical_db_model::ModelError::UnsupportedGuarantees {
                class,
                missing,
            }) => {
                assert_eq!(
                    class,
                    logical_db_model::BackendClass::DistributedTransactional,
                    "refusal must name the distributed transactional class"
                );
                assert_eq!(
                    missing,
                    vec![Capability::ExportRestore],
                    "refusal must list every missing guarantee"
                );
            }
            other => return Err(format!("unexpected refusal: {other:?}").into()),
        }
        Ok(())
    }

    #[test]
    fn exact_retry_is_duplicate_and_forged_checkpoint_fails_closed() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let operation = OperationId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([7; 32], 100)?,
        );
        let request = PushRequest::new(
            PROTOCOL_VERSION,
            replica,
            vec![change(scope, replica, operation)?],
        );
        assert!(
            matches!(
                lgwks_std::task::block_on(service.push(&request, 10))?.outcomes[0],
                PushOutcome::Accepted { .. }
            ),
            "first admission must be accepted"
        );
        assert!(
            matches!(
                lgwks_std::task::block_on(service.push(&request, 10))?.outcomes[0],
                PushOutcome::Duplicate { .. }
            ),
            "exact retry must be a duplicate"
        );
        let pull = PullRequest::new(
            PROTOCOL_VERSION,
            scope,
            replica,
            OperationDigest::new([9; 32]),
            None,
            10,
            1024,
        );
        let ServicePull::Page(page) = lgwks_std::task::block_on(service.pull(&pull, 10))? else {
            return Err("page expected".into());
        };
        let mut forged = page.checkpoint.as_wire().to_vec();
        forged[40] ^= 1;
        let forged = PullRequest::new(
            pull.protocol,
            pull.scope,
            pull.replica,
            pull.request,
            Some(Checkpoint::from_wire(forged)?),
            pull.max_changes,
            pull.max_bytes,
        );
        assert!(
            matches!(
                lgwks_std::task::block_on(service.pull(&forged, 11)),
                Err(ServiceError::Checkpoint(CheckpointError::InvalidMac))
            ),
            "forged checkpoint must fail the MAC check"
        );
        Ok(())
    }

    #[test]
    fn valid_expired_checkpoint_requests_explicit_full_resync() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service =
            ProtocolService::new(Memory::default(), Allow, CheckpointCodec::new([8; 32], 5)?);
        let pull = PullRequest::new(
            PROTOCOL_VERSION,
            scope,
            replica,
            OperationDigest::new([1; 32]),
            None,
            10,
            1024,
        );
        let ServicePull::Page(page) = lgwks_std::task::block_on(service.pull(&pull, 10))? else {
            return Err("page expected".into());
        };
        let expired = PullRequest::new(
            pull.protocol,
            pull.scope,
            pull.replica,
            pull.request,
            Some(page.checkpoint),
            pull.max_changes,
            pull.max_bytes,
        );
        assert_eq!(
            lgwks_std::task::block_on(service.pull(&expired, 15))?,
            ServicePull::CheckpointExpired,
            "valid expired checkpoint must request explicit full resync"
        );
        Ok(())
    }

    #[test]
    fn push_distinguishes_malformed_change_from_digest_mismatch() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([5; 32], 100)?,
        );
        let mut malformed = change(scope, replica, OperationId::new())?;
        malformed.bytes = vec![1];
        let mut mismatched = change(scope, replica, OperationId::new())?;
        mismatched.digest = ChangeDigest::new([0; 32]);
        let response = lgwks_std::task::block_on(service.push(
            &PushRequest::new(PROTOCOL_VERSION, replica, vec![malformed, mismatched]),
            10,
        ))?;
        assert!(
            matches!(
                response.outcomes[0],
                PushOutcome::Rejected {
                    code: RejectionCode::InvalidChange,
                    ..
                }
            ),
            "malformed change bytes must reject as invalid change"
        );
        assert!(
            matches!(
                response.outcomes[1],
                PushOutcome::Rejected {
                    code: RejectionCode::DigestMismatch,
                    ..
                }
            ),
            "digest mismatch must reject as digest mismatch"
        );
        Ok(())
    }

    #[test]
    fn pull_store_consumes_exact_grant_and_rejects_stale_frontier() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service =
            ProtocolService::new(Memory::default(), Stale, CheckpointCodec::new([8; 32], 50)?);
        let pull = PullRequest::new(
            PROTOCOL_VERSION,
            scope,
            replica,
            OperationDigest::new([3; 32]),
            None,
            1,
            1024,
        );
        assert!(
            matches!(
                lgwks_std::task::block_on(service.pull(&pull, 10)),
                Err(ServiceError::StaleAuthority)
            ),
            "stale frontier grant must be rejected"
        );
        Ok(())
    }

    #[test]
    fn checkpoint_shape_scope_axes_and_integer_decode_fail_independently()
    -> Result<(), Box<dyn Error>> {
        let account = AccountId::new();
        let space = SpaceId::new();
        let scope = Scope::new(account, space);
        let codec = CheckpointCodec::new([13; 32], 100)?;
        let token = codec.issue(scope, 2, 10)?;
        assert_eq!(
            codec.open(scope, &token, 11),
            Ok(2),
            "well-formed checkpoint must decode to its position"
        );
        assert_eq!(
            decode_u64(&2_u64.to_be_bytes()),
            Ok(2),
            "eight-byte big-endian integer must decode"
        );
        assert_eq!(
            decode_u64(&[0; 7]),
            Err(CheckpointError::Malformed),
            "short integer slice must be malformed"
        );

        let mut wrong_magic = token.as_wire().to_vec();
        wrong_magic[..4].copy_from_slice(b"NOPE");
        let wrong_magic = Checkpoint::from_wire(wrong_magic)?;
        assert_eq!(
            codec.open(scope, &wrong_magic, 11),
            Err(CheckpointError::Malformed),
            "wrong magic must be malformed"
        );
        let short = Checkpoint::from_wire(CHECKPOINT_MAGIC.to_vec())?;
        assert_eq!(
            codec.open(scope, &short, 11),
            Err(CheckpointError::Malformed),
            "short token must be malformed"
        );
        assert_eq!(
            codec.open(Scope::new(AccountId::new(), space), &token, 11),
            Err(CheckpointError::WrongScope),
            "account axis must be authenticated"
        );
        assert_eq!(
            codec.open(Scope::new(account, SpaceId::new()), &token, 11),
            Err(CheckpointError::WrongScope),
            "space axis must be authenticated"
        );
        Ok(())
    }

    #[test]
    fn stored_page_shape_cannot_skip_or_lie_about_the_bound_frontier() -> Result<(), Box<dyn Error>>
    {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let page = StoredPage {
            changes: vec![change(scope, replica, OperationId::new())?],
            position: 5,
            more: true,
        };
        assert!(
            valid_stored_page(4, 6, &page),
            "valid page must pass shape check"
        );
        let stalled = StoredPage {
            changes: Vec::new(),
            position: 4,
            more: true,
        };
        assert!(
            !valid_stored_page(4, 6, &stalled),
            "empty page claiming more must fail shape check"
        );
        let skipped = StoredPage {
            position: 6,
            ..page.clone()
        };
        assert!(
            !valid_stored_page(4, 6, &skipped),
            "skipped position must fail shape check"
        );
        let false_end = StoredPage {
            more: false,
            ..page.clone()
        };
        assert!(
            !valid_stored_page(4, 6, &false_end),
            "false end claim must fail shape check"
        );
        let beyond = StoredPage {
            position: 6,
            more: false,
            ..page
        };
        assert!(
            !valid_stored_page(4, 6, &beyond),
            "position past frontier must fail shape check"
        );
        Ok(())
    }

    /// Authority fixture that always refuses authorization.
    struct Deny;

    impl Authority for Deny {
        type Grant = Grant;

        fn authorize(
            &self,
            _scope: Scope,
            _effect: Effect,
            _action: ActionDigest,
            _frontier: u64,
            _now: u64,
        ) -> Option<Self::Grant> {
            None
        }
    }

    /// Builds a fixture envelope with an explicit request digest byte.
    fn envelope_with(
        scope: Scope,
        replica: ReplicaId,
        operation: OperationId,
        request_byte: u8,
    ) -> Result<ChangeEnvelope, Box<dyn Error>> {
        let mut envelope = change(scope, replica, operation)?;
        envelope.request = OperationDigest::new([request_byte; 32]);
        Ok(envelope)
    }

    /// Builds a fixture envelope whose payload is a document tombstone.
    fn tombstone_envelope(
        scope: Scope,
        replica: ReplicaId,
        operation: OperationId,
        request_byte: u8,
    ) -> Result<ChangeEnvelope, Box<dyn Error>> {
        let mut document = Document::new();
        document.edit(|editor| editor.put(&Object::root(), "k", Scalar::Integer(1)))?;
        let mut staged = document.delete()?;
        let encoded = staged.pop().ok_or("delete must stage exactly one change")?;
        assert!(staged.is_empty(), "delete must stage exactly one change");
        Ok(ChangeEnvelope::new(
            scope,
            DocumentId::new(),
            replica,
            operation,
            OperationDigest::new([request_byte; 32]),
            encoded.digest,
            encoded.bytes,
        ))
    }

    /// Pushes one bounded page of fixture changes through the service.
    fn push_all<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        replica: ReplicaId,
        changes: Vec<ChangeEnvelope>,
    ) -> Result<PushResponse, Box<dyn Error>>
    where
        Store: ServiceStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        let request = PushRequest::new(PROTOCOL_VERSION, replica, changes);
        Ok(lgwks_std::task::block_on(service.push(&request, 10))
            .map_err(|error| format!("push failed: {error:?}"))?)
    }

    fn pull_after<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        replica: ReplicaId,
        after: Option<Checkpoint>,
        now: u64,
    ) -> Result<ServicePull, Box<dyn Error>>
    where
        Store: ServiceStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        let request = PullRequest::new(
            PROTOCOL_VERSION,
            scope,
            replica,
            OperationDigest::new([91; 32]),
            after,
            10,
            1_048_576,
        );
        Ok(lgwks_std::task::block_on(service.pull(&request, now))
            .map_err(|error| format!("pull failed: {error:?}"))?)
    }

    fn recover_at<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        replica: ReplicaId,
        after: Option<Checkpoint>,
        now: u64,
    ) -> Result<ServiceBaseline, Box<dyn Error>>
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        let request = BaselineRequest::new(
            PROTOCOL_VERSION,
            scope,
            replica,
            OperationDigest::new([92; 32]),
            after,
            None,
        );
        Ok(lgwks_std::task::block_on(service.recover(&request, now))
            .map_err(|error| format!("recover failed: {error:?}"))?)
    }

    fn recover_page_at<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        replica: ReplicaId,
        after_document: Option<DocumentId>,
        now: u64,
    ) -> Result<ServiceBaseline, Box<dyn Error>>
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        let request = BaselineRequest::new(
            PROTOCOL_VERSION,
            scope,
            replica,
            OperationDigest::new([93; 32]),
            None,
            after_document,
        );
        Ok(lgwks_std::task::block_on(service.recover(&request, now))
            .map_err(|error| format!("recover page failed: {error:?}"))?)
    }

    #[test]
    fn a_recovery_grant_for_one_cursor_cannot_authorize_another() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let intent = OperationDigest::new([92; 32]);
        let cursor = DocumentId::new();
        let authorized = recover_action(scope, replica, intent, None, None);
        let captured = recover_action(scope, replica, intent, None, Some(&cursor));
        assert_ne!(
            authorized, captured,
            "the exclusive cursor must change the recover action digest"
        );

        let service = ProtocolService::new(
            Memory::default(),
            ExactRecovery {
                scope,
                action: authorized,
                frontier: 0,
            },
            CheckpointCodec::new([6; 32], 100)?,
        );
        let other =
            BaselineRequest::new(PROTOCOL_VERSION, scope, replica, intent, None, Some(cursor));
        assert!(
            matches!(
                lgwks_std::task::block_on(service.recover(&other, 10)),
                Err(ServiceError::Forbidden)
            ),
            "cursor mismatch must refuse the recovery grant"
        );
        let exact = BaselineRequest::new(PROTOCOL_VERSION, scope, replica, intent, None, None);
        assert!(
            matches!(
                lgwks_std::task::block_on(service.recover(&exact, 10)),
                Ok(ServiceBaseline::Unavailable)
            ),
            "exact grant without a sealed baseline must report unavailable"
        );
        Ok(())
    }

    #[test]
    fn recovery_pages_a_baseline_larger_than_one_page() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([6; 32], 100)?,
        );
        let total = logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS + 44;
        let changes = (0..total)
            .map(|index| {
                envelope_with(
                    scope,
                    replica,
                    OperationId::new(),
                    u8::try_from(index % 256).unwrap_or(0),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        for chunk in changes.chunks(logical_db_model::MAX_CHANGES_PER_PAGE) {
            push_all(&service, replica, chunk.to_vec())?;
        }
        assert!(
            matches!(
                seal_at(
                    &service,
                    scope,
                    u64::try_from(total).unwrap_or(u64::MAX),
                    10
                )?,
                SealResult::Sealed { .. }
            ),
            "multi-page baseline must seal"
        );

        let ServiceBaseline::Artifact(first) = recover_page_at(&service, scope, replica, None, 11)?
        else {
            return Err("sealed baseline must be available".into());
        };
        assert!(first.more, "first page must report more documents");
        assert_eq!(
            first.documents.len(),
            logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS,
            "first page must be full"
        );
        let cursor = first
            .next_document
            .ok_or("continued page carries a cursor")?;
        let ServiceBaseline::Artifact(second) =
            recover_page_at(&service, scope, replica, Some(cursor), 12)?
        else {
            return Err("continued baseline page must be available".into());
        };
        assert!(!second.more, "final page must not report more documents");
        assert_eq!(second.documents.len(), 44, "final page must hold the tail");
        assert_eq!(
            second.descriptor, first.descriptor,
            "pages must share one descriptor"
        );
        assert!(
            second.next_document.is_none(),
            "final page must not carry a cursor"
        );

        let mut accumulator =
            logical_db_protocol::BaselineAccumulator::new(scope, first.validate_page(scope)?);
        assert!(
            !accumulator.push(&first)?,
            "first page must not complete the accumulator"
        );
        assert!(
            accumulator.push(&second)?,
            "second page must complete the accumulator"
        );
        assert_eq!(
            accumulator.finish()?.documents.len(),
            total,
            "accumulator must reassemble every document"
        );
        Ok(())
    }

    fn seal_at<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        position: u64,
        now: u64,
    ) -> Result<SealResult, Box<dyn Error>>
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        Ok(lgwks_std::task::block_on(service.seal_snapshot(
            scope,
            position,
            OperationDigest::new([61; 32]),
            now,
        ))
        .map_err(|error| format!("seal failed: {error:?}"))?)
    }

    fn consume_through<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        through: u64,
        now: u64,
    ) -> Result<ConsumedResult, Box<dyn Error>>
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        Ok(lgwks_std::task::block_on(service.mark_consumed(
            scope,
            through,
            OperationDigest::new([62; 32]),
            now,
        ))
        .map_err(|error| format!("mark_consumed failed: {error:?}"))?)
    }

    fn collect<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        up_to: u64,
        min_live: Option<u64>,
        now: u64,
    ) -> Result<PruneResult, Box<dyn Error>>
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        Ok(lgwks_std::task::block_on(service.prune(
            scope,
            up_to,
            min_live,
            OperationDigest::new([63; 32]),
            now,
        ))
        .map_err(|error| format!("prune failed: {error:?}"))?)
    }

    #[test]
    fn prune_refuses_a_prefix_still_referenced_by_a_live_checkpoint() -> Result<(), Box<dyn Error>>
    {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([21; 32], 10_000)?,
        );
        let changes = (30u8..33)
            .map(|request| envelope_with(scope, replica, OperationId::new(), request))
            .collect::<Result<Vec<_>, _>>()?;
        let response = push_all(&service, replica, changes)?;
        assert!(
            response
                .outcomes
                .iter()
                .all(|outcome| matches!(outcome, PushOutcome::Accepted { .. })),
            "every fixture change must be accepted"
        );
        assert_eq!(
            consume_through(&service, scope, 3, 11)?,
            ConsumedResult::Advanced {
                consumed_through: 3
            },
            "watermark must advance through the prefix"
        );
        assert_eq!(
            collect(&service, scope, 3, Some(1), 12)?,
            PruneResult::Refused(PruneRefusal::LiveCheckpoint { min_live: 1 }),
            "live checkpoint must refuse collection"
        );
        assert_eq!(
            collect(&service, scope, 0, None, 13)?,
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 0,
                floor: 0
            }),
            "request at or below the floor is an idempotent no-op"
        );
        let ServicePull::Page(page) = pull_after(&service, scope, replica, None, 14)? else {
            return Err("page expected".into());
        };
        assert_eq!(
            page.changes.len(),
            3,
            "page must serve every retained change"
        );
        Ok(())
    }

    #[test]
    fn prune_refuses_a_prefix_with_unconsumed_operation_records() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([22; 32], 10_000)?,
        );
        let changes = (40u8..43)
            .map(|request| envelope_with(scope, replica, OperationId::new(), request))
            .collect::<Result<Vec<_>, _>>()?;
        push_all(&service, replica, changes)?;
        assert_eq!(
            collect(&service, scope, 2, None, 11)?,
            PruneResult::Refused(PruneRefusal::PendingOperations {
                consumed_through: 0
            }),
            "unconsumed operations must refuse collection"
        );
        assert_eq!(
            consume_through(&service, scope, 2, 12)?,
            ConsumedResult::Advanced {
                consumed_through: 2
            },
            "watermark must advance"
        );
        assert_eq!(
            collect(&service, scope, 2, None, 13)?,
            PruneResult::Refused(PruneRefusal::MissingBaseline),
            "missing baseline must refuse collection"
        );
        assert_eq!(
            seal_at(&service, scope, 2, 14)?,
            SealResult::Sealed { position: 2 },
            "baseline must seal at position two"
        );
        assert_eq!(
            collect(&service, scope, 2, None, 15)?,
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 2,
                floor: 2
            }),
            "covered prefix must collect"
        );
        assert_eq!(
            collect(&service, scope, 3, None, 16)?,
            PruneResult::Refused(PruneRefusal::PendingOperations {
                consumed_through: 2
            }),
            "prefix past the watermark must refuse"
        );
        Ok(())
    }

    #[test]
    fn prune_refuses_a_prefix_beyond_the_state_bearing_baseline() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([29; 32], 10_000)?,
        );
        let changes = (70u8..73)
            .map(|request| envelope_with(scope, replica, OperationId::new(), request))
            .collect::<Result<Vec<_>, _>>()?;
        push_all(&service, replica, changes)?;
        assert_eq!(
            seal_at(&service, scope, 2, 11)?,
            SealResult::Sealed { position: 2 },
            "baseline must seal at position two"
        );
        assert_eq!(
            consume_through(&service, scope, 3, 12)?,
            ConsumedResult::Advanced {
                consumed_through: 3
            },
            "watermark must advance past the baseline"
        );
        assert_eq!(
            collect(&service, scope, 3, None, 13)?,
            PruneResult::Refused(PruneRefusal::BaselineBehind { position: 2 }),
            "prefix past the baseline must refuse"
        );
        assert_eq!(
            collect(&service, scope, 2, None, 14)?,
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 2,
                floor: 2
            }),
            "prefix covered by the baseline must collect"
        );
        Ok(())
    }

    #[test]
    fn behind_floor_checkpoint_requires_baseline_then_suffix_completes()
    -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([23; 32], 10_000)?,
        );
        let first = envelope_with(scope, replica, OperationId::new(), 50)?;
        push_all(&service, replica, vec![first])?;
        let ServicePull::Page(first_page) = pull_after(&service, scope, replica, None, 11)? else {
            return Err("page expected".into());
        };
        assert_eq!(
            first_page.changes.len(),
            1,
            "first page must hold one change"
        );
        let stale_checkpoint = first_page.checkpoint;
        let second = envelope_with(scope, replica, OperationId::new(), 51)?;
        let tombstone = tombstone_envelope(scope, replica, OperationId::new(), 52)?;
        let tombstone_bytes = tombstone.bytes.clone();
        let fourth = envelope_with(scope, replica, OperationId::new(), 53)?;
        push_all(&service, replica, vec![second, tombstone, fourth])?;
        assert_eq!(
            seal_at(&service, scope, 2, 12)?,
            SealResult::Sealed { position: 2 },
            "baseline must seal at position two"
        );
        assert_eq!(
            consume_through(&service, scope, 4, 13)?,
            ConsumedResult::Advanced {
                consumed_through: 4
            },
            "watermark must advance through the prefix"
        );
        assert_eq!(
            collect(&service, scope, 2, None, 14)?,
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 2,
                floor: 2
            }),
            "covered prefix must collect"
        );
        assert_eq!(
            pull_after(&service, scope, replica, Some(stale_checkpoint.clone()), 15)?,
            ServicePull::BaselineRequired { floor: 2 },
            "checkpoint behind the floor must require baseline recovery"
        );
        assert_eq!(
            pull_after(&service, scope, replica, None, 15)?,
            ServicePull::BaselineRequired { floor: 2 },
            "fresh replica facing a nonzero floor must require baseline recovery"
        );
        let ServiceBaseline::Artifact(baseline) =
            recover_at(&service, scope, replica, Some(stale_checkpoint), 16)?
        else {
            return Err("state-bearing baseline expected".into());
        };
        let artifact = baseline.validate(scope)?;
        assert_eq!(
            artifact.baseline.position(),
            2,
            "recovered baseline must cover position two"
        );
        assert_eq!(
            artifact.documents.len(),
            2,
            "recovered baseline must hold two documents"
        );
        let ServicePull::Page(recovered) = pull_after(
            &service,
            scope,
            replica,
            Some(baseline.checkpoint.clone()),
            17,
        )?
        else {
            return Err("recovery suffix page expected".into());
        };
        assert_eq!(
            recovered.changes.len(),
            2,
            "suffix must hold the tail changes"
        );
        assert_eq!(
            recovered.changes[0].bytes, tombstone_bytes,
            "suffix must replay the tombstone payload"
        );
        assert!(!recovered.more, "suffix page must be final");
        Ok(())
    }

    #[test]
    fn chunk_manifest_and_invocation_references_stay_consistent_after_collection()
    -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([24; 32], 10_000)?,
        );
        let operations = [OperationId::new(), OperationId::new(), OperationId::new()];
        let changes = operations
            .iter()
            .zip(60u8..63)
            .map(|(operation, request)| envelope_with(scope, replica, *operation, request))
            .collect::<Result<Vec<_>, _>>()?;
        push_all(&service, replica, changes.clone())?;
        assert_eq!(
            seal_at(&service, scope, 2, 11)?,
            SealResult::Sealed { position: 2 },
            "baseline must seal at position two"
        );
        assert_eq!(
            consume_through(&service, scope, 3, 12)?,
            ConsumedResult::Advanced {
                consumed_through: 3
            },
            "watermark must advance"
        );
        assert_eq!(
            collect(&service, scope, 2, None, 13)?,
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 2,
                floor: 2
            }),
            "covered prefix must collect"
        );
        let duplicate = push_all(&service, replica, vec![changes[0].clone()])?;
        assert!(
            matches!(duplicate.outcomes[0], PushOutcome::Duplicate { .. }),
            "exact retry after collection must remain a duplicate"
        );
        let mut conflicting = changes[1].clone();
        conflicting.request = OperationDigest::new([99; 32]);
        let conflict = push_all(&service, replica, vec![conflicting])?;
        assert!(
            matches!(
                conflict.outcomes[0],
                PushOutcome::Rejected {
                    code: RejectionCode::OperationConflict,
                    ..
                }
            ),
            "conflicting request bytes must reject as operation conflict"
        );
        let ServiceBaseline::Artifact(baseline) = recover_at(&service, scope, replica, None, 13)?
        else {
            return Err("state-bearing baseline expected".into());
        };
        let ServicePull::Page(page) = pull_after(
            &service,
            scope,
            replica,
            Some(baseline.checkpoint.clone()),
            13,
        )?
        else {
            return Err("page expected".into());
        };
        assert_eq!(page.changes.len(), 1, "suffix must hold one change");
        assert_eq!(
            page.changes[0].operation, operations[2],
            "suffix must carry the uncollected operation"
        );
        assert!(!page.more, "suffix page must be final");
        Ok(())
    }

    #[test]
    fn retention_floor_and_baseline_survive_service_restart() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let store = std::sync::Arc::new(Memory::default());
        let changes = (70u8..73)
            .map(|request| envelope_with(scope, replica, OperationId::new(), request))
            .collect::<Result<Vec<_>, _>>()?;
        let stale_checkpoint = {
            let service = ProtocolService::new(
                Arc::clone(&store),
                Allow,
                CheckpointCodec::new([25; 32], 10_000)?,
            );
            push_all(&service, replica, vec![changes[0].clone()])?;
            let ServicePull::Page(first) = pull_after(&service, scope, replica, None, 11)? else {
                return Err("page expected".into());
            };
            push_all(&service, replica, changes[1..].to_vec())?;
            assert_eq!(
                seal_at(&service, scope, 2, 12)?,
                SealResult::Sealed { position: 2 },
                "baseline must seal at position two"
            );
            assert_eq!(
                consume_through(&service, scope, 3, 13)?,
                ConsumedResult::Advanced {
                    consumed_through: 3
                },
                "watermark must advance"
            );
            assert_eq!(
                collect(&service, scope, 2, None, 14)?,
                PruneResult::Pruned(PruneReceipt {
                    pruned_through: 2,
                    floor: 2
                }),
                "covered prefix must collect"
            );
            first.checkpoint
        };
        let service = ProtocolService::new(
            Arc::clone(&store),
            Allow,
            CheckpointCodec::new([25; 32], 10_000)?,
        );
        assert_eq!(
            lgwks_std::task::block_on(store.retention_floor(scope))?,
            2,
            "retention floor must survive restart"
        );
        assert!(
            matches!(
                lgwks_std::task::block_on(store.snapshot_baseline(scope))?,
                Some(SnapshotRecord::State(ref baseline)) if baseline.position() == 2
            ),
            "sealed baseline must survive restart"
        );
        assert_eq!(
            pull_after(&service, scope, replica, Some(stale_checkpoint.clone()), 15)?,
            ServicePull::BaselineRequired { floor: 2 },
            "stale checkpoint after restart must require baseline recovery"
        );
        let ServiceBaseline::Artifact(baseline) =
            recover_at(&service, scope, replica, Some(stale_checkpoint), 16)?
        else {
            return Err("state-bearing baseline must survive restart".into());
        };
        let artifact = baseline.validate(scope)?;
        assert_eq!(
            artifact.baseline.position(),
            2,
            "recovered baseline must cover position two"
        );
        assert_eq!(
            artifact.documents.len(),
            2,
            "recovered baseline must hold two documents"
        );
        Ok(())
    }

    #[test]
    fn prune_decision_rejects_every_uncollectible_edge() {
        assert_eq!(
            prune_decision(10, 2, 2, None, 5),
            Ok(2),
            "request at floor is a no-op"
        );
        assert_eq!(
            prune_decision(10, 2, 1, None, 5),
            Ok(2),
            "request below floor is a no-op"
        );
        assert_eq!(
            prune_decision(10, 2, 5, None, 5),
            Ok(5),
            "covered prefix advances"
        );
        assert_eq!(
            prune_decision(10, 2, 5, Some(5), 9),
            Ok(5),
            "prefix at the live checkpoint advances"
        );
        assert_eq!(
            prune_decision(3, 0, 4, None, 4),
            Err(PruneRefusal::BeyondFrontier { frontier: 3 }),
            "prefix past the frontier must refuse"
        );
        assert_eq!(
            prune_decision(10, 0, 5, Some(4), 9),
            Err(PruneRefusal::LiveCheckpoint { min_live: 4 }),
            "prefix past a live checkpoint must refuse"
        );
        assert_eq!(
            prune_decision(10, 0, 5, None, 4),
            Err(PruneRefusal::PendingOperations {
                consumed_through: 4
            }),
            "prefix past the consumption watermark must refuse"
        );
    }

    #[test]
    fn retention_writes_require_fresh_authority() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let stale = ProtocolService::new(
            Memory::default(),
            Stale,
            CheckpointCodec::new([26; 32], 10_000)?,
        );
        assert_eq!(
            lgwks_std::task::block_on(stale.seal_snapshot(
                scope,
                0,
                OperationDigest::new([61; 32]),
                10,
            ))?,
            SealResult::StaleAuthority,
            "seal must require fresh authority"
        );
        assert_eq!(
            lgwks_std::task::block_on(stale.mark_consumed(
                scope,
                0,
                OperationDigest::new([62; 32]),
                10,
            ))?,
            ConsumedResult::StaleAuthority,
            "mark_consumed must require fresh authority"
        );
        assert_eq!(
            lgwks_std::task::block_on(stale.prune(
                scope,
                0,
                None,
                OperationDigest::new([63; 32]),
                10,
            ))?,
            PruneResult::StaleAuthority,
            "prune must require fresh authority"
        );
        let denied = ProtocolService::new(
            Memory::default(),
            Deny,
            CheckpointCodec::new([27; 32], 10_000)?,
        );
        assert!(
            matches!(
                lgwks_std::task::block_on(denied.prune(
                    scope,
                    0,
                    None,
                    OperationDigest::new([63; 32]),
                    10,
                )),
                Err(ServiceError::Forbidden)
            ),
            "denied authority must fail closed"
        );
        Ok(())
    }
}
