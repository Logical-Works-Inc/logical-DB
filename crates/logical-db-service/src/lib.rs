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
    ActionDigest, Effect, OperationDigest, Scope, SnapshotArtifact, SnapshotBaseline,
    SnapshotDocument, SnapshotRecord, is_behind_retention,
};
use logical_db_protocol::{
    BaselineDocument, BaselinePage, BaselineRequest, ChangeEnvelope, Checkpoint, PROTOCOL_VERSION,
    PageAction, ProtocolError, PullPage, PullRequest, PushOutcome, PushRequest, PushResponse,
    RecoveryAction, RejectionCode, mark_consumed_action, prune_action, seal_snapshot_action,
    write_action,
};
use thiserror::Error;

const CHECKPOINT_MAGIC: &[u8; 4] = b"LCP\x01";
const CHECKPOINT_BODY_BYTES: usize = 52;
const CHECKPOINT_BYTES: usize = CHECKPOINT_BODY_BYTES + 32;

/// Compact, typed failures from state-bearing baseline materialization.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Error)]
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
            artifact.validate(scope).map_err(|error| {
                eprintln!("logical-db-service: materialize_snapshot base validation: {error}");
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
    if expected != changes.len() as u64 {
        eprintln!(
            "logical-db-service: materialize_snapshot coverage: through={through} base={base_position} changes={}",
            changes.len()
        );
        return Err(SnapshotMaterializationError::Coverage);
    }

    let mut documents = BTreeMap::<logical_db_model::DocumentId, Document>::new();
    if let Some(artifact) = base {
        for state in &artifact.documents {
            let mut document = Document::load(&state.snapshot).map_err(|error| {
                eprintln!("logical-db-service: materialize_snapshot document load: {error}");
                SnapshotMaterializationError::InvalidBase {
                    context: "document load",
                }
            })?;
            let tombstone = document.is_deleted().map_err(|error| {
                eprintln!("logical-db-service: materialize_snapshot tombstone read: {error}");
                SnapshotMaterializationError::InvalidBase {
                    context: "tombstone read",
                }
            })?;
            if tombstone != state.tombstone {
                eprintln!(
                    "logical-db-service: materialize_snapshot tombstone mismatch: document={}",
                    state.document
                );
                return Err(SnapshotMaterializationError::InvalidBase {
                    context: "tombstone mismatch",
                });
            }
            documents.insert(state.document, document);
        }
    }

    for change in changes {
        if change.scope != scope {
            eprintln!(
                "logical-db-service: materialize_snapshot scope mismatch: document={}",
                change.document
            );
            return Err(SnapshotMaterializationError::ScopeMismatch);
        }
        change.validate().map_err(|error| {
            eprintln!("logical-db-service: materialize_snapshot change validation: {error}");
            SnapshotMaterializationError::InvalidChange
        })?;
        let encoded = EncodedChange {
            digest: change.digest,
            bytes: change.bytes.clone(),
        };
        encoded.validate().map_err(|error| {
            eprintln!("logical-db-service: materialize_snapshot encoded validation: {error}");
            SnapshotMaterializationError::InvalidChange
        })?;
        documents
            .entry(change.document)
            .or_default()
            .apply(&[encoded])
            .map_err(|error| {
                eprintln!("logical-db-service: materialize_snapshot apply: {error}");
                SnapshotMaterializationError::InvalidChange
            })?;
    }

    let states = documents
        .into_iter()
        .map(|(document, mut state)| {
            let tombstone = state.is_deleted().map_err(|error| {
                eprintln!("logical-db-service: materialize_snapshot output tombstone: {error}");
                SnapshotMaterializationError::InvalidChange
            })?;
            let snapshot = state.save().map_err(|error| {
                eprintln!("logical-db-service: materialize_snapshot save: {error}");
                SnapshotMaterializationError::ResourceLimit
            })?;
            Ok(logical_db_model::SnapshotDocument {
                document,
                snapshot,
                tombstone,
            })
        })
        .collect::<Result<Vec<_>, SnapshotMaterializationError>>()?;
    SnapshotArtifact::new(scope, through, states).map_err(|error| {
        eprintln!("logical-db-service: materialize_snapshot artifact: {error}");
        SnapshotMaterializationError::ResourceLimit
    })
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

/// Host authority boundary. Production composition uses a Braid runnable invocation.
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

/// Durable outcome of accepting one immutable change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
pub struct StoredPage {
    /// Changes strictly after the requested position.
    pub changes: Vec<ChangeEnvelope>,
    /// Durable position covering every returned change.
    pub position: u64,
    /// Whether another page is immediately available.
    pub more: bool,
}

/// Durable outcome of executing one frontier-bound page invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
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

    /// Reads the current monotonic scoped frontier used for Braid justification.
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
        eprintln!("logical-db-service: prune_decision: BeyondFrontier");
        return Err(PruneRefusal::BeyondFrontier { frontier });
    }
    if let Some(min_live) = min_live
        && requested_up_to > min_live
    {
        eprintln!("logical-db-service: prune_decision: LiveCheckpoint");
        return Err(PruneRefusal::LiveCheckpoint { min_live });
    }
    if requested_up_to > consumed_through {
        eprintln!("logical-db-service: prune_decision: PendingOperations");
        return Err(PruneRefusal::PendingOperations { consumed_through });
    }
    Ok(requested_up_to)
}

/// Durable receipt for one collected log prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PruneReceipt {
    /// Highest log position collected by this decision, inclusive.
    pub pruned_through: u64,
    /// Durable retention floor after collection.
    pub floor: u64,
}

/// Durable outcome of executing one frontier-bound prune invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
pub struct BaselinePageSlice {
    /// Strictly ascending documents after the requested cursor.
    pub documents: Vec<SnapshotDocument>,
    /// Whether further documents follow this slice for the same descriptor.
    pub more: bool,
}

/// Durable outcome of one grant-bound recovery page read.
#[derive(Clone, Debug, Eq, PartialEq)]
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
    key: [u8; 32],
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
            eprintln!("logical-db-service: new: WeakKey");
            return Err(CheckpointError::WeakKey);
        }
        if ttl_ms == 0 {
            eprintln!("logical-db-service: new: InvalidTtl");
            return Err(CheckpointError::InvalidTtl);
        }
        Ok(Self { key, ttl_ms })
    }

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

    fn open(
        &self,
        scope: Scope,
        token: &Checkpoint,
        now_unix_ms: u64,
    ) -> Result<u64, CheckpointError> {
        let bytes = token.as_wire();
        if bytes.len() != CHECKPOINT_BYTES || &bytes[..4] != CHECKPOINT_MAGIC {
            eprintln!("logical-db-service: open: Malformed");
            return Err(CheckpointError::Malformed);
        }
        let expected = lgwks_std::hash::keyed(&self.key, &bytes[..CHECKPOINT_BODY_BYTES]);
        let Ok(supplied) = <[u8; 32]>::try_from(&bytes[CHECKPOINT_BODY_BYTES..]) else {
            return Err(CheckpointError::Malformed);
        };
        if !tags_equal(expected.as_bytes(), &supplied) {
            eprintln!("logical-db-service: CheckpointError::InvalidMac");
            return Err(CheckpointError::InvalidMac);
        }
        if bytes[4..20] != *scope.account().as_uuid().as_bytes()
            || bytes[20..36] != *scope.space().as_uuid().as_bytes()
        {
            eprintln!("logical-db-service: open: WrongScope");
            return Err(CheckpointError::WrongScope);
        }
        let position = decode_u64(&bytes[36..44])?;
        let expires = decode_u64(&bytes[44..52])?;
        if now_unix_ms >= expires {
            eprintln!("logical-db-service: open: Expired");
            return Err(CheckpointError::Expired);
        }
        Ok(position)
    }

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
pub enum ServiceBaseline {
    /// A sealed state-bearing artifact covers the retained prefix.
    Artifact(Box<BaselinePage>),
    /// No state-bearing baseline exists, so the durable log still covers every
    /// position and a full pull from the floor is complete.
    Unavailable,
}

/// Stateless service orchestration over a durable storage adapter.
pub struct ProtocolService<Store, Policy> {
    store: Store,
    policy: Policy,
    checkpoints: CheckpointCodec,
}

impl<Store, Policy> ProtocolService<Store, Policy>
where
    Store: ServiceStore,
    Policy: Authority,
{
    /// Constructs a service from explicit durable, authority, and token ports.
    #[must_use]
    pub const fn new(store: Store, policy: Policy, checkpoints: CheckpointCodec) -> Self {
        Self {
            store,
            policy,
            checkpoints,
        }
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
            let validation = (EncodedChange {
                digest: change.digest,
                bytes: change.bytes.clone(),
            })
            .validate();
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
        Ok(PushResponse {
            protocol: PROTOCOL_VERSION,
            outcomes,
        })
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
        let checkpoint_position = match &request.after {
            Some(token) => match self.checkpoints.open(request.scope, token, now_unix_ms) {
                Ok(position) if is_behind_retention(position, floor) => {
                    eprintln!("logical-db-service: pull: BaselineRequired");
                    return Ok(ServicePull::BaselineRequired { floor });
                }
                Ok(position) => Some(position),
                Err(CheckpointError::Expired) if floor > 0 => {
                    eprintln!("logical-db-service: pull: BaselineRequired");
                    return Ok(ServicePull::BaselineRequired { floor });
                }
                Err(CheckpointError::Expired) => return Ok(ServicePull::CheckpointExpired),
                Err(error) => {
                    eprintln!("logical-db-service: pull checkpoint: {error}");
                    return Err(ServiceError::Checkpoint(error));
                }
            },
            None if floor > 0 => {
                eprintln!("logical-db-service: pull: BaselineRequired");
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
        let action = PageAction {
            scope: request.scope,
            replica: request.replica,
            intent: request.request,
            after,
            max_changes: request.max_changes,
            max_bytes: request.max_bytes,
        };
        let Some(grant) = self.policy.authorize(
            request.scope,
            Effect::Sync,
            action.digest(),
            frontier,
            now_unix_ms,
        ) else {
            eprintln!("logical-db-service: pull: Forbidden");
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
            PageResult::StaleAuthority => {
                eprintln!("logical-db-service: pull: StaleAuthority");
                return Err(ServiceError::StaleAuthority);
            }
        };
        if !valid_stored_page(after, frontier, &stored) {
            eprintln!("logical-db-service: pull: CorruptStoreData");
            return Err(ServiceError::CorruptStoreData);
        }
        if stored
            .changes
            .iter()
            .any(|change| change.scope != request.scope)
        {
            eprintln!("logical-db-service: pull: CrossScopeStoreData");
            return Err(ServiceError::CrossScopeStoreData);
        }
        for change in &stored.changes {
            EncodedChange {
                digest: change.digest,
                bytes: change.bytes.clone(),
            }
            .validate()
            .map_err(|error| {
                eprintln!("logical-db-service: ServiceError::CorruptStoreData: {error}");
                ServiceError::CorruptStoreData
            })?;
        }
        let checkpoint = self
            .checkpoints
            .issue(request.scope, stored.position, now_unix_ms)
            .map_err(ServiceError::Checkpoint)?;
        let page = PullPage {
            protocol: PROTOCOL_VERSION,
            changes: stored.changes,
            checkpoint,
            more: stored.more,
        };
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
            eprintln!("logical-db-service: seal_snapshot: Forbidden");
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
        let action = RecoveryAction {
            scope: request.scope,
            replica: request.replica,
            intent: request.request,
            after: request.after.clone(),
            after_document: request.after_document,
        };
        let digest = action.digest();
        let Some(grant) =
            self.policy
                .authorize(request.scope, Effect::Sync, digest, frontier, now_unix_ms)
        else {
            eprintln!("logical-db-service: recover: Forbidden");
            return Err(ServiceError::Forbidden);
        };
        if grant.scope() != request.scope
            || grant.effect() != Effect::Sync
            || grant.action() != digest
            || grant.frontier() != frontier
        {
            eprintln!("logical-db-service: recover: Forbidden");
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
            BaselinePageResult::StaleAuthority => {
                eprintln!("logical-db-service: recover: StaleAuthority");
                return Err(ServiceError::StaleAuthority);
            }
        };
        if slice.more && slice.documents.is_empty() {
            eprintln!("logical-db-service: recover: CorruptStoreData");
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
        let page = BaselinePage {
            protocol: PROTOCOL_VERSION,
            descriptor: descriptor.encode().to_vec(),
            documents: slice
                .documents
                .iter()
                .map(|document| BaselineDocument {
                    document: document.document,
                    tombstone: document.tombstone,
                    snapshot: document.snapshot.clone(),
                })
                .collect(),
            checkpoint,
            more: slice.more,
            next_document,
        };
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
            eprintln!("logical-db-service: mark_consumed: Forbidden");
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
            eprintln!("logical-db-service: prune: Forbidden");
            return Err(ServiceError::Forbidden);
        };
        self.store
            .prune(scope, up_to, min_live, request, &grant)
            .await
            .map_err(ServiceError::Store)
    }
}

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
}

fn decode_u64(bytes: &[u8]) -> Result<u64, CheckpointError> {
    bytes.try_into().map(u64::from_be_bytes).map_err(|error| {
        eprintln!("logical-db-service: CheckpointError::Malformed: {error}");
        CheckpointError::Malformed
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, convert::Infallible, sync::Mutex};

    use logical_db_crdt::{Document, Object, Scalar};
    use logical_db_model::{AccountId, ChangeDigest, DocumentId, OperationId, ReplicaId, SpaceId};

    use super::*;

    use logical_db_protocol::recover_action;

    struct Allow;
    struct Stale;
    struct Grant {
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
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
        scope: Scope,
        action: ActionDigest,
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

    #[derive(Clone, Default)]
    struct RetentionState {
        floor: u64,
        consumed: u64,
        baseline: Option<SnapshotArtifact>,
    }

    #[derive(Default)]
    struct Memory {
        changes: Mutex<Vec<ChangeEnvelope>>,
        operations: Mutex<HashMap<OperationId, (OperationDigest, ChangeDigest)>>,
        retention: Mutex<HashMap<Scope, RetentionState>>,
    }

    impl Memory {
        fn scope_frontier(&self, scope: Scope) -> u64 {
            let changes = self.changes.lock().unwrap();
            let state = self
                .retention
                .lock()
                .unwrap()
                .get(&scope)
                .cloned()
                .unwrap_or_default();
            state.floor + Self::retained_count(&changes, scope)
        }

        fn retained_count(changes: &[ChangeEnvelope], scope: Scope) -> u64 {
            changes
                .iter()
                .filter(|change| change.scope == scope)
                .count() as u64
        }

        fn retention_state(&self, scope: Scope) -> RetentionState {
            self.retention
                .lock()
                .unwrap()
                .get(&scope)
                .cloned()
                .unwrap_or_default()
        }

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
        type Error = Infallible;
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
            std::future::ready(Ok(self.build_snapshot_result(scope, through)))
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
            std::future::ready(Ok(self.prune_result(scope, up_to, min_live, request, grant)))
        }
    }

    impl Memory {
        fn accept_result(&self, change: &ChangeEnvelope) -> AcceptResult {
            let mut operations = self.operations.lock().unwrap();
            if let Some(existing) = operations.get(&change.operation) {
                return if *existing == (change.request, change.digest) {
                    AcceptResult::Duplicate
                } else {
                    AcceptResult::Conflict
                };
            }
            operations.insert(change.operation, (change.request, change.digest));
            self.changes.lock().unwrap().push(change.clone());
            AcceptResult::Accepted
        }
        fn frontier_result(&self, scope: Scope) -> u64 {
            self.scope_frontier(scope)
        }
        fn retention_floor_result(&self, scope: Scope) -> u64 {
            self.retention_state(scope).floor
        }
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
            let changes = self.changes.lock().unwrap();
            let frontier = floor + Self::retained_count(&changes, scope);
            if grant.frontier() != frontier {
                return PageResult::StaleAuthority;
            }
            let skip = usize::try_from(after - floor).unwrap_or(usize::MAX);
            let mut bytes = 0usize;
            let page = changes
                .iter()
                .filter(|change| change.scope == scope)
                .skip(skip)
                .take(usize::from(max_changes))
                .take_while(|change| {
                    bytes += change.bytes.len();
                    bytes <= max_bytes as usize
                })
                .cloned()
                .collect::<Vec<_>>();
            let position = after + page.len() as u64;
            let more = frontier > position;
            PageResult::Page(StoredPage {
                changes: page,
                position,
                more,
            })
        }
        fn consumed_through_result(&self, scope: Scope) -> u64 {
            self.retention_state(scope).consumed
        }
        fn snapshot_baseline_result(&self, scope: Scope) -> Option<SnapshotRecord> {
            self.retention_state(scope)
                .baseline
                .map(|artifact| SnapshotRecord::State(artifact.baseline))
        }
        fn build_snapshot_result(&self, scope: Scope, through: u64) -> SnapshotArtifact {
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
            let changes = self.changes.lock().unwrap();
            let changes = changes
                .iter()
                .filter(|change| change.scope == scope)
                .skip(skip)
                .take(retained)
                .cloned()
                .collect::<Vec<_>>();
            materialize_snapshot(scope, through, base.as_ref(), &changes)
                .expect("memory store must build a bounded test artifact")
        }
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
                .unwrap()
                .entry(scope)
                .or_default()
                .baseline = Some(artifact.clone());
            SealResult::Sealed {
                position: baseline.position(),
            }
        }
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
            let mut retention = self.retention.lock().unwrap();
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
        fn prune_result(
            &self,
            scope: Scope,
            up_to: u64,
            min_live: Option<u64>,
            request: OperationDigest,
            grant: &impl ExecutionGrant,
        ) -> PruneResult {
            let Some(frontier) = self.check_retention_grant(
                scope,
                prune_action(scope, request, up_to, min_live),
                grant,
            ) else {
                return PruneResult::StaleAuthority;
            };
            let state = self.retention_state(scope);
            let through =
                match prune_decision(frontier, state.floor, up_to, min_live, state.consumed) {
                    Ok(through) => through,
                    Err(refusal) => return PruneResult::Refused(refusal),
                };
            if through > state.floor {
                let Some(artifact) = state.baseline else {
                    return PruneResult::Refused(PruneRefusal::MissingBaseline);
                };
                if artifact.baseline.position() < through {
                    return PruneResult::Refused(PruneRefusal::BaselineBehind {
                        position: artifact.baseline.position(),
                    });
                }
                artifact
                    .validate(scope)
                    .expect("memory store retains validated snapshot artifacts");
                let floor = state.floor;
                let mut position = floor;
                self.changes.lock().unwrap().retain(|change| {
                    if change.scope != scope {
                        return true;
                    }
                    position += 1;
                    position > through
                });
                self.retention
                    .lock()
                    .unwrap()
                    .entry(scope)
                    .or_default()
                    .floor = through;
            }
            PruneResult::Pruned(PruneReceipt {
                pruned_through: through,
                floor: through.max(state.floor),
            })
        }
    }

    fn change(scope: Scope, replica: ReplicaId, operation: OperationId) -> ChangeEnvelope {
        let mut document = Document::new();
        let ((), mut changes) = document
            .edit(|editor| editor.put(&Object::root(), "x", Scalar::Integer(1)))
            .unwrap();
        let encoded = changes.pop().unwrap();
        ChangeEnvelope {
            scope,
            document: DocumentId::new(),
            replica,
            operation,
            request: OperationDigest::new([4; 32]),
            digest: encoded.digest,
            bytes: encoded.bytes,
        }
    }

    #[test]
    fn recovered_and_refused_admissions_keep_their_wire_meaning() {
        let operation = OperationId::new();
        let recovered = AcceptResult::RecoveredUnknownCommit;
        let stale = AcceptResult::StaleAuthority;
        let consumed = AcceptResult::InvocationConsumed;
        assert_eq!(
            push_outcome(operation, recovered),
            PushOutcome::Duplicate { operation }
        );
        assert_eq!(
            push_outcome(operation, stale),
            PushOutcome::Rejected {
                operation,
                code: RejectionCode::StaleAuthority
            }
        );
        assert_eq!(
            push_outcome(operation, consumed),
            PushOutcome::Rejected {
                operation,
                code: RejectionCode::InvocationConsumed
            }
        );
    }

    #[test]
    fn exact_retry_is_duplicate_and_forged_checkpoint_fails_closed() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let operation = OperationId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([7; 32], 100).unwrap(),
        );
        let request = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![change(scope, replica, operation)],
        };
        assert!(matches!(
            lgwks_std::task::block_on(service.push(&request, 10))
                .unwrap()
                .outcomes[0],
            PushOutcome::Accepted { .. }
        ));
        assert!(matches!(
            lgwks_std::task::block_on(service.push(&request, 10))
                .unwrap()
                .outcomes[0],
            PushOutcome::Duplicate { .. }
        ));
        let pull = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([9; 32]),
            after: None,
            max_changes: 10,
            max_bytes: 1024,
        };
        let ServicePull::Page(page) = lgwks_std::task::block_on(service.pull(&pull, 10)).unwrap()
        else {
            panic!("page expected")
        };
        let mut forged = page.checkpoint.as_wire().to_vec();
        forged[40] ^= 1;
        let forged = PullRequest {
            after: Some(Checkpoint::from_wire(forged).unwrap()),
            ..pull
        };
        assert!(matches!(
            lgwks_std::task::block_on(service.pull(&forged, 11)),
            Err(ServiceError::Checkpoint(CheckpointError::InvalidMac))
        ));
    }

    #[test]
    fn valid_expired_checkpoint_requests_explicit_full_resync() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([8; 32], 5).unwrap(),
        );
        let pull = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([1; 32]),
            after: None,
            max_changes: 10,
            max_bytes: 1024,
        };
        let ServicePull::Page(page) = lgwks_std::task::block_on(service.pull(&pull, 10)).unwrap()
        else {
            panic!("page expected")
        };
        let expired = PullRequest {
            after: Some(page.checkpoint),
            ..pull
        };
        assert_eq!(
            lgwks_std::task::block_on(service.pull(&expired, 15)).unwrap(),
            ServicePull::CheckpointExpired
        );
    }

    #[test]
    fn push_distinguishes_malformed_change_from_digest_mismatch() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([5; 32], 100).unwrap(),
        );
        let mut malformed = change(scope, replica, OperationId::new());
        malformed.bytes = vec![1];
        let mut mismatched = change(scope, replica, OperationId::new());
        mismatched.digest = ChangeDigest::new([0; 32]);
        let response = lgwks_std::task::block_on(service.push(
            &PushRequest {
                protocol: PROTOCOL_VERSION,
                replica,
                changes: vec![malformed, mismatched],
            },
            10,
        ))
        .unwrap();
        assert!(matches!(
            response.outcomes[0],
            PushOutcome::Rejected {
                code: RejectionCode::InvalidChange,
                ..
            }
        ));
        assert!(matches!(
            response.outcomes[1],
            PushOutcome::Rejected {
                code: RejectionCode::DigestMismatch,
                ..
            }
        ));
    }

    #[test]
    fn pull_store_consumes_exact_grant_and_rejects_stale_frontier() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Stale,
            CheckpointCodec::new([8; 32], 50).unwrap(),
        );
        let pull = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([3; 32]),
            after: None,
            max_changes: 1,
            max_bytes: 1024,
        };
        assert!(matches!(
            lgwks_std::task::block_on(service.pull(&pull, 10)),
            Err(ServiceError::StaleAuthority)
        ));
    }

    #[test]
    fn checkpoint_shape_scope_axes_and_integer_decode_fail_independently() {
        let account = AccountId::new();
        let space = SpaceId::new();
        let scope = Scope::new(account, space);
        let codec = CheckpointCodec::new([13; 32], 100).unwrap();
        let token = codec.issue(scope, 2, 10).unwrap();
        assert_eq!(codec.open(scope, &token, 11), Ok(2));
        assert_eq!(decode_u64(&2_u64.to_be_bytes()), Ok(2));
        assert_eq!(decode_u64(&[0; 7]), Err(CheckpointError::Malformed));

        let mut wrong_magic = token.as_wire().to_vec();
        wrong_magic[..4].copy_from_slice(b"NOPE");
        let wrong_magic = Checkpoint::from_wire(wrong_magic).unwrap();
        assert_eq!(
            codec.open(scope, &wrong_magic, 11),
            Err(CheckpointError::Malformed)
        );
        let short = Checkpoint::from_wire(CHECKPOINT_MAGIC.to_vec()).unwrap();
        assert_eq!(
            codec.open(scope, &short, 11),
            Err(CheckpointError::Malformed)
        );
        assert_eq!(
            codec.open(Scope::new(AccountId::new(), space), &token, 11),
            Err(CheckpointError::WrongScope)
        );
        assert_eq!(
            codec.open(Scope::new(account, SpaceId::new()), &token, 11),
            Err(CheckpointError::WrongScope)
        );
    }

    #[test]
    fn stored_page_shape_cannot_skip_or_lie_about_the_bound_frontier() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let page = StoredPage {
            changes: vec![change(scope, replica, OperationId::new())],
            position: 5,
            more: true,
        };
        assert!(valid_stored_page(4, 6, &page));
        let stalled = StoredPage {
            changes: Vec::new(),
            position: 4,
            more: true,
        };
        assert!(!valid_stored_page(4, 6, &stalled));
        let skipped = StoredPage {
            position: 6,
            ..page.clone()
        };
        assert!(!valid_stored_page(4, 6, &skipped));
        let false_end = StoredPage {
            more: false,
            ..page.clone()
        };
        assert!(!valid_stored_page(4, 6, &false_end));
        let beyond = StoredPage {
            position: 6,
            more: false,
            ..page
        };
        assert!(!valid_stored_page(4, 6, &beyond));
    }

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

    fn envelope_with(
        scope: Scope,
        replica: ReplicaId,
        operation: OperationId,
        request_byte: u8,
    ) -> ChangeEnvelope {
        let mut envelope = change(scope, replica, operation);
        envelope.request = OperationDigest::new([request_byte; 32]);
        envelope
    }

    fn tombstone_envelope(
        scope: Scope,
        replica: ReplicaId,
        operation: OperationId,
        request_byte: u8,
    ) -> ChangeEnvelope {
        let mut document = Document::new();
        document
            .edit(|editor| editor.put(&Object::root(), "k", Scalar::Integer(1)))
            .unwrap();
        let mut staged = document.delete().unwrap();
        let encoded = staged.pop().unwrap();
        assert!(staged.is_empty());
        ChangeEnvelope {
            scope,
            document: DocumentId::new(),
            replica,
            operation,
            request: OperationDigest::new([request_byte; 32]),
            digest: encoded.digest,
            bytes: encoded.bytes,
        }
    }

    fn push_all<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        replica: ReplicaId,
        changes: Vec<ChangeEnvelope>,
    ) -> PushResponse
    where
        Store: ServiceStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        let request = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes,
        };
        lgwks_std::task::block_on(service.push(&request, 10)).unwrap()
    }

    fn pull_after<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        replica: ReplicaId,
        after: Option<Checkpoint>,
        now: u64,
    ) -> ServicePull
    where
        Store: ServiceStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        let request = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([91; 32]),
            after,
            max_changes: 10,
            max_bytes: 1_048_576,
        };
        lgwks_std::task::block_on(service.pull(&request, now)).unwrap()
    }

    fn recover_at<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        replica: ReplicaId,
        after: Option<Checkpoint>,
        now: u64,
    ) -> ServiceBaseline
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        let request = BaselineRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([92; 32]),
            after,
            after_document: None,
        };
        lgwks_std::task::block_on(service.recover(&request, now)).unwrap()
    }

    fn recover_page_at<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        replica: ReplicaId,
        after_document: Option<DocumentId>,
        now: u64,
    ) -> ServiceBaseline
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        let request = BaselineRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([93; 32]),
            after: None,
            after_document,
        };
        lgwks_std::task::block_on(service.recover(&request, now)).unwrap()
    }

    #[test]
    fn a_recovery_grant_for_one_cursor_cannot_authorize_another() {
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
            CheckpointCodec::new([6; 32], 100).unwrap(),
        );
        let other = BaselineRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: intent,
            after: None,
            after_document: Some(cursor),
        };
        assert!(matches!(
            lgwks_std::task::block_on(service.recover(&other, 10)),
            Err(ServiceError::Forbidden)
        ));
        let exact = BaselineRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: intent,
            after: None,
            after_document: None,
        };
        assert!(matches!(
            lgwks_std::task::block_on(service.recover(&exact, 10)),
            Ok(ServiceBaseline::Unavailable)
        ));
    }

    #[test]
    fn recovery_pages_a_baseline_larger_than_one_page() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([6; 32], 100).unwrap(),
        );
        let total = logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS + 44;
        let changes = (0..total)
            .map(|index| {
                envelope_with(
                    scope,
                    replica,
                    OperationId::new(),
                    u8::try_from(index % 256).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        for chunk in changes.chunks(logical_db_model::MAX_CHANGES_PER_PAGE) {
            push_all(&service, replica, chunk.to_vec());
        }
        assert!(matches!(
            seal_at(&service, scope, total as u64, 10),
            SealResult::Sealed { .. }
        ));

        let ServiceBaseline::Artifact(first) = recover_page_at(&service, scope, replica, None, 11)
        else {
            panic!("sealed baseline must be available");
        };
        assert!(first.more);
        assert_eq!(
            first.documents.len(),
            logical_db_model::MAX_BASELINE_PAGE_DOCUMENTS
        );
        let cursor = first
            .next_document
            .expect("continued page carries a cursor");
        let ServiceBaseline::Artifact(second) =
            recover_page_at(&service, scope, replica, Some(cursor), 12)
        else {
            panic!("continued baseline page must be available");
        };
        assert!(!second.more);
        assert_eq!(second.documents.len(), 44);
        assert_eq!(second.descriptor, first.descriptor);
        assert!(second.next_document.is_none());

        let mut accumulator = logical_db_protocol::BaselineAccumulator::new(
            scope,
            first.validate_page(scope).unwrap(),
        );
        assert!(!accumulator.push(&first).unwrap());
        assert!(accumulator.push(&second).unwrap());
        assert_eq!(accumulator.finish().unwrap().documents.len(), total);
    }

    fn seal_at<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        position: u64,
        now: u64,
    ) -> SealResult
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        lgwks_std::task::block_on(service.seal_snapshot(
            scope,
            position,
            OperationDigest::new([61; 32]),
            now,
        ))
        .unwrap()
    }

    fn consume_through<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        through: u64,
        now: u64,
    ) -> ConsumedResult
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        lgwks_std::task::block_on(service.mark_consumed(
            scope,
            through,
            OperationDigest::new([62; 32]),
            now,
        ))
        .unwrap()
    }

    fn collect<Store, Policy>(
        service: &ProtocolService<Store, Policy>,
        scope: Scope,
        up_to: u64,
        min_live: Option<u64>,
        now: u64,
    ) -> PruneResult
    where
        Store: RetentionStore,
        Store::Error: std::fmt::Debug,
        Policy: Authority,
    {
        lgwks_std::task::block_on(service.prune(
            scope,
            up_to,
            min_live,
            OperationDigest::new([63; 32]),
            now,
        ))
        .unwrap()
    }

    #[test]
    fn prune_refuses_a_prefix_still_referenced_by_a_live_checkpoint() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([21; 32], 10_000).unwrap(),
        );
        let changes = (30u8..33)
            .map(|request| envelope_with(scope, replica, OperationId::new(), request))
            .collect::<Vec<_>>();
        let response = push_all(&service, replica, changes);
        assert!(
            response
                .outcomes
                .iter()
                .all(|outcome| matches!(outcome, PushOutcome::Accepted { .. }))
        );
        assert_eq!(
            consume_through(&service, scope, 3, 11),
            ConsumedResult::Advanced {
                consumed_through: 3
            }
        );
        assert_eq!(
            collect(&service, scope, 3, Some(1), 12),
            PruneResult::Refused(PruneRefusal::LiveCheckpoint { min_live: 1 })
        );
        assert_eq!(
            collect(&service, scope, 0, None, 13),
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 0,
                floor: 0
            })
        );
        let ServicePull::Page(page) = pull_after(&service, scope, replica, None, 14) else {
            panic!("page expected")
        };
        assert_eq!(page.changes.len(), 3);
    }

    #[test]
    fn prune_refuses_a_prefix_with_unconsumed_operation_records() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([22; 32], 10_000).unwrap(),
        );
        let changes = (40u8..43)
            .map(|request| envelope_with(scope, replica, OperationId::new(), request))
            .collect::<Vec<_>>();
        push_all(&service, replica, changes);
        assert_eq!(
            collect(&service, scope, 2, None, 11),
            PruneResult::Refused(PruneRefusal::PendingOperations {
                consumed_through: 0
            })
        );
        assert_eq!(
            consume_through(&service, scope, 2, 12),
            ConsumedResult::Advanced {
                consumed_through: 2
            }
        );
        assert_eq!(
            collect(&service, scope, 2, None, 13),
            PruneResult::Refused(PruneRefusal::MissingBaseline)
        );
        assert_eq!(
            seal_at(&service, scope, 2, 14),
            SealResult::Sealed { position: 2 }
        );
        assert_eq!(
            collect(&service, scope, 2, None, 15),
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 2,
                floor: 2
            })
        );
        assert_eq!(
            collect(&service, scope, 3, None, 16),
            PruneResult::Refused(PruneRefusal::PendingOperations {
                consumed_through: 2
            })
        );
    }

    #[test]
    fn prune_refuses_a_prefix_beyond_the_state_bearing_baseline() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([29; 32], 10_000).unwrap(),
        );
        let changes = (70u8..73)
            .map(|request| envelope_with(scope, replica, OperationId::new(), request))
            .collect::<Vec<_>>();
        push_all(&service, replica, changes);
        assert_eq!(
            seal_at(&service, scope, 2, 11),
            SealResult::Sealed { position: 2 }
        );
        assert_eq!(
            consume_through(&service, scope, 3, 12),
            ConsumedResult::Advanced {
                consumed_through: 3
            }
        );
        assert_eq!(
            collect(&service, scope, 3, None, 13),
            PruneResult::Refused(PruneRefusal::BaselineBehind { position: 2 })
        );
        assert_eq!(
            collect(&service, scope, 2, None, 14),
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 2,
                floor: 2
            })
        );
    }

    #[test]
    fn behind_floor_checkpoint_requires_baseline_then_suffix_completes() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([23; 32], 10_000).unwrap(),
        );
        let first = envelope_with(scope, replica, OperationId::new(), 50);
        push_all(&service, replica, vec![first]);
        let ServicePull::Page(first_page) = pull_after(&service, scope, replica, None, 11) else {
            panic!("page expected")
        };
        assert_eq!(first_page.changes.len(), 1);
        let stale_checkpoint = first_page.checkpoint.clone();
        let second = envelope_with(scope, replica, OperationId::new(), 51);
        let tombstone = tombstone_envelope(scope, replica, OperationId::new(), 52);
        let tombstone_bytes = tombstone.bytes.clone();
        let fourth = envelope_with(scope, replica, OperationId::new(), 53);
        push_all(&service, replica, vec![second, tombstone, fourth]);
        assert_eq!(
            seal_at(&service, scope, 2, 12),
            SealResult::Sealed { position: 2 }
        );
        assert_eq!(
            consume_through(&service, scope, 4, 13),
            ConsumedResult::Advanced {
                consumed_through: 4
            }
        );
        assert_eq!(
            collect(&service, scope, 2, None, 14),
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 2,
                floor: 2
            })
        );
        assert_eq!(
            pull_after(&service, scope, replica, Some(stale_checkpoint.clone()), 15),
            ServicePull::BaselineRequired { floor: 2 }
        );
        assert_eq!(
            pull_after(&service, scope, replica, None, 15),
            ServicePull::BaselineRequired { floor: 2 }
        );
        let ServiceBaseline::Artifact(baseline) =
            recover_at(&service, scope, replica, Some(stale_checkpoint), 16)
        else {
            panic!("state-bearing baseline expected")
        };
        let artifact = baseline.validate(scope).unwrap();
        assert_eq!(artifact.baseline.position(), 2);
        assert_eq!(artifact.documents.len(), 2);
        let ServicePull::Page(recovered) = pull_after(
            &service,
            scope,
            replica,
            Some(baseline.checkpoint.clone()),
            17,
        ) else {
            panic!("recovery suffix page expected")
        };
        assert_eq!(recovered.changes.len(), 2);
        assert_eq!(recovered.changes[0].bytes, tombstone_bytes);
        assert!(!recovered.more);
    }

    #[test]
    fn chunk_manifest_and_invocation_references_stay_consistent_after_collection() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let service = ProtocolService::new(
            Memory::default(),
            Allow,
            CheckpointCodec::new([24; 32], 10_000).unwrap(),
        );
        let operations = [OperationId::new(), OperationId::new(), OperationId::new()];
        let changes = operations
            .iter()
            .zip(60u8..63)
            .map(|(operation, request)| envelope_with(scope, replica, *operation, request))
            .collect::<Vec<_>>();
        push_all(&service, replica, changes.clone());
        assert_eq!(
            seal_at(&service, scope, 2, 11),
            SealResult::Sealed { position: 2 }
        );
        assert_eq!(
            consume_through(&service, scope, 3, 12),
            ConsumedResult::Advanced {
                consumed_through: 3
            }
        );
        assert_eq!(
            collect(&service, scope, 2, None, 13),
            PruneResult::Pruned(PruneReceipt {
                pruned_through: 2,
                floor: 2
            })
        );
        let duplicate = push_all(&service, replica, vec![changes[0].clone()]);
        assert!(matches!(
            duplicate.outcomes[0],
            PushOutcome::Duplicate { .. }
        ));
        let mut conflicting = changes[1].clone();
        conflicting.request = OperationDigest::new([99; 32]);
        let conflict = push_all(&service, replica, vec![conflicting]);
        assert!(matches!(
            conflict.outcomes[0],
            PushOutcome::Rejected {
                code: RejectionCode::OperationConflict,
                ..
            }
        ));
        let ServiceBaseline::Artifact(baseline) = recover_at(&service, scope, replica, None, 13)
        else {
            panic!("state-bearing baseline expected")
        };
        let ServicePull::Page(page) = pull_after(
            &service,
            scope,
            replica,
            Some(baseline.checkpoint.clone()),
            13,
        ) else {
            panic!("page expected")
        };
        assert_eq!(page.changes.len(), 1);
        assert_eq!(page.changes[0].operation, operations[2]);
        assert!(!page.more);
    }

    #[test]
    fn retention_floor_and_baseline_survive_service_restart() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let store = std::sync::Arc::new(Memory::default());
        let changes = (70u8..73)
            .map(|request| envelope_with(scope, replica, OperationId::new(), request))
            .collect::<Vec<_>>();
        let stale_checkpoint = {
            let service = ProtocolService::new(
                store.clone(),
                Allow,
                CheckpointCodec::new([25; 32], 10_000).unwrap(),
            );
            push_all(&service, replica, vec![changes[0].clone()]);
            let ServicePull::Page(first) = pull_after(&service, scope, replica, None, 11) else {
                panic!("page expected")
            };
            push_all(&service, replica, changes[1..].to_vec());
            assert_eq!(
                seal_at(&service, scope, 2, 12),
                SealResult::Sealed { position: 2 }
            );
            assert_eq!(
                consume_through(&service, scope, 3, 13),
                ConsumedResult::Advanced {
                    consumed_through: 3
                }
            );
            assert_eq!(
                collect(&service, scope, 2, None, 14),
                PruneResult::Pruned(PruneReceipt {
                    pruned_through: 2,
                    floor: 2
                })
            );
            first.checkpoint
        };
        let service = ProtocolService::new(
            store.clone(),
            Allow,
            CheckpointCodec::new([25; 32], 10_000).unwrap(),
        );
        assert_eq!(
            lgwks_std::task::block_on(store.retention_floor(scope)).unwrap(),
            2
        );
        assert!(matches!(
            lgwks_std::task::block_on(store.snapshot_baseline(scope)).unwrap(),
            Some(SnapshotRecord::State(ref baseline)) if baseline.position() == 2
        ));
        assert_eq!(
            pull_after(&service, scope, replica, Some(stale_checkpoint.clone()), 15),
            ServicePull::BaselineRequired { floor: 2 }
        );
        let ServiceBaseline::Artifact(baseline) =
            recover_at(&service, scope, replica, Some(stale_checkpoint), 16)
        else {
            panic!("state-bearing baseline must survive restart")
        };
        let artifact = baseline.validate(scope).unwrap();
        assert_eq!(artifact.baseline.position(), 2);
        assert_eq!(artifact.documents.len(), 2);
    }

    #[test]
    fn prune_decision_rejects_every_uncollectible_edge() {
        assert_eq!(prune_decision(10, 2, 2, None, 5), Ok(2));
        assert_eq!(prune_decision(10, 2, 1, None, 5), Ok(2));
        assert_eq!(prune_decision(10, 2, 5, None, 5), Ok(5));
        assert_eq!(prune_decision(10, 2, 5, Some(5), 9), Ok(5));
        assert_eq!(
            prune_decision(3, 0, 4, None, 4),
            Err(PruneRefusal::BeyondFrontier { frontier: 3 })
        );
        assert_eq!(
            prune_decision(10, 0, 5, Some(4), 9),
            Err(PruneRefusal::LiveCheckpoint { min_live: 4 })
        );
        assert_eq!(
            prune_decision(10, 0, 5, None, 4),
            Err(PruneRefusal::PendingOperations {
                consumed_through: 4
            })
        );
    }

    #[test]
    fn retention_writes_require_fresh_authority() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let stale = ProtocolService::new(
            Memory::default(),
            Stale,
            CheckpointCodec::new([26; 32], 10_000).unwrap(),
        );
        assert_eq!(
            lgwks_std::task::block_on(stale.seal_snapshot(
                scope,
                0,
                OperationDigest::new([61; 32]),
                10,
            ))
            .unwrap(),
            SealResult::StaleAuthority
        );
        assert_eq!(
            lgwks_std::task::block_on(stale.mark_consumed(
                scope,
                0,
                OperationDigest::new([62; 32]),
                10,
            ))
            .unwrap(),
            ConsumedResult::StaleAuthority
        );
        assert_eq!(
            lgwks_std::task::block_on(stale.prune(
                scope,
                0,
                None,
                OperationDigest::new([63; 32]),
                10,
            ))
            .unwrap(),
            PruneResult::StaleAuthority
        );
        let denied = ProtocolService::new(
            Memory::default(),
            Deny,
            CheckpointCodec::new([27; 32], 10_000).unwrap(),
        );
        assert!(matches!(
            lgwks_std::task::block_on(denied.prune(
                scope,
                0,
                None,
                OperationDigest::new([63; 32]),
                10,
            )),
            Err(ServiceError::Forbidden)
        ));
    }
}
