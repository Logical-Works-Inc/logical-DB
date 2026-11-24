//! Shared in-memory service composition for black-box loopback journeys.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! This is the reviewed journey composition used by the in-process e2e tests,
//! the live-FDB journeys, and the loopback host binary that an *external*
//! consumer syncs against. It exists once so the store, authority,
//! authentication, and clock the offline tests exercise are byte-for-byte the
//! ones the external tape exercises. It is a test composition: it never ships
//! in a product binary and makes no durability claim.

use std::{
    collections::HashMap,
    fmt,
    future::{Future, ready},
    io,
    net::{Ipv4Addr, SocketAddr},
    num::TryFromIntError,
    sync::{Arc, Mutex, PoisonError},
};

use lgwks_std::json::serde::de::DeserializeOwned;
use logical_db_model::{
    ActionDigest, ChangeDigest, Effect, MAX_BASELINE_PAGE_DOCUMENTS, OperationDigest, OperationId,
    Scope, SnapshotArtifact, SnapshotBaseline, SnapshotRecord,
};
use logical_db_protocol::{
    ChangeEnvelope, PageAction, RecoveryAction, mark_consumed_action, prune_action,
    seal_snapshot_action, write_action,
};
use logical_db_server::{AuthenticationError, Authenticator, Clock, router};
use logical_db_service::{
    AcceptResult, Authority, BaselinePageResult, BaselinePageSlice, CheckpointCodec,
    ConsumedResult, ExecutionGrant, PageResult, PruneReceipt, PruneRefusal, PruneResult,
    RetentionStore, SealResult, ServiceStore, SnapshotMaterializationError, StoredPage,
    materialize_snapshot, prune_decision,
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
    /// Single-use invocation identity; [`Grant::new`] derives it from `action`.
    pub invocation: [u8; 32],
}

impl Grant {
    /// Mints a grant for exactly these facts, with the invocation identity
    /// derived from the action.
    #[must_use]
    pub const fn new(scope: Scope, effect: Effect, action: ActionDigest, frontier: u64) -> Self {
        Self {
            scope,
            effect,
            action,
            frontier,
            invocation: action.into_bytes(),
        }
    }
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
        self.invocation
    }
}

/// The token the loopback journey host and the black-box journeys share.
pub const BLACK_BOX_TOKEN: &str = "black-box";

/// Authority that mints a grant for each justified request its predicate admits.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct Allow<Admit> {
    /// Decides, from scope, effect and action, whether a request is admitted.
    admit: Admit,
}

impl<Admit> Allow<Admit> {
    /// Admits exactly the requests `admit` accepts.
    #[must_use]
    pub const fn when(admit: Admit) -> Self {
        Self { admit }
    }
}

impl<Admit: Fn(Scope, Effect, ActionDigest) -> bool + Send + Sync> Authority for Allow<Admit> {
    type Grant = Grant;

    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        _at_unix_ms: u64,
    ) -> Option<Self::Grant> {
        (self.admit)(scope, effect, action).then(|| Grant::new(scope, effect, action, frontier))
    }
}

/// Bearer authentication: one token, one authority policy for every caller.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct TestAuthentication<P> {
    /// The accepted bearer token, without the `Bearer ` prefix.
    token: &'static str,
    /// The policy every authenticated caller receives.
    policy: P,
}

impl<P> TestAuthentication<P> {
    /// Authenticates `Bearer <token>` callers as `policy`.
    #[must_use]
    pub const fn new(token: &'static str, policy: P) -> Self {
        Self { token, policy }
    }
}

impl<P: Authority + Clone + Send + Sync> Authenticator for TestAuthentication<P> {
    type Policy = P;

    fn authenticate(
        &self,
        authorization: &axum::http::HeaderValue,
        _at_unix_ms: u64,
    ) -> Result<Self::Policy, AuthenticationError> {
        if authorization.as_bytes().strip_prefix(b"Bearer ") == Some(self.token.as_bytes()) {
            Ok(self.policy.clone())
        } else {
            Err(AuthenticationError::Unauthorized)
        }
    }
}

/// Clock pinned to one instant so checkpoint expiry is deterministic.
#[derive(Debug)]
#[non_exhaustive]
pub struct FixedClock;

impl Clock for FixedClock {
    fn now_unix_ms(&self) -> Option<u64> {
        Some(10_000)
    }
}

/// Retention bookkeeping for one scope.
#[derive(Clone, Debug, Default)]
struct RetentionState {
    /// Retention floor: the first position that is still addressable.
    floor: u64,
    /// Highest position marked consumed.
    consumed: u64,
    /// Sealed state-bearing baseline, when one exists.
    baseline: Option<SnapshotArtifact>,
}

/// Why the in-memory journey store refused to answer.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum MemoryStoreError {
    /// A writer panicked while holding the store lock.
    Poisoned,
    /// A count or position did not fit its integer type.
    CountOverflow(TryFromIntError),
    /// The retained log could not be materialized into a snapshot.
    Snapshot(SnapshotMaterializationError),
    /// A baseline read named a descriptor other than the sealed one.
    BaselineMismatch,
}

impl fmt::Display for MemoryStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Poisoned => formatter.write_str("journey store lock poisoned"),
            Self::CountOverflow(ref error) => {
                write!(formatter, "journey store count overflow: {error}")
            }
            Self::Snapshot(ref error) => {
                write!(formatter, "snapshot materialization failed: {error:?}")
            }
            Self::BaselineMismatch => {
                formatter.write_str("baseline descriptor disagrees with the sealed artifact")
            }
        }
    }
}

impl std::error::Error for MemoryStoreError {}

impl<Guard> From<PoisonError<Guard>> for MemoryStoreError {
    fn from(_poison: PoisonError<Guard>) -> Self {
        Self::Poisoned
    }
}

impl From<TryFromIntError> for MemoryStoreError {
    fn from(error: TryFromIntError) -> Self {
        Self::CountOverflow(error)
    }
}

/// Every scope's log and retention state, behind one lock.
#[derive(Debug, Default)]
struct Ledger {
    /// Durable change log for every scope served by this store.
    changes: Vec<ChangeEnvelope>,
    /// Idempotency map binding each operation to its request and change.
    operations: HashMap<OperationId, (OperationDigest, ChangeDigest)>,
    /// Per-scope retention floor, consumed position, and sealed baseline.
    retention: HashMap<Scope, RetentionState>,
}

impl Ledger {
    /// One scope's retention state; a scope never written starts fresh.
    fn retention(&self, scope: Scope) -> RetentionState {
        match self.retention.get(&scope) {
            Some(state) => state.clone(),
            None => RetentionState::default(),
        }
    }

    /// One scope's retention floor; a scope never pruned starts at zero.
    fn floor(&self, scope: Scope) -> u64 {
        match self.retention.get(&scope) {
            Some(state) => state.floor,
            None => 0,
        }
    }

    /// The retained changes of one scope, in log order.
    fn retained(&self, scope: Scope) -> impl Iterator<Item = &ChangeEnvelope> {
        self.changes
            .iter()
            .filter(move |change| change.scope() == scope)
    }

    /// The durable frontier of one scope: floor plus retained changes.
    fn frontier(&self, scope: Scope) -> Result<u64, MemoryStoreError> {
        let retained = u64::try_from(self.retained(scope).count())?;
        Ok(self.floor(scope).saturating_add(retained))
    }

    /// The frontier when `grant` is a current sync grant for `action`.
    fn check_retention_grant(
        &self,
        scope: Scope,
        action: ActionDigest,
        grant: &impl ExecutionGrant,
    ) -> Result<Option<u64>, MemoryStoreError> {
        let frontier = self.frontier(scope)?;
        Ok((grant.scope() == scope
            && grant.effect() == Effect::Sync
            && grant.action() == action
            && grant.frontier() == frontier)
            .then_some(frontier))
    }
}

/// In-memory service store implementing the full retention contract.
#[derive(Debug, Default)]
#[non_exhaustive]
pub(crate) struct MemoryServiceStore {
    /// The one lock over every scope's state.
    ledger: Mutex<Ledger>,
}

impl MemoryServiceStore {
    /// Runs `operation` under the store lock.
    fn with_ledger<T>(
        &self,
        operation: impl FnOnce(&mut Ledger) -> Result<T, MemoryStoreError>,
    ) -> Result<T, MemoryStoreError> {
        let mut ledger = self.ledger.lock()?;
        operation(&mut ledger)
    }
}

impl ServiceStore for MemoryServiceStore {
    type Error = MemoryStoreError;

    fn accept(
        &self,
        change: &ChangeEnvelope,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        ready(self.with_ledger(|ledger| {
            if grant.scope() != change.scope()
                || grant.effect() != Effect::Write
                || grant.action() != write_action(change)
            {
                return Ok(AcceptResult::StaleAuthority);
            }
            Ok(match ledger.operations.get(&change.operation()) {
                Some(existing) if *existing == (change.request(), change.digest()) => {
                    AcceptResult::Duplicate
                }
                Some(_) => AcceptResult::Conflict,
                None => {
                    ledger
                        .operations
                        .insert(change.operation(), (change.request(), change.digest()));
                    ledger.changes.push(change.clone());
                    AcceptResult::Accepted
                }
            })
        }))
    }

    fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(self.with_ledger(|ledger| ledger.frontier(scope)))
    }

    fn retention_floor(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(self.with_ledger(|ledger| Ok(ledger.floor(scope))))
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
        ready(self.with_ledger(|ledger| {
            let floor = ledger.floor(scope);
            if after < floor {
                return Ok(PageResult::BehindFloor);
            }
            let frontier = ledger.frontier(scope)?;
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.action() != action.digest()
                || grant.frontier() != frontier
            {
                return Ok(PageResult::StaleAuthority);
            }
            let budget = usize::try_from(max_bytes)?;
            let mut bytes = 0_usize;
            let page = ledger
                .retained(scope)
                .skip(usize::try_from(after.saturating_sub(floor))?)
                .take(usize::from(max_changes))
                .take_while(|change| {
                    bytes = bytes.saturating_add(change.bytes().len());
                    bytes <= budget
                })
                .cloned()
                .collect::<Vec<_>>();
            let position = after.saturating_add(u64::try_from(page.len())?);
            Ok(PageResult::Page(StoredPage::new(
                page,
                position,
                frontier > position,
            )))
        }))
    }
}

impl RetentionStore for MemoryServiceStore {
    fn consumed_through(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(self.with_ledger(|ledger| Ok(ledger.retention(scope).consumed)))
    }

    fn snapshot_baseline(
        &self,
        scope: Scope,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, Self::Error>> + Send {
        ready(self.with_ledger(|ledger| {
            Ok(ledger
                .retention(scope)
                .baseline
                .map(|artifact| SnapshotRecord::State(artifact.baseline().clone())))
        }))
    }

    fn build_snapshot(
        &self,
        scope: Scope,
        through: u64,
    ) -> impl Future<Output = Result<SnapshotArtifact, Self::Error>> + Send {
        ready(self.with_ledger(|ledger| {
            let state = ledger.retention(scope);
            let base = state
                .baseline
                .filter(|artifact| artifact.baseline().position() <= through);
            let base_position = match base.as_ref() {
                Some(artifact) => artifact.baseline().position(),
                None => state.floor,
            };
            let retained_changes = ledger
                .retained(scope)
                .skip(usize::try_from(base_position.saturating_sub(state.floor))?)
                .take(usize::try_from(through.saturating_sub(base_position))?)
                .cloned()
                .collect::<Vec<_>>();
            materialize_snapshot(scope, through, base.as_ref(), &retained_changes)
                .map_err(MemoryStoreError::Snapshot)
        }))
    }

    fn read_baseline_page(
        &self,
        action: &RecoveryAction,
        descriptor: &SnapshotBaseline,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<BaselinePageResult, Self::Error>> + Send {
        ready(self.with_ledger(|ledger| {
            if ledger
                .check_retention_grant(action.scope, action.digest(), grant)?
                .is_none()
            {
                return Ok(BaselinePageResult::StaleAuthority);
            }
            let Some(artifact) = ledger.retention(action.scope).baseline else {
                return Ok(BaselinePageResult::Page(BaselinePageSlice::new(
                    Vec::new(),
                    false,
                )));
            };
            if artifact.baseline() != descriptor {
                return Err(MemoryStoreError::BaselineMismatch);
            }
            let mut documents = artifact
                .into_documents()
                .into_iter()
                .filter(|document| {
                    action
                        .after_document
                        .is_none_or(|cursor| document.document() > cursor)
                })
                .take(MAX_BASELINE_PAGE_DOCUMENTS.saturating_add(1))
                .collect::<Vec<_>>();
            let more = documents.len() > MAX_BASELINE_PAGE_DOCUMENTS;
            documents.truncate(MAX_BASELINE_PAGE_DOCUMENTS);
            Ok(BaselinePageResult::Page(BaselinePageSlice::new(
                documents, more,
            )))
        }))
    }

    fn seal_snapshot(
        &self,
        scope: Scope,
        artifact: &SnapshotArtifact,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<SealResult, Self::Error>> + Send {
        let baseline = artifact.baseline();
        ready(self.with_ledger(|ledger| {
            let action = seal_snapshot_action(scope, request, baseline);
            let Some(frontier) = ledger.check_retention_grant(scope, action, grant)? else {
                return Ok(SealResult::StaleAuthority);
            };
            if baseline.position() > frontier {
                return Ok(SealResult::BeyondFrontier { frontier });
            }
            ledger.retention.entry(scope).or_default().baseline = Some(artifact.clone());
            Ok(SealResult::Sealed {
                position: baseline.position(),
            })
        }))
    }

    fn mark_consumed(
        &self,
        scope: Scope,
        through: u64,
        request: OperationDigest,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<ConsumedResult, Self::Error>> + Send {
        ready(self.with_ledger(|ledger| {
            let action = mark_consumed_action(scope, request, through);
            let Some(frontier) = ledger.check_retention_grant(scope, action, grant)? else {
                return Ok(ConsumedResult::StaleAuthority);
            };
            if through > frontier {
                return Ok(ConsumedResult::BeyondFrontier { frontier });
            }
            let state = ledger.retention.entry(scope).or_default();
            if through <= state.consumed {
                return Ok(ConsumedResult::Unchanged {
                    consumed_through: state.consumed,
                });
            }
            state.consumed = through;
            Ok(ConsumedResult::Advanced {
                consumed_through: through,
            })
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
        ready(self.with_ledger(|ledger| {
            let action = prune_action(scope, request, up_to, min_live);
            let Some(frontier) = ledger.check_retention_grant(scope, action, grant)? else {
                return Ok(PruneResult::StaleAuthority);
            };
            let state = ledger.retention(scope);
            let through =
                match prune_decision(frontier, state.floor, up_to, min_live, state.consumed) {
                    Ok(through) => through,
                    Err(refusal) => return Ok(PruneResult::Refused(refusal)),
                };
            if through > state.floor {
                let Some(artifact) = state.baseline.as_ref() else {
                    return Ok(PruneResult::Refused(PruneRefusal::MissingBaseline));
                };
                if artifact.baseline().position() < through {
                    return Ok(PruneResult::Refused(PruneRefusal::BaselineBehind {
                        position: artifact.baseline().position(),
                    }));
                }
                let mut position = state.floor;
                ledger.changes.retain(|change| {
                    if change.scope() != scope {
                        return true;
                    }
                    position = position.saturating_add(1);
                    position > through
                });
                ledger.retention.entry(scope).or_default().floor = through;
            }
            Ok(PruneResult::Pruned(PruneReceipt::new(
                through,
                through.max(state.floor),
            )))
        }))
    }
}

/// Why a client-side journey binary failed, one variant per layer it crosses.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientJourneyError {
    /// Configuration, runtime, temp-file, seam or report I/O failed.
    Io(io::Error),
    /// The bearer credential was refused.
    Credential(logical_db::http::CredentialError),
    /// The HTTP transport refused its endpoint.
    Http(logical_db::http::HttpError),
    /// A replica refused an open, edit or read.
    Db(logical_db::DatabaseError),
    /// A synchronization round failed.
    Sync(Box<logical_db::SyncError<logical_db::DatabaseError, logical_db::http::HttpError>>),
}

impl fmt::Display for ClientJourneyError {
    /// Names the failing layer and its cause for the journey log.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Io(ref error) => write!(formatter, "journey I/O failed: {error}"),
            Self::Credential(ref error) => write!(formatter, "journey credential refused: {error}"),
            Self::Http(ref error) => write!(formatter, "journey transport refused: {error}"),
            Self::Db(ref error) => write!(formatter, "journey replica failed: {error}"),
            Self::Sync(ref error) => write!(formatter, "journey synchronization failed: {error}"),
        }
    }
}

impl std::error::Error for ClientJourneyError {}

/// Implements `From` for each `ClientJourneyError` source, keeping its variant.
macro_rules! client_journey_error_from {
    ($($source:ty => $variant:ident),+ $(,)?) => {
        $(impl From<$source> for ClientJourneyError {
            /// Adapts the source failure without discarding it.
            fn from(error: $source) -> Self {
                Self::$variant(error)
            }
        })+
    };
}

client_journey_error_from! {
    io::Error => Io,
    logical_db::http::CredentialError => Credential,
    logical_db::http::HttpError => Http,
    logical_db::DatabaseError => Db,
}

impl From<logical_db::SyncError<logical_db::DatabaseError, logical_db::http::HttpError>>
    for ClientJourneyError
{
    /// Adapts a synchronization failure, boxed so the error stays small on the stack.
    fn from(
        error: logical_db::SyncError<logical_db::DatabaseError, logical_db::http::HttpError>,
    ) -> Self {
        Self::Sync(Box::new(error))
    }
}

/// Parses one identifier from its canonical hyphenated UUID text through the
/// identifier type's own serde contract, so no journey needs a UUID crate.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] naming `name` when `text` is not a
/// canonical identifier of type `T`.
pub fn parse_identifier<T: DeserializeOwned>(text: &str, name: &str) -> Result<T, io::Error> {
    let invalid = |error: lgwks_std::json::Error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} is not a canonical identifier: {error}"),
        )
    };
    let quoted = lgwks_std::json::to_string(text).map_err(invalid)?;
    lgwks_std::json::from_str(&quoted).map_err(invalid)
}

/// Binds a loopback listener on an ephemeral port and serves the journey
/// composition over a fresh in-memory store behind `authentication`, returning
/// the address and the set that owns the server task.
///
/// # Errors
///
/// Returns the bind failure when no loopback port is available, or the
/// checkpoint-codec failure when its fixed key is refused.
pub async fn serve_journey<Policy>(
    authentication: TestAuthentication<Policy>,
) -> Result<(SocketAddr, lgwks_bot::rt::task::JoinSet<io::Result<()>>), io::Error>
where
    Policy: Authority + Clone + Send + Sync + 'static,
{
    let app = router(
        Arc::new(MemoryServiceStore::default()),
        authentication,
        CheckpointCodec::new([23; 32], 60_000)
            .map_err(|error| io::Error::other(error.to_string()))?,
        FixedClock,
    );
    // An ephemeral loopback port: only this process's journeys connect to it.
    let listener =
        lgwks_bot::rt::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
    let address = listener.local_addr()?;
    let mut server = lgwks_bot::rt::task::JoinSet::new();
    server.spawn(async move { axum::serve(listener, app).await });
    Ok((address, server))
}
