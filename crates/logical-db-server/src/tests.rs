//! Test doubles and router fixtures shared by the server flow tests.

mod flows;

use std::{
    collections::HashMap,
    future::{Future, pending, ready},
    sync::Mutex,
};

use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use lgwks_bot::rt::sync::Notify;
use logical_db_crdt::{Document, Object, Scalar};
use logical_db_model::{
    AccountId, ChangeDigest, DocumentId, Effect, ModelError, OperationDigest, OperationId,
    ReplicaId, Scope, SnapshotArtifact, SnapshotBaseline, SnapshotRecord, SpaceId,
};
use logical_db_protocol::{
    ChangeEnvelope, PROTOCOL_VERSION, PullPage, PushOutcome, PushResponse, RejectionCode,
};
use logical_db_service::{
    AcceptResult, BaselinePageResult, BaselinePageSlice, ConsumedResult, ExecutionGrant,
    PageResult, PruneRefusal, PruneResult, SealResult, StoredPage,
};
use tower::ServiceExt;

use super::*;

use logical_db_model::ActionDigest;
use logical_db_protocol::{PageAction, RecoveryAction, write_action};

/// Allow
struct Allow;
#[derive(Clone, Copy)]
/// ExactAction
struct ExactAction {
    scope: Scope,
    /// Action
    action: ActionDigest,
}

/// Implements `Authority` for a test double from one admission predicate over
/// the double, the requested scope, effect and action, and the clock.
macro_rules! admit_when {
    ($double:ty, |$this:ident, $asked:ident, $mode:ident, $digest:ident, $now:ident| $admit:expr) => {
        impl Authority for $double {
            /// Grant
            type Grant = Grant;

            /// Authorize
            fn authorize(
                &self,
                $asked: Scope,
                $mode: Effect,
                $digest: ActionDigest,
                observed: u64,
                $now: u64,
            ) -> Option<Grant> {
                let $this = self;
                if !$admit {
                    return None;
                }
                Some(Grant::bind($asked, $mode, $digest, observed))
            }
        }
    };
}

admit_when!(ExactAction, |this, asked, mode, digest, _now| {
    asked == this.scope && mode == Effect::Write && digest == this.action
});

impl Authenticator for ExactAction {
    /// Policy
    type Policy = Self;

    /// Authenticate
    fn authenticate(
        &self,
        authorization: &HeaderValue,
        _now: u64,
    ) -> Result<Self, AuthenticationError> {
        if authorization.as_bytes() == b"Bearer integration-test" {
            Ok(*self)
        } else {
            Err(AuthenticationError::Unauthorized)
        }
    }
}
/// The proven scope, strongest effect, bound digest and justified frontier of
/// one captured invocation.
struct Grant {
    bound: Scope,
    strength: Effect,
    digest: ActionDigest,
    observed: u64,
}

impl Grant {
    /// Captures one invocation exactly as the authority justified it.
    const fn bind(bound: Scope, strength: Effect, digest: ActionDigest, observed: u64) -> Self {
        Self {
            bound,
            strength,
            digest,
            observed,
        }
    }
}

impl ExecutionGrant for Grant {
    /// Invocation id
    fn invocation_id(&self) -> [u8; 32] {
        self.digest.into_bytes()
    }
    /// Action
    fn action(&self) -> ActionDigest {
        self.digest
    }
    /// Frontier
    fn frontier(&self) -> u64 {
        self.observed
    }
    /// Effect
    fn effect(&self) -> Effect {
        self.strength
    }
    /// Scope
    fn scope(&self) -> Scope {
        self.bound
    }
}

admit_when!(Allow, |_this, _asked, _mode, _digest, _now| true);

/// Bearer
struct Bearer;

impl Authenticator for Bearer {
    /// Policy
    type Policy = Allow;

    /// Authenticate
    fn authenticate(
        &self,
        authorization: &HeaderValue,
        _now_unix_ms: u64,
    ) -> Result<Self::Policy, AuthenticationError> {
        if authorization.as_bytes() == b"Bearer integration-test" {
            Ok(Allow)
        } else if !authorization.as_bytes().starts_with(b"Bearer ") {
            Err(AuthenticationError::Malformed {
                context: "bearer scheme",
            })
        } else {
            Err(AuthenticationError::Unauthorized)
        }
    }
}

/// Test double with the default composition's exact posture: one bound
/// scope, a short expiry, and read/scan effects only. Write and sync
/// authorization always fails, mirroring `LocalScopeAuthority`.
struct DefaultComposition {
    scope: Scope,
    /// Expires at unix ms
    expires_at_unix_ms: u64,
}

admit_when!(
    DefaultComposition,
    |this, wanted, kind, _digest, now_unix_ms| {
        wanted == this.scope
            && matches!(kind, Effect::Read | Effect::Scan)
            && now_unix_ms < this.expires_at_unix_ms
    }
);

/// DefaultAuthentication
struct DefaultAuthentication {
    scope: Scope,
}

impl Authenticator for DefaultAuthentication {
    /// Policy
    type Policy = DefaultComposition;

    /// Authenticate
    fn authenticate(
        &self,
        authorization: &HeaderValue,
        now_unix_ms: u64,
    ) -> Result<Self::Policy, AuthenticationError> {
        if authorization.as_bytes() != b"Bearer default-composition" {
            return Err(AuthenticationError::Unauthorized);
        }
        let expires_at = now_unix_ms
            .checked_add(60_000)
            .ok_or(AuthenticationError::Forbidden)?;
        Ok(DefaultComposition {
            scope: self.scope,
            expires_at_unix_ms: expires_at,
        })
    }
}

/// Store double reporting every invocation already consumed, as the
/// durable adapter does after a grant's single use.
struct ConsumedInvocations;

impl ServiceStore for ConsumedInvocations {
    /// Error
    type Error = ModelError;

    /// Accept
    fn accept(
        &self,
        _change: &ChangeEnvelope,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        ready(Ok(AcceptResult::InvocationConsumed))
    }

    /// Frontier
    fn frontier(&self, _scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(Ok(0))
    }

    /// Page
    fn page(
        &self,
        _action: PageAction,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
        ready(Ok(PageResult::StaleAuthority))
    }
}

/// FixedClock
struct FixedClock;

impl Clock for FixedClock {
    /// Now unix ms
    fn now_unix_ms(&self) -> Option<u64> {
        Some(1_000)
    }
}

/// ClassifiedStoreError
type ClassifiedStoreError = std::io::ErrorKind;

/// ClassifiedStore
struct ClassifiedStore;

impl ServiceStore for ClassifiedStore {
    /// Error
    type Error = ClassifiedStoreError;

    /// Classify error
    fn classify_error(error: &Self::Error) -> StoreFailureClass {
        match error {
            &ClassifiedStoreError::WouldBlock => StoreFailureClass::Transient,
            _ => StoreFailureClass::Permanent,
        }
    }

    /// Accept
    fn accept(
        &self,
        _change: &ChangeEnvelope,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        ready(Err(ClassifiedStoreError::InvalidData))
    }

    /// Frontier
    fn frontier(&self, _scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(Err(ClassifiedStoreError::InvalidData))
    }

    /// Page
    fn page(
        &self,
        _action: PageAction,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
        ready(Err(ClassifiedStoreError::InvalidData))
    }
}

#[test]
/// Only explicitly transient store failures are retryable
fn only_explicitly_transient_store_failures_are_retryable() -> Result<(), Box<dyn std::error::Error>>
{
    let transient = map_service_error::<ClassifiedStore>(&ServiceError::Store(
        ClassifiedStoreError::WouldBlock,
    ))
    .into_response();
    assert_eq!(transient.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        transient
            .headers()
            .get(RETRY_AFTER)
            .ok_or("expected Some")?,
        "1"
    );

    let permanent = map_service_error::<ClassifiedStore>(&ServiceError::Store(
        ClassifiedStoreError::InvalidData,
    ))
    .into_response();
    assert_eq!(permanent.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(permanent.headers().get(RETRY_AFTER).is_none());
    Ok(())
}

/// Failure of the in-memory store double.
#[derive(Debug)]
enum MemoryFault {
    /// A lock holder panicked, so the shared state cannot be trusted.
    Poisoned,
    /// A count did not fit the protocol integer width.
    Overflow(std::num::TryFromIntError),
    /// The model refused a value.
    Model(ModelError),
}

impl std::fmt::Display for MemoryFault {
    /// Names the failed store-double step with its cause.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Poisoned => formatter.write_str("memory store lock was poisoned"),
            Self::Overflow(ref error) => write!(formatter, "memory store count overflow: {error}"),
            Self::Model(ref error) => write!(formatter, "memory store model refusal: {error}"),
        }
    }
}

impl std::error::Error for MemoryFault {
    /// Exposes the underlying integer or model failure.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Self::Poisoned => None,
            Self::Overflow(ref error) => Some(error),
            Self::Model(ref error) => Some(error),
        }
    }
}

impl<Guard> From<std::sync::PoisonError<Guard>> for MemoryFault {
    /// Poisoned lock
    fn from(_poisoned: std::sync::PoisonError<Guard>) -> Self {
        Self::Poisoned
    }
}

impl From<std::num::TryFromIntError> for MemoryFault {
    /// Integer overflow
    fn from(error: std::num::TryFromIntError) -> Self {
        Self::Overflow(error)
    }
}

impl From<ModelError> for MemoryFault {
    /// Model refusal
    fn from(error: ModelError) -> Self {
        Self::Model(error)
    }
}

#[derive(Default)]
/// Memory
struct Memory {
    /// Accepted changes in arrival order.
    changes: Mutex<Vec<ChangeEnvelope>>,
    /// Operations
    operations: Mutex<HashMap<OperationId, (OperationDigest, ChangeDigest)>>,
}

impl Memory {
    /// Idempotently stores one change when the grant matches it exactly.
    fn accept_now(
        &self,
        change: &ChangeEnvelope,
        grant: &impl ExecutionGrant,
    ) -> Result<AcceptResult, MemoryFault> {
        if grant.scope() != change.scope()
            || grant.effect() != Effect::Write
            || grant.action() != write_action(change)
        {
            return Ok(AcceptResult::StaleAuthority);
        }
        let mut operations = self.operations.lock()?;
        if let Some(existing) = operations.get(&change.operation()) {
            return Ok(if *existing == (change.request(), change.digest()) {
                AcceptResult::Duplicate
            } else {
                AcceptResult::Conflict
            });
        }
        operations.insert(change.operation(), (change.request(), change.digest()));
        self.changes.lock()?.push(change.clone());
        Ok(AcceptResult::Accepted)
    }

    /// Counts the stored changes of one scope.
    fn frontier_now(&self, scope: Scope) -> Result<u64, MemoryFault> {
        let changes = self.changes.lock()?;
        Ok(u64::try_from(
            changes
                .iter()
                .filter(|change| change.scope() == scope)
                .count(),
        )?)
    }

    /// Serves one page when the grant matches the action and frontier.
    fn page_now(
        &self,
        action: PageAction,
        grant: &impl ExecutionGrant,
    ) -> Result<PageResult, MemoryFault> {
        let PageAction {
            scope,
            after,
            max_changes,
            ..
        } = action;
        let frontier = self.frontier_now(scope)?;
        let changes = self.changes.lock()?;
        if grant.scope() != scope
            || grant.effect() != Effect::Sync
            || grant.action() != action.digest()
            || grant.frontier() != frontier
        {
            return Ok(PageResult::StaleAuthority);
        }
        let page = changes
            .iter()
            .filter(|change| change.scope() == scope)
            .skip(usize::try_from(after)?)
            .take(usize::from(max_changes))
            .cloned()
            .collect::<Vec<_>>();
        let position = after.saturating_add(u64::try_from(page.len())?);
        let has_more = usize::try_from(position)? < changes.len();
        Ok(PageResult::Page(StoredPage::new(page, position, has_more)))
    }
}

impl ServiceStore for Memory {
    /// Error
    type Error = MemoryFault;

    /// Accept
    fn accept(
        &self,
        change: &ChangeEnvelope,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        ready(self.accept_now(change, grant))
    }

    /// Frontier
    fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(self.frontier_now(scope))
    }

    /// Page
    fn page(
        &self,
        action: PageAction,
        grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
        ready(self.page_now(action, grant))
    }
}

/// Inert retention for doubles whose tests never reach recovery or pruning.
macro_rules! inert_retention {
    ($store:ty) => {
        impl RetentionStore for $store {
            /// Consumed through
            fn consumed_through(
                &self,
                _scope: Scope,
            ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
                ready(Ok(0))
            }
            /// Snapshot baseline
            fn snapshot_baseline(
                &self,
                _scope: Scope,
            ) -> impl Future<Output = Result<Option<SnapshotRecord>, Self::Error>> + Send {
                ready(Ok(None))
            }
            /// Build snapshot
            fn build_snapshot(
                &self,
                scope: Scope,
                through: u64,
            ) -> impl Future<Output = Result<SnapshotArtifact, Self::Error>> + Send {
                ready(SnapshotArtifact::new(scope, through, Vec::new()).map_err(Into::into))
            }
            /// Read baseline page
            fn read_baseline_page(
                &self,
                _action: &RecoveryAction,
                _descriptor: &SnapshotBaseline,
                _grant: &impl ExecutionGrant,
            ) -> impl Future<Output = Result<BaselinePageResult, Self::Error>> + Send {
                ready(Ok(BaselinePageResult::Page(BaselinePageSlice::new(
                    Vec::new(),
                    false,
                ))))
            }
            /// Seal snapshot
            fn seal_snapshot(
                &self,
                _scope: Scope,
                _artifact: &SnapshotArtifact,
                _request: OperationDigest,
                _grant: &impl ExecutionGrant,
            ) -> impl Future<Output = Result<SealResult, Self::Error>> + Send {
                ready(Ok(SealResult::BeyondFrontier { frontier: 0 }))
            }
            /// Mark consumed
            fn mark_consumed(
                &self,
                _scope: Scope,
                _through: u64,
                _request: OperationDigest,
                _grant: &impl ExecutionGrant,
            ) -> impl Future<Output = Result<ConsumedResult, Self::Error>> + Send {
                ready(Ok(ConsumedResult::Unchanged {
                    consumed_through: 0,
                }))
            }
            /// Prune
            fn prune(
                &self,
                _scope: Scope,
                _up_to: u64,
                _min_live: Option<u64>,
                _request: OperationDigest,
                _grant: &impl ExecutionGrant,
            ) -> impl Future<Output = Result<PruneResult, Self::Error>> + Send {
                ready(Ok(PruneResult::Refused(PruneRefusal::MissingBaseline)))
            }
        }
    };
}

inert_retention!(Memory);
inert_retention!(Stalled);
inert_retention!(ConsumedInvocations);

/// Stalled
struct Stalled {
    entered: Arc<Notify>,
}

impl ServiceStore for Stalled {
    /// Error
    type Error = ModelError;

    /// Accept
    fn accept(
        &self,
        _change: &ChangeEnvelope,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        let entered = Arc::clone(&self.entered);
        async move {
            entered.notify_one();
            pending::<()>().await;
            Ok(AcceptResult::Accepted)
        }
    }

    /// Frontier
    fn frontier(&self, _scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(Ok(0))
    }

    /// Page
    fn page(
        &self,
        _action: PageAction,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
        ready(Ok(PageResult::Page(StoredPage::new(Vec::new(), 0, false))))
    }
}

/// Encode
fn encode<Value: serde::Serialize>(value: &Value) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut body = Vec::new();
    ciborium::into_writer(value, &mut body)?;
    Ok(body)
}

/// Request
fn request(path: &str, body: Vec<u8>) -> Result<Request<Body>, Box<dyn std::error::Error>> {
    request_as(path, body, "Bearer integration-test")
}

/// Request as
fn request_as(
    path: &str,
    body: Vec<u8>,
    authorization: &str,
) -> Result<Request<Body>, Box<dyn std::error::Error>> {
    Ok(Request::post(path)
        .header(CONTENT_TYPE, MEDIA_TYPE)
        .header(ACCEPT, MEDIA_TYPE)
        .header(AUTHORIZATION, authorization)
        .body(Body::from(body))?)
}

/// Change
fn change(scope: Scope, replica: ReplicaId) -> Result<ChangeEnvelope, Box<dyn std::error::Error>> {
    let mut document = Document::new();
    let ((), mut changes) =
        document.edit(|editor| editor.put(&Object::root(), "field", Scalar::Integer(7)))?;
    let encoded = changes.pop().ok_or("expected a single encoded change")?;
    Ok(ChangeEnvelope::new(
        scope,
        DocumentId::new(),
        replica,
        OperationId::new(),
        OperationDigest::new([8; 32]),
        encoded.digest(),
        encoded.into_bytes(),
    ))
}

/// Handles one versioned change push.
async fn push_one(
    app: Router,
    change: ChangeEnvelope,
) -> Result<PushOutcome, Box<dyn std::error::Error>> {
    let push = PushRequest::new(PROTOCOL_VERSION, change.replica(), vec![change]);
    let response = app
        .oneshot(request("/v1/changes:push", encode(&push)?)?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
    let decoded: PushResponse = ciborium::from_reader(body.as_ref())?;
    assert_eq!(decoded.outcomes().len(), 1);
    decoded
        .outcomes()
        .first()
        .cloned()
        .ok_or_else(|| "expected exactly one push outcome".into())
}

/// Test runtime
fn test_runtime() -> Result<lgwks_bot::rt::runtime::Runtime, Box<dyn std::error::Error>> {
    Ok(lgwks_bot::rt::runtime::Builder::new()
        .worker_threads(std::num::NonZeroUsize::new(2))
        .build()?)
}
