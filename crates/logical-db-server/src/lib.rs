//! Bounded HTTP ingress for the Logical-DB synchronization service.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{
    io::Cursor,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Request, State},
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{
            ACCEPT, AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER, WWW_AUTHENTICATE,
        },
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use lgwks_bot::rt::{sync::Semaphore, time::timeout};
use lgwks_std::json::serde;
use logical_db_protocol::{BaselineRequest, PullRequest, PushRequest};
use logical_db_service::{
    Authority, CheckpointCodec, ProtocolService, RetentionStore, ServiceBaseline, ServiceError,
    ServicePull, ServiceStore, StoreFailureClass,
};

/// Versioned representation accepted and emitted by the synchronization API.
pub const MEDIA_TYPE: &str = "application/vnd.logical-db.v1+cbor";

/// Hard ceiling enforced before a complete request body reaches a handler.
pub const MAX_HTTP_BODY_BYTES: usize = 12_582_912;
/// Maximum requests admitted across parsing, authentication, and store work.
pub const MAX_IN_FLIGHT_REQUESTS: usize = 16;
/// Wall-clock ceiling for one complete admitted HTTP exchange.
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(30);

/// Admission
struct Admission {
    /// In-flight request permit source, bounded at construction.
    requests: Arc<Semaphore>,
    /// Wall-clock ceiling applied to one admitted exchange.
    deadline: Duration,
}

/// Authenticates one credential and returns a request-scoped authority policy.
///
/// The policy still receives every exact scope, effect, request digest, and
/// timestamp from the service. Authentication alone never implies authority.
pub trait Authenticator: Send + Sync {
    /// Exact authority representation consumed by the protocol service.
    type Policy: Authority + Send + Sync;

    /// Validates an opaque `Authorization` value without logging or retaining it.
    ///
    /// # Errors
    ///
    /// Returns only a stable category; secret-bearing diagnostics must remain in
    /// the host's protected telemetry boundary.
    fn authenticate(
        &self,
        authorization: &HeaderValue,
        now_unix_ms: u64,
    ) -> Result<Self::Policy, AuthenticationError>;
}

/// Clock port so expiry behavior can be tested without weakening production time.
pub trait Clock: Send + Sync {
    /// Returns milliseconds since the Unix epoch, or `None` if time is invalid.
    fn now_unix_ms(&self) -> Option<u64>;
}

/// Production wall clock.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct SystemClock;

impl SystemClock {
    /// Builds the production wall clock (the only way outside this crate, since
    /// the unit struct is `#[non_exhaustive]`).
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Clock for SystemClock {
    /// Now unix ms
    fn now_unix_ms(&self) -> Option<u64> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis();
        u64::try_from(millis).ok()
    }
}

/// Stable authentication failure classes safe to map onto HTTP.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AuthenticationError {
    /// Credential is absent, malformed, invalid, or expired.
    Unauthorized,
    /// Credential is valid but barred from the service itself.
    Forbidden,
    /// Credential bytes did not have the required scheme or encoding.
    Malformed {
        /// Authentication boundary that rejected the representation.
        context: &'static str,
    },
}

impl std::fmt::Display for AuthenticationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Unauthorized => write!(formatter, "authentication failed"),
            Self::Forbidden => write!(formatter, "credential is forbidden"),
            Self::Malformed { context } => {
                write!(formatter, "malformed credential at {context}")
            }
        }
    }
}

impl std::error::Error for AuthenticationError {}

/// ApiState
struct ApiState<Store, Authentication, Time> {
    /// Durable service store behind one shared handle.
    store: Arc<Store>,
    /// Credential validator for this router instance.
    authentication: Arc<Authentication>,
    /// Checkpoint codec bound to this router's TTL and key.
    checkpoints: CheckpointCodec,
    /// Clock port used for expiry decisions.
    clock: Arc<Time>,
}

impl<Store, Authentication, Time> Clone for ApiState<Store, Authentication, Time> {
    /// Clone
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            authentication: Arc::clone(&self.authentication),
            checkpoints: self.checkpoints.clone(),
            clock: Arc::clone(&self.clock),
        }
    }
}

/// Builds the complete version-one synchronization API.
///
/// The returned router contains no listener, TLS, process lifecycle, or global
/// state. A standalone service owns those concerns; an existing host can nest the
/// same router under its own listener and middleware stack.
pub fn router<Store, Authentication, Time>(
    store: Store,
    authentication: Authentication,
    checkpoints: CheckpointCodec,
    clock: Time,
) -> Router
where
    Store: RetentionStore + 'static,
    Store::Error: Send + 'static,
    Authentication: Authenticator + 'static,
    Time: Clock + 'static,
{
    router_with_limits(
        store,
        authentication,
        checkpoints,
        clock,
        MAX_IN_FLIGHT_REQUESTS,
        REQUEST_DEADLINE,
    )
}

/// Router with limits
fn router_with_limits<Store, Authentication, Time>(
    store: Store,
    authentication: Authentication,
    checkpoints: CheckpointCodec,
    clock: Time,
    max_in_flight: usize,
    deadline: Duration,
) -> Router
where
    Store: RetentionStore + 'static,
    Store::Error: Send + 'static,
    Authentication: Authenticator + 'static,
    Time: Clock + 'static,
{
    let state = ApiState {
        store: Arc::new(store),
        authentication: Arc::new(authentication),
        checkpoints,
        clock: Arc::new(clock),
    };
    let admission = Arc::new(Admission {
        requests: Arc::new(Semaphore::new(max_in_flight)),
        deadline,
    });
    Router::new()
        .route(
            "/v1/changes:push",
            post(push::<Store, Authentication, Time>),
        )
        .route(
            "/v1/changes:pull",
            post(pull::<Store, Authentication, Time>),
        )
        .route(
            "/v1/recovery:baseline",
            post(recover::<Store, Authentication, Time>),
        )
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .layer(middleware::from_fn_with_state(admission, admit_request))
        .with_state(state)
}

/// Bounds in-flight requests and enforces the exchange deadline.
async fn admit_request(
    State(admission): State<Arc<Admission>>,
    request: Request,
    next: Next,
) -> Response {
    let Ok(permit) = Arc::clone(&admission.requests).try_acquire_owned() else {
        return ApiError::Busy.into_response();
    };
    let result = timeout(admission.deadline, next.run(request)).await;
    drop(permit);
    match result {
        Ok(response) => response,
        Err(_) => ApiError::Deadline.into_response(),
    }
}

/// Handles one versioned change push.
async fn push<Store, Authentication, Time>(
    State(state): State<ApiState<Store, Authentication, Time>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    Store: ServiceStore + 'static,
    Store::Error: Send + 'static,
    Authentication: Authenticator + 'static,
    Time: Clock + 'static,
{
    let result = async {
        validate_representation(&headers)?;
        let now = state.clock.now_unix_ms().ok_or(ApiError::Clock)?;
        let policy = authenticate(&state, &headers, now)?;
        let request: PushRequest = decode(&body)?;
        let service = ProtocolService::new(state.store, policy, state.checkpoints);
        let response = service
            .push(&request, now)
            .await
            .map_err(|error| map_service_error::<Store>(&error))?;
        encode_response(StatusCode::OK, &response)
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

/// Handles one versioned change pull.
async fn pull<Store, Authentication, Time>(
    State(state): State<ApiState<Store, Authentication, Time>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    Store: ServiceStore + 'static,
    Store::Error: Send + 'static,
    Authentication: Authenticator + 'static,
    Time: Clock + 'static,
{
    let result = async {
        validate_representation(&headers)?;
        let now = state.clock.now_unix_ms().ok_or(ApiError::Clock)?;
        let policy = authenticate(&state, &headers, now)?;
        let request: PullRequest = decode(&body)?;
        let service = ProtocolService::new(state.store, policy, state.checkpoints);
        match service
            .pull(&request, now)
            .await
            .map_err(|error| map_service_error::<Store>(&error))?
        {
            ServicePull::Page(page) => encode_response(StatusCode::OK, &page),
            ServicePull::CheckpointExpired => Err(ApiError::CheckpointExpired),
            ServicePull::BaselineRequired { .. } => Err(ApiError::BaselineRequired),
            // Fail closed on any future pull outcome rather than assuming the
            // enum is closed outside the defining crate.
            _ => Err(ApiError::Internal),
        }
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

/// Handles one paged recovery baseline request.
async fn recover<Store, Authentication, Time>(
    State(state): State<ApiState<Store, Authentication, Time>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response
where
    Store: RetentionStore + 'static,
    Store::Error: Send + 'static,
    Authentication: Authenticator + 'static,
    Time: Clock + 'static,
{
    let result = async {
        validate_representation(&headers)?;
        let now = state.clock.now_unix_ms().ok_or(ApiError::Clock)?;
        let policy = authenticate(&state, &headers, now)?;
        let request: BaselineRequest = decode(&body)?;
        let service = ProtocolService::new(state.store, policy, state.checkpoints);
        match service
            .recover(&request, now)
            .await
            .map_err(|error| map_service_error::<Store>(&error))?
        {
            ServiceBaseline::Artifact(page) => encode_response(StatusCode::OK, &page),
            ServiceBaseline::Unavailable => Err(ApiError::BaselineUnavailable),
            // Fail closed on any future baseline outcome rather than assuming
            // the enum is closed outside the defining crate.
            _ => Err(ApiError::Internal),
        }
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

/// Authenticate
fn authenticate<Store, Authentication, Time>(
    state: &ApiState<Store, Authentication, Time>,
    headers: &HeaderMap,
    now_unix_ms: u64,
) -> Result<Authentication::Policy, ApiError>
where
    Authentication: Authenticator,
{
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let authorization = values.next().ok_or(ApiError::Unauthorized)?;
    if values.next().is_some() {
        return Err(ApiError::Unauthorized);
    }
    state
        .authentication
        .authenticate(authorization, now_unix_ms)
        .map_err(|error| match error {
            AuthenticationError::Unauthorized => ApiError::Unauthorized,
            AuthenticationError::Forbidden => ApiError::Forbidden,
            AuthenticationError::Malformed { context: _ } => ApiError::Unauthorized,
        })
}

/// Validate representation
fn validate_representation(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next());
    if content_type != Some(MEDIA_TYPE) {
        return Err(ApiError::UnsupportedMediaType);
    }
    let accept = headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next());
    if !matches!(accept, Some(MEDIA_TYPE | "*/*")) {
        return Err(ApiError::NotAcceptable);
    }
    Ok(())
}

/// Decode
fn decode<Value: serde::de::DeserializeOwned>(body: &[u8]) -> Result<Value, ApiError> {
    let mut reader = Cursor::new(body);
    let value = ciborium::from_reader(&mut reader).map_err(|_| ApiError::Malformed {
        context: "CBOR decode",
    })?;
    if reader.position() != u64::try_from(body.len()).unwrap_or(u64::MAX) {
        return Err(ApiError::Malformed {
            context: "trailing CBOR bytes",
        });
    }
    Ok(value)
}

/// Encode response
fn encode_response<Value: serde::Serialize>(
    status: StatusCode,
    value: &Value,
) -> Result<Response, ApiError> {
    let mut body = Vec::new();
    ciborium::into_writer(value, &mut body).map_err(|_| ApiError::Internal)?;
    if body.len() > MAX_HTTP_BODY_BYTES {
        return Err(ApiError::Internal);
    }
    Ok((
        status,
        [
            (CONTENT_TYPE, HeaderValue::from_static(MEDIA_TYPE)),
            (CACHE_CONTROL, HeaderValue::from_static("no-store")),
        ],
        body,
    )
        .into_response())
}

/// Map service error
fn map_service_error<Store>(error: &ServiceError<Store::Error>) -> ApiError
where
    Store: ServiceStore,
{
    match *error {
        ServiceError::Protocol(_) | ServiceError::Checkpoint(_) => ApiError::Malformed {
            context: "service protocol or checkpoint",
        },
        ServiceError::Forbidden => ApiError::Forbidden,
        ServiceError::StaleAuthority => ApiError::Unavailable,
        ServiceError::Store(ref store_error) => match Store::classify_error(store_error) {
            StoreFailureClass::Transient => ApiError::Unavailable,
            StoreFailureClass::Permanent => ApiError::Internal,
            // Unclassified store failures fail closed as internal rather than
            // being assumed permanent or transient outside the crate.
            _ => ApiError::Internal,
        },
        ServiceError::CrossScopeStoreData
        | ServiceError::CorruptStoreData
        | ServiceError::Model(_) => ApiError::Internal,
        // Unknown service failures map to an internal error rather than being
        // assumed absent outside the defining crate.
        _ => ApiError::Internal,
    }
}

#[derive(Clone, Copy, Debug)]
/// Stable HTTP-facing failure classes produced by the ingress handlers.
enum ApiError {
    /// Credential is absent, malformed, invalid, or expired.
    Unauthorized,
    /// Credential is valid but barred from this request or scope.
    Forbidden,
    /// Request representation is not a supported media type.
    UnsupportedMediaType,
    /// Response representation is not acceptable to the client.
    NotAcceptable,
    /// Request bytes failed a decode or framing check.
    Malformed {
        /// Framing boundary that rejected the bytes.
        context: &'static str,
    },
    /// Supplied checkpoint is expired and recovery is required.
    CheckpointExpired,
    /// Client must fetch a baseline before this pull.
    BaselineRequired,
    /// Baseline is not currently available.
    BaselineUnavailable,
    /// In-flight admission budget is exhausted.
    Busy,
    /// Exchange deadline elapsed before completion.
    Deadline,
    /// Dependency is temporarily unavailable.
    Unavailable,
    /// Server clock did not yield a usable timestamp.
    Clock,
    /// Unclassified internal failure; fails closed.
    Internal,
}

impl IntoResponse for ApiError {
    /// Into response
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::NotAcceptable => StatusCode::NOT_ACCEPTABLE,
            Self::Malformed { .. } => StatusCode::BAD_REQUEST,
            Self::CheckpointExpired => StatusCode::GONE,
            Self::BaselineRequired => StatusCode::CONFLICT,
            Self::BaselineUnavailable => StatusCode::NOT_FOUND,
            Self::Busy | Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Deadline => StatusCode::GATEWAY_TIMEOUT,
            Self::Clock | Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let mut response = status.into_response();
        if let Self::Malformed { context } = self {
            let mut message = context.to_owned();
            message.push('\n');
            *response.body_mut() = Body::from(message);
        }
        response
            .headers_mut()
            .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
        if matches!(self, Self::Unauthorized) {
            response
                .headers_mut()
                .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        if matches!(self, Self::Busy | Self::Unavailable) {
            response
                .headers_mut()
                .insert(RETRY_AFTER, HeaderValue::from_static("1"));
        }
        response
    }
}

#[cfg(test)]
mod tests {
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

    impl Authority for ExactAction {
        /// Grant
        type Grant = Grant;

        /// Authorize
        fn authorize(
            &self,
            scope: Scope,
            effect: Effect,
            action: ActionDigest,
            frontier: u64,
            _now: u64,
        ) -> Option<Grant> {
            if scope != self.scope || effect != Effect::Write || action != self.action {
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
    /// Grant
    struct Grant {
        scope: Scope,
        /// Effect
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
    }

    impl ExecutionGrant for Grant {
        /// Scope
        fn scope(&self) -> Scope {
            self.scope
        }
        /// Effect
        fn effect(&self) -> Effect {
            self.effect
        }
        /// Action
        fn action(&self) -> ActionDigest {
            self.action
        }
        /// Frontier
        fn frontier(&self) -> u64 {
            self.frontier
        }
        /// Invocation id
        fn invocation_id(&self) -> [u8; 32] {
            self.action.into_bytes()
        }
    }

    impl Authority for Allow {
        /// Grant
        type Grant = Grant;

        /// Authorize
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

    impl Authority for DefaultComposition {
        /// Grant
        type Grant = Grant;

        /// Authorize
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
            Some(Grant {
                scope,
                effect,
                action,
                frontier,
            })
        }
    }

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
    fn only_explicitly_transient_store_failures_are_retryable()
    -> Result<(), Box<dyn std::error::Error>> {
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

    #[derive(Default)]
    /// Memory
    struct Memory {
        changes: Mutex<Vec<ChangeEnvelope>>,
        /// Operations
        operations: Mutex<HashMap<OperationId, (OperationDigest, ChangeDigest)>>,
    }

    impl ServiceStore for Memory {
        /// Error
        type Error = ModelError;

        /// Accept
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
            let outcome = if let Some(existing) = operations.get(&change.operation) {
                if *existing == (change.request, change.digest) {
                    AcceptResult::Duplicate
                } else {
                    AcceptResult::Conflict
                }
            } else {
                operations.insert(change.operation, (change.request, change.digest));
                self.changes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(change.clone());
                AcceptResult::Accepted
            };
            ready(Ok(outcome))
        }

        /// Frontier
        fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            let frontier = u64::try_from(
                self.changes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .filter(|change| change.scope == scope)
                    .count(),
            )
            .unwrap_or(u64::MAX);
            ready(Ok(frontier))
        }

        /// Page
        fn page(
            &self,
            action: PageAction,
            grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
            let PageAction {
                scope,
                after,
                max_changes,
                max_bytes: _max_bytes,
                ..
            } = action;

            let changes = self
                .changes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.action() != action.digest()
                || grant.frontier()
                    != u64::try_from(
                        changes
                            .iter()
                            .filter(|change| change.scope == scope)
                            .count(),
                    )
                    .unwrap_or(u64::MAX)
            {
                return ready(Ok(PageResult::StaleAuthority));
            }
            let page = changes
                .iter()
                .filter(|change| change.scope == scope)
                .skip(usize::try_from(after).unwrap_or(usize::MAX))
                .take(usize::from(max_changes))
                .cloned()
                .collect::<Vec<_>>();
            let position = after.saturating_add(u64::try_from(page.len()).unwrap_or(u64::MAX));
            ready(Ok(PageResult::Page(StoredPage::new(
                page,
                position,
                usize::try_from(position).is_ok_and(|position| position < changes.len()),
            ))))
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
                    ready(SnapshotArtifact::new(scope, through, Vec::new()))
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
    fn encode<Value: serde::Serialize>(
        value: &Value,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
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
    fn change(
        scope: Scope,
        replica: ReplicaId,
    ) -> Result<ChangeEnvelope, Box<dyn std::error::Error>> {
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
            encoded.digest,
            encoded.bytes,
        ))
    }

    /// Handles one versioned change push.
    async fn push_one(
        app: Router,
        change: ChangeEnvelope,
    ) -> Result<PushOutcome, Box<dyn std::error::Error>> {
        let push = PushRequest::new(PROTOCOL_VERSION, change.replica, vec![change]);
        let response = app
            .oneshot(request("/v1/changes:push", encode(&push)?)?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
        let decoded: PushResponse = ciborium::from_reader(body.as_ref())?;
        assert_eq!(decoded.outcomes.len(), 1);
        decoded
            .outcomes
            .into_iter()
            .next()
            .ok_or_else(|| "expected exactly one push outcome".into())
    }

    /// Test runtime
    fn test_runtime() -> Result<lgwks_bot::Runtime, Box<dyn std::error::Error>> {
        Ok(lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()?)
    }

    #[test]
    /// Fixed action rejects wire mutations and accepts exact retry
    fn fixed_action_rejects_wire_mutations_and_accepts_exact_retry()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(fixed_action_body())?;
        Ok(())
    }

    async fn fixed_action_body() -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let original = change(scope, ReplicaId::new())?;
        let action = ExactAction {
            scope,
            action: write_action(&original),
        };
        let app = router(
            Memory::default(),
            action,
            CheckpointCodec::new([9; 32], 60_000)?,
            FixedClock,
        );
        let mut mutations = vec![original.clone(); 8];
        mutations[0].scope = Scope::new(AccountId::new(), scope.space());
        mutations[1].scope = Scope::new(scope.account(), SpaceId::new());
        mutations[2].document = DocumentId::new();
        mutations[3].operation = OperationId::new();
        mutations[4].replica = ReplicaId::new();
        mutations[5].request = OperationDigest::new([99; 32]);
        mutations[6].digest = ChangeDigest::new([99; 32]);
        let replacement = change(scope, original.replica)?;
        mutations[7].bytes = replacement.bytes;
        mutations[7].digest = replacement.digest;
        for mutation in mutations {
            assert!(matches!(
                push_one(app.clone(), mutation).await?,
                PushOutcome::Rejected { .. }
            ));
        }
        assert!(matches!(
            push_one(app.clone(), original.clone()).await?,
            PushOutcome::Accepted { .. }
        ));
        assert!(matches!(
            push_one(app, original).await?,
            PushOutcome::Duplicate { .. }
        ));
        Ok(())
    }

    #[test]
    /// Production clock and response encoder preserve exact resource edge
    fn production_clock_and_response_encoder_preserve_exact_resource_edge()
    -> Result<(), Box<dyn std::error::Error>> {
        let before = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        let observed = SystemClock.now_unix_ms().ok_or("expected Some")?;
        let after = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        assert!((before..=after).contains(&observed));
        assert!(observed > 1);

        let exact_payload = vec![1; MAX_HTTP_BODY_BYTES - 5];
        assert!(encode_response(StatusCode::OK, &serde_bytes::Bytes::new(&exact_payload)).is_ok());
        let excessive_payload = vec![1; MAX_HTTP_BODY_BYTES - 4];
        assert!(matches!(
            encode_response(StatusCode::OK, &serde_bytes::Bytes::new(&excessive_payload)),
            Err(ApiError::Internal)
        ));
        Ok(())
    }

    #[test]
    /// Router pushes then pulls versioned cbor without leaking credentials
    fn router_pushes_then_pulls_versioned_cbor_without_leaking_credentials()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(router_pushes_then_pulls_body())?;
        Ok(())
    }

    async fn router_pushes_then_pulls_body() -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let change = change(scope, replica)?;
        let app = router(
            Memory::default(),
            Bearer,
            CheckpointCodec::new([9; 32], 60_000)?,
            FixedClock,
        );
        let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change.clone()]);
        let response = app
            .clone()
            .oneshot(request("/v1/changes:push", encode(&push)?)?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
        let response: PushResponse = ciborium::from_reader(body.as_ref())?;
        assert!(matches!(
            response.outcomes.as_slice(),
            [PushOutcome::Accepted { operation }] if *operation == change.operation
        ));

        let pull = PullRequest::new(
            PROTOCOL_VERSION,
            scope,
            replica,
            OperationDigest::new([3; 32]),
            None,
            16,
            1_048_576,
        );
        let response = app
            .oneshot(request("/v1/changes:pull", encode(&pull)?)?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
        let page: PullPage = ciborium::from_reader(body.as_ref())?;
        assert_eq!(page.changes, vec![change]);
        Ok(())
    }

    #[test]
    /// Ingress rejects ambiguous auth and wrong representations
    fn ingress_rejects_ambiguous_auth_and_wrong_representations()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(ingress_rejects_body())?;
        Ok(())
    }

    async fn ingress_rejects_body() -> Result<(), Box<dyn std::error::Error>> {
        let app = router(
            Memory::default(),
            Bearer,
            CheckpointCodec::new([9; 32], 60_000)?,
            FixedClock,
        );
        let mut ambiguous = request("/v1/changes:push", vec![0x80])?;
        ambiguous
            .headers_mut()
            .append(AUTHORIZATION, HeaderValue::from_static("Bearer second"));
        let response = app.clone().oneshot(ambiguous).await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let malformed = request_as("/v1/changes:push", vec![0x80], "Basic invalid")?;
        let response = app.clone().oneshot(malformed).await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(
                Request::post("/v1/changes:push")
                    .header(CONTENT_TYPE, "application/json")
                    .header(ACCEPT, MEDIA_TYPE)
                    .header(AUTHORIZATION, "Bearer integration-test")
                    .body(Body::from("{}"))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change(scope, replica)?]);
        let mut trailing = encode(&push)?;
        trailing.push(0);
        let app = router(
            Memory::default(),
            Bearer,
            CheckpointCodec::new([9; 32], 60_000)?,
            FixedClock,
        );
        let response = app.oneshot(request("/v1/changes:push", trailing)?).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        Ok(())
    }

    #[test]
    /// Admission bounds complete requests and enforces deadline
    fn admission_bounds_complete_requests_and_enforces_deadline()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(admission_bounds_body())?;
        Ok(())
    }

    async fn admission_bounds_body() -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change(scope, replica)?]);
        let entered = Arc::new(Notify::new());
        let app = router_with_limits(
            Stalled {
                entered: Arc::clone(&entered),
            },
            Bearer,
            CheckpointCodec::new([9; 32], 60_000)?,
            FixedClock,
            1,
            Duration::from_millis(50),
        );
        let first = lgwks_bot::rt::task::spawn(
            app.clone()
                .oneshot(request("/v1/changes:push", encode(&push)?)?),
        );
        entered.notified().await;
        let saturated = app
            .oneshot(request("/v1/changes:push", encode(&push)?)?)
            .await?;
        assert_eq!(saturated.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            saturated
                .headers()
                .get(RETRY_AFTER)
                .ok_or("expected Some")?,
            "1"
        );
        assert_eq!(first.await??.status(), StatusCode::GATEWAY_TIMEOUT);
        Ok(())
    }

    #[test]
    /// Default composition refuses writes and sync over http
    fn default_composition_refuses_writes_and_sync_over_http()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(default_composition_body())?;
        Ok(())
    }

    async fn default_composition_body() -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let app = router(
            Memory::default(),
            DefaultAuthentication { scope },
            CheckpointCodec::new([9; 32], 60_000)?,
            FixedClock,
        );
        let change = change(scope, replica)?;
        let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change.clone()]);
        let response = app
            .clone()
            .oneshot(request_as(
                "/v1/changes:push",
                encode(&push)?,
                "Bearer default-composition",
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
        let response: PushResponse = ciborium::from_reader(body.as_ref())?;
        assert!(matches!(
            response.outcomes.as_slice(),
            [PushOutcome::Rejected { operation, code: RejectionCode::Forbidden }]
                if *operation == change.operation
        ));

        let pull = PullRequest::new(
            PROTOCOL_VERSION,
            scope,
            replica,
            OperationDigest::new([3; 32]),
            None,
            16,
            1_048_576,
        );
        let response = app
            .oneshot(request_as(
                "/v1/changes:pull",
                encode(&pull)?,
                "Bearer default-composition",
            )?)
            .await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        Ok(())
    }

    #[test]
    /// Consumed invocation surfaces as typed rejection over http
    fn consumed_invocation_surfaces_as_typed_rejection_over_http()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(consumed_invocation_body())?;
        Ok(())
    }

    async fn consumed_invocation_body() -> Result<(), Box<dyn std::error::Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let app = router(
            ConsumedInvocations,
            Bearer,
            CheckpointCodec::new([9; 32], 60_000)?,
            FixedClock,
        );
        let change = change(scope, replica)?;
        let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change.clone()]);
        let response = app
            .oneshot(request("/v1/changes:push", encode(&push)?)?)
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
        let response: PushResponse = ciborium::from_reader(body.as_ref())?;
        assert!(matches!(
            response.outcomes.as_slice(),
            [PushOutcome::Rejected {
                operation,
                code: RejectionCode::InvocationConsumed
            }] if *operation == change.operation
        ));
        Ok(())
    }
}
