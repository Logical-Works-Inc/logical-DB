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
    body::Bytes,
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
use logical_db_protocol::{PullRequest, PushRequest};
use logical_db_service::{
    Authority, CheckpointCodec, ProtocolService, ServiceError, ServicePull, ServiceStore,
    StoreFailureClass,
};

/// Versioned representation accepted and emitted by the synchronization API.
pub const MEDIA_TYPE: &str = "application/vnd.logical-db.v1+cbor";

/// Hard ceiling enforced before a complete request body reaches a handler.
pub const MAX_HTTP_BODY_BYTES: usize = 12_582_912;
/// Maximum requests admitted across parsing, authentication, and store work.
pub const MAX_IN_FLIGHT_REQUESTS: usize = 16;
/// Wall-clock ceiling for one complete admitted HTTP exchange.
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(30);

struct Admission {
    requests: Arc<Semaphore>,
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
pub struct SystemClock;

impl Clock for SystemClock {
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

struct ApiState<Store, Authentication, Time> {
    store: Arc<Store>,
    authentication: Arc<Authentication>,
    checkpoints: CheckpointCodec,
    clock: Arc<Time>,
}

impl<Store, Authentication, Time> Clone for ApiState<Store, Authentication, Time> {
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
    Store: ServiceStore + 'static,
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

fn router_with_limits<Store, Authentication, Time>(
    store: Store,
    authentication: Authentication,
    checkpoints: CheckpointCodec,
    clock: Time,
    max_in_flight: usize,
    deadline: Duration,
) -> Router
where
    Store: ServiceStore + 'static,
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
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .layer(middleware::from_fn_with_state(admission, admit_request))
        .with_state(state)
}

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
        }
    }
    .await;
    result.unwrap_or_else(IntoResponse::into_response)
}

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
        eprintln!("logical-db-server: authenticate: Unauthorized");
        return Err(ApiError::Unauthorized);
    }
    state
        .authentication
        .authenticate(authorization, now_unix_ms)
        .map_err(|error| match error {
            AuthenticationError::Unauthorized => ApiError::Unauthorized,
            AuthenticationError::Forbidden => ApiError::Forbidden,
            AuthenticationError::Malformed { context } => {
                eprintln!("logical-db-server: authentication rejected at {context}");
                ApiError::Unauthorized
            }
        })
}

fn validate_representation(headers: &HeaderMap) -> Result<(), ApiError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next());
    if content_type != Some(MEDIA_TYPE) {
        eprintln!("logical-db-server: validate_representation: UnsupportedMediaType");
        return Err(ApiError::UnsupportedMediaType);
    }
    let accept = headers
        .get(ACCEPT)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next());
    if !matches!(accept, Some(MEDIA_TYPE | "*/*")) {
        eprintln!("logical-db-server: validate_representation: NotAcceptable");
        return Err(ApiError::NotAcceptable);
    }
    Ok(())
}

fn decode<Value: serde::de::DeserializeOwned>(body: &[u8]) -> Result<Value, ApiError> {
    let mut reader = Cursor::new(body);
    let value = ciborium::from_reader(&mut reader).map_err(|error| {
        eprintln!(
            "logical-db-server: CBOR decode category {:?}, body bytes {}",
            std::mem::discriminant(&error),
            body.len()
        );
        ApiError::Malformed {
            context: "CBOR decode",
        }
    })?;
    if reader.position() != body.len() as u64 {
        eprintln!("logical-db-server: decode: Malformed");
        return Err(ApiError::Malformed {
            context: "trailing CBOR bytes",
        });
    }
    Ok(value)
}

fn encode_response<Value: serde::Serialize>(
    status: StatusCode,
    value: &Value,
) -> Result<Response, ApiError> {
    let mut body = Vec::new();
    ciborium::into_writer(value, &mut body).map_err(|error| {
        eprintln!(
            "logical-db-server: CBOR encode category {:?}",
            std::mem::discriminant(&error)
        );
        ApiError::Internal
    })?;
    if body.len() > MAX_HTTP_BODY_BYTES {
        eprintln!("logical-db-server: encode_response: Internal");
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

fn map_service_error<Store>(error: &ServiceError<Store::Error>) -> ApiError
where
    Store: ServiceStore,
{
    match error {
        ServiceError::Protocol(_) | ServiceError::Checkpoint(_) => ApiError::Malformed {
            context: "service protocol or checkpoint",
        },
        ServiceError::Forbidden => ApiError::Forbidden,
        ServiceError::StaleAuthority => ApiError::Unavailable,
        ServiceError::Store(error) => match Store::classify_error(error) {
            StoreFailureClass::Transient => ApiError::Unavailable,
            StoreFailureClass::Permanent => ApiError::Internal,
        },
        ServiceError::CrossScopeStoreData | ServiceError::CorruptStoreData => ApiError::Internal,
    }
}

#[derive(Clone, Copy, Debug)]
enum ApiError {
    Unauthorized,
    Forbidden,
    UnsupportedMediaType,
    NotAcceptable,
    Malformed { context: &'static str },
    CheckpointExpired,
    Busy,
    Deadline,
    Unavailable,
    Clock,
    Internal,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::NotAcceptable => StatusCode::NOT_ACCEPTABLE,
            Self::Malformed { context } => {
                eprintln!("logical-db-server: malformed request at {context}");
                StatusCode::BAD_REQUEST
            }
            Self::CheckpointExpired => StatusCode::GONE,
            Self::Busy | Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Deadline => StatusCode::GATEWAY_TIMEOUT,
            Self::Clock | Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let mut response = status.into_response();
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
        convert::Infallible,
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
        AccountId, ChangeDigest, DocumentId, Effect, OperationDigest, OperationId, ReplicaId,
        Scope, SpaceId,
    };
    use logical_db_protocol::{
        ChangeEnvelope, PROTOCOL_VERSION, PullPage, PushOutcome, PushResponse, RejectionCode,
    };
    use logical_db_service::{AcceptResult, ExecutionGrant, PageResult, StoredPage};
    use tower::ServiceExt;

    use super::*;

    use logical_db_model::ActionDigest;
    use logical_db_protocol::{PageAction, write_action};

    struct Allow;
    #[derive(Clone, Copy)]
    struct ExactAction {
        scope: Scope,
        action: ActionDigest,
    }

    impl Authority for ExactAction {
        type Grant = Grant;

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
        type Policy = Self;

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

    struct Bearer;

    impl Authenticator for Bearer {
        type Policy = Allow;

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
    /// authorization always fails, mirroring `LocalBraidAuthority`.
    struct DefaultComposition {
        scope: Scope,
        expires_at_unix_ms: u64,
    }

    impl Authority for DefaultComposition {
        type Grant = Grant;

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

    struct DefaultAuthentication {
        scope: Scope,
    }

    impl Authenticator for DefaultAuthentication {
        type Policy = DefaultComposition;

        fn authenticate(
            &self,
            authorization: &HeaderValue,
            now_unix_ms: u64,
        ) -> Result<Self::Policy, AuthenticationError> {
            if authorization.as_bytes() != b"Bearer default-composition" {
                eprintln!("logical-db-server: authenticate: Unauthorized");
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
        type Error = Infallible;

        fn accept(
            &self,
            _change: &ChangeEnvelope,
            _grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
            ready(Ok(AcceptResult::InvocationConsumed))
        }

        fn frontier(&self, _scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            ready(Ok(0))
        }

        fn page(
            &self,
            _action: PageAction,
            _grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
            ready(Ok(PageResult::StaleAuthority))
        }
    }

    struct FixedClock;

    impl Clock for FixedClock {
        fn now_unix_ms(&self) -> Option<u64> {
            Some(1_000)
        }
    }

    type ClassifiedStoreError = std::io::ErrorKind;

    struct ClassifiedStore;

    impl ServiceStore for ClassifiedStore {
        type Error = ClassifiedStoreError;

        fn classify_error(error: &Self::Error) -> StoreFailureClass {
            match error {
                ClassifiedStoreError::WouldBlock => StoreFailureClass::Transient,
                _ => StoreFailureClass::Permanent,
            }
        }

        fn accept(
            &self,
            _change: &ChangeEnvelope,
            _grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
            ready(Err(ClassifiedStoreError::InvalidData))
        }

        fn frontier(&self, _scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            ready(Err(ClassifiedStoreError::InvalidData))
        }

        fn page(
            &self,
            _action: PageAction,
            _grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
            ready(Err(ClassifiedStoreError::InvalidData))
        }
    }

    #[test]
    fn only_explicitly_transient_store_failures_are_retryable() {
        let transient = map_service_error::<ClassifiedStore>(&ServiceError::Store(
            ClassifiedStoreError::WouldBlock,
        ))
        .into_response();
        assert_eq!(transient.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(transient.headers().get(RETRY_AFTER).unwrap(), "1");

        let permanent = map_service_error::<ClassifiedStore>(&ServiceError::Store(
            ClassifiedStoreError::InvalidData,
        ))
        .into_response();
        assert_eq!(permanent.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(permanent.headers().get(RETRY_AFTER).is_none());
    }

    #[derive(Default)]
    struct Memory {
        changes: Mutex<Vec<ChangeEnvelope>>,
        operations: Mutex<HashMap<OperationId, (OperationDigest, ChangeDigest)>>,
    }

    impl ServiceStore for Memory {
        type Error = Infallible;

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
            let mut operations = self.operations.lock().unwrap();
            let outcome = if let Some(existing) = operations.get(&change.operation) {
                if *existing == (change.request, change.digest) {
                    AcceptResult::Duplicate
                } else {
                    AcceptResult::Conflict
                }
            } else {
                operations.insert(change.operation, (change.request, change.digest));
                self.changes.lock().unwrap().push(change.clone());
                AcceptResult::Accepted
            };
            ready(Ok(outcome))
        }

        fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            let frontier = self
                .changes
                .lock()
                .unwrap()
                .iter()
                .filter(|change| change.scope == scope)
                .count() as u64;
            ready(Ok(frontier))
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
                max_bytes: _max_bytes,
                ..
            } = action;

            let changes = self.changes.lock().unwrap();
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.action() != action.digest()
                || grant.frontier()
                    != changes
                        .iter()
                        .filter(|change| change.scope == scope)
                        .count() as u64
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
            let position = after + page.len() as u64;
            ready(Ok(PageResult::Page(StoredPage {
                more: usize::try_from(position).is_ok_and(|position| position < changes.len()),
                changes: page,
                position,
            })))
        }
    }

    struct Stalled {
        entered: Arc<Notify>,
    }

    impl ServiceStore for Stalled {
        type Error = Infallible;

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

        fn frontier(&self, _scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            ready(Ok(0))
        }

        fn page(
            &self,
            _action: PageAction,
            _grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
            ready(Ok(PageResult::Page(StoredPage {
                changes: Vec::new(),
                position: 0,
                more: false,
            })))
        }
    }

    fn encode<Value: serde::Serialize>(value: &Value) -> Vec<u8> {
        let mut body = Vec::new();
        ciborium::into_writer(value, &mut body).unwrap();
        body
    }

    fn request(path: &str, body: Vec<u8>) -> Request<Body> {
        request_as(path, body, "Bearer integration-test")
    }

    fn request_as(path: &str, body: Vec<u8>, authorization: &str) -> Request<Body> {
        Request::post(path)
            .header(CONTENT_TYPE, MEDIA_TYPE)
            .header(ACCEPT, MEDIA_TYPE)
            .header(AUTHORIZATION, authorization)
            .body(Body::from(body))
            .unwrap()
    }

    fn change(scope: Scope, replica: ReplicaId) -> ChangeEnvelope {
        let mut document = Document::new();
        let ((), mut changes) = document
            .edit(|editor| editor.put(&Object::root(), "field", Scalar::Integer(7)))
            .unwrap();
        let encoded = changes.pop().unwrap();
        ChangeEnvelope {
            scope,
            document: DocumentId::new(),
            replica,
            operation: OperationId::new(),
            request: OperationDigest::new([8; 32]),
            digest: encoded.digest,
            bytes: encoded.bytes,
        }
    }

    async fn push_one(app: Router, change: ChangeEnvelope) -> PushOutcome {
        let push = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica: change.replica,
            changes: vec![change],
        };
        let response = app
            .oneshot(request("/v1/changes:push", encode(&push)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        let mut decoded: PushResponse = ciborium::from_reader(body.as_ref()).unwrap();
        assert_eq!(decoded.outcomes.len(), 1);
        decoded.outcomes.remove(0)
    }

    fn test_runtime() -> lgwks_bot::Runtime {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
    }

    #[test]
    fn fixed_action_rejects_wire_mutations_and_accepts_exact_retry() {
        test_runtime().block_on(fixed_action_body());
    }

    async fn fixed_action_body() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let original = change(scope, ReplicaId::new());
        let action = ExactAction {
            scope,
            action: write_action(&original),
        };
        let app = router(
            Memory::default(),
            action,
            CheckpointCodec::new([9; 32], 60_000).unwrap(),
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
        let replacement = change(scope, original.replica);
        mutations[7].bytes = replacement.bytes;
        mutations[7].digest = replacement.digest;
        for mutation in mutations {
            assert!(matches!(
                push_one(app.clone(), mutation).await,
                PushOutcome::Rejected { .. }
            ));
        }
        assert!(matches!(
            push_one(app.clone(), original.clone()).await,
            PushOutcome::Accepted { .. }
        ));
        assert!(matches!(
            push_one(app, original).await,
            PushOutcome::Duplicate { .. }
        ));
    }

    #[test]
    fn production_clock_and_response_encoder_preserve_exact_resource_edge() {
        let before = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        let observed = SystemClock.now_unix_ms().unwrap();
        let after = u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap();
        assert!((before..=after).contains(&observed));
        assert!(observed > 1);

        let exact_payload = vec![1; MAX_HTTP_BODY_BYTES - 5];
        assert!(encode_response(StatusCode::OK, &serde_bytes::Bytes::new(&exact_payload)).is_ok());
        let excessive_payload = vec![1; MAX_HTTP_BODY_BYTES - 4];
        assert!(matches!(
            encode_response(StatusCode::OK, &serde_bytes::Bytes::new(&excessive_payload)),
            Err(ApiError::Internal)
        ));
    }

    #[test]
    fn router_pushes_then_pulls_versioned_cbor_without_leaking_credentials() {
        test_runtime().block_on(router_pushes_then_pulls_body());
    }

    async fn router_pushes_then_pulls_body() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let change = change(scope, replica);
        let app = router(
            Memory::default(),
            Bearer,
            CheckpointCodec::new([9; 32], 60_000).unwrap(),
            FixedClock,
        );
        let push = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![change.clone()],
        };
        let response = app
            .clone()
            .oneshot(request("/v1/changes:push", encode(&push)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        let response: PushResponse = ciborium::from_reader(body.as_ref()).unwrap();
        assert!(matches!(
            response.outcomes.as_slice(),
            [PushOutcome::Accepted { operation }] if *operation == change.operation
        ));

        let pull = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([3; 32]),
            after: None,
            max_changes: 16,
            max_bytes: 1_048_576,
        };
        let response = app
            .oneshot(request("/v1/changes:pull", encode(&pull)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        let page: PullPage = ciborium::from_reader(body.as_ref()).unwrap();
        assert_eq!(page.changes, vec![change]);
    }

    #[test]
    fn ingress_rejects_ambiguous_auth_and_wrong_representations() {
        test_runtime().block_on(ingress_rejects_body());
    }

    async fn ingress_rejects_body() {
        let app = router(
            Memory::default(),
            Bearer,
            CheckpointCodec::new([9; 32], 60_000).unwrap(),
            FixedClock,
        );
        let mut ambiguous = request("/v1/changes:push", vec![0x80]);
        ambiguous
            .headers_mut()
            .append(AUTHORIZATION, HeaderValue::from_static("Bearer second"));
        let response = app.clone().oneshot(ambiguous).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let malformed = request_as("/v1/changes:push", vec![0x80], "Basic invalid");
        let response = app.clone().oneshot(malformed).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .oneshot(
                Request::post("/v1/changes:push")
                    .header(CONTENT_TYPE, "application/json")
                    .header(ACCEPT, MEDIA_TYPE)
                    .header(AUTHORIZATION, "Bearer integration-test")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let push = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![change(scope, replica)],
        };
        let mut trailing = encode(&push);
        trailing.push(0);
        let app = router(
            Memory::default(),
            Bearer,
            CheckpointCodec::new([9; 32], 60_000).unwrap(),
            FixedClock,
        );
        let response = app
            .oneshot(request("/v1/changes:push", trailing))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn admission_bounds_complete_requests_and_enforces_deadline() {
        test_runtime().block_on(admission_bounds_body());
    }

    async fn admission_bounds_body() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let push = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![change(scope, replica)],
        };
        let entered = Arc::new(Notify::new());
        let app = router_with_limits(
            Stalled {
                entered: Arc::clone(&entered),
            },
            Bearer,
            CheckpointCodec::new([9; 32], 60_000).unwrap(),
            FixedClock,
            1,
            Duration::from_millis(50),
        );
        let first = lgwks_bot::rt::task::spawn(
            app.clone()
                .oneshot(request("/v1/changes:push", encode(&push))),
        );
        entered.notified().await;
        let saturated = app
            .oneshot(request("/v1/changes:push", encode(&push)))
            .await
            .unwrap();
        assert_eq!(saturated.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(saturated.headers().get(RETRY_AFTER).unwrap(), "1");
        assert_eq!(
            first.await.unwrap().unwrap().status(),
            StatusCode::GATEWAY_TIMEOUT
        );
    }

    #[test]
    fn default_composition_refuses_writes_and_sync_over_http() {
        test_runtime().block_on(default_composition_body());
    }

    async fn default_composition_body() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let app = router(
            Memory::default(),
            DefaultAuthentication { scope },
            CheckpointCodec::new([9; 32], 60_000).unwrap(),
            FixedClock,
        );
        let change = change(scope, replica);
        let push = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![change.clone()],
        };
        let response = app
            .clone()
            .oneshot(request_as(
                "/v1/changes:push",
                encode(&push),
                "Bearer default-composition",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        let response: PushResponse = ciborium::from_reader(body.as_ref()).unwrap();
        assert!(matches!(
            response.outcomes.as_slice(),
            [PushOutcome::Rejected { operation, code: RejectionCode::Forbidden }]
                if *operation == change.operation
        ));

        let pull = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope,
            replica,
            request: OperationDigest::new([3; 32]),
            after: None,
            max_changes: 16,
            max_bytes: 1_048_576,
        };
        let response = app
            .oneshot(request_as(
                "/v1/changes:pull",
                encode(&pull),
                "Bearer default-composition",
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[test]
    fn consumed_invocation_surfaces_as_typed_rejection_over_http() {
        test_runtime().block_on(consumed_invocation_body());
    }

    async fn consumed_invocation_body() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let app = router(
            ConsumedInvocations,
            Bearer,
            CheckpointCodec::new([9; 32], 60_000).unwrap(),
            FixedClock,
        );
        let change = change(scope, replica);
        let push = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![change.clone()],
        };
        let response = app
            .oneshot(request("/v1/changes:push", encode(&push)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        let response: PushResponse = ciborium::from_reader(body.as_ref()).unwrap();
        assert!(matches!(
            response.outcomes.as_slice(),
            [PushOutcome::Rejected {
                operation,
                code: RejectionCode::InvocationConsumed
            }] if *operation == change.operation
        ));
    }
}
