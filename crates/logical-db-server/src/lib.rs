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
        // A clock set before the epoch has no valid reading; the trait answers
        // `None` and the handlers fail closed with `ApiError::Clock`.
        let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) else {
            return None;
        };
        elapsed
            .as_secs()
            .checked_mul(1_000)?
            .checked_add(u64::from(elapsed.subsec_millis()))
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

/// Turns a handler outcome into the single response sent to the client.
fn into_reply(result: Result<Response, ApiError>) -> Response {
    match result {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}

/// Declares one ingress handler: the shared prelude (representation, clock,
/// authentication, bounded decode, service construction), then the route's own
/// service call.
macro_rules! ingress_handler {
    (
        $(#[$doc:meta])*
        $name:ident, $bound:ident, $request_type:ty,
        |$service:ident, $request:ident, $now:ident| $outcome:expr
    ) => {
        $(#[$doc])*
        async fn $name<Store, Authentication, Time>(
            State(state): State<ApiState<Store, Authentication, Time>>,
            headers: HeaderMap,
            body: Bytes,
        ) -> Response
        where
            Store: $bound + 'static,
            Store::Error: Send + 'static,
            Authentication: Authenticator + 'static,
            Time: Clock + 'static,
        {
            let result = async {
                validate_representation(&headers)?;
                let $now = state.clock.now_unix_ms().ok_or(ApiError::Clock)?;
                let policy = authenticate(&state, &headers, $now)?;
                let $request: $request_type = decode(&body)?;
                let $service = ProtocolService::new(state.store, policy, state.checkpoints);
                $outcome
            }
            .await;
            into_reply(result)
        }
    };
}

ingress_handler!(
    /// Handles one versioned change push.
    push,
    ServiceStore,
    PushRequest,
    |service, request, now| {
        let response = service
            .push(&request, now)
            .await
            .map_err(|error| map_service_error::<Store>(&error))?;
        encode_response(StatusCode::OK, &response)
    }
);

ingress_handler!(
    /// Handles one versioned change pull.
    pull,
    ServiceStore,
    PullRequest,
    |service, request, now| {
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
);

ingress_handler!(
    /// Handles one paged recovery baseline request.
    recover,
    RetentionStore,
    BaselineRequest,
    |service, request, now| {
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
);

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
    let value = ciborium::from_reader(&mut reader).map_err(|error| ApiError::Malformed {
        context: decode_context(&error),
    })?;
    let fully_consumed =
        matches!(usize::try_from(reader.position()), Ok(consumed) if consumed == body.len());
    if !fully_consumed {
        return Err(ApiError::Malformed {
            context: "trailing CBOR bytes",
        });
    }
    Ok(value)
}

/// Names the CBOR boundary a decode failure came from.
const fn decode_context<Source>(error: &ciborium::de::Error<Source>) -> &'static str {
    match *error {
        ciborium::de::Error::Io(_) => "CBOR read",
        ciborium::de::Error::Syntax(_) => "CBOR syntax",
        ciborium::de::Error::Semantic(..) => "CBOR value",
        ciborium::de::Error::RecursionLimitExceeded => "CBOR recursion limit",
    }
}

/// Encode response
fn encode_response<Value: serde::Serialize>(
    status: StatusCode,
    value: &Value,
) -> Result<Response, ApiError> {
    let mut body = Vec::new();
    ciborium::into_writer(value, &mut body).map_err(|error| match error {
        ciborium::ser::Error::Io(_) | ciborium::ser::Error::Value(_) => ApiError::EncodeFailed,
    })?;
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
    /// Response bytes could not be produced; the cause is never sent to the client.
    EncodeFailed,
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
            Self::Clock | Self::EncodeFailed | Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
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
mod tests;
