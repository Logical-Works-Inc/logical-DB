//! Bounded HTTP/CBOR transport for thin Logical-DB clients.
//!
//! The wire client is the estate `lgwks_std::http` pooled client (ureq with
//! rustls-only TLS). Each exchange runs on the bounded `lgwks_std::task`
//! blocking pool, under one async operation deadline.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::expect_used, clippy::unwrap_used))]

use std::{
    fmt,
    future::{Future, ready},
    io::Cursor,
    net::IpAddr,
    time::Duration,
};

use lgwks_bot::rt::time::{Instant, timeout_at};
use lgwks_std::http::{
    BodyPolicy, Client, Error as WireError, FailureKind, Options, PoolConfig, RedirectPolicy,
    Response,
};
use lgwks_std::json::serde::{Serialize, de::DeserializeOwned};
use lgwks_std::task::{SpawnError, try_spawn_blocking};
use logical_db_protocol::{
    BaselinePage, BaselineRequest, ProtocolError, PullPage, PullRequest, PushRequest, PushResponse,
};
use logical_db_sync::{BaselineResult, PullResult, Transport};
use thiserror::Error;

/// Versioned CBOR media type used on every request and response.
const MEDIA_TYPE: &str = "application/vnd.logical-db.v1+cbor";
/// Hard ceiling on one encoded request or response body.
const MAX_HTTP_BODY_BYTES: usize = 12_582_912;
/// Wall-clock ceiling covering one complete exchange.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum server-directed retry delay accepted from an untrusted response.
pub const MAX_RETRY_AFTER_SECONDS: u64 = 3_600;
/// Status a pull answers when the requested checkpoint has expired.
const STATUS_GONE: u16 = 410;
/// Status a pull answers when the replica must recover from a baseline.
const STATUS_CONFLICT: u16 = 409;
/// Status a baseline request answers when no baseline exists.
const STATUS_NOT_FOUND: u16 = 404;

/// Validated service base URL.
///
/// Absolute `https`, or `http` on a loopback host only; no userinfo, query
/// or fragment; the path always ends in `/` so routes join beneath it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Endpoint {
    /// Normalized base URL text, ending in `/`.
    base: String,
}

impl Endpoint {
    /// Parses and validates one service base URL.
    ///
    /// # Errors
    ///
    /// [`HttpError::InvalidEndpoint`] for credentials, a query, a fragment or
    /// a malformed URL; [`HttpError::InsecureEndpoint`] for non-loopback HTTP.
    pub fn parse(raw: &str) -> Result<Self, HttpError> {
        let (scheme, rest) = raw.split_once("://").ok_or(HttpError::InvalidEndpoint)?;
        let (authority, path) = match rest.find('/') {
            Some(at) => rest.split_at(at),
            None => (rest, "/"),
        };
        if authority.is_empty() || authority.contains('@') || raw.contains(['?', '#']) {
            return Err(HttpError::InvalidEndpoint);
        }
        let host = host_of(authority);
        let loopback = host.eq_ignore_ascii_case("localhost")
            || matches!(host.parse::<IpAddr>(), Ok(address) if address.is_loopback());
        let scheme = scheme.to_ascii_lowercase();
        if scheme != "https" && !(scheme == "http" && loopback) {
            return Err(HttpError::InsecureEndpoint);
        }
        let mut base = format!("{scheme}://{authority}{path}");
        if !base.ends_with('/') {
            base.push('/');
        }
        if lgwks_std::http::validate_url(&base).is_err() {
            return Err(HttpError::InvalidEndpoint);
        }
        Ok(Self { base })
    }

    /// Joins a relative route beneath the base path.
    fn route(&self, relative: &str) -> String {
        format!("{}{relative}", self.base)
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.base)
    }
}

/// Returns the host of a URL authority, without port or IPv6 brackets.
fn host_of(authority: &str) -> &str {
    if let Some(bracketed) = authority.strip_prefix('[') {
        return match bracketed.split_once(']') {
            Some((host, _)) => host,
            None => authority,
        };
    }
    match authority.rsplit_once(':') {
        Some((host, _)) => host,
        None => authority,
    }
}

/// Sensitive complete `Authorization` header value; `Debug` is redacted.
#[derive(Clone)]
pub struct Authorization(String);

impl Authorization {
    /// Wraps a complete header value such as `Bearer <token>`.
    ///
    /// # Errors
    ///
    /// [`CredentialError::Empty`] for an empty value and
    /// [`CredentialError::InvalidHeader`] for bytes invalid in an HTTP header.
    pub fn new(value: String) -> Result<Self, CredentialError> {
        if value.is_empty() {
            return Err(CredentialError::Empty);
        }
        if !value
            .bytes()
            .all(|byte| byte == b' ' || byte.is_ascii_graphic())
        {
            return Err(CredentialError::InvalidHeader);
        }
        Ok(Self(value))
    }
}

impl fmt::Debug for Authorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Authorization([REDACTED])")
    }
}

/// Refreshable credential boundary invoked before every request.
///
/// Refresh shares the transport's operation deadline. Providers must yield
/// promptly, avoid blocking the executor, and keep refresh work owned by the
/// returned future: dropping it must release that work and leave the provider
/// reusable. Do not detach refresh tasks. Async timers cannot preempt a provider
/// that blocks inside a poll or while constructing its future.
pub trait CredentialProvider: Send {
    /// Produces a sensitive complete `Authorization` header value.
    ///
    /// # Errors
    ///
    /// Returns a typed credential refresh or encoding failure.
    fn authorization(
        &mut self,
    ) -> impl Future<Output = Result<Authorization, CredentialError>> + Send;
}

/// Static bearer credential for hosts that manage refresh externally.
pub struct StaticBearer {
    /// Sensitive `Authorization` header value retained by this provider.
    value: Authorization,
}

impl StaticBearer {
    /// Builds a sensitive bearer header from a nonempty token.
    ///
    /// # Errors
    ///
    /// Refuses empty tokens or tokens outside the RFC 6750 b64token grammar.
    pub fn new(token: &str) -> Result<Self, CredentialError> {
        if token.is_empty() {
            return Err(CredentialError::Empty);
        }
        if token.len() > 4_096 || !valid_b64token(token.as_bytes()) {
            return Err(CredentialError::InvalidToken);
        }
        Ok(Self {
            value: Authorization::new(format!("Bearer {token}"))?,
        })
    }
}

impl fmt::Debug for StaticBearer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StaticBearer([REDACTED])")
    }
}

impl CredentialProvider for StaticBearer {
    fn authorization(
        &mut self,
    ) -> impl Future<Output = Result<Authorization, CredentialError>> + Send {
        ready(Ok(self.value.clone()))
    }
}

/// Reusable pooled thin-client transport.
///
/// Each push, pull, or baseline exchange has one 30-second deadline covering
/// request encoding, credential refresh, HTTP headers, body consumption, and
/// response validation. The wire exchange runs on the bounded blocking pool
/// and carries the remaining deadline itself, so a dropped operation leaves at
/// most one exchange that ends by that same deadline. Neither cancellation
/// nor timeout implies remote rollback.
pub struct HttpTransport<Credentials> {
    /// Pooled estate HTTP client: fixed timeouts, no redirects, bounded body.
    client: Client,
    /// Base service endpoint; relative routes are joined beneath it.
    endpoint: Endpoint,
    /// Credential provider invoked before every dispatch.
    credentials: Credentials,
    /// Per-exchange deadline covering encode, refresh, and body consumption.
    operation_timeout: Duration,
}

impl<Credentials> fmt::Debug for HttpTransport<Credentials> {
    /// Names the endpoint and deadline; credentials and the client stay out.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpTransport")
            .field("endpoint", &self.endpoint)
            .field("operation_timeout", &self.operation_timeout)
            .finish_non_exhaustive()
    }
}

impl<Credentials> HttpTransport<Credentials>
where
    Credentials: CredentialProvider,
{
    /// Creates a bounded transport over a validated [`Endpoint`].
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::Network`] when the pooled client cannot be built.
    pub fn new(endpoint: Endpoint, credentials: Credentials) -> Result<Self, HttpError> {
        Ok(Self {
            client: pooled_client()?,
            endpoint,
            credentials,
            operation_timeout: OPERATION_TIMEOUT,
        })
    }

    /// Dispatches one encoded request under the operation deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed credential, worker, network, decode, or deadline failure.
    async fn send<Request, Reply>(
        &mut self,
        route: &str,
        request: &Request,
        decode: impl FnOnce(&Response) -> Result<Reply, HttpError>,
    ) -> Result<Reply, HttpError>
    where
        Request: Serialize,
    {
        let deadline = Instant::now()
            .checked_add(self.operation_timeout)
            .ok_or(HttpError::OperationTimeout)?;
        let body = encode(request)?;
        let authorization = within_deadline(deadline, HttpError::CredentialTimeout, async {
            self.credentials.authorization().await
        })
        .await??;
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(HttpError::OperationTimeout)?;
        let options = self
            .client
            .options()
            .clone()
            .deadline(remaining)
            .header("Authorization", authorization.0)
            .header("Accept", MEDIA_TYPE);
        let client = self.client.clone();
        let url = self.endpoint.route(route);
        let exchange =
            try_spawn_blocking(move || client.post_with(&url, MEDIA_TYPE, &body, &options))
                .map_err(HttpError::Worker)?;
        let response = within_deadline(deadline, HttpError::OperationTimeout, exchange)
            .await?
            .map_err(wire_failure)?;
        decode(&response)
    }
}

/// Builds the pooled estate client: the operation timeout bounds every
/// phase, redirects are never followed, and bodies are read whole under the
/// protocol ceiling.
///
/// # Errors
///
/// Returns [`HttpError::Network`] when the client cannot be built.
fn pooled_client() -> Result<Client, HttpError> {
    let options = Options::default()
        .timeout(OPERATION_TIMEOUT)
        .max_body_bytes(MAX_HTTP_BODY_BYTES)
        .body_policy(BodyPolicy::Whole)
        .redirect_policy(RedirectPolicy::NoFollow);
    let pool = PoolConfig::default().max_idle_age(Duration::from_secs(90));
    Client::with_pool(options, pool).map_err(HttpError::Network)
}

/// Maps an estate HTTP failure onto this transport's typed failure.
fn wire_failure(error: WireError) -> HttpError {
    match error {
        WireError::BodyTooLarge { .. } => HttpError::BodyTooLarge,
        WireError::Failure {
            kind: FailureKind::Timeout,
            ..
        } => HttpError::OperationTimeout,
        other => HttpError::Network(other),
    }
}

// A timer may poll a ready future before checking expiry. Refuse both an
// already-expired stage and a value returned after the deadline, so a late
// credential cannot dispatch a request and a late page cannot be installed.
/// Runs `future` under `deadline`, mapping expiry onto `error`.
///
/// # Errors
///
/// Returns `error` when the deadline is already past or elapses first.
async fn within_deadline<T>(
    deadline: Instant,
    error: HttpError,
    future: impl Future<Output = T>,
) -> Result<T, HttpError> {
    if Instant::now() >= deadline {
        return Err(error);
    }
    match timeout_at(deadline, future).await {
        Ok(value) if Instant::now() < deadline => Ok(value),
        _ => Err(error),
    }
}

/// Returns whether `token` is a bounded RFC 6750 b64token.
fn valid_b64token(token: &[u8]) -> bool {
    let mut padding = false;
    token.iter().all(|byte| {
        if *byte == b'=' {
            padding = true;
            true
        } else {
            !padding
                && (byte.is_ascii_alphanumeric()
                    || matches!(*byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/'))
        }
    })
}

impl<Credentials> Transport for HttpTransport<Credentials>
where
    Credentials: CredentialProvider,
{
    type Error = HttpError;

    async fn push(&mut self, request: PushRequest) -> Result<PushResponse, Self::Error> {
        request.validate().map_err(HttpError::Protocol)?;
        self.send("v1/changes:push", &request, |response| {
            let reply: PushResponse = decode_response(response)?;
            reply.validate(&request).map_err(HttpError::Protocol)?;
            Ok(reply)
        })
        .await
    }

    async fn pull(&mut self, request: PullRequest) -> Result<PullResult, Self::Error> {
        request.validate().map_err(HttpError::Protocol)?;
        let scope = request.scope;
        self.send("v1/changes:pull", &request, move |response| {
            match response.status() {
                STATUS_GONE => return Ok(PullResult::CheckpointExpired),
                STATUS_CONFLICT => return Ok(PullResult::BaselineRequired),
                _ => {}
            }
            let page = decode_response::<PullPage>(response)?;
            page.validate(scope).map_err(HttpError::Protocol)?;
            Ok(PullResult::Page(page))
        })
        .await
    }

    async fn baseline(&mut self, request: BaselineRequest) -> Result<BaselineResult, Self::Error> {
        request.validate().map_err(HttpError::Protocol)?;
        let scope = request.scope;
        self.send("v1/recovery:baseline", &request, move |response| {
            if response.status() == STATUS_NOT_FOUND {
                return Ok(BaselineResult::Unavailable);
            }
            let page = decode_response::<BaselinePage>(response)?;
            page.validate_page(scope).map_err(HttpError::Protocol)?;
            Ok(BaselineResult::Artifact(page))
        })
        .await
    }
}

/// Encodes one CBOR request body under the body-size ceiling.
///
/// # Errors
///
/// Returns [`HttpError::Encode`] or [`HttpError::BodyTooLarge`].
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, HttpError> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes).map_err(HttpError::Encode)?;
    if bytes.len() > MAX_HTTP_BODY_BYTES {
        return Err(HttpError::BodyTooLarge);
    }
    Ok(bytes)
}

/// Returns the first value of header `name`, compared case-insensitively.
fn header<'response>(response: &'response Response, name: &str) -> Option<&'response str> {
    response
        .headers()
        .iter()
        .find(|header| header.0.eq_ignore_ascii_case(name))
        .map(|header| header.1.as_str())
}

/// Validates status and media type, then decodes one whole CBOR reply.
///
/// The body ceiling is enforced while reading, by the client.
///
/// # Errors
///
/// Returns a typed status, media-type, trailing-data, or decode failure.
fn decode_response<T: DeserializeOwned>(response: &Response) -> Result<T, HttpError> {
    let status = response.status();
    if !(200..300).contains(&status) {
        return Err(HttpError::Status {
            code: status,
            retry_after: header(response, "Retry-After").and_then(parse_retry_after),
        });
    }
    if header(response, "Content-Type").and_then(|value| value.split(';').next())
        != Some(MEDIA_TYPE)
    {
        return Err(HttpError::MediaType);
    }
    let bytes = response.body();
    let mut reader = Cursor::new(bytes);
    let value = ciborium::from_reader(&mut reader).map_err(HttpError::Decode)?;
    let fully_consumed =
        matches!(usize::try_from(reader.position()), Ok(consumed) if consumed == bytes.len());
    if !fully_consumed {
        return Err(HttpError::TrailingData);
    }
    Ok(value)
}

/// Parses a bounded `Retry-After` delta-seconds header into a duration.
fn parse_retry_after(value: &str) -> Option<Duration> {
    match value.parse::<u64>() {
        Ok(seconds) if seconds <= MAX_RETRY_AFTER_SECONDS => Some(Duration::from_secs(seconds)),
        _ => None,
    }
}

/// Credential provider failure with no secret-bearing payload.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CredentialError {
    /// Token was empty.
    #[error("credential token is empty")]
    Empty,
    /// The header value holds bytes invalid in an HTTP header; the refusal
    /// carries no token bytes.
    #[error("credential token contains invalid header bytes")]
    InvalidHeader,
    /// Token violated the RFC 6750 bearer-token grammar or resource ceiling.
    #[error("credential token is not a bounded RFC 6750 b64token")]
    InvalidToken,
    /// External refresh failed without exposing credential contents.
    #[error("credential refresh failed")]
    Refresh,
}

/// Thin-client transport failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum HttpError {
    /// Endpoint contains forbidden URL components.
    #[error("HTTP endpoint is not a clean service base URL")]
    InvalidEndpoint,
    /// Non-loopback service endpoint does not use TLS.
    #[error("non-loopback HTTP endpoint must use HTTPS")]
    InsecureEndpoint,
    /// Credential refresh or encoding failed.
    #[error("HTTP credential failed: {0}")]
    Credential(#[from] CredentialError),
    /// The shared operation deadline expired before credential acquisition
    /// completed. No HTTP request was dispatched by this operation.
    #[error("HTTP operation deadline expired during credential refresh")]
    CredentialTimeout,
    /// The shared deadline expired during the HTTP exchange or validation.
    /// The request may have committed remotely; retry only the exact operation.
    #[error("HTTP operation deadline expired; remote outcome may be unknown")]
    OperationTimeout,
    /// Request or response body exceeded its hard ceiling.
    #[error("HTTP body exceeds the bounded protocol ceiling")]
    BodyTooLarge,
    /// Response did not use the versioned CBOR media type.
    #[error("HTTP response has an unsupported media type")]
    MediaType,
    /// Service returned a non-success status; body is intentionally discarded.
    #[error("HTTP service returned status {code}")]
    Status {
        /// HTTP status code without a potentially sensitive response body.
        code: u16,
        /// Server-supplied delta delay, when syntactically valid.
        retry_after: Option<Duration>,
    },
    /// Request or response violated protocol invariants.
    #[error("HTTP protocol validation failed: {0}")]
    Protocol(ProtocolError),
    /// Request serialization failed.
    #[error("HTTP request encoding failed: {0}")]
    Encode(ciborium::ser::Error<std::io::Error>),
    /// Response deserialization failed.
    #[error("HTTP response decoding failed: {0}")]
    Decode(ciborium::de::Error<std::io::Error>),
    /// A valid CBOR value was followed by ambiguous trailing bytes.
    #[error("HTTP response contains trailing data after the CBOR value")]
    TrailingData,
    /// The bounded blocking pool refused the exchange; nothing was sent.
    #[error("HTTP worker pool refused the exchange: {0}")]
    Worker(SpawnError),
    /// Bounded network exchange failed.
    #[error("HTTP network failed: {0}")]
    Network(WireError),
}

impl HttpError {
    /// Returns whether retrying the exact idempotent sync request is reasonable.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match *self {
            Self::Network(_)
            | Self::Worker(_)
            | Self::CredentialTimeout
            | Self::OperationTimeout => true,
            Self::Status { code, .. } => matches!(code, 408 | 425 | 429 | 502 | 503 | 504),
            _ => false,
        }
    }

    /// Returns the server's bounded delta delay when one was supplied.
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        match *self {
            Self::Status { retry_after, .. } => retry_after,
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
