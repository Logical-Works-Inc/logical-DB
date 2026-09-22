//! Bounded async HTTP/CBOR transport for thin Logical-DB clients.
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
use lgwks_std::json::serde::{Serialize, de::DeserializeOwned};
use logical_db_protocol::{
    BaselinePage, BaselineRequest, ProtocolError, PullPage, PullRequest, PushRequest, PushResponse,
};
use logical_db_sync::{BaselineResult, PullResult, Transport};
pub use reqwest::Url;
use reqwest::{
    Client, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, RETRY_AFTER},
    redirect::Policy,
};
use thiserror::Error;

/// Versioned CBOR media type used on every request and response.
const MEDIA_TYPE: &str = "application/vnd.logical-db.v1+cbor";
/// Hard ceiling on one encoded request or response body.
const MAX_HTTP_BODY_BYTES: usize = 12_582_912;
/// Wall-clock ceiling covering one complete exchange.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum server-directed retry delay accepted from an untrusted response.
pub const MAX_RETRY_AFTER_SECONDS: u64 = 3_600;

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
    ) -> impl Future<Output = Result<HeaderValue, CredentialError>> + Send;
}

/// Static bearer credential for hosts that manage refresh externally.
pub struct StaticBearer {
    /// Sensitive `Authorization` header value retained by this provider.
    value: HeaderValue,
}

impl StaticBearer {
    /// Builds a sensitive bearer header from a nonempty token.
    ///
    /// # Errors
    ///
    /// Refuses empty tokens or bytes invalid in an HTTP header.
    pub fn new(token: &str) -> Result<Self, CredentialError> {
        if token.is_empty() {
            return Err(CredentialError::Empty);
        }
        if token.len() > 4_096 || !valid_b64token(token.as_bytes()) {
            return Err(CredentialError::InvalidToken);
        }
        let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| {
            CredentialError::InvalidHeader {
                context: "BearerCredential::new",
            }
        })?;
        value.set_sensitive(true);
        Ok(Self { value })
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
    ) -> impl Future<Output = Result<HeaderValue, CredentialError>> + Send {
        ready(Ok(self.value.clone()))
    }
}

/// Reusable pooled thin-client transport.
///
/// Each push, pull, or baseline exchange has one 30-second deadline covering
/// request encoding, credential refresh, HTTP headers, body consumption, and
/// response validation. Dropping the operation cancels its owned local work;
/// after dispatch, neither cancellation nor timeout implies remote rollback.
/// Timer enforcement requires a running executor and cooperative providers.
pub struct HttpTransport<Credentials> {
    /// Pooled HTTP client with the transport's fixed timeouts and no redirects.
    client: Client,
    /// Base service endpoint; relative routes are joined onto it.
    endpoint: Url,
    /// Credential provider invoked before every dispatch.
    credentials: Credentials,
    /// Per-exchange deadline covering encode, refresh, and body consumption.
    operation_timeout: Duration,
}

impl<Credentials> HttpTransport<Credentials>
where
    Credentials: CredentialProvider,
{
    /// Creates a bounded transport. Production endpoints must use HTTPS;
    /// loopback HTTP remains available for process-local integration tests.
    ///
    /// # Errors
    ///
    /// Refuses credentials in URLs, queries/fragments, non-loopback plaintext,
    /// or client construction failures.
    pub fn new(mut endpoint: Url, credentials: Credentials) -> Result<Self, HttpError> {
        if !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(HttpError::InvalidEndpoint);
        }
        let loopback = endpoint.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        });
        if endpoint.scheme() != "https" && !(endpoint.scheme() == "http" && loopback) {
            return Err(HttpError::InsecureEndpoint);
        }
        if !endpoint.path().ends_with('/') {
            let path = format!("{}/", endpoint.path());
            endpoint.set_path(&path);
        }
        let mut builder = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .redirect(Policy::none())
            .pool_idle_timeout(Duration::from_secs(90));
        if loopback {
            builder = builder.no_proxy();
        }
        let client = builder.build().map_err(HttpError::Network)?;
        Ok(Self {
            client,
            endpoint,
            credentials,
            operation_timeout: OPERATION_TIMEOUT,
        })
    }

    /// Joins a relative route onto the configured endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`HttpError::InvalidEndpoint`] when the join cannot form a URL.
    fn route(&self, relative: &str) -> Result<Url, HttpError> {
        self.endpoint
            .join(relative)
            .map_err(|_| HttpError::InvalidEndpoint)
    }

    /// Dispatches one encoded request under the operation deadline.
    ///
    /// # Errors
    ///
    /// Returns a typed credential, network, decode, or deadline failure.
    async fn send<Request, Reply, Decode, Decoded>(
        &mut self,
        route: &str,
        request: &Request,
        decode: Decode,
    ) -> Result<Reply, HttpError>
    where
        Request: Serialize,
        Decode: FnOnce(Response) -> Decoded,
        Decoded: Future<Output = Result<Reply, HttpError>>,
    {
        let deadline = Instant::now()
            .checked_add(self.operation_timeout)
            .ok_or(HttpError::OperationTimeout)?;
        let body = encode(request)?;
        let mut authorization = within_deadline(deadline, HttpError::CredentialTimeout, async {
            self.credentials.authorization().await
        })
        .await??;
        authorization.set_sensitive(true);
        within_deadline(deadline, HttpError::OperationTimeout, async {
            let response = self
                .client
                .post(self.route(route)?)
                .header(AUTHORIZATION, authorization)
                .header(CONTENT_TYPE, MEDIA_TYPE)
                .header(ACCEPT, MEDIA_TYPE)
                .body(body)
                .send()
                .await
                .map_err(HttpError::Network)?;
            decode(response).await
        })
        .await?
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
        let expected = &request;
        self.send("v1/changes:push", &request, move |response| async move {
            let response: PushResponse = decode_response(response).await?;
            response.validate(expected).map_err(HttpError::Protocol)?;
            Ok(response)
        })
        .await
    }

    async fn pull(&mut self, request: PullRequest) -> Result<PullResult, Self::Error> {
        request.validate().map_err(HttpError::Protocol)?;
        let scope = request.scope;
        self.send("v1/changes:pull", &request, move |response| async move {
            if response.status() == StatusCode::GONE {
                return Ok(PullResult::CheckpointExpired);
            }
            if response.status() == StatusCode::CONFLICT {
                return Ok(PullResult::BaselineRequired);
            }
            let page = decode_response::<PullPage>(response).await?;
            page.validate(scope).map_err(HttpError::Protocol)?;
            Ok(PullResult::Page(page))
        })
        .await
    }

    async fn baseline(&mut self, request: BaselineRequest) -> Result<BaselineResult, Self::Error> {
        request.validate().map_err(HttpError::Protocol)?;
        let scope = request.scope;
        self.send(
            "v1/recovery:baseline",
            &request,
            move |response| async move {
                if response.status() == StatusCode::NOT_FOUND {
                    return Ok(BaselineResult::Unavailable);
                }
                let page = decode_response::<BaselinePage>(response).await?;
                page.validate_page(scope).map_err(HttpError::Protocol)?;
                Ok(BaselineResult::Artifact(page))
            },
        )
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
    ciborium::into_writer(value, &mut bytes)
        .map_err(|error| HttpError::Encode(error.to_string()))?;
    if bytes.len() > MAX_HTTP_BODY_BYTES {
        return Err(HttpError::BodyTooLarge);
    }
    Ok(bytes)
}

/// Validates status, media type, and body bounds, then decodes one CBOR reply.
///
/// # Errors
///
/// Returns a typed status, media-type, body, network, or decode failure.
async fn decode_response<T: DeserializeOwned>(response: Response) -> Result<T, HttpError> {
    let mut response = response;
    let status = response.status();
    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(parse_retry_after);
        return Err(HttpError::Status {
            code: status.as_u16(),
            retry_after,
        });
    }
    if response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        != Some(MEDIA_TYPE)
    {
        return Err(HttpError::MediaType);
    }
    if response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_HTTP_BODY_BYTES)
    {
        return Err(HttpError::BodyTooLarge);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(HttpError::Network)? {
        if bytes.len().saturating_add(chunk.len()) > MAX_HTTP_BODY_BYTES {
            return Err(HttpError::BodyTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    let mut reader = Cursor::new(bytes.as_slice());
    let value =
        ciborium::from_reader(&mut reader).map_err(|error| HttpError::Decode(error.to_string()))?;
    if reader.position() != u64::try_from(bytes.len()).unwrap_or(u64::MAX) {
        return Err(HttpError::TrailingData);
    }
    Ok(value)
}

/// Parses a bounded `Retry-After` delta-seconds header into a duration.
fn parse_retry_after(value: &HeaderValue) -> Option<Duration> {
    let seconds = value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    seconds
        .filter(|seconds| *seconds <= MAX_RETRY_AFTER_SECONDS)
        .map(Duration::from_secs)
}

/// Credential provider failure with no secret-bearing payload.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum CredentialError {
    /// Token was empty.
    #[error("credential token is empty")]
    Empty,
    /// Token cannot be represented as an HTTP header.
    #[error("credential token contains invalid header bytes")]
    InvalidHeader {
        /// Credential construction boundary.
        context: &'static str,
    },
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
    Encode(String),
    /// Response deserialization failed.
    #[error("HTTP response decoding failed: {0}")]
    Decode(String),
    /// A valid CBOR value was followed by ambiguous trailing bytes.
    #[error("HTTP response contains trailing data after the CBOR value")]
    TrailingData,
    /// Bounded network request failed.
    #[error("HTTP network failed: {0}")]
    Network(reqwest::Error),
}

impl HttpError {
    /// Returns whether retrying the exact idempotent sync request is reasonable.
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        match self {
            &Self::Network(_) | &Self::CredentialTimeout | &Self::OperationTimeout => true,
            &Self::Status { code, .. } => matches!(code, 408 | 425 | 429 | 502 | 503 | 504),
            _ => false,
        }
    }

    /// Returns the server's bounded delta delay when one was supplied.
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        match self {
            &Self::Status { retry_after, .. } => retry_after,
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        body::Body,
        http::{Response as AxumResponse, StatusCode as AxumStatus},
        response::IntoResponse,
        routing::{get, post},
    };

    mod deadlines;

    async fn endpoint() -> Result<Url, Box<dyn std::error::Error>> {
        let mut exact = Vec::with_capacity(MAX_HTTP_BODY_BYTES);
        exact.push(0x5a);
        exact.extend_from_slice(&u32::try_from(MAX_HTTP_BODY_BYTES - 5)?.to_be_bytes());
        exact.resize(MAX_HTTP_BODY_BYTES, 0);
        let app = Router::new()
            .route(
                "/ok",
                get(|| async {
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, MEDIA_TYPE)
                        .body(Body::from(vec![0x18, 42]))
                        .unwrap_or_else(|error| {
                            (AxumStatus::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
                        })
                }),
            )
            .route(
                "/wrong-media",
                get(|| async {
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, "application/json")
                        .body(Body::from(vec![0]))
                        .unwrap_or_else(|error| {
                            (AxumStatus::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
                        })
                }),
            )
            .route(
                "/bad-status",
                get(|| async {
                    AxumResponse::builder()
                        .status(AxumStatus::TOO_MANY_REQUESTS)
                        .header(RETRY_AFTER, "7")
                        .body(Body::empty())
                        .unwrap_or_else(|error| {
                            (AxumStatus::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
                        })
                }),
            )
            .route(
                "/hostile-retry",
                get(|| async {
                    AxumResponse::builder()
                        .status(AxumStatus::SERVICE_UNAVAILABLE)
                        .header(RETRY_AFTER, u64::MAX.to_string())
                        .body(Body::empty())
                        .unwrap_or_else(|error| {
                            (AxumStatus::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
                        })
                }),
            )
            .route(
                "/too-large",
                get(|| async {
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, MEDIA_TYPE)
                        .body(Body::from(vec![0; MAX_HTTP_BODY_BYTES + 1]))
                        .unwrap_or_else(|error| {
                            (AxumStatus::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
                        })
                }),
            )
            .route(
                "/exact",
                get(move || {
                    let exact = exact.clone();
                    async move {
                        AxumResponse::builder()
                            .header(CONTENT_TYPE, MEDIA_TYPE)
                            .body(Body::from(exact))
                            .unwrap_or_else(|error| {
                                (AxumStatus::INTERNAL_SERVER_ERROR, error.to_string())
                                    .into_response()
                            })
                    }
                }),
            )
            .route(
                "/trailing",
                get(|| async {
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, MEDIA_TYPE)
                        .body(Body::from(vec![0x18, 42, 0]))
                        .unwrap_or_else(|error| {
                            (AxumStatus::INTERNAL_SERVER_ERROR, error.to_string()).into_response()
                        })
                }),
            )
            .route(
                "/gone/v1/changes:pull",
                post(|| async { (AxumStatus::GONE, Body::empty()) }),
            );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        // Keep the server task owned by this test; dropping the handle at test
        // end aborts the loop instead of leaving a runtime task detached.
        let _server = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, app).await;
            drop(serve_result);
        });
        Ok(format!("http://{address}/").parse()?)
    }

    fn test_runtime() -> Result<lgwks_bot::Runtime, Box<dyn std::error::Error>> {
        Ok(lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()?)
    }

    #[test]
    fn remote_plaintext_and_url_credentials_are_refused() -> Result<(), Box<dyn std::error::Error>>
    {
        let credential = StaticBearer::new("secret")?;
        assert!(matches!(
            HttpTransport::new(Url::parse("http://example.com/")?, credential),
            Err(HttpError::InsecureEndpoint)
        ));
        let credential = StaticBearer::new("secret")?;
        assert!(matches!(
            HttpTransport::new(Url::parse("https://user:pass@example.com/")?, credential),
            Err(HttpError::InvalidEndpoint)
        ));
        Ok(())
    }

    #[test]
    fn credential_debug_output_is_redacted() -> Result<(), Box<dyn std::error::Error>> {
        let credential = StaticBearer::new("never-print-me")?;
        let rendered = format!("{credential:?}");
        assert!(!rendered.contains("never-print-me"));
        assert!(rendered.contains("REDACTED"));
        Ok(())
    }

    #[test]
    fn static_bearer_rejects_ambiguous_whitespace_padding_and_excess()
    -> Result<(), Box<dyn std::error::Error>> {
        assert!(matches!(
            StaticBearer::new("space inside"),
            Err(CredentialError::InvalidToken)
        ));
        assert!(matches!(
            StaticBearer::new("valid==then-invalid"),
            Err(CredentialError::InvalidToken)
        ));
        assert!(StaticBearer::new("valid-token_123+/==").is_ok());
        assert!(matches!(
            StaticBearer::new(&"a".repeat(4_097)),
            Err(CredentialError::InvalidToken)
        ));
        Ok(())
    }

    #[test]
    fn cbor_request_encoding_is_nonempty_and_enforces_exact_body_edge()
    -> Result<(), Box<dyn std::error::Error>> {
        let encoded = encode(&vec![1_u8, 2, 3])?;
        assert!(!encoded.is_empty());
        assert_eq!(
            ciborium::from_reader::<Vec<u8>, _>(encoded.as_slice())?,
            vec![1, 2, 3]
        );
        let exact_payload = vec![1; MAX_HTTP_BODY_BYTES - 5];
        assert_eq!(
            encode(&serde_bytes::Bytes::new(&exact_payload))?.len(),
            MAX_HTTP_BODY_BYTES
        );
        let excessive_payload = vec![1; MAX_HTTP_BODY_BYTES - 4];
        assert!(matches!(
            encode(&serde_bytes::Bytes::new(&excessive_payload)),
            Err(HttpError::BodyTooLarge)
        ));
        Ok(())
    }

    #[test]
    fn response_decoder_enforces_status_media_and_exact_stream_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(response_decoder_body())?;
        Ok(())
    }

    async fn response_decoder_body() -> Result<(), Box<dyn std::error::Error>> {
        let endpoint = endpoint().await?;
        let client = Client::new();
        let response = client.get(endpoint.join("ok")?).send().await?;
        assert_eq!(decode_response::<u8>(response).await?, 42);
        let response = client.get(endpoint.join("bad-status")?).send().await?;
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::Status {
                code: 429,
                retry_after: Some(delay),
            }) if delay == Duration::from_secs(7)
        ));
        let response = client.get(endpoint.join("hostile-retry")?).send().await?;
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::Status {
                code: 503,
                retry_after: None,
            })
        ));
        let response = client.get(endpoint.join("wrong-media")?).send().await?;
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::MediaType)
        ));
        let response = client.get(endpoint.join("too-large")?).send().await?;
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::BodyTooLarge)
        ));
        let response = client.get(endpoint.join("exact")?).send().await?;
        assert_eq!(
            decode_response::<serde_bytes::ByteBuf>(response)
                .await?
                .len(),
            MAX_HTTP_BODY_BYTES - 5
        );
        let response = client.get(endpoint.join("trailing")?).send().await?;
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::TrailingData)
        ));
        Ok(())
    }

    #[test]
    fn pull_maps_only_http_gone_to_checkpoint_expiry() -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(pull_gone_body())?;
        Ok(())
    }

    async fn pull_gone_body() -> Result<(), Box<dyn std::error::Error>> {
        use logical_db_model::{AccountId, OperationDigest, ReplicaId, Scope, SpaceId};

        let mut transport =
            HttpTransport::new(endpoint().await?, StaticBearer::new("integration")?)?;
        let request = PullRequest::new(
            logical_db_protocol::PROTOCOL_VERSION,
            Scope::new(AccountId::new(), SpaceId::new()),
            ReplicaId::new(),
            OperationDigest::new([1; 32]),
            None,
            1,
            1_024,
        );
        transport.endpoint = transport.endpoint.join("gone/")?;
        assert!(matches!(
            transport.pull(request).await,
            Ok(PullResult::CheckpointExpired)
        ));
        Ok(())
    }

    /// Serves one hand-written HTTP/1.1 response on a loopback port so a test
    /// controls the exact framing bytes rather than a framework's defaults.
    /// Joins its fixture thread on drop so no OS thread is left unjoined.
    struct RawServerHandle {
        /// Loopback address the fixture thread is bound to.
        address: std::net::SocketAddr,
        /// Fixture thread; joined on drop after a shutdown poke.
        handle: Option<std::thread::JoinHandle<()>>,
        /// Cooperative stop flag checked around `accept`.
        shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }

    impl Drop for RawServerHandle {
        fn drop(&mut self) {
            self.shutdown
                .store(true, std::sync::atomic::Ordering::SeqCst);
            if let Some(handle) = self.handle.take() {
                // Unblock `accept` and any `park_timeout` stall so the fixture
                // thread can observe shutdown and exit before the join.
                drop(std::net::TcpStream::connect(self.address));
                handle.thread().unpark();
                drop(handle.join());
            }
        }
    }

    fn spawn_raw_server<F>(
        responder: F,
    ) -> Result<(Url, RawServerHandle), Box<dyn std::error::Error>>
    where
        F: FnOnce(&mut std::net::TcpStream) + Send + 'static,
    {
        use std::{
            io::{Read, Write},
            net::TcpListener,
            sync::{
                Arc,
                atomic::{AtomicBool, Ordering},
            },
        };
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_flag = Arc::clone(&shutdown);
        let handle = std::thread::Builder::new()
            .name("logical-db-http-raw-fixture".into())
            .spawn(move || {
                listener.set_nonblocking(true).unwrap_or(());
                loop {
                    if thread_flag.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok((mut stream, _)) = listener.accept() else {
                        std::thread::park_timeout(std::time::Duration::from_millis(10));
                        continue;
                    };
                    drop(stream.set_nonblocking(false));
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while stream.read(&mut byte).unwrap_or(0) == 1 {
                        head.push(byte[0]);
                        if head.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                    let declared: usize = String::from_utf8_lossy(&head)
                        .lines()
                        .find_map(|line| {
                            let lowered = line.to_ascii_lowercase();
                            let (_, value) = lowered.split_once("content-length:")?;
                            value.trim().parse().ok()
                        })
                        .unwrap_or(0);
                    let mut body = vec![0u8; declared];
                    let mut read = 0;
                    while read < declared {
                        match stream.read(&mut body[read..]) {
                            Ok(0) | Err(_) => break,
                            Ok(got) => read = read.saturating_add(got),
                        }
                    }
                    responder(&mut stream);
                    drop(stream.flush());
                    drop(stream.shutdown(std::net::Shutdown::Both));
                    return;
                }
            })?;
        Ok((
            format!("http://{address}/").parse()?,
            RawServerHandle {
                address,
                handle: Some(handle),
                shutdown,
            },
        ))
    }

    fn write_bytes(stream: &mut std::net::TcpStream, bytes: &[u8]) {
        use std::io::Write;
        drop(stream.write_all(bytes));
        drop(stream.flush());
    }

    fn chunked_head() -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {MEDIA_TYPE}\r\ntransfer-encoding: chunked\r\n\r\n"
        )
        .into_bytes()
    }

    fn chunk(stream: &mut std::net::TcpStream, data: &[u8]) {
        write_bytes(&mut *stream, format!("{:x}\r\n", data.len()).as_bytes());
        write_bytes(&mut *stream, data);
        write_bytes(&mut *stream, b"\r\n");
    }

    /// A CBOR unsigned 42 is exactly `0x18 0x2a`.
    const CBOR_FORTY_TWO: &[u8] = &[0x18, 0x2a];

    fn raw_client() -> Result<Client, HttpError> {
        Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(HttpError::Network)
    }

    #[test]
    fn a_chunked_response_without_a_content_length_is_decoded()
    -> Result<(), Box<dyn std::error::Error>> {
        let (endpoint, _server) = spawn_raw_server(|stream| {
            write_bytes(&mut *stream, &chunked_head());
            chunk(&mut *stream, CBOR_FORTY_TWO);
            write_bytes(&mut *stream, b"0\r\n\r\n");
        })?;
        let decoded = test_runtime()?.block_on(decode_forty_two(endpoint))?;
        assert_eq!(decoded, 42, "a chunked payload decodes to 42");
        Ok(())
    }

    async fn decode_forty_two(endpoint: Url) -> Result<u8, Box<dyn std::error::Error>> {
        let response = raw_client()?
            .get(endpoint)
            .send()
            .await
            .map_err(HttpError::Network)?;
        assert!(
            response.headers().get(CONTENT_LENGTH).is_none(),
            "the server must not declare a length"
        );
        decode_response::<u8>(response).await.map_err(Into::into)
    }

    #[test]
    fn a_chunked_body_crossing_the_ceiling_fails_closed_without_a_content_length()
    -> Result<(), Box<dyn std::error::Error>> {
        let (endpoint, _server) = spawn_raw_server(|stream| {
            write_bytes(&mut *stream, &chunked_head());
            let filler = vec![0u8; 64 * 1024];
            let mut sent = 0;
            while sent <= MAX_HTTP_BODY_BYTES + 64 * 1024 {
                chunk(&mut *stream, &filler);
                sent += filler.len();
            }
            write_bytes(&mut *stream, b"0\r\n\r\n");
        })?;
        assert!(matches!(
            test_runtime()?.block_on(ceiling_error(endpoint)),
            Err(HttpError::BodyTooLarge)
        ));
        Ok(())
    }

    async fn ceiling_error(endpoint: Url) -> Result<u8, HttpError> {
        let response = raw_client()?
            .get(endpoint)
            .send()
            .await
            .map_err(HttpError::Network)?;
        decode_response::<u8>(response).await
    }

    #[test]
    fn a_declared_length_above_the_ceiling_is_refused_before_the_body_is_read()
    -> Result<(), Box<dyn std::error::Error>> {
        let (endpoint, _server) = spawn_raw_server(|stream| {
            write_bytes(
                &mut *stream,
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: {MEDIA_TYPE}\r\ncontent-length: {}\r\n\r\n",
                    MAX_HTTP_BODY_BYTES + 1
                )
                .as_bytes(),
            );
            chunk(&mut *stream, CBOR_FORTY_TWO);
        })?;
        assert!(matches!(
            test_runtime()?.block_on(ceiling_error(endpoint)),
            Err(HttpError::BodyTooLarge)
        ));
        Ok(())
    }

    #[test]
    fn a_body_shorter_than_its_declared_length_is_an_error_never_a_hang()
    -> Result<(), Box<dyn std::error::Error>> {
        let (endpoint, _server) = spawn_raw_server(|stream| {
            write_bytes(
                &mut *stream,
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: {MEDIA_TYPE}\r\ncontent-length: 5\r\n\r\n"
                )
                .as_bytes(),
            );
            write_bytes(&mut *stream, CBOR_FORTY_TWO);
        })?;
        let started = std::time::Instant::now();
        let outcome = test_runtime()?.block_on(short_body(endpoint));
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "must not stall"
        );
        assert!(outcome.is_err(), "a truncated body must not decode");
        Ok(())
    }

    async fn short_body(endpoint: Url) -> Result<u8, HttpError> {
        let response = raw_client()?
            .get(endpoint)
            .send()
            .await
            .map_err(HttpError::Network)?;
        decode_response::<u8>(response).await
    }

    #[test]
    fn a_redirect_is_refused_and_its_target_is_never_contacted()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let followed = Arc::new(AtomicBool::new(false));
        let marker = Arc::clone(&followed);
        let (target, _target_server) = spawn_raw_server(move |stream| {
            marker.store(true, Ordering::SeqCst);
            write_bytes(&mut *stream, &chunked_head());
            chunk(&mut *stream, CBOR_FORTY_TWO);
            write_bytes(&mut *stream, b"0\r\n\r\n");
        })?;
        let (redirect, _redirect_server) = spawn_raw_server(move |stream| {
            write_bytes(&mut *stream,
                format!(
                    "HTTP/1.1 307 Temporary Redirect\r\nlocation: {target}\r\ncontent-length: 0\r\n\r\n"
                )
                .as_bytes(),
            );
        })?;
        let error = test_runtime()?.block_on(pull_from(redirect));
        assert!(
            matches!(error, Err(HttpError::Status { code: 307, .. })),
            "a redirect must surface as a status error, got {error:?}"
        );
        assert!(
            !followed.load(Ordering::SeqCst),
            "the transport followed a redirect it was told to refuse"
        );
        Ok(())
    }

    async fn pull_from(endpoint: Url) -> Result<PullResult, HttpError> {
        use logical_db_model::{AccountId, OperationDigest, ReplicaId, Scope, SpaceId};
        let mut transport = HttpTransport::new(endpoint, StaticBearer::new("blackbox")?)?;
        transport
            .pull(PullRequest::new(
                logical_db_protocol::PROTOCOL_VERSION,
                Scope::new(AccountId::new(), SpaceId::new()),
                ReplicaId::new(),
                OperationDigest::new([2; 32]),
                None,
                1,
                1_024,
            ))
            .await
    }

    #[test]
    fn a_stalled_response_is_released_by_the_request_timeout()
    -> Result<(), Box<dyn std::error::Error>> {
        let (endpoint, _server) = spawn_raw_server(|stream| {
            write_bytes(&mut *stream, &chunked_head());
            std::thread::park_timeout(Duration::from_secs(45));
        })?;
        let started = std::time::Instant::now();
        let outcome = test_runtime()?.block_on(pull_from(endpoint));
        let elapsed = started.elapsed();
        assert!(outcome.is_err(), "a stalled response must not succeed");
        assert!(
            elapsed >= Duration::from_secs(25) && elapsed < Duration::from_secs(44),
            "the client timeout must bound the stall, elapsed {elapsed:?}"
        );
        Ok(())
    }

    #[test]
    fn cancelling_an_in_flight_request_leaves_the_transport_usable()
    -> Result<(), Box<dyn std::error::Error>> {
        let (slow, _slow_server) = spawn_raw_server(|stream| {
            write_bytes(&mut *stream, &chunked_head());
            chunk(&mut *stream, &[0x18]);
            std::thread::park_timeout(Duration::from_secs(20));
        })?;
        let (fast, _fast_server) = spawn_raw_server(|stream| {
            write_bytes(&mut *stream, &chunked_head());
            chunk(&mut *stream, CBOR_FORTY_TWO);
            write_bytes(&mut *stream, b"0\r\n\r\n");
        })?;
        let runtime = test_runtime()?;
        runtime.block_on(cancel_then_succeed(slow, fast))?;
        Ok(())
    }

    async fn cancel_then_succeed(slow: Url, fast: Url) -> Result<(), Box<dyn std::error::Error>> {
        let pending = lgwks_bot::rt::task::spawn(pull_from(slow));
        lgwks_bot::rt::task::yield_now().await;
        pending.abort();
        let decoded = decode_forty_two(fast).await?;
        assert_eq!(decoded, 42, "the transport must stay usable");
        Ok(())
    }
}
