//! Bounded async HTTP/CBOR transport for thin Logical-DB clients.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{
    fmt,
    future::{Future, ready},
    io::Cursor,
    net::IpAddr,
    time::Duration,
};

use futures::TryStreamExt;
use logical_db_protocol::{ProtocolError, PullPage, PullRequest, PushRequest, PushResponse};
use logical_db_sync::{PullResult, Transport};
pub use reqwest::Url;
use reqwest::{
    Client, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, RETRY_AFTER},
    redirect::Policy,
};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

const MEDIA_TYPE: &str = "application/vnd.logical-db.v1+cbor";
const MAX_HTTP_BODY_BYTES: usize = 12_582_912;
/// Maximum server-directed retry delay accepted from an untrusted response.
pub const MAX_RETRY_AFTER_SECONDS: u64 = 3_600;

/// Refreshable credential boundary invoked before every request.
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
        let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| CredentialError::InvalidHeader)?;
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
pub struct HttpTransport<Credentials> {
    client: Client,
    endpoint: Url,
    credentials: Credentials,
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
        })
    }

    fn route(&self, relative: &str) -> Result<Url, HttpError> {
        self.endpoint
            .join(relative)
            .map_err(|_| HttpError::InvalidEndpoint)
    }

    async fn send<Request, Reply>(
        &mut self,
        route: &str,
        request: &Request,
    ) -> Result<Reply, HttpError>
    where
        Request: Serialize,
        Reply: DeserializeOwned,
    {
        let body = encode(request)?;
        let authorization = self.credentials.authorization().await?;
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
        decode_response(response).await
    }
}

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
        let response: PushResponse = self.send("v1/changes:push", &request).await?;
        response.validate(&request).map_err(HttpError::Protocol)?;
        Ok(response)
    }

    async fn pull(&mut self, request: PullRequest) -> Result<PullResult, Self::Error> {
        request.validate().map_err(HttpError::Protocol)?;
        let body = encode(&request)?;
        let authorization = self.credentials.authorization().await?;
        let response = self
            .client
            .post(self.route("v1/changes:pull")?)
            .header(AUTHORIZATION, authorization)
            .header(CONTENT_TYPE, MEDIA_TYPE)
            .header(ACCEPT, MEDIA_TYPE)
            .body(body)
            .send()
            .await
            .map_err(HttpError::Network)?;
        if response.status() == StatusCode::GONE {
            return Ok(PullResult::CheckpointExpired);
        }
        let page = decode_response::<PullPage>(response).await?;
        page.validate(request.scope).map_err(HttpError::Protocol)?;
        Ok(PullResult::Page(page))
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, HttpError> {
    let mut bytes = Vec::new();
    ciborium::into_writer(value, &mut bytes)
        .map_err(|error| HttpError::Encode(error.to_string()))?;
    if bytes.len() > MAX_HTTP_BODY_BYTES {
        return Err(HttpError::BodyTooLarge);
    }
    Ok(bytes)
}

async fn decode_response<T: DeserializeOwned>(response: Response) -> Result<T, HttpError> {
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
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.try_next().await.map_err(HttpError::Network)? {
        if bytes.len().saturating_add(chunk.len()) > MAX_HTTP_BODY_BYTES {
            return Err(HttpError::BodyTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    let mut reader = Cursor::new(bytes.as_slice());
    let value =
        ciborium::from_reader(&mut reader).map_err(|error| HttpError::Decode(error.to_string()))?;
    if reader.position() != bytes.len() as u64 {
        return Err(HttpError::TrailingData);
    }
    Ok(value)
}

fn parse_retry_after(value: &HeaderValue) -> Option<Duration> {
    value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds <= MAX_RETRY_AFTER_SECONDS)
        .map(Duration::from_secs)
}

/// Credential provider failure with no secret-bearing payload.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CredentialError {
    /// Token was empty.
    #[error("credential token is empty")]
    Empty,
    /// Token cannot be represented as an HTTP header.
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
            Self::Network(_) => true,
            Self::Status { code, .. } => matches!(*code, 408 | 425 | 429 | 502 | 503 | 504),
            _ => false,
        }
    }

    /// Returns the server's bounded delta delay when one was supplied.
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::Status { retry_after, .. } => *retry_after,
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
        routing::{get, post},
    };

    async fn endpoint() -> Url {
        let mut exact = Vec::with_capacity(MAX_HTTP_BODY_BYTES);
        exact.push(0x5a);
        exact.extend_from_slice(
            &u32::try_from(MAX_HTTP_BODY_BYTES - 5)
                .unwrap()
                .to_be_bytes(),
        );
        exact.resize(MAX_HTTP_BODY_BYTES, 0);
        let app = Router::new()
            .route(
                "/ok",
                get(|| async {
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, MEDIA_TYPE)
                        .body(Body::from(vec![0x18, 42]))
                        .unwrap()
                }),
            )
            .route(
                "/wrong-media",
                get(|| async {
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, "application/json")
                        .body(Body::from(vec![0]))
                        .unwrap()
                }),
            )
            .route(
                "/bad-status",
                get(|| async {
                    AxumResponse::builder()
                        .status(AxumStatus::TOO_MANY_REQUESTS)
                        .header(RETRY_AFTER, "7")
                        .body(Body::empty())
                        .unwrap()
                }),
            )
            .route(
                "/hostile-retry",
                get(|| async {
                    AxumResponse::builder()
                        .status(AxumStatus::SERVICE_UNAVAILABLE)
                        .header(RETRY_AFTER, u64::MAX.to_string())
                        .body(Body::empty())
                        .unwrap()
                }),
            )
            .route(
                "/too-large",
                get(|| async {
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, MEDIA_TYPE)
                        .body(Body::from(vec![0; MAX_HTTP_BODY_BYTES + 1]))
                        .unwrap()
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
                            .unwrap()
                    }
                }),
            )
            .route(
                "/trailing",
                get(|| async {
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, MEDIA_TYPE)
                        .body(Body::from(vec![0x18, 42, 0]))
                        .unwrap()
                }),
            )
            .route(
                "/gone/v1/changes:pull",
                post(|| async { (AxumStatus::GONE, Body::empty()) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}/").parse().unwrap()
    }

    #[test]
    fn remote_plaintext_and_url_credentials_are_refused() {
        let credential = StaticBearer::new("secret").unwrap();
        assert!(matches!(
            HttpTransport::new(Url::parse("http://example.com/").unwrap(), credential),
            Err(HttpError::InsecureEndpoint)
        ));
        let credential = StaticBearer::new("secret").unwrap();
        assert!(matches!(
            HttpTransport::new(
                Url::parse("https://user:pass@example.com/").unwrap(),
                credential
            ),
            Err(HttpError::InvalidEndpoint)
        ));
    }

    #[test]
    fn credential_debug_output_is_redacted() {
        let credential = StaticBearer::new("never-print-me").unwrap();
        let rendered = format!("{credential:?}");
        assert!(!rendered.contains("never-print-me"));
        assert!(rendered.contains("REDACTED"));
    }

    #[test]
    fn static_bearer_rejects_ambiguous_whitespace_padding_and_excess() {
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
    }

    #[test]
    fn cbor_request_encoding_is_nonempty_and_enforces_exact_body_edge() {
        let encoded = encode(&vec![1_u8, 2, 3]).unwrap();
        assert!(!encoded.is_empty());
        assert_eq!(
            ciborium::from_reader::<Vec<u8>, _>(encoded.as_slice()).unwrap(),
            vec![1, 2, 3]
        );
        let exact_payload = vec![1; MAX_HTTP_BODY_BYTES - 5];
        assert_eq!(
            encode(&serde_bytes::Bytes::new(&exact_payload))
                .unwrap()
                .len(),
            MAX_HTTP_BODY_BYTES
        );
        let excessive_payload = vec![1; MAX_HTTP_BODY_BYTES - 4];
        assert!(matches!(
            encode(&serde_bytes::Bytes::new(&excessive_payload)),
            Err(HttpError::BodyTooLarge)
        ));
    }

    #[tokio::test]
    async fn response_decoder_enforces_status_media_and_exact_stream_bounds() {
        let endpoint = endpoint().await;
        let client = Client::new();
        let response = client
            .get(endpoint.join("ok").unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(decode_response::<u8>(response).await.unwrap(), 42);
        let response = client
            .get(endpoint.join("bad-status").unwrap())
            .send()
            .await
            .unwrap();
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::Status {
                code: 429,
                retry_after: Some(delay),
            }) if delay == Duration::from_secs(7)
        ));
        let response = client
            .get(endpoint.join("hostile-retry").unwrap())
            .send()
            .await
            .unwrap();
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::Status {
                code: 503,
                retry_after: None,
            })
        ));
        let response = client
            .get(endpoint.join("wrong-media").unwrap())
            .send()
            .await
            .unwrap();
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::MediaType)
        ));
        let response = client
            .get(endpoint.join("too-large").unwrap())
            .send()
            .await
            .unwrap();
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::BodyTooLarge)
        ));
        let response = client
            .get(endpoint.join("exact").unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(
            decode_response::<serde_bytes::ByteBuf>(response)
                .await
                .unwrap()
                .len(),
            MAX_HTTP_BODY_BYTES - 5
        );
        let response = client
            .get(endpoint.join("trailing").unwrap())
            .send()
            .await
            .unwrap();
        assert!(matches!(
            decode_response::<u8>(response).await,
            Err(HttpError::TrailingData)
        ));
    }

    #[tokio::test]
    async fn pull_maps_only_http_gone_to_checkpoint_expiry() {
        use logical_db_model::{AccountId, OperationDigest, ReplicaId, Scope, SpaceId};

        let mut transport =
            HttpTransport::new(endpoint().await, StaticBearer::new("integration").unwrap())
                .unwrap();
        let request = PullRequest {
            protocol: logical_db_protocol::PROTOCOL_VERSION,
            scope: Scope::new(AccountId::new(), SpaceId::new()),
            replica: ReplicaId::new(),
            request: OperationDigest::new([1; 32]),
            after: None,
            max_changes: 1,
            max_bytes: 1_024,
        };
        transport.endpoint = transport.endpoint.join("gone/").unwrap();
        assert!(matches!(
            transport.pull(request).await,
            Ok(PullResult::CheckpointExpired)
        ));
    }
}
