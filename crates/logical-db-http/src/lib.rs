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

use lgwks_std::json::serde::{Serialize, de::DeserializeOwned};
use logical_db_protocol::{ProtocolError, PullPage, PullRequest, PushRequest, PushResponse};
use logical_db_sync::{PullResult, Transport};
pub use reqwest::Url;
use reqwest::{
    Client, Response, StatusCode,
    header::{ACCEPT, AUTHORIZATION, CONTENT_LENGTH, CONTENT_TYPE, HeaderValue, RETRY_AFTER},
    redirect::Policy,
};
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
            eprintln!("logical-db-http: new: Empty");
            return Err(CredentialError::Empty);
        }
        if token.len() > 4_096 || !valid_b64token(token.as_bytes()) {
            eprintln!("logical-db-http: new: InvalidToken");
            return Err(CredentialError::InvalidToken);
        }
        let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|error| {
            eprintln!("logical-db-http: CredentialError::InvalidHeader: {error}");
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
            eprintln!("logical-db-http: new: InvalidEndpoint");
            return Err(HttpError::InvalidEndpoint);
        }
        let loopback = endpoint.host_str().is_some_and(|host| {
            host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<IpAddr>()
                    .is_ok_and(|address| address.is_loopback())
        });
        if endpoint.scheme() != "https" && !(endpoint.scheme() == "http" && loopback) {
            eprintln!("logical-db-http: new: InsecureEndpoint");
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
        self.endpoint.join(relative).map_err(|error| {
            eprintln!("logical-db-http: HttpError::InvalidEndpoint: {error}");
            HttpError::InvalidEndpoint
        })
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
        eprintln!("logical-db-http: encode: BodyTooLarge");
        return Err(HttpError::BodyTooLarge);
    }
    Ok(bytes)
}

async fn decode_response<T: DeserializeOwned>(response: Response) -> Result<T, HttpError> {
    let mut response = response;
    let status = response.status();
    if !status.is_success() {
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(parse_retry_after);
        eprintln!("logical-db-http: decode_response: Status");
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
        eprintln!("logical-db-http: decode_response: MediaType");
        return Err(HttpError::MediaType);
    }
    if response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > MAX_HTTP_BODY_BYTES)
    {
        eprintln!("logical-db-http: decode_response: BodyTooLarge");
        return Err(HttpError::BodyTooLarge);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(HttpError::Network)? {
        if bytes.len().saturating_add(chunk.len()) > MAX_HTTP_BODY_BYTES {
            eprintln!("logical-db-http: decode_response: BodyTooLarge");
            return Err(HttpError::BodyTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    let mut reader = Cursor::new(bytes.as_slice());
    let value =
        ciborium::from_reader(&mut reader).map_err(|error| HttpError::Decode(error.to_string()))?;
    if reader.position() != bytes.len() as u64 {
        eprintln!("logical-db-http: decode_response: TrailingData");
        return Err(HttpError::TrailingData);
    }
    Ok(value)
}

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
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        lgwks_bot::rt::task::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}/").parse().unwrap()
    }

    fn test_runtime() -> lgwks_bot::Runtime {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
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

    #[test]
    fn response_decoder_enforces_status_media_and_exact_stream_bounds() {
        test_runtime().block_on(response_decoder_body());
    }

    async fn response_decoder_body() {
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

    #[test]
    fn pull_maps_only_http_gone_to_checkpoint_expiry() {
        test_runtime().block_on(pull_gone_body());
    }

    async fn pull_gone_body() {
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

    /// Serves one hand-written HTTP/1.1 response on a loopback port so a test
    /// controls the exact framing bytes rather than a framework's defaults.
    fn spawn_raw_server<F>(responder: F) -> Url
    where
        F: FnOnce(&mut std::net::TcpStream) + Send + 'static,
    {
        use std::{
            io::{Read, Write},
            net::TcpListener,
        };
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind raw server");
        let address = listener.local_addr().expect("raw server address");
        std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
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
                    Ok(got) => read += got,
                }
            }
            responder(&mut stream);
            let _ = stream.flush();
            let _ = stream.shutdown(std::net::Shutdown::Both);
        });
        format!("http://{address}/")
            .parse()
            .expect("raw server url")
    }

    fn write_bytes(stream: &mut std::net::TcpStream, bytes: &[u8]) {
        use std::io::Write;
        let _ = stream.write_all(bytes);
        let _ = stream.flush();
    }

    fn chunked_head() -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: {MEDIA_TYPE}\r\ntransfer-encoding: chunked\r\n\r\n"
        )
        .into_bytes()
    }

    fn chunk(stream: &mut std::net::TcpStream, data: &[u8]) {
        write_bytes(stream, format!("{:x}\r\n", data.len()).as_bytes());
        write_bytes(stream, data);
        write_bytes(stream, b"\r\n");
    }

    /// A CBOR unsigned 42 is exactly `0x18 0x2a`.
    const CBOR_FORTY_TWO: &[u8] = &[0x18, 0x2a];

    fn raw_client() -> Client {
        Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(30))
            .build()
            .expect("raw client")
    }

    #[test]
    fn a_chunked_response_without_a_content_length_is_decoded() {
        let endpoint = spawn_raw_server(|stream| {
            write_bytes(stream, &chunked_head());
            chunk(stream, CBOR_FORTY_TWO);
            write_bytes(stream, b"0\r\n\r\n");
        });
        let decoded = test_runtime().block_on(decode_forty_two(endpoint));
        assert_eq!(decoded, 42);
    }

    async fn decode_forty_two(endpoint: Url) -> u8 {
        let response = raw_client()
            .get(endpoint)
            .send()
            .await
            .expect("chunked request");
        assert!(
            response.headers().get(CONTENT_LENGTH).is_none(),
            "the server must not declare a length"
        );
        decode_response::<u8>(response).await.expect("decode")
    }

    #[test]
    fn a_chunked_body_crossing_the_ceiling_fails_closed_without_a_content_length() {
        let endpoint = spawn_raw_server(|stream| {
            write_bytes(stream, &chunked_head());
            let filler = vec![0u8; 64 * 1024];
            let mut sent = 0;
            while sent <= MAX_HTTP_BODY_BYTES + 64 * 1024 {
                chunk(stream, &filler);
                sent += filler.len();
            }
            write_bytes(stream, b"0\r\n\r\n");
        });
        assert!(matches!(
            test_runtime().block_on(ceiling_error(endpoint)),
            Err(HttpError::BodyTooLarge)
        ));
    }

    async fn ceiling_error(endpoint: Url) -> Result<u8, HttpError> {
        let response = raw_client()
            .get(endpoint)
            .send()
            .await
            .expect("over-ceiling request");
        decode_response::<u8>(response).await
    }

    #[test]
    fn a_declared_length_above_the_ceiling_is_refused_before_the_body_is_read() {
        let endpoint = spawn_raw_server(|stream| {
            write_bytes(
                stream,
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: {MEDIA_TYPE}\r\ncontent-length: {}\r\n\r\n",
                    MAX_HTTP_BODY_BYTES + 1
                )
                .as_bytes(),
            );
            chunk(stream, CBOR_FORTY_TWO);
        });
        assert!(matches!(
            test_runtime().block_on(ceiling_error(endpoint)),
            Err(HttpError::BodyTooLarge)
        ));
    }

    #[test]
    fn a_body_shorter_than_its_declared_length_is_an_error_never_a_hang() {
        let endpoint = spawn_raw_server(|stream| {
            write_bytes(
                stream,
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: {MEDIA_TYPE}\r\ncontent-length: 5\r\n\r\n"
                )
                .as_bytes(),
            );
            write_bytes(stream, CBOR_FORTY_TWO);
        });
        let started = std::time::Instant::now();
        let outcome = test_runtime().block_on(short_body(endpoint));
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "must not stall"
        );
        assert!(outcome.is_err(), "a truncated body must not decode");
    }

    async fn short_body(endpoint: Url) -> Result<u8, HttpError> {
        let response = raw_client()
            .get(endpoint)
            .send()
            .await
            .map_err(HttpError::Network)?;
        decode_response::<u8>(response).await
    }

    #[test]
    fn a_redirect_is_refused_and_its_target_is_never_contacted() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let followed = Arc::new(AtomicBool::new(false));
        let marker = Arc::clone(&followed);
        let target = spawn_raw_server(move |stream| {
            marker.store(true, Ordering::SeqCst);
            write_bytes(stream, &chunked_head());
            chunk(stream, CBOR_FORTY_TWO);
            write_bytes(stream, b"0\r\n\r\n");
        });
        let redirect = spawn_raw_server(move |stream| {
            write_bytes(
                stream,
                format!(
                    "HTTP/1.1 307 Temporary Redirect\r\nlocation: {target}\r\ncontent-length: 0\r\n\r\n"
                )
                .as_bytes(),
            );
        });
        let error = test_runtime().block_on(pull_from(redirect));
        assert!(
            matches!(error, Err(HttpError::Status { code: 307, .. })),
            "a redirect must surface as a status error, got {error:?}"
        );
        assert!(
            !followed.load(Ordering::SeqCst),
            "the transport followed a redirect it was told to refuse"
        );
    }

    async fn pull_from(endpoint: Url) -> Result<PullResult, HttpError> {
        use logical_db_model::{AccountId, OperationDigest, ReplicaId, Scope, SpaceId};
        let mut transport =
            HttpTransport::new(endpoint, StaticBearer::new("blackbox").expect("bearer"))
                .expect("transport");
        transport
            .pull(PullRequest {
                protocol: logical_db_protocol::PROTOCOL_VERSION,
                scope: Scope::new(AccountId::new(), SpaceId::new()),
                replica: ReplicaId::new(),
                request: OperationDigest::new([2; 32]),
                after: None,
                max_changes: 1,
                max_bytes: 1_024,
            })
            .await
    }

    #[test]
    fn a_stalled_response_is_released_by_the_request_timeout() {
        let endpoint = spawn_raw_server(|stream| {
            write_bytes(stream, &chunked_head());
            std::thread::sleep(Duration::from_secs(45));
        });
        let started = std::time::Instant::now();
        let outcome = test_runtime().block_on(pull_from(endpoint));
        let elapsed = started.elapsed();
        assert!(outcome.is_err(), "a stalled response must not succeed");
        assert!(
            elapsed >= Duration::from_secs(25) && elapsed < Duration::from_secs(44),
            "the client timeout must bound the stall, elapsed {elapsed:?}"
        );
    }

    #[test]
    fn cancelling_an_in_flight_request_leaves_the_transport_usable() {
        let slow = spawn_raw_server(|stream| {
            write_bytes(stream, &chunked_head());
            chunk(stream, &[0x18]);
            std::thread::sleep(Duration::from_secs(20));
        });
        let fast = spawn_raw_server(|stream| {
            write_bytes(stream, &chunked_head());
            chunk(stream, CBOR_FORTY_TWO);
            write_bytes(stream, b"0\r\n\r\n");
        });
        let runtime = test_runtime();
        runtime.block_on(cancel_then_succeed(slow, fast));
    }

    async fn cancel_then_succeed(slow: Url, fast: Url) {
        let pending = lgwks_bot::rt::task::spawn(pull_from(slow));
        lgwks_bot::rt::task::yield_now().await;
        pending.abort();
        let decoded = decode_forty_two(fast).await;
        assert_eq!(decoded, 42, "the transport must stay usable");
    }
}
