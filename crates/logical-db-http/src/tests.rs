//! Black-box tests of the HTTP transport against axum and hand-framed fixtures.

use super::*;
use axum::{
    Router,
    body::Body,
    http::{
        Response as AxumResponse, StatusCode as AxumStatus,
        header::{CONTENT_TYPE, RETRY_AFTER},
    },
    response::IntoResponse,
    routing::{get, post},
};
use lgwks_bot::rt::task::JoinSet as TaskSet;

mod deadlines;

/// GETs `url` with the transport's own pooled client on the blocking pool.
async fn fetch(url: String) -> Result<Response, HttpError> {
    let client = pooled_client()?;
    let exchange = try_spawn_blocking(move || client.get_with(&url, client.options()))
        .map_err(HttpError::Worker)?;
    exchange.await.map_err(wire_failure)
}

/// GETs `url` and decodes the reply exactly as the transport does.
async fn fetch_decoded<T: DeserializeOwned>(url: String) -> Result<T, HttpError> {
    decode_response(&fetch(url).await?)
}

/// A fixture route's reply: the built response, or a 500 naming why the builder refused it.
fn respond(built: Result<AxumResponse<Body>, axum::http::Error>) -> axum::response::Response {
    match built {
        Ok(response) => response,
        Err(error) => (AxumStatus::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn endpoint() -> Result<(Endpoint, TaskSet<()>), Box<dyn std::error::Error>> {
    let mut exact = Vec::with_capacity(MAX_HTTP_BODY_BYTES);
    exact.push(0x5a);
    exact.extend_from_slice(&u32::try_from(MAX_HTTP_BODY_BYTES - 5)?.to_be_bytes());
    exact.resize(MAX_HTTP_BODY_BYTES, 0);
    let app = Router::new()
        .route(
            "/ok",
            get(|| async {
                respond(
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, MEDIA_TYPE)
                        .body(Body::from(vec![0x18, 42])),
                )
            }),
        )
        .route(
            "/wrong-media",
            get(|| async {
                respond(
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, "application/json")
                        .body(Body::from(vec![0])),
                )
            }),
        )
        .route(
            "/bad-status",
            get(|| async {
                respond(
                    AxumResponse::builder()
                        .status(AxumStatus::TOO_MANY_REQUESTS)
                        .header(RETRY_AFTER, "7")
                        .body(Body::empty()),
                )
            }),
        )
        .route(
            "/hostile-retry",
            get(|| async {
                respond(
                    AxumResponse::builder()
                        .status(AxumStatus::SERVICE_UNAVAILABLE)
                        .header(RETRY_AFTER, u64::MAX.to_string())
                        .body(Body::empty()),
                )
            }),
        )
        .route(
            "/too-large",
            get(|| async {
                respond(
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, MEDIA_TYPE)
                        .body(Body::from(vec![0; MAX_HTTP_BODY_BYTES + 1])),
                )
            }),
        )
        .route(
            "/exact",
            get(move || {
                let exact = exact.clone();
                async move {
                    respond(
                        AxumResponse::builder()
                            .header(CONTENT_TYPE, MEDIA_TYPE)
                            .body(Body::from(exact)),
                    )
                }
            }),
        )
        .route(
            "/trailing",
            get(|| async {
                respond(
                    AxumResponse::builder()
                        .header(CONTENT_TYPE, MEDIA_TYPE)
                        .body(Body::from(vec![0x18, 42, 0])),
                )
            }),
        )
        .route(
            "/gone/v1/changes:pull",
            post(|| async { (AxumStatus::GONE, Body::empty()) }),
        );
    let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    // The server task is owned by the returned set; dropping the set at
    // test end aborts the loop instead of leaving a runtime task detached.
    let mut server = TaskSet::new();
    server.spawn(async move {
        let serve_result = axum::serve(listener, app).await;
        drop(serve_result);
    });
    Ok((Endpoint::parse(&format!("http://{address}/"))?, server))
}

fn test_runtime() -> Result<lgwks_bot::rt::runtime::Runtime, Box<dyn std::error::Error>> {
    Ok(lgwks_bot::rt::runtime::Builder::new()
        .worker_threads(std::num::NonZeroUsize::new(2))
        .build()?)
}

#[test]
fn remote_plaintext_and_url_credentials_are_refused() {
    assert!(matches!(
        Endpoint::parse("http://example.com/"),
        Err(HttpError::InsecureEndpoint)
    ));
    for refused in [
        "https://user:pass@example.com/",
        "https://example.com/?token=1",
        "https://example.com/#part",
        "https:///path",
        "example.com/",
    ] {
        assert!(
            matches!(Endpoint::parse(refused), Err(HttpError::InvalidEndpoint)),
            "{refused} must be refused"
        );
    }
    for (raw, normalized) in [
        ("HTTPS://example.com", "https://example.com/"),
        ("http://localhost:8080/base", "http://localhost:8080/base/"),
        ("http://[::1]:9/", "http://[::1]:9/"),
        ("http://127.0.0.1/v", "http://127.0.0.1/v/"),
    ] {
        assert!(
            matches!(Endpoint::parse(raw), Ok(ref endpoint) if endpoint.to_string() == normalized),
            "{raw} must normalize to {normalized}"
        );
    }
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
    let (endpoint, _server) = endpoint().await?;
    assert_eq!(fetch_decoded::<u8>(endpoint.route("ok")).await?, 42);
    assert!(matches!(
        fetch_decoded::<u8>(endpoint.route("bad-status")).await,
        Err(HttpError::Status {
            code: 429,
            retry_after: Some(delay),
        }) if delay == Duration::from_secs(7)
    ));
    assert!(matches!(
        fetch_decoded::<u8>(endpoint.route("hostile-retry")).await,
        Err(HttpError::Status {
            code: 503,
            retry_after: None,
        })
    ));
    assert!(matches!(
        fetch_decoded::<u8>(endpoint.route("wrong-media")).await,
        Err(HttpError::MediaType)
    ));
    assert!(matches!(
        fetch_decoded::<u8>(endpoint.route("too-large")).await,
        Err(HttpError::BodyTooLarge)
    ));
    assert_eq!(
        fetch_decoded::<serde_bytes::ByteBuf>(endpoint.route("exact"))
            .await?
            .len(),
        MAX_HTTP_BODY_BYTES - 5
    );
    assert!(matches!(
        fetch_decoded::<u8>(endpoint.route("trailing")).await,
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

    let (endpoint, _server) = endpoint().await?;
    let mut transport = HttpTransport::new(endpoint, StaticBearer::new("integration")?)?;
    let request = PullRequest::new(
        logical_db_protocol::PROTOCOL_VERSION,
        Scope::new(AccountId::new(), SpaceId::new()),
        ReplicaId::new(),
        OperationDigest::new([1; 32]),
        None,
        1,
        1_024,
    );
    transport.endpoint = Endpoint::parse(&transport.endpoint.route("gone/"))?;
    assert!(matches!(
        transport.pull(request).await,
        Ok(PullResult::CheckpointExpired)
    ));
    Ok(())
}

/// Serves one hand-written HTTP/1.1 response on a loopback port so a test
/// controls the exact framing bytes rather than a framework's defaults.
/// `finish` stops and joins the fixture thread and returns its own I/O
/// outcome, so a fixture failure fails the test instead of vanishing.
struct RawServerHandle {
    /// Loopback address; `finish` connects once to wake a blocked `accept`.
    address: std::net::SocketAddr,
    /// Fixture thread and its I/O outcome.
    handle: std::thread::JoinHandle<std::io::Result<()>>,
    /// Releases a responder held open by [`stall`].
    release: std::sync::mpsc::SyncSender<()>,
    /// Stop flag the thread reads once `accept` returns.
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl RawServerHandle {
    /// Raises the stop flag, releases a stalled responder, wakes a blocked
    /// `accept`, joins the thread and returns the fixture's outcome.
    fn finish(self) -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::mpsc::TrySendError;
        let Self {
            address,
            handle,
            release,
            shutdown,
        } = self;
        shutdown.store(true, std::sync::atomic::Ordering::SeqCst);
        // Full: a release is already queued. Disconnected: the thread is done.
        match release.try_send(()) {
            Ok(()) | Err(TrySendError::Full(()) | TrySendError::Disconnected(())) => {}
        }
        // A refused connect means the thread already dropped its listener.
        let wake = match std::net::TcpStream::connect(address) {
            Ok(wake) => Some(wake),
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => None,
            Err(error) => return Err(error.into()),
        };
        let outcome = match handle.join() {
            Ok(outcome) => outcome,
            Err(payload) => {
                return Err(format!("the raw fixture thread panicked: {payload:?}").into());
            }
        };
        if let Some(wake) = wake {
            closed(wake.shutdown(std::net::Shutdown::Both))?;
        }
        Ok(outcome?)
    }
}

/// Shutting down a socket the peer already closed is the expected end. A
/// client that gives up with response bytes unread resets the connection
/// (RFC 9293 section 3.10.7.3), and Linux then reports `ConnectionReset`
/// from `shutdown` where macOS reports `NotConnected`.
fn closed(outcome: std::io::Result<()>) -> std::io::Result<()> {
    match outcome {
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotConnected | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            Ok(())
        }
        other => other,
    }
}

/// Holds a response open until `finish` releases it or `limit` passes,
/// waiting on the release channel itself rather than on a timer.
fn stall(release: &std::sync::mpsc::Receiver<()>, limit: Duration) {
    use std::sync::mpsc::RecvTimeoutError;
    match release.recv_timeout(limit) {
        Ok(()) | Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {}
    }
}

fn spawn_raw_server<F>(
    responder: F,
) -> Result<(Endpoint, RawServerHandle), Box<dyn std::error::Error>>
where
    F: FnOnce(&mut std::net::TcpStream, &std::sync::mpsc::Receiver<()>) -> std::io::Result<()>
        + Send
        + 'static,
{
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
    };
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let shutdown = Arc::new(AtomicBool::new(false));
    let thread_flag = Arc::clone(&shutdown);
    let (release, released) = mpsc::sync_channel::<()>(1);
    let handle = std::thread::Builder::new()
        .name("logical-db-http-raw-fixture".into())
        .spawn(move || -> std::io::Result<()> {
            let (mut stream, _peer) = listener.accept()?;
            if thread_flag.load(Ordering::SeqCst) {
                return Ok(());
            }
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while stream.read(&mut byte)? == 1 {
                head.extend_from_slice(&byte);
                if head.ends_with(b"\r\n\r\n") {
                    break;
                }
            }
            // RFC 9112 section 6.3: a request with no Content-Length carries no body.
            let declared: Option<usize> = String::from_utf8_lossy(&head).lines().find_map(|line| {
                let lowered = line.to_ascii_lowercase();
                let (_, value) = lowered.split_once("content-length:")?;
                value.trim().parse().ok()
            });
            if let Some(declared) = declared {
                let mut body = vec![0u8; declared];
                let mut read = 0;
                while read < declared {
                    let Some(rest) = body.get_mut(read..) else {
                        break;
                    };
                    match stream.read(rest)? {
                        0 => break,
                        got => read = read.saturating_add(got),
                    }
                }
            }
            responder(&mut stream, &released)?;
            stream.flush()?;
            closed(stream.shutdown(std::net::Shutdown::Both))
        })?;
    Ok((
        Endpoint::parse(&format!("http://{address}/"))?,
        RawServerHandle {
            address,
            handle,
            release,
            shutdown,
        },
    ))
}

fn write_bytes(stream: &mut std::net::TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    stream.write_all(bytes)?;
    stream.flush()
}

fn chunked_head() -> Vec<u8> {
    format!("HTTP/1.1 200 OK\r\ncontent-type: {MEDIA_TYPE}\r\ntransfer-encoding: chunked\r\n\r\n")
        .into_bytes()
}

fn chunk(stream: &mut std::net::TcpStream, data: &[u8]) -> std::io::Result<()> {
    write_bytes(&mut *stream, format!("{:x}\r\n", data.len()).as_bytes())?;
    write_bytes(&mut *stream, data)?;
    write_bytes(&mut *stream, b"\r\n")
}

/// A CBOR unsigned 42 is exactly `0x18 0x2a`.
const CBOR_FORTY_TWO: &[u8] = &[0x18, 0x2a];

#[test]
fn a_chunked_response_without_a_content_length_is_decoded() -> Result<(), Box<dyn std::error::Error>>
{
    let (endpoint, server) = spawn_raw_server(|stream, _release| {
        write_bytes(&mut *stream, &chunked_head())?;
        chunk(&mut *stream, CBOR_FORTY_TWO)?;
        write_bytes(&mut *stream, b"0\r\n\r\n")
    })?;
    let decoded = test_runtime()?.block_on(decode_forty_two(endpoint))?;
    assert_eq!(decoded, 42, "a chunked payload decodes to 42");
    server.finish()
}

async fn decode_forty_two(endpoint: Endpoint) -> Result<u8, Box<dyn std::error::Error>> {
    let response = fetch(endpoint.route("")).await?;
    assert!(
        header(&response, "Content-Length").is_none(),
        "the server must not declare a length"
    );
    Ok(decode_response::<u8>(&response)?)
}

#[test]
fn a_chunked_body_crossing_the_ceiling_fails_closed_without_a_content_length()
-> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, server) = spawn_raw_server(|stream, _release| {
        // The client hangs up once the ceiling is crossed, so a refused
        // write is this fixture's expected end.
        peer_gone((|| {
            write_bytes(&mut *stream, &chunked_head())?;
            let filler = vec![0u8; 64 * 1024];
            let mut sent: usize = 0;
            while sent <= MAX_HTTP_BODY_BYTES.saturating_add(filler.len()) {
                chunk(&mut *stream, &filler)?;
                sent = sent.saturating_add(filler.len());
            }
            write_bytes(&mut *stream, b"0\r\n\r\n")
        })())
    })?;
    assert!(
        matches!(
            test_runtime()?.block_on(ceiling_error(endpoint)),
            Err(HttpError::BodyTooLarge)
        ),
        "a chunked body past the ceiling must fail closed"
    );
    server.finish()
}

/// A write the peer refused because it already hung up is the expected end.
fn peer_gone(outcome: std::io::Result<()>) -> std::io::Result<()> {
    match outcome {
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ) =>
        {
            Ok(())
        }
        other => other,
    }
}

async fn ceiling_error(endpoint: Endpoint) -> Result<u8, HttpError> {
    fetch_decoded(endpoint.route("")).await
}

/// RFC 9110 section 8.6: a declared length past the ceiling fails closed, and
/// the client never buffers past the ceiling. The estate client reads the
/// short body and fails at the body stage; refusing on the declared length
/// before any body byte is read is Logical-Works-Inc/logicalworks-crates#1152.
#[test]
fn a_declared_length_above_the_ceiling_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, server) = spawn_raw_server(|stream, _release| {
        peer_gone((|| {
            write_bytes(
                &mut *stream,
                format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: {MEDIA_TYPE}\r\ncontent-length: {}\r\n\r\n",
                    MAX_HTTP_BODY_BYTES.saturating_add(1)
                )
                .as_bytes(),
            )?;
            chunk(&mut *stream, CBOR_FORTY_TWO)
        })())
    })?;
    let outcome = test_runtime()?.block_on(ceiling_error(endpoint));
    assert!(
        matches!(
            outcome,
            Err(HttpError::BodyTooLarge
                | HttpError::Network(WireError::Failure {
                    stage: lgwks_std::http::FailureStage::Body,
                    ..
                }))
        ),
        "a declared length past the ceiling must fail closed, got {outcome:?}"
    );
    server.finish()
}

#[test]
fn a_body_shorter_than_its_declared_length_is_an_error_never_a_hang()
-> Result<(), Box<dyn std::error::Error>> {
    let (endpoint, server) = spawn_raw_server(|stream, _release| {
        write_bytes(
            &mut *stream,
            format!("HTTP/1.1 200 OK\r\ncontent-type: {MEDIA_TYPE}\r\ncontent-length: 5\r\n\r\n")
                .as_bytes(),
        )?;
        write_bytes(&mut *stream, CBOR_FORTY_TWO)
    })?;
    let started = std::time::Instant::now();
    let outcome = test_runtime()?.block_on(ceiling_error(endpoint));
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "must not stall"
    );
    assert!(outcome.is_err(), "a truncated body must not decode");
    server.finish()
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
    let (target, target_server) = spawn_raw_server(move |stream, _release| {
        marker.store(true, Ordering::SeqCst);
        write_bytes(&mut *stream, &chunked_head())?;
        chunk(&mut *stream, CBOR_FORTY_TWO)?;
        write_bytes(&mut *stream, b"0\r\n\r\n")
    })?;
    let (redirect, redirect_server) = spawn_raw_server(move |stream, _release| {
        write_bytes(
            &mut *stream,
            format!(
                "HTTP/1.1 307 Temporary Redirect\r\nlocation: {target}\r\ncontent-length: 0\r\n\r\n"
            )
            .as_bytes(),
        )
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
    redirect_server.finish()?;
    target_server.finish()
}

async fn pull_from(endpoint: Endpoint) -> Result<PullResult, HttpError> {
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
fn a_stalled_response_is_released_by_the_request_timeout() -> Result<(), Box<dyn std::error::Error>>
{
    let (endpoint, server) = spawn_raw_server(|stream, release| {
        write_bytes(&mut *stream, &chunked_head())?;
        stall(release, Duration::from_secs(45));
        Ok(())
    })?;
    let started = std::time::Instant::now();
    let outcome = test_runtime()?.block_on(pull_from(endpoint));
    let elapsed = started.elapsed();
    assert!(outcome.is_err(), "a stalled response must not succeed");
    assert!(
        elapsed >= Duration::from_secs(25) && elapsed < Duration::from_secs(44),
        "the client timeout must bound the stall, elapsed {elapsed:?}"
    );
    server.finish()
}

#[test]
fn cancelling_an_in_flight_request_leaves_the_transport_usable()
-> Result<(), Box<dyn std::error::Error>> {
    let (slow, slow_server) = spawn_raw_server(|stream, release| {
        write_bytes(&mut *stream, &chunked_head())?;
        chunk(&mut *stream, &[0x18])?;
        stall(release, Duration::from_secs(20));
        Ok(())
    })?;
    let (fast, fast_server) = spawn_raw_server(|stream, _release| {
        write_bytes(&mut *stream, &chunked_head())?;
        chunk(&mut *stream, CBOR_FORTY_TWO)?;
        write_bytes(&mut *stream, b"0\r\n\r\n")
    })?;
    let runtime = test_runtime()?;
    runtime.block_on(cancel_then_succeed(slow, fast))?;
    fast_server.finish()?;
    slow_server.finish()
}

async fn cancel_then_succeed(
    slow: Endpoint,
    fast: Endpoint,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut pending: TaskSet<_> = TaskSet::new();
    let handle = pending.spawn(pull_from(slow));
    lgwks_bot::rt::task::yield_now().await;
    handle.abort();
    let decoded = decode_forty_two(fast).await?;
    assert_eq!(decoded, 42, "the transport must stay usable");
    Ok(())
}
