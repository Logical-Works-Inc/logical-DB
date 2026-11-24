//! Default-composition and credential-parsing tests for the daemon root.

use std::{
    fmt,
    future::{Future, ready},
};

use logical_db_crdt::{Document, Object, Scalar};
use logical_db_model::{
    ActionDigest, DocumentId, Effect, OperationDigest, OperationId, ReplicaId, SnapshotArtifact,
    SnapshotBaseline, SnapshotRecord,
};
use logical_db_protocol::{
    BaselineRequest, ChangeEnvelope, PROTOCOL_VERSION, PageAction, PullRequest, PushOutcome,
    PushRequest, RecoveryAction, RejectionCode,
};
use logical_db_service::{
    AcceptResult, Authority, BaselinePageResult, CheckpointCodec, ConsumedResult, ExecutionGrant,
    PageResult, ProtocolService, PruneResult, RetentionStore, SealResult, ServiceError,
    ServiceStore,
};

use super::*;

/// KEY
const KEY: [u8; 32] = [0x5a; 32];

/// Runs `scope`.
/// Builds the deterministic scope for `seed`: account `seed`, space `seed + 1`,
/// both as simple-form UUID text parsed through the identifiers' own serde.
fn scope(seed: u128) -> Result<Scope, lgwks_std::json::Error> {
    let account = lgwks_std::json::from_str(&format!("\"{seed:032x}\""))?;
    let space = lgwks_std::json::from_str(&format!("\"{:032x}\"", seed.saturating_add(1)))?;
    Ok(Scope::new(account, space))
}

/// Runs `action`.
fn action(seed: u8) -> ActionDigest {
    ActionDigest::new([seed; 32])
}

/// Store double that must never be contacted: the default composition
/// refuses write and sync authorization before any durable work.
struct RefusedStore;

/// Typed sentinel returned if the default composition ever reaches the store.
#[derive(Debug)]
/// StoreContacted
struct StoreContacted {
    /// Store method that must never run under the default composition.
    method: &'static str,
}

impl fmt::Display for StoreContacted {
    /// Names the store method the default composition must never reach.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "default composition refuses before the store is contacted: {}",
            self.method
        )
    }
}

impl Error for StoreContacted {}

impl ServiceStore for RefusedStore {
    type Error = StoreContacted;

    /// Runs `accept`.
    fn accept(
        &self,
        _change: &ChangeEnvelope,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
        ready(Err(StoreContacted { method: "accept" }))
    }

    /// Runs `frontier`.
    fn frontier(&self, _scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(Ok(0))
    }

    /// Runs `page`.
    fn page(
        &self,
        _action: PageAction,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
        ready(Err(StoreContacted { method: "page" }))
    }
}

impl RetentionStore for RefusedStore {
    /// Runs `consumed_through`.
    fn consumed_through(
        &self,
        _scope: Scope,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        ready(Err(StoreContacted {
            method: "consumed_through",
        }))
    }

    /// Runs `snapshot_baseline`.
    fn snapshot_baseline(
        &self,
        _scope: Scope,
    ) -> impl Future<Output = Result<Option<SnapshotRecord>, Self::Error>> + Send {
        ready(Err(StoreContacted {
            method: "snapshot_baseline",
        }))
    }

    /// Runs `build_snapshot`.
    fn build_snapshot(
        &self,
        _scope: Scope,
        _through: u64,
    ) -> impl Future<Output = Result<SnapshotArtifact, Self::Error>> + Send {
        ready(Err(StoreContacted {
            method: "build_snapshot",
        }))
    }

    /// Runs `read_baseline_page`.
    fn read_baseline_page(
        &self,
        _action: &RecoveryAction,
        _descriptor: &SnapshotBaseline,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<BaselinePageResult, Self::Error>> + Send {
        ready(Err(StoreContacted {
            method: "read_baseline_page",
        }))
    }

    /// Runs `seal_snapshot`.
    fn seal_snapshot(
        &self,
        _scope: Scope,
        _artifact: &SnapshotArtifact,
        _request: OperationDigest,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<SealResult, Self::Error>> + Send {
        ready(Err(StoreContacted {
            method: "seal_snapshot",
        }))
    }

    /// Runs `mark_consumed`.
    fn mark_consumed(
        &self,
        _scope: Scope,
        _through: u64,
        _request: OperationDigest,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<ConsumedResult, Self::Error>> + Send {
        ready(Err(StoreContacted {
            method: "mark_consumed",
        }))
    }

    /// Runs `prune`.
    fn prune(
        &self,
        _scope: Scope,
        _up_to: u64,
        _min_live: Option<u64>,
        _request: OperationDigest,
        _grant: &impl ExecutionGrant,
    ) -> impl Future<Output = Result<PruneResult, Self::Error>> + Send {
        ready(Err(StoreContacted { method: "prune" }))
    }
}

/// Runs `default_push_request`.
fn default_push_request(
    scope: Scope,
    replica: ReplicaId,
) -> Result<(PushRequest, OperationId), Box<dyn Error>> {
    let mut document = Document::new();
    let ((), mut changes) =
        document.edit(|editor| editor.put(&Object::root(), "field", Scalar::Integer(7)))?;
    let encoded = changes.pop().ok_or("expected one encoded change")?;
    let operation = OperationId::new();
    let request = PushRequest::new(
        PROTOCOL_VERSION,
        replica,
        vec![ChangeEnvelope::new(
            scope,
            DocumentId::new(),
            replica,
            operation,
            OperationDigest::new([8; 32]),
            encoded.digest(),
            encoded.into_bytes(),
        )],
    );
    Ok((request, operation))
}

/// Runs `test_runtime`.
fn test_runtime() -> Result<lgwks_bot::rt::runtime::Runtime, Box<dyn Error>> {
    Ok(lgwks_bot::rt::runtime::Builder::new()
        .worker_threads(std::num::NonZeroUsize::new(2))
        .build()?)
}

#[test]
/// Runs `default_composition_refuses_writes_and_sync_end_to_end`.
fn default_composition_refuses_writes_and_sync_end_to_end() -> Result<(), Box<dyn Error>> {
    test_runtime()?.block_on(default_composition_body())
}

/// Runs `default_composition_body`.
async fn default_composition_body() -> Result<(), Box<dyn Error>> {
    let expected_scope = scope(41)?;
    let authentication =
        BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0041", expected_scope)?;
    let policy = authentication
        .authenticate(
            &HeaderValue::from_static("Bearer correct-correct-correct-correct-0041"),
            1_000,
        )
        .map_err(|error| io::Error::other(format!("default authentication: {error:?}")))?;
    let service =
        ProtocolService::new(RefusedStore, policy, CheckpointCodec::new([7; 32], 60_000)?);
    let replica = ReplicaId::new();
    let (push, operation) = default_push_request(expected_scope, replica)?;
    let response = service.push(&push, 2_000).await?;
    assert!(
        matches!(
            response.outcomes(),
            [PushOutcome::Rejected {
                operation: outcome,
                code: RejectionCode::Forbidden
            }] if *outcome == operation
        ),
        "an unauthenticated-default write must reject as forbidden, got {response:?}"
    );
    let pull = PullRequest::new(
        PROTOCOL_VERSION,
        expected_scope,
        replica,
        OperationDigest::new([3; 32]),
        None,
        16,
        1_048_576,
    );
    assert!(
        matches!(
            service.pull(&pull, 2_000).await,
            Err(ServiceError::Forbidden)
        ),
        "an unauthenticated-default pull must refuse as forbidden"
    );
    Ok(())
}

#[test]
/// Runs `checkpoint_key_parser_rejects_ambiguous_or_weak_values`.
fn checkpoint_key_parser_rejects_ambiguous_or_weak_values() -> Result<(), Box<dyn Error>> {
    assert_eq!(
        parse_key(&"5a".repeat(32))?,
        KEY,
        "canonical key must parse"
    );
    for invalid in ["", "5a", &"0".repeat(64), &"zz".repeat(32)] {
        assert!(
            parse_key(invalid).is_err(),
            "weak key {invalid:?} must refuse"
        );
    }
    Ok(())
}

#[test]
/// Runs `bearer_authentication_is_exact_and_constant_time_verified`.
fn bearer_authentication_is_exact_and_constant_time_verified() -> Result<(), Box<dyn Error>> {
    let expected_scope = scope(11)?;
    let authentication =
        BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0001", expected_scope)?;
    let policy = authentication.authenticate(
        &HeaderValue::from_static("Bearer correct-correct-correct-correct-0001"),
        1_000,
    )?;
    assert!(
        policy
            .authorize(expected_scope, Effect::Write, action(1), 0, 60_999)
            .is_none(),
        "local authority must refuse write"
    );
    assert!(
        policy
            .authorize(expected_scope, Effect::Sync, action(2), 0, 60_999)
            .is_none(),
        "local authority must refuse sync"
    );
    assert!(
        policy
            .authorize(scope(20)?, Effect::Write, action(3), 0, 60_999)
            .is_none(),
        "local authority must refuse a foreign scope"
    );
    assert!(
        policy
            .authorize(expected_scope, Effect::Read, action(4), 0, 60_999)
            .is_some(),
        "local authority must admit an in-scope read"
    );
    assert!(
        policy
            .authorize(expected_scope, Effect::Write, action(5), 0, 61_000)
            .is_none(),
        "local authority must refuse write at the expiry boundary"
    );
    assert!(
        authentication
            .authenticate(&HeaderValue::from_static("Bearer incorrect"), 1_000)
            .is_err(),
        "an incorrect bearer must be refused"
    );
    assert!(
        authentication
            .authenticate(
                &HeaderValue::from_static("Basic correct-correct-correct-correct-0001"),
                1_000,
            )
            .is_err(),
        "a non-bearer scheme must be refused"
    );
    Ok(())
}

#[test]
/// Runs `policy_expiry_cannot_overflow`.
fn policy_expiry_cannot_overflow() -> Result<(), Box<dyn Error>> {
    let authentication =
        BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0001", scope(30)?)?;
    assert!(
        authentication
            .authenticate(
                &HeaderValue::from_static("Bearer correct-correct-correct-correct-0001"),
                u64::MAX,
            )
            .is_err(),
        "expiry computation must fail closed rather than wrap"
    );
    Ok(())
}

#[test]
/// Runs `daemon_binds_only_to_loopback`.
fn daemon_binds_only_to_loopback() -> Result<(), Box<dyn Error>> {
    for loopback in ["127.0.0.1:8080", "[::1]:8080"] {
        let address: SocketAddr = loopback.parse()?;
        assert!(ensure_loopback(&address).is_ok(), "loopback must bind");
    }
    for public in ["0.0.0.0:8080", "192.0.2.1:8080", "[::]:8080"] {
        let address: SocketAddr = public.parse()?;
        assert!(
            ensure_loopback(&address).is_err(),
            "public plaintext bind must refuse {public}"
        );
    }
    Ok(())
}

#[test]
/// Runs `daemon_configuration_rejects_weak_tokens_and_nil_scope_axes`.
fn daemon_configuration_rejects_weak_tokens_and_nil_scope_axes() {
    for invalid in [b"short".as_slice(), b"contains a space________________"] {
        assert!(validate_bearer_token(invalid).is_err());
    }
    assert!(validate_bearer_token(b"valid-valid-valid-valid-valid-token").is_ok());
    assert!(validate_bearer_token(b"valid-valid-valid-valid-valid=====").is_ok());
    assert!(validate_bearer_token(b"valid-valid-valid=====invalid____").is_err());
    let account_is_nil = |account: &AccountId| is_nil(&account.as_uuid());
    assert!(
        parse_identifier(
            "00000000-0000-0000-0000-000000000000",
            "nil",
            account_is_nil
        )
        .is_err(),
        "the nil UUID must never name an account"
    );
    assert!(
        parse_identifier(&AccountId::new().to_string(), "valid", account_is_nil).is_ok(),
        "a generated account identifier must parse back"
    );
    assert!(
        parse_identifier("not-a-uuid", "garbage", account_is_nil).is_err(),
        "non-UUID text must be refused"
    );
}

#[test]
/// Runs `the_daemon_starts_serves_and_releases_its_listener_on_shutdown`.
fn the_daemon_starts_serves_and_releases_its_listener_on_shutdown() -> Result<(), Box<dyn Error>> {
    test_runtime()?.block_on(daemon_lifecycle_body())
}

/// Runs `daemon_lifecycle_body`.
/// A loopback daemon over the refusing store, plus its shutdown handle.
struct RunningDaemon {
    /// Bound loopback address.
    address: std::net::SocketAddr,
    /// Notifier that starts graceful shutdown.
    stopped: std::sync::Arc<lgwks_bot::rt::sync::Notify>,
    /// The serve task, owned so it is released with the fixture.
    serving: lgwks_bot::rt::task::JoinSet<()>,
}

/// Starts the default composition on an ephemeral loopback port.
async fn start_daemon() -> Result<RunningDaemon, Box<dyn Error>> {
    let authentication =
        BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0041", scope(41)?)?;
    let application = router(
        RefusedStore,
        authentication,
        CheckpointCodec::new(KEY, 86_400_000)?,
        SystemClock::new(),
    );
    let loopback = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0));
    let listener = lgwks_bot::rt::net::TcpListener::bind(loopback).await?;
    let address: SocketAddr = listener.local_addr()?;
    let stopped = std::sync::Arc::new(lgwks_bot::rt::sync::Notify::new());
    let shutdown = std::sync::Arc::clone(&stopped);
    let mut serving = lgwks_bot::rt::task::JoinSet::new();
    serving.spawn(async move {
        let served = axum::serve(listener, application)
            .with_graceful_shutdown(async move { shutdown.notified().await })
            .await;
        assert!(served.is_ok(), "the daemon serve loop failed: {served:?}");
    });
    Ok(RunningDaemon {
        address,
        stopped,
        serving,
    })
}

/// Runs `daemon_lifecycle_body`.
async fn daemon_lifecycle_body() -> Result<(), Box<dyn Error>> {
    let RunningDaemon {
        address,
        stopped,
        mut serving,
    } = start_daemon().await?;

    let status = unauthenticated_push_status(address)?;
    assert!(status.starts_with("HTTP/1.1 4"), "daemon served {status}");

    stopped.notify_waiters();
    lgwks_bot::rt::task::yield_now().await;
    serving
        .join_next()
        .await
        .ok_or("the daemon serve task finished before shutdown")??;

    let rebound = std::net::TcpListener::bind(address);
    assert!(rebound.is_ok(), "the daemon kept its listener bound");
    Ok(())
}

#[test]
/// Runs `recovery_is_refused_before_the_baseline_store_is_contacted`.
fn recovery_is_refused_before_the_baseline_store_is_contacted() -> Result<(), Box<dyn Error>> {
    test_runtime()?.block_on(recovery_refusal_body())
}

/// Runs `recovery_refusal_body`.
async fn recovery_refusal_body() -> Result<(), Box<dyn Error>> {
    let RunningDaemon {
        address,
        stopped,
        mut serving,
    } = start_daemon().await?;

    // A well-formed recovery request with the correct bearer and scope:
    // every earlier boundary (representation, authentication, protocol
    // validation) passes, so only the recovery authorization may refuse.
    // `frontier` is read before authorization by design; any contact with
    // the retention surface trips the RefusedStore sentinel instead.
    let request = BaselineRequest::new(
        PROTOCOL_VERSION,
        scope(41)?,
        ReplicaId::new(),
        OperationDigest::new([4; 32]),
        None,
        None,
    );
    let mut body = Vec::new();
    ciborium::into_writer(&request, &mut body)?;
    let status = post_status(
        address,
        "/v1/recovery:baseline",
        &body,
        "Bearer correct-correct-correct-correct-0041",
    )?;
    assert_eq!(status, "HTTP/1.1 403 Forbidden", "daemon served {status}");

    stopped.notify_waiters();
    lgwks_bot::rt::task::yield_now().await;
    serving
        .join_next()
        .await
        .ok_or("the daemon serve task finished before shutdown")??;
    Ok(())
}

/// Runs `unauthenticated_push_status`.
fn unauthenticated_push_status(address: std::net::SocketAddr) -> Result<String, Box<dyn Error>> {
    post_status(address, "/v1/changes:push", b"", "")
}

/// Runs `post_status`.
fn post_status(
    address: std::net::SocketAddr,
    path: &str,
    body: &[u8],
    authorization: &str,
) -> Result<String, Box<dyn Error>> {
    use std::{
        io::{Read, Write},
        net::TcpStream,
        time::Duration,
    };
    let authorization_line = if authorization.is_empty() {
        String::new()
    } else {
        format!("authorization: {authorization}\r\n")
    };
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let head = format!(
        "POST {path} HTTP/1.1\r\nhost: 127.0.0.1\r\n\
             content-type: application/vnd.logical-db.v1+cbor\r\n\
             accept: application/vnd.logical-db.v1+cbor\r\n\
             {authorization_line}\
             content-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    stream.write_all(&head)?;
    stream.write_all(body)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    let text = String::from_utf8_lossy(&response);
    let status = text
        .lines()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "empty HTTP response"))?;
    Ok(status.to_owned())
}
