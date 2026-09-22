//! Standalone, loopback-only Logical-DB service composition root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![expect(
    clippy::print_stderr,
    reason = "src/bin/ is the PRINTS-exempt binary surface per the execution contract"
)]
use std::{error::Error, io, net::SocketAddr};

use axum::http::HeaderValue;
use foundationdb::directory::{Directory, DirectoryLayer};
use foundationdb_tuple::Subspace;
use lgwks_std::hash::keyed;
use logical_db_model::{AccountId, Scope, SpaceId};
use logical_db_server::{AuthenticationError, Authenticator, SystemClock, router};
use logical_db_service::{CheckpointCodec, LocalScopeAuthority};
use logical_db_store_fdb::FdbServiceStore;
use uuid::Uuid;

/// Loopback standalone bearer verifier bound to one authenticated scope.
struct BearerAuthenticator {
    /// Keyed-hash key for the bearer tag.
    key: [u8; 32],
    /// Expected tag for the bound token.
    expected_tag: [u8; 32],
    /// Exact account and application space this authenticator admits.
    scope: Scope,
}

impl BearerAuthenticator {
    /// Validates the token shape and binds one scope to its expected tag.
    fn new(key: [u8; 32], token: &[u8], scope: Scope) -> Result<Self, io::Error> {
        validate_bearer_token(token)?;
        Ok(Self {
            key,
            expected_tag: bearer_tag(key, token),
            scope,
        })
    }
}

impl Authenticator for BearerAuthenticator {
    type Policy = LocalScopeAuthority;

    /// Runs `authenticate`.
    fn authenticate(
        &self,
        authorization: &HeaderValue,
        now_unix_ms: u64,
    ) -> Result<Self::Policy, AuthenticationError> {
        let token = authorization.as_bytes().strip_prefix(b"Bearer ").ok_or(
            AuthenticationError::Malformed {
                context: "bearer scheme",
            },
        )?;
        let mut verifier_message = Vec::with_capacity(
            b"logical-db/standalone/bearer/v1\0"
                .len()
                .saturating_add(token.len()),
        );
        verifier_message.extend_from_slice(b"logical-db/standalone/bearer/v1\0");
        verifier_message.extend_from_slice(token);
        let computed = keyed(&self.key, &verifier_message);
        if !logical_db_service::tags_equal(computed.as_bytes(), &self.expected_tag) {
            eprintln!("logical-db-daemon: bearer verification failed");
            return Err(AuthenticationError::Unauthorized);
        }
        let expires_at = now_unix_ms
            .checked_add(60_000)
            .ok_or(AuthenticationError::Forbidden)?;
        Ok(LocalScopeAuthority::new(self.scope, expires_at))
    }
}

/// Computes the keyed bearer tag for one token under `key`.
fn bearer_tag(key: [u8; 32], token: &[u8]) -> [u8; 32] {
    let mut message = Vec::with_capacity(
        b"logical-db/standalone/bearer/v1\0"
            .len()
            .saturating_add(token.len()),
    );
    message.extend_from_slice(b"logical-db/standalone/bearer/v1\0");
    message.extend_from_slice(token);
    *keyed(&key, &message).as_bytes()
}

/// Reads one required process environment value.
fn required(name: &'static str) -> Result<String, io::Error> {
    std::env::var(name).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} is unavailable: {error}"),
        )
    })
}

/// Parses a 64-character hexadecimal checkpoint key, refusing the all-zero key.
fn parse_key(value: &str) -> Result<[u8; 32], io::Error> {
    if value.len() != 64 {
        eprintln!(
            "logical-db-daemon: checkpoint key length was {}",
            value.len()
        );
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "checkpoint key must contain 64 hexadecimal characters",
        ));
    }
    let mut key = [0; 32];
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    if !remainder.is_empty() {
        eprintln!("logical-db-daemon: checkpoint key ended in a partial octet");
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "checkpoint key must contain complete hexadecimal octets",
        ));
    }
    for (index, pair) in pairs.iter().enumerate() {
        let pair = std::str::from_utf8(pair)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        key[index] = u8::from_str_radix(pair, 16).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid checkpoint key octet {index}: {error}"),
            )
        })?;
    }
    if key == [0; 32] {
        eprintln!("logical-db-daemon: checkpoint key was all zero");
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "checkpoint key must be nonzero",
        ));
    }
    Ok(key)
}

/// Runs `validate_bearer_token`.
fn validate_bearer_token(token: &[u8]) -> Result<(), io::Error> {
    let mut padding = false;
    let valid = (32..=4_096).contains(&token.len())
        && token.iter().all(|byte| {
            if *byte == b'=' {
                padding = true;
                true
            } else {
                !padding
                    && (byte.is_ascii_alphanumeric()
                        || matches!(*byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/'))
            }
        });
    if valid {
        Ok(())
    } else {
        eprintln!(
            "logical-db-daemon: rejected bearer token length {} or invalid alphabet",
            token.len()
        );
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "bearer token must be 32..=4096 RFC 6750 b64token bytes",
        ))
    }
}

/// Runs `parse_non_nil_uuid`.
fn parse_non_nil_uuid(value: &str, name: &'static str) -> Result<Uuid, io::Error> {
    let value = Uuid::parse_str(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, format!("{name}: {error}")))?;
    if value.is_nil() {
        eprintln!("logical-db-daemon: rejected nil UUID for {name}");
        return Err(io::Error::new(io::ErrorKind::InvalidInput, name));
    }
    Ok(value)
}

/// Runs `shutdown_signal`.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) = lgwks_bot::rt::signal::unix::signal(
            lgwks_bot::rt::signal::unix::SignalKind::terminate(),
        ) {
            lgwks_bot::select! {
                _ = lgwks_bot::rt::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    if let Err(error) = lgwks_bot::rt::signal::ctrl_c().await {
        eprintln!("logical-db-daemon: ctrl-c signal listener failed: {error}");
    }
}

/// Runs `ensure_loopback`.
fn ensure_loopback(address: &SocketAddr) -> Result<(), io::Error> {
    if address.ip().is_loopback() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the daemon has no TLS terminator and therefore binds only to loopback",
        ))
    }
}

/// Runs `main`.
fn main() -> Result<(), Box<dyn Error>> {
    lgwks_bot::Runtime::new()?.block_on(service_main())
}

/// Runs `service_main`.
async fn service_main() -> Result<(), Box<dyn Error>> {
    let bind = std::env::var("LOGICAL_DB_BIND").unwrap_or_else(|error| {
        eprintln!("logical-db-daemon: LOGICAL_DB_BIND defaulted: {error}");
        "127.0.0.1:8080".into()
    });
    let address: SocketAddr = bind.parse()?;
    ensure_loopback(&address)?;
    let cluster_file = required("LOGICAL_DB_FDB_CLUSTER_FILE")?;
    let bearer = required("LOGICAL_DB_BEARER_TOKEN")?;
    let key = parse_key(&required("LOGICAL_DB_CHECKPOINT_KEY_HEX")?)?;
    let scope = Scope::new(
        AccountId::from_uuid(parse_non_nil_uuid(
            &required("LOGICAL_DB_ACCOUNT_ID")?,
            "LOGICAL_DB_ACCOUNT_ID must be a non-nil UUID",
        )?),
        SpaceId::from_uuid(parse_non_nil_uuid(
            &required("LOGICAL_DB_SPACE_ID")?,
            "LOGICAL_DB_SPACE_ID must be a non-nil UUID",
        )?),
    );

    #[expect(
        unsafe_code,
        reason = "FoundationDB network-boot FFI is the one reviewed unsafe leaf in this binary; the safe surface forbids unsafe_code at the crate root"
    )]
    // SAFETY: FoundationDB requires exactly one network bootstrap per process.
    // This composition root calls it once, retains the returned guard until the
    // listener and all database-owning router state have been dropped, and does
    // not expose the guard or any FFI handle through its safe public libraries.
    let network = unsafe { foundationdb::boot() };
    let database = foundationdb::Database::new_compat(Some(&cluster_file)).await?;
    let transaction = database.create_trx()?;
    let allocated = DirectoryLayer::default()
        .create_or_open(
            &transaction,
            &["logical-db".into(), "v3".into()],
            None,
            None,
        )
        .await
        .map_err(|error| io::Error::other(format!("directory allocation failed: {error:?}")))?;
    transaction.commit().await?;
    let root = Subspace::from_bytes(
        allocated
            .bytes()
            .map_err(|error| io::Error::other(format!("directory prefix failed: {error:?}")))?
            .to_vec(),
    );
    let store = FdbServiceStore::new(database, root)?;
    let authentication = BearerAuthenticator::new(key, bearer.as_bytes(), scope)?;
    let checkpoints = CheckpointCodec::new(key, 86_400_000)?;
    let application = router(store, authentication, checkpoints, SystemClock::new());
    let listener = lgwks_bot::rt::net::TcpListener::bind(address).await?;
    axum::serve(listener, application)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    drop(network);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        future::{Future, ready},
    };

    use logical_db_crdt::{Document, Object, Scalar};
    use logical_db_model::{
        ActionDigest, DocumentId, Effect, OperationDigest, OperationId, ReplicaId,
        SnapshotArtifact, SnapshotBaseline, SnapshotRecord,
    };
    use logical_db_protocol::{
        BaselineRequest, ChangeEnvelope, PROTOCOL_VERSION, PageAction, PullRequest, PushOutcome,
        PushRequest, RecoveryAction, RejectionCode,
    };
    use logical_db_service::{
        AcceptResult, Authority, BaselinePageResult, CheckpointCodec, ConsumedResult,
        ExecutionGrant, PageResult, ProtocolService, PruneResult, RetentionStore, SealResult,
        ServiceError, ServiceStore,
    };

    use super::*;

    /// KEY
    const KEY: [u8; 32] = [0x5a; 32];

    /// Runs `scope`.
    fn scope(seed: u128) -> Scope {
        Scope::new(
            AccountId::from_uuid(Uuid::from_u128(seed)),
            SpaceId::from_uuid(Uuid::from_u128(seed.saturating_add(1))),
        )
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
                encoded.digest,
                encoded.bytes,
            )],
        );
        Ok((request, operation))
    }

    /// Runs `test_runtime`.
    fn test_runtime() -> Result<lgwks_bot::Runtime, Box<dyn Error>> {
        Ok(lgwks_bot::Builder::new()
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
        let expected_scope = scope(41);
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
                response.outcomes.as_slice(),
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
        let expected_scope = scope(11);
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
                .authorize(scope(20), Effect::Write, action(3), 0, 60_999)
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
            BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0001", scope(30))?;
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
        assert!(parse_non_nil_uuid(&Uuid::nil().to_string(), "nil").is_err());
        assert!(parse_non_nil_uuid(&Uuid::now_v7().to_string(), "valid").is_ok());
    }

    #[test]
    /// Runs `the_daemon_starts_serves_and_releases_its_listener_on_shutdown`.
    fn the_daemon_starts_serves_and_releases_its_listener_on_shutdown() -> Result<(), Box<dyn Error>>
    {
        test_runtime()?.block_on(daemon_lifecycle_body())
    }

    /// Runs `daemon_lifecycle_body`.
    async fn daemon_lifecycle_body() -> Result<(), Box<dyn Error>> {
        use std::{net::SocketAddr, sync::Arc};
        let authentication =
            BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0041", scope(41))?;
        let application = router(
            RefusedStore,
            authentication,
            CheckpointCodec::new(KEY, 86_400_000)?,
            SystemClock::new(),
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address: SocketAddr = listener.local_addr()?;
        let stopped = Arc::new(lgwks_bot::rt::sync::Notify::new());
        let shutdown = Arc::clone(&stopped);
        let serving = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, application)
                .with_graceful_shutdown(async move { shutdown.notified().await })
                .await;
            drop(serve_result);
        });

        let status = unauthenticated_push_status(address)?;
        assert!(status.starts_with("HTTP/1.1 4"), "daemon served {status}");

        stopped.notify_waiters();
        lgwks_bot::rt::task::yield_now().await;
        serving.await?;

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
        use std::{net::SocketAddr, sync::Arc};
        let authentication =
            BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0041", scope(41))?;
        let application = router(
            RefusedStore,
            authentication,
            CheckpointCodec::new(KEY, 86_400_000)?,
            SystemClock::new(),
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address: SocketAddr = listener.local_addr()?;
        let stopped = Arc::new(lgwks_bot::rt::sync::Notify::new());
        let shutdown = Arc::clone(&stopped);
        let serving = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, application)
                .with_graceful_shutdown(async move { shutdown.notified().await })
                .await;
            drop(serve_result);
        });

        // A well-formed recovery request with the correct bearer and scope:
        // every earlier boundary (representation, authentication, protocol
        // validation) passes, so only the recovery authorization may refuse.
        // `frontier` is read before authorization by design; any contact with
        // the retention surface trips the RefusedStore sentinel instead.
        let request = BaselineRequest::new(
            PROTOCOL_VERSION,
            scope(41),
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
        serving.await?;
        Ok(())
    }

    /// Runs `unauthenticated_push_status`.
    fn unauthenticated_push_status(
        address: std::net::SocketAddr,
    ) -> Result<String, Box<dyn Error>> {
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
             /// content-length
             content-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        stream.write_all(&head)?;
        stream.write_all(body)?;
        let mut response = Vec::new();
        drop(stream.read_to_end(&mut response));
        Ok(String::from_utf8_lossy(&response)
            .lines()
            .next()
            .unwrap_or_default()
            .to_owned())
    }
}
