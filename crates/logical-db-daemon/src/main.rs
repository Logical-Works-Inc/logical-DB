//! Standalone, loopback-only Logical-DB service composition root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

use std::{error::Error, io, net::SocketAddr};

use axum::http::HeaderValue;
use foundationdb::directory::{Directory, DirectoryLayer};
use foundationdb_tuple::Subspace;
use hmac::{Hmac, KeyInit, Mac};
use logical_db_braid::LocalBraidAuthority;
use logical_db_model::{AccountId, Scope, SpaceId};
use logical_db_server::{AuthenticationError, Authenticator, SystemClock, router};
use logical_db_service::CheckpointCodec;
use logical_db_store_fdb::FdbServiceStore;
use sha2::Sha256;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

struct BearerAuthenticator {
    key: [u8; 32],
    expected_tag: [u8; 32],
    scope: Scope,
}

impl BearerAuthenticator {
    fn new(key: [u8; 32], token: &[u8], scope: Scope) -> Result<Self, io::Error> {
        validate_bearer_token(token)?;
        Ok(Self {
            key,
            expected_tag: bearer_tag(key, token)?,
            scope,
        })
    }
}

impl Authenticator for BearerAuthenticator {
    type Policy = LocalBraidAuthority;

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
        let mut verifier = <HmacSha256 as KeyInit>::new_from_slice(&self.key).map_err(|error| {
            eprintln!("logical-db-daemon: HMAC verifier initialization failed: {error}");
            AuthenticationError::Forbidden
        })?;
        verifier.update(b"logical-db/standalone/bearer/v1\0");
        verifier.update(token);
        verifier.verify_slice(&self.expected_tag).map_err(|error| {
            eprintln!("logical-db-daemon: bearer verification failed: {error}");
            AuthenticationError::Unauthorized
        })?;
        let expires_at = now_unix_ms
            .checked_add(60_000)
            .ok_or(AuthenticationError::Forbidden)?;
        Ok(LocalBraidAuthority::new(self.scope, expires_at))
    }
}

fn bearer_tag(key: [u8; 32], token: &[u8]) -> Result<[u8; 32], io::Error> {
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(&key)
        .map_err(|error| io::Error::other(format!("HMAC initialization failed: {error}")))?;
    mac.update(b"logical-db/standalone/bearer/v1\0");
    mac.update(token);
    Ok(mac.finalize().into_bytes().into())
}

fn required(name: &'static str) -> Result<String, io::Error> {
    std::env::var(name).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{name} is unavailable: {error}"),
        )
    })
}

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

fn parse_non_nil_uuid(value: &str, name: &'static str) -> Result<Uuid, io::Error> {
    let value = Uuid::parse_str(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, format!("{name}: {error}")))?;
    if value.is_nil() {
        eprintln!("logical-db-daemon: rejected nil UUID for {name}");
        return Err(io::Error::new(io::ErrorKind::InvalidInput, name));
    }
    Ok(value)
}

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

fn main() -> Result<(), Box<dyn Error>> {
    lgwks_bot::Runtime::new()?.block_on(service_main())
}

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

    #[allow(unsafe_code)]
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
    let application = router(store, authentication, checkpoints, SystemClock);
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
        convert::Infallible,
        future::{Future, ready},
    };

    use logical_db_crdt::{Document, Object, Scalar};
    use logical_db_model::{
        ActionDigest, DocumentId, Effect, OperationDigest, OperationId, ReplicaId,
    };
    use logical_db_protocol::{
        ChangeEnvelope, PROTOCOL_VERSION, PageAction, PullRequest, PushOutcome, PushRequest,
        RejectionCode,
    };
    use logical_db_service::{
        AcceptResult, Authority, CheckpointCodec, ExecutionGrant, PageResult, ProtocolService,
        ServiceError, ServiceStore,
    };

    use super::*;

    const KEY: [u8; 32] = [0x5a; 32];

    fn scope(seed: u128) -> Scope {
        Scope::new(
            AccountId::from_uuid(Uuid::from_u128(seed)),
            SpaceId::from_uuid(Uuid::from_u128(seed + 1)),
        )
    }

    fn action(seed: u8) -> ActionDigest {
        ActionDigest::new([seed; 32])
    }

    /// Store double that must never be contacted: the default composition
    /// refuses write and sync authorization before any durable work.
    struct RefusedStore;

    impl ServiceStore for RefusedStore {
        type Error = Infallible;

        fn accept(
            &self,
            _change: &ChangeEnvelope,
            _grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
            std::future::poll_fn(|_| {
                panic!("default composition refuses before the store is contacted")
            })
        }

        fn frontier(&self, _scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            ready(Ok(0))
        }

        fn page(
            &self,
            _action: PageAction,
            _grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
            std::future::poll_fn(|_| {
                panic!("default composition refuses before the store is contacted")
            })
        }
    }

    fn default_push_request(scope: Scope, replica: ReplicaId) -> (PushRequest, OperationId) {
        let mut document = Document::new();
        let ((), mut changes) = document
            .edit(|editor| editor.put(&Object::root(), "field", Scalar::Integer(7)))
            .unwrap();
        let encoded = changes.pop().unwrap();
        let operation = OperationId::new();
        let request = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![ChangeEnvelope {
                scope,
                document: DocumentId::new(),
                replica,
                operation,
                request: OperationDigest::new([8; 32]),
                digest: encoded.digest,
                bytes: encoded.bytes,
            }],
        };
        (request, operation)
    }

    fn test_runtime() -> lgwks_bot::Runtime {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
    }

    #[test]
    fn default_composition_refuses_writes_and_sync_end_to_end() {
        test_runtime().block_on(default_composition_body());
    }

    async fn default_composition_body() {
        let expected_scope = scope(41);
        let authentication =
            BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0041", expected_scope)
                .unwrap();
        let policy = authentication
            .authenticate(
                &HeaderValue::from_static("Bearer correct-correct-correct-correct-0041"),
                1_000,
            )
            .unwrap();
        let service = ProtocolService::new(
            RefusedStore,
            policy,
            CheckpointCodec::new([7; 32], 60_000).unwrap(),
        );
        let replica = ReplicaId::new();
        let (push, operation) = default_push_request(expected_scope, replica);
        let response = service.push(&push, 2_000).await.unwrap();
        assert!(matches!(
            response.outcomes.as_slice(),
            [PushOutcome::Rejected {
                operation: outcome,
                code: RejectionCode::Forbidden
            }] if *outcome == operation
        ));
        let pull = PullRequest {
            protocol: PROTOCOL_VERSION,
            scope: expected_scope,
            replica,
            request: OperationDigest::new([3; 32]),
            after: None,
            max_changes: 16,
            max_bytes: 1_048_576,
        };
        assert!(matches!(
            service.pull(&pull, 2_000).await,
            Err(ServiceError::Forbidden)
        ));
    }

    #[test]
    fn checkpoint_key_parser_rejects_ambiguous_or_weak_values() {
        assert_eq!(parse_key(&"5a".repeat(32)).unwrap(), KEY);
        for invalid in ["", "5a", &"0".repeat(64), &"zz".repeat(32)] {
            assert!(parse_key(invalid).is_err());
        }
    }

    #[test]
    fn bearer_authentication_is_exact_and_constant_time_verified() {
        let expected_scope = scope(11);
        let authentication =
            BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0001", expected_scope)
                .unwrap();
        let policy = authentication
            .authenticate(
                &HeaderValue::from_static("Bearer correct-correct-correct-correct-0001"),
                1_000,
            )
            .unwrap();
        assert!(
            policy
                .authorize(expected_scope, Effect::Write, action(1), 0, 60_999)
                .is_none()
        );
        assert!(
            policy
                .authorize(expected_scope, Effect::Sync, action(2), 0, 60_999)
                .is_none()
        );
        assert!(
            policy
                .authorize(scope(20), Effect::Write, action(3), 0, 60_999)
                .is_none()
        );
        assert!(
            policy
                .authorize(expected_scope, Effect::Read, action(4), 0, 60_999)
                .is_some()
        );
        assert!(
            policy
                .authorize(expected_scope, Effect::Write, action(5), 0, 61_000)
                .is_none()
        );
        assert!(
            authentication
                .authenticate(&HeaderValue::from_static("Bearer incorrect"), 1_000)
                .is_err()
        );
        assert!(
            authentication
                .authenticate(
                    &HeaderValue::from_static("Basic correct-correct-correct-correct-0001"),
                    1_000,
                )
                .is_err()
        );
    }

    #[test]
    fn policy_expiry_cannot_overflow() {
        let authentication =
            BearerAuthenticator::new(KEY, b"correct-correct-correct-correct-0001", scope(30))
                .unwrap();
        assert!(
            authentication
                .authenticate(
                    &HeaderValue::from_static("Bearer correct-correct-correct-correct-0001"),
                    u64::MAX,
                )
                .is_err()
        );
    }

    #[test]
    fn daemon_binds_only_to_loopback() {
        for loopback in ["127.0.0.1:8080", "[::1]:8080"] {
            let address: SocketAddr = loopback.parse().unwrap();
            assert!(ensure_loopback(&address).is_ok());
        }
        for public in ["0.0.0.0:8080", "192.0.2.1:8080", "[::]:8080"] {
            let address: SocketAddr = public.parse().unwrap();
            assert!(ensure_loopback(&address).is_err());
        }
    }

    #[test]
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
}
