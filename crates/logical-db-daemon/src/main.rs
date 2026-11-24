//! Standalone, loopback-only Logical-DB service composition root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

use std::{
    error::Error,
    fmt, io,
    net::{Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};

use axum::http::HeaderValue;
use foundationdb::directory::{Directory, DirectoryLayer};
use foundationdb_tuple::Subspace;
use lgwks_std::hash::keyed;
use lgwks_std::json::serde::de::DeserializeOwned;
use logical_db_daemon::boot_network;
use logical_db_model::{AccountId, Scope, SpaceId};
use logical_db_server::{AuthenticationError, Authenticator, SystemClock, router};
use logical_db_service::{CheckpointCodec, LocalScopeAuthority};
use logical_db_store_fdb::FdbServiceStore;

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
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "checkpoint key must contain 64 hexadecimal characters, found {}",
                value.len()
            ),
        ));
    }
    let octets = lgwks_std::hex::decode(value).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid checkpoint key: {error}"),
        )
    })?;
    let key = <[u8; 32]>::try_from(octets).map_err(|octets| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("checkpoint key must be 32 octets, found {}", octets.len()),
        )
    })?;
    if key.iter().all(|octet| *octet == 0) {
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
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "bearer token must be 32..=4096 RFC 6750 b64token bytes, found length {}",
                token.len()
            ),
        ))
    }
}

/// Returns whether `id` is the nil UUID, all 128 bits zero (RFC 9562 section 5.9).
fn is_nil(id: &lgwks_std::id::Uuid) -> bool {
    *id.as_bytes() == [0_u8; 16]
}

/// Parses one identifier through the identifier type's own serde contract
/// (RFC 9562 UUID text) and refuses the nil UUID, so the daemon carries no
/// UUID crate of its own.
fn parse_identifier<T: DeserializeOwned>(
    value: &str,
    name: &'static str,
    is_nil: fn(&T) -> bool,
) -> Result<T, io::Error> {
    let invalid = |error: lgwks_std::json::Error| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("{name}: {error}"))
    };
    let quoted = lgwks_std::json::to_string(value).map_err(invalid)?;
    let identifier: T = lgwks_std::json::from_str(&quoted).map_err(invalid)?;
    if is_nil(&identifier) {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, name));
    }
    Ok(identifier)
}

/// Runs `shutdown_signal`.
async fn shutdown_signal() -> Result<(), io::Error> {
    #[cfg(unix)]
    {
        let mut terminate = lgwks_bot::rt::signal::unix::signal(
            lgwks_bot::rt::signal::unix::SignalKind::terminate(),
        )?;
        lgwks_bot::rt::select! {
            interrupt = lgwks_bot::rt::signal::ctrl_c() => interrupt,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        lgwks_bot::rt::signal::ctrl_c().await
    }
}

/// Holds the first shutdown-listener failure so `service_main` can report it.
type SignalFailure = Arc<Mutex<Option<io::Error>>>;

/// Records a shutdown-listener failure for the composition root.
fn record_signal_failure(slot: &SignalFailure, error: io::Error) {
    match slot.lock() {
        Ok(mut guard) => *guard = Some(error),
        Err(poisoned) => *poisoned.into_inner() = Some(error),
    }
}

/// Takes the recorded shutdown-listener failure, if any.
fn take_signal_failure(slot: &SignalFailure) -> Option<io::Error> {
    match slot.lock() {
        Ok(mut guard) => guard.take(),
        Err(poisoned) => poisoned.into_inner().take(),
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

/// Why the daemon refused to start or stopped serving, one variant per layer.
#[derive(Debug)]
enum DaemonError {
    /// Configuration, socket, listener or shutdown-signal I/O failed.
    Io(io::Error),
    /// `LOGICAL_DB_BIND` is set but is not a socket address.
    Bind(std::net::AddrParseError),
    /// `LOGICAL_DB_BIND` is set but is not Unicode.
    Setting(std::env::VarError),
    /// The FoundationDB client refused the network, cluster or transaction.
    Fdb(foundationdb::FdbError),
    /// The directory-allocation transaction failed to commit.
    Commit(foundationdb::TransactionCommitError),
    /// The FoundationDB service store refused its root.
    Store(logical_db_store_fdb::FdbStoreError),
    /// The checkpoint codec refused its key or lifetime.
    Checkpoint(logical_db_service::CheckpointError),
}

impl fmt::Display for DaemonError {
    /// Names the failing layer and its cause for the operator.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Io(ref error) => write!(formatter, "daemon I/O failed: {error}"),
            Self::Bind(ref error) => write!(formatter, "LOGICAL_DB_BIND is invalid: {error}"),
            Self::Setting(ref error) => write!(formatter, "LOGICAL_DB_BIND is unreadable: {error}"),
            Self::Fdb(ref error) => write!(formatter, "FoundationDB failed: {error}"),
            Self::Commit(ref error) => write!(formatter, "FoundationDB commit failed: {error}"),
            Self::Store(ref error) => write!(formatter, "service store refused: {error}"),
            Self::Checkpoint(ref error) => write!(formatter, "checkpoint codec refused: {error}"),
        }
    }
}

impl Error for DaemonError {}

/// Implements `From` for each `DaemonError` source, keeping its variant.
macro_rules! daemon_error_from {
    ($($source:ty => $variant:ident),+ $(,)?) => {
        $(impl From<$source> for DaemonError {
            /// Adapts the source failure without discarding it.
            fn from(error: $source) -> Self {
                Self::$variant(error)
            }
        })+
    };
}

daemon_error_from! {
    io::Error => Io,
    std::net::AddrParseError => Bind,
    std::env::VarError => Setting,
    foundationdb::FdbError => Fdb,
    foundationdb::TransactionCommitError => Commit,
    logical_db_store_fdb::FdbStoreError => Store,
    logical_db_service::CheckpointError => Checkpoint,
}

/// Runs `main`.
fn main() -> Result<(), DaemonError> {
    lgwks_bot::rt::runtime::Builder::new()
        .build()?
        .block_on(service_main())
}

/// Runs `service_main`.
async fn service_main() -> Result<(), DaemonError> {
    let address: SocketAddr = match std::env::var("LOGICAL_DB_BIND") {
        Ok(bind) => bind.parse()?,
        Err(std::env::VarError::NotPresent) => SocketAddr::from((Ipv4Addr::LOCALHOST, 8080)),
        Err(error @ std::env::VarError::NotUnicode(_)) => return Err(error.into()),
    };
    ensure_loopback(&address)?;
    let cluster_file = required("LOGICAL_DB_FDB_CLUSTER_FILE")?;
    let bearer = required("LOGICAL_DB_BEARER_TOKEN")?;
    let key = parse_key(&required("LOGICAL_DB_CHECKPOINT_KEY_HEX")?)?;
    let scope = Scope::new(
        parse_identifier(
            &required("LOGICAL_DB_ACCOUNT_ID")?,
            "LOGICAL_DB_ACCOUNT_ID must be a non-nil UUID",
            |account: &AccountId| is_nil(&account.as_uuid()),
        )?,
        parse_identifier(
            &required("LOGICAL_DB_SPACE_ID")?,
            "LOGICAL_DB_SPACE_ID must be a non-nil UUID",
            |space: &SpaceId| is_nil(&space.as_uuid()),
        )?,
    );

    // The one network boot of this process. `network` outlives the listener and
    // every database-owning router state, and is dropped last, below.
    let network = boot_network(Vec::new())?;
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
    let signal_failure = SignalFailure::default();
    let recorder = Arc::clone(&signal_failure);
    axum::serve(listener, application)
        .with_graceful_shutdown(async move {
            if let Err(error) = shutdown_signal().await {
                record_signal_failure(&recorder, error);
            }
        })
        .await?;
    drop(network);
    match take_signal_failure(&signal_failure) {
        Some(error) => Err(error.into()),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests;
