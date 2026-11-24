//! Authenticated, expiring, scope-bound checkpoint tokens.

use logical_db_model::Scope;
use logical_db_protocol::{Checkpoint, ProtocolError};
use thiserror::Error;

/// Magic prefix identifying a versioned checkpoint token body.
pub(crate) const CHECKPOINT_MAGIC: &[u8; 4] = b"LCP\x01";
/// Byte length of the authenticated checkpoint body before the MAC tag.
const CHECKPOINT_BODY_BYTES: usize = 52;
/// Total checkpoint token length: body plus a 32-byte authentication tag.
const CHECKPOINT_BYTES: usize = CHECKPOINT_BODY_BYTES + 32;

/// Constant-time authenticated codec for opaque, expiring scoped positions.
#[derive(Clone)]
pub struct CheckpointCodec {
    /// Host-managed 256-bit authentication key.
    key: [u8; 32],
    /// Token lifetime in milliseconds.
    ttl_ms: u64,
}

impl std::fmt::Debug for CheckpointCodec {
    /// Shows the lifetime only; the authentication key never leaves the codec.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CheckpointCodec")
            .field("ttl_ms", &self.ttl_ms)
            .finish_non_exhaustive()
    }
}

impl CheckpointCodec {
    /// Constructs a codec from host-managed key material and a nonzero TTL.
    ///
    /// # Errors
    ///
    /// Refuses an all-zero key or zero TTL.
    pub fn new(key: [u8; 32], ttl_ms: u64) -> Result<Self, CheckpointError> {
        if key == [0; 32] {
            return Err(CheckpointError::WeakKey);
        }
        if ttl_ms == 0 {
            return Err(CheckpointError::InvalidTtl);
        }
        Ok(Self { key, ttl_ms })
    }

    /// Mints an authenticated checkpoint for one scoped position.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::TimeOverflow`] when expiry arithmetic overflows,
    /// or [`CheckpointError::Protocol`] when the encoded token is rejected.
    pub(crate) fn issue(
        &self,
        scope: Scope,
        position: u64,
        now_unix_ms: u64,
    ) -> Result<Checkpoint, CheckpointError> {
        let expires = now_unix_ms
            .checked_add(self.ttl_ms)
            .ok_or(CheckpointError::TimeOverflow)?;
        let mut body = [0; CHECKPOINT_BODY_BYTES];
        body[..4].copy_from_slice(CHECKPOINT_MAGIC);
        body[4..20].copy_from_slice(scope.account().as_uuid().as_bytes());
        body[20..36].copy_from_slice(scope.space().as_uuid().as_bytes());
        body[36..44].copy_from_slice(&position.to_be_bytes());
        body[44..52].copy_from_slice(&expires.to_be_bytes());
        let tag = self.tag(&body);
        let mut token = Vec::with_capacity(CHECKPOINT_BYTES);
        token.extend_from_slice(&body);
        token.extend_from_slice(&tag);
        Checkpoint::from_wire(token).map_err(CheckpointError::Protocol)
    }

    /// Verifies an opaque checkpoint and returns its exclusive position.
    ///
    /// # Errors
    ///
    /// Returns the typed authentication, scope, expiry, or encoding failure.
    pub(crate) fn open(
        &self,
        scope: Scope,
        token: &Checkpoint,
        now_unix_ms: u64,
    ) -> Result<u64, CheckpointError> {
        let Ok(wire) = <&[u8; CHECKPOINT_BYTES]>::try_from(token.as_wire()) else {
            return Err(CheckpointError::Malformed);
        };
        let body = wire
            .first_chunk::<CHECKPOINT_BODY_BYTES>()
            .ok_or(CheckpointError::Malformed)?;
        let supplied = wire.last_chunk::<32>().ok_or(CheckpointError::Malformed)?;
        let fields = BodyFields::parse(body).ok_or(CheckpointError::Malformed)?;
        if fields.magic != CHECKPOINT_MAGIC {
            return Err(CheckpointError::Malformed);
        }
        if !tags_equal(lgwks_std::hash::keyed(&self.key, body).as_bytes(), supplied) {
            return Err(CheckpointError::InvalidMac);
        }
        if fields.account != scope.account().as_uuid().as_bytes()
            || fields.space != scope.space().as_uuid().as_bytes()
        {
            return Err(CheckpointError::WrongScope);
        }
        if now_unix_ms >= fields.expires {
            return Err(CheckpointError::Expired);
        }
        Ok(fields.position)
    }

    /// Computes the keyed authentication tag for one checkpoint body.
    fn tag(&self, body: &[u8]) -> [u8; 32] {
        *lgwks_std::hash::keyed(&self.key, body).as_bytes()
    }
}

/// Compares two authentication tags in constant time.
///
/// The folded-xor walk exposes no timing dependence on where the bytes first
/// differ, matching the constant-time equality the estate digest type
/// provides internally.
#[must_use]
pub fn tags_equal(computed: &[u8; 32], expected: &[u8; 32]) -> bool {
    let mut difference = 0_u8;
    for (left, right) in computed.iter().zip(expected.iter()) {
        difference |= left ^ right;
    }
    difference == 0
}

/// Opaque checkpoint validation failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum CheckpointError {
    /// Key material is an obvious unconfigured sentinel.
    #[error("checkpoint key must not be all zero")]
    WeakKey,
    /// Checkpoint TTL must be nonzero.
    #[error("checkpoint TTL must be nonzero")]
    InvalidTtl,
    /// Expiry arithmetic overflowed.
    #[error("checkpoint expiry overflowed")]
    TimeOverflow,
    /// Token length, magic, or integer encoding is malformed.
    #[error("checkpoint is malformed")]
    Malformed,
    /// Authentication tag did not verify.
    #[error("checkpoint authentication failed")]
    InvalidMac,
    /// Token belongs to another account or application space.
    #[error("checkpoint belongs to another scope")]
    WrongScope,
    /// Token's exclusive expiration has passed.
    #[error("checkpoint expired")]
    Expired,
    /// Protocol checkpoint constructor rejected the encoded token.
    #[error("checkpoint protocol encoding failed: {0}")]
    Protocol(#[source] ProtocolError),
}

/// Fixed fields of one checkpoint body, read in wire order.
struct BodyFields<'body> {
    /// Version magic, `CHECKPOINT_MAGIC` when well formed.
    magic: &'body [u8; 4],
    /// Account identifier bytes.
    account: &'body [u8; 16],
    /// Application space identifier bytes.
    space: &'body [u8; 16],
    /// Exclusive scoped position.
    position: u64,
    /// Exclusive expiry in Unix milliseconds.
    expires: u64,
}

impl<'body> BodyFields<'body> {
    /// Splits a body into its fields; `None` only if the layout outgrows the body.
    fn parse(body: &'body [u8; CHECKPOINT_BODY_BYTES]) -> Option<Self> {
        let (magic, rest) = body.split_first_chunk::<4>()?;
        let (account, rest) = rest.split_first_chunk::<16>()?;
        let (space, rest) = rest.split_first_chunk::<16>()?;
        let (position, rest) = rest.split_first_chunk::<8>()?;
        let (expires, _) = rest.split_first_chunk::<8>()?;
        Some(Self {
            magic,
            account,
            space,
            position: u64::from_be_bytes(*position),
            expires: u64::from_be_bytes(*expires),
        })
    }
}
