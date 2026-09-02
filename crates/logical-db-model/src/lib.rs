//! Stable identities, authority decisions, limits, and errors for Logical-DB.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

/// Maximum UTF-8 bytes in a user-controlled display-independent name.
pub const MAX_NAME_BYTES: usize = 512;
/// Maximum causal change bytes accepted in one envelope.
pub const MAX_CHANGE_BYTES: usize = 1_048_576;
/// Maximum changes accepted in one protocol page.
pub const MAX_CHANGES_PER_PAGE: usize = 256;
/// Maximum aggregate bytes accepted in one protocol page.
pub const MAX_PAGE_BYTES: usize = 8_388_608;
/// Maximum encoded state for one materialized document snapshot.
pub const MAX_SNAPSHOT_BYTES: usize = 16_777_216;

macro_rules! entity_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Generates a time-ordered `UUIDv7` identifier.
            #[must_use]
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            /// Wraps a previously validated UUID.
            #[must_use]
            pub const fn from_uuid(value: Uuid) -> Self {
                Self(value)
            }

            /// Exposes the portable UUID value without exposing storage bytes.
            #[must_use]
            pub const fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

entity_id!(AccountId, "Identifies one person's logical account.");
entity_id!(
    SpaceId,
    "Identifies an application-owned namespace within an account."
);
entity_id!(DocumentId, "Identifies a convergent structured document.");
entity_id!(IndexId, "Identifies one schema-declared secondary index.");
entity_id!(
    ReplicaId,
    "Identifies one durable local replica installation."
);
entity_id!(OperationId, "Identifies one retryable logical operation.");
entity_id!(PeerId, "Identifies one synchronization service or peer.");

/// Portable scalar accepted by an ordered secondary index.
///
/// Floating-point and CRDT container values are deliberately excluded: their
/// ordering and conflict semantics must be made explicit by an application
/// projection before they enter a durable query key.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum IndexValue {
    /// Boolean ordering is `false`, then `true`.
    Boolean(bool),
    /// Signed integer ordered by mathematical value.
    Integer(i64),
    /// Unsigned integer ordered by mathematical value.
    Unsigned(u64),
    /// UTF-8 text ordered by encoded byte sequence.
    Text(String),
    /// Opaque bytes ordered lexicographically.
    Bytes(Vec<u8>),
    /// Milliseconds since the Unix epoch, ordered chronologically.
    Timestamp(i64),
}

/// One materialized secondary-index value for a document.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IndexEntry {
    /// Stable schema identity of the projection.
    pub index: IndexId,
    /// Current projected value. Absence is represented by no entry.
    pub value: IndexValue,
}

/// Monotonic optimistic-concurrency marker for one materialized document.
///
/// This is deliberately independent from a CRDT change hash: one snapshot can
/// incorporate several concurrent changes and therefore has multiple causal
/// heads rather than one privileged last change.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct DocumentRevision(u64);

impl DocumentRevision {
    /// Constructs a revision read from durable storage.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the portable numeric revision.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the checked successor.
    #[must_use]
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Content identity of immutable CRDT change bytes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct ChangeDigest([u8; 32]);

impl ChangeDigest {
    /// Wraps a digest computed by the CRDT boundary.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the stable digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Canonical identity of the caller's requested logical mutation.
///
/// This is distinct from [`ChangeDigest`]: retries bind to what the caller
/// requested, while the change digest identifies the CRDT result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct OperationDigest([u8; 32]);

impl OperationDigest {
    /// Wraps a digest produced from the application's canonical request bytes.
    #[must_use]
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the stable digest bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Logical ownership scope independent from physical backend keys.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Scope {
    account: AccountId,
    space: SpaceId,
}

impl Scope {
    /// Creates a scope from typed identifiers.
    #[must_use]
    pub const fn new(account: AccountId, space: SpaceId) -> Self {
        Self { account, space }
    }

    /// Returns the owning account.
    #[must_use]
    pub const fn account(self) -> AccountId {
        self.account
    }

    /// Returns the application space.
    #[must_use]
    pub const fn space(self) -> SpaceId {
        self.space
    }
}

/// Effects understood by the core authorization boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    /// Read materialized document state.
    Read,
    /// Commit a local or replicated document change.
    Write,
    /// Read a bounded query or change page.
    Scan,
    /// Manage synchronization state for the scope.
    Sync,
}

/// Minimal authority result accepted by the core.
///
/// A Braid adapter may produce this value, but Braid runtime types never cross
/// the model or storage boundary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AuthorityDecision {
    scope: Scope,
    effect: Effect,
    operation: OperationDigest,
    expires_at_unix_ms: u64,
    decision_id: String,
}

impl AuthorityDecision {
    /// Builds a bounded decision after the host has authenticated its source.
    ///
    /// # Errors
    ///
    /// Returns [`ModelError::InvalidIdentifierLength`] for an empty or
    /// oversized decision identity and [`ModelError::InvalidAuthorityExpiry`]
    /// for the reserved zero expiry.
    pub fn new(
        scope: Scope,
        effect: Effect,
        operation: OperationDigest,
        expires_at_unix_ms: u64,
        decision_id: impl Into<String>,
    ) -> Result<Self, ModelError> {
        let decision_id = decision_id.into();
        if decision_id.is_empty() || decision_id.len() > MAX_NAME_BYTES {
            return Err(ModelError::InvalidIdentifierLength {
                kind: "authority decision",
                actual: decision_id.len(),
                maximum: MAX_NAME_BYTES,
            });
        }
        if expires_at_unix_ms == 0 {
            return Err(ModelError::InvalidAuthorityExpiry);
        }
        Ok(Self {
            scope,
            effect,
            operation,
            expires_at_unix_ms,
            decision_id,
        })
    }

    /// Checks exact scope, effect, request digest, and exclusive expiry.
    #[must_use]
    pub fn permits(
        &self,
        scope: Scope,
        effect: Effect,
        operation: OperationDigest,
        now_unix_ms: u64,
    ) -> bool {
        self.scope == scope
            && self.effect == effect
            && self.operation == operation
            && now_unix_ms < self.expires_at_unix_ms
    }

    /// Returns the audit identity assigned by the authority adapter.
    #[must_use]
    pub fn decision_id(&self) -> &str {
        &self.decision_id
    }
}

/// Stable model and validation failures.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ModelError {
    /// A caller-controlled identifier violated its encoded bound.
    #[error("{kind} length {actual} is outside the maximum {maximum}")]
    InvalidIdentifierLength {
        /// Semantic identifier class.
        kind: &'static str,
        /// Observed byte count.
        actual: usize,
        /// Accepted byte count.
        maximum: usize,
    },
    /// An authority decision used a sentinel rather than a real expiry.
    #[error("authority decision expiry must be nonzero")]
    InvalidAuthorityExpiry,
    /// The supplied decision did not cover an operation.
    #[error("authority decision does not cover the requested scope and effect")]
    AuthorityDenied,
    /// One encoded change exceeded the bounded envelope.
    #[error("change length {actual} exceeds maximum {maximum}")]
    ChangeTooLarge {
        /// Observed encoded byte count.
        actual: usize,
        /// Accepted encoded byte count.
        maximum: usize,
    },
    /// A protocol page exceeded a count or byte limit.
    #[error("page exceeds bounded protocol limits")]
    PageTooLarge,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_round_trip_as_typed_uuid_values() {
        let id = DocumentId::new();
        assert_eq!(DocumentId::from_uuid(id.as_uuid()), id);
    }

    #[test]
    fn authority_is_exactly_scope_bound() {
        let account = AccountId::new();
        let first = Scope::new(account, SpaceId::new());
        let second = Scope::new(account, SpaceId::new());
        let operation = OperationDigest::new([7; 32]);
        let decision =
            AuthorityDecision::new(first, Effect::Read, operation, 100, "local-policy").unwrap();
        assert!(decision.permits(first, Effect::Read, operation, 99));
        assert!(!decision.permits(first, Effect::Write, operation, 99));
        assert!(!decision.permits(second, Effect::Read, operation, 99));
        assert!(!decision.permits(first, Effect::Read, OperationDigest::new([8; 32]), 99));
        assert!(!decision.permits(first, Effect::Read, operation, 100));
        assert_eq!(decision.decision_id(), "local-policy");
    }

    #[test]
    fn revision_digest_and_resource_values_preserve_every_exact_bit() {
        assert_eq!(MAX_PAGE_BYTES, 8_388_608);
        let revision = DocumentRevision::new(41);
        assert_eq!(revision.get(), 41);
        assert_eq!(revision.next(), Some(DocumentRevision::new(42)));
        assert_eq!(DocumentRevision::new(u64::MAX).next(), None);
        let change = ChangeDigest::new([17; 32]);
        let operation = OperationDigest::new([29; 32]);
        assert_eq!(change.into_bytes(), [17; 32]);
        assert_eq!(operation.into_bytes(), [29; 32]);
    }
}
