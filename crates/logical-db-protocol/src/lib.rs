//! Versioned, transport-neutral synchronization messages.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

pub use logical_db_model::ChangeDigest;
use logical_db_model::{
    DocumentId, MAX_CHANGE_BYTES, MAX_CHANGES_PER_PAGE, MAX_PAGE_BYTES, OperationDigest,
    OperationId, ReplicaId, Scope,
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error as _, SeqAccess, Visitor},
};
use thiserror::Error;

/// Current wire protocol version.
pub const PROTOCOL_VERSION: u16 = 1;
const MAX_CHECKPOINT_BYTES: usize = 4_096;

/// Opaque server-issued progress position for exactly one account and space.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Checkpoint(
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "deserialize_checkpoint_bytes"
    )]
    Vec<u8>,
);

impl Checkpoint {
    /// Builds a checkpoint from trusted wire bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidCheckpoint`] when the value is empty or
    /// exceeds the opaque-token bound.
    pub fn from_wire(bytes: Vec<u8>) -> Result<Self, ProtocolError> {
        if bytes.is_empty() || bytes.len() > MAX_CHECKPOINT_BYTES {
            return Err(ProtocolError::InvalidCheckpoint);
        }
        Ok(Self(bytes))
    }

    /// Returns opaque bytes for persistence or transport only.
    #[must_use]
    pub fn as_wire(&self) -> &[u8] {
        &self.0
    }
}

/// One immutable, retryable document change.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChangeEnvelope {
    /// Logical ownership scope.
    pub scope: Scope,
    /// Document receiving the change.
    pub document: DocumentId,
    /// Replica that originated the operation.
    pub replica: ReplicaId,
    /// Idempotency identity for transport retries.
    pub operation: OperationId,
    /// Canonical caller request identity used by authority and retry binding.
    pub request: OperationDigest,
    /// CRDT-native change hash.
    pub digest: ChangeDigest,
    /// Canonical Automerge change bytes.
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "deserialize_change_bytes"
    )]
    pub bytes: Vec<u8>,
}

impl ChangeEnvelope {
    /// Validates an untrusted envelope before storage or CRDT decoding.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::InvalidChangeSize`] when the encoded change is
    /// empty or exceeds the per-change bound.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.bytes.is_empty() || self.bytes.len() > MAX_CHANGE_BYTES {
            return Err(ProtocolError::InvalidChangeSize(self.bytes.len()));
        }
        Ok(())
    }
}

/// Bounded client-to-service change upload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PushRequest {
    /// Exact protocol version encoded by the client.
    pub protocol: u16,
    /// Stable sender replica.
    pub replica: ReplicaId,
    /// Ordered, independently idempotent changes.
    #[serde(deserialize_with = "deserialize_changes")]
    pub changes: Vec<ChangeEnvelope>,
}

impl PushRequest {
    /// Enforces count, byte, version, and sender invariants.
    ///
    /// # Errors
    ///
    /// Returns a typed [`ProtocolError`] for unsupported versions, invalid page
    /// bounds, malformed changes, or cross-replica attribution.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_protocol(self.protocol)?;
        validate_page(&self.changes)?;
        if self
            .changes
            .iter()
            .any(|change| change.replica != self.replica)
        {
            return Err(ProtocolError::ReplicaMismatch);
        }
        Ok(())
    }
}

/// Per-change upload result so one invalid change cannot hide other outcomes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum PushOutcome {
    /// The service durably accepted the change.
    Accepted {
        /// Idempotency identity echoed from the request.
        operation: OperationId,
    },
    /// The exact operation had already been accepted.
    Duplicate {
        /// Idempotency identity echoed from the request.
        operation: OperationId,
    },
    /// The service rejected the change without advancing it.
    Rejected {
        /// Idempotency identity echoed from the request.
        operation: OperationId,
        /// Stable machine-readable rejection category.
        code: RejectionCode,
    },
}

/// Stable rejection categories carried inside successful batch responses.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionCode {
    /// Change bytes did not match the supplied digest.
    DigestMismatch,
    /// Change bytes were malformed or causally invalid.
    InvalidChange,
    /// Operation identity was previously used for different bytes.
    OperationConflict,
    /// Authority was justified against a scope frontier that has since advanced.
    StaleAuthority,
    /// A single-use execution invocation was already consumed by another write.
    InvocationConsumed,
    /// Caller lacks authority for the scope.
    Forbidden,
}

/// Service response to one bounded push page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PushResponse {
    /// Exact protocol version encoded by the service.
    pub protocol: u16,
    /// One result for every request entry, in request order.
    #[serde(deserialize_with = "deserialize_outcomes")]
    pub outcomes: Vec<PushOutcome>,
}

impl PushResponse {
    /// Verifies version, cardinality, and request-order operation correlation.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError::OutcomeMismatch`] if a response omits,
    /// duplicates, reorders, or substitutes an operation result.
    pub fn validate(&self, request: &PushRequest) -> Result<(), ProtocolError> {
        validate_protocol(self.protocol)?;
        if self.outcomes.len() != request.changes.len() {
            return Err(ProtocolError::OutcomeMismatch);
        }
        for (outcome, change) in self.outcomes.iter().zip(&request.changes) {
            let operation = match outcome {
                PushOutcome::Accepted { operation }
                | PushOutcome::Duplicate { operation }
                | PushOutcome::Rejected { operation, .. } => operation,
            };
            if *operation != change.operation {
                return Err(ProtocolError::OutcomeMismatch);
            }
        }
        Ok(())
    }
}

/// Request for changes after an opaque durable checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PullRequest {
    /// Exact protocol version encoded by the client.
    pub protocol: u16,
    /// Scope whose ordered service log is requested.
    pub scope: Scope,
    /// Pulling replica, used for audit and policy.
    pub replica: ReplicaId,
    /// Canonical identity of this bounded pull request for authority binding.
    pub request: OperationDigest,
    /// Last atomically installed service checkpoint, or none for full pull.
    pub after: Option<Checkpoint>,
    /// Requested count bound, capped by the protocol maximum.
    pub max_changes: u16,
    /// Requested encoded byte bound, capped by the protocol maximum.
    pub max_bytes: u32,
}

impl PullRequest {
    /// Validates bounds and protocol version before service work begins.
    ///
    /// # Errors
    ///
    /// Returns a typed [`ProtocolError`] for an unsupported version or invalid
    /// count and byte limits.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_protocol(self.protocol)?;
        if self.max_changes == 0 || usize::from(self.max_changes) > MAX_CHANGES_PER_PAGE {
            return Err(ProtocolError::PageLimit);
        }
        if self.max_bytes == 0 || self.max_bytes as usize > MAX_PAGE_BYTES {
            return Err(ProtocolError::PageLimit);
        }
        Ok(())
    }
}

/// One bounded service-to-client page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PullPage {
    /// Exact protocol version encoded by the service.
    pub protocol: u16,
    /// Changes preceding `checkpoint` in the scoped service log.
    #[serde(deserialize_with = "deserialize_changes")]
    pub changes: Vec<ChangeEnvelope>,
    /// Replacement checkpoint persisted atomically with `changes`.
    pub checkpoint: Checkpoint,
    /// Whether another page is immediately available.
    pub more: bool,
}

impl PullPage {
    /// Validates a page before any durable local mutation starts.
    ///
    /// # Errors
    ///
    /// Returns a typed [`ProtocolError`] for unsupported versions, invalid page
    /// bounds, malformed changes, or material crossing the requested scope.
    pub fn validate(&self, expected_scope: Scope) -> Result<(), ProtocolError> {
        validate_protocol(self.protocol)?;
        validate_page(&self.changes)?;
        if self
            .changes
            .iter()
            .any(|change| change.scope != expected_scope)
        {
            return Err(ProtocolError::ScopeMismatch);
        }
        Ok(())
    }
}

/// Stable transport-neutral protocol failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ProtocolError {
    /// Sender and receiver do not implement the same protocol.
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u16),
    /// A checkpoint violated its opaque wire bounds.
    #[error("checkpoint is empty or exceeds the wire limit")]
    InvalidCheckpoint,
    /// A single change violated its encoded bound.
    #[error("change byte length {0} is outside protocol bounds")]
    InvalidChangeSize(usize),
    /// A page violated count or aggregate byte bounds.
    #[error("page exceeds protocol count or byte limits")]
    PageLimit,
    /// A push contained changes attributed to a different sender.
    #[error("change replica differs from push sender")]
    ReplicaMismatch,
    /// A pull response crossed its requested scope.
    #[error("change scope differs from pull scope")]
    ScopeMismatch,
    /// Push outcomes did not correspond one-for-one in request order.
    #[error("push outcomes do not match request operations")]
    OutcomeMismatch,
}

fn validate_protocol(version: u16) -> Result<(), ProtocolError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion(version))
    }
}

fn validate_page(changes: &[ChangeEnvelope]) -> Result<(), ProtocolError> {
    if changes.len() > MAX_CHANGES_PER_PAGE {
        return Err(ProtocolError::PageLimit);
    }
    let mut bytes = 0usize;
    for change in changes {
        change.validate()?;
        bytes = bytes.saturating_add(change.bytes.len());
        if bytes > MAX_PAGE_BYTES {
            return Err(ProtocolError::PageLimit);
        }
    }
    Ok(())
}

fn deserialize_checkpoint_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = deserialize_bounded_bytes(deserializer, MAX_CHECKPOINT_BYTES, "checkpoint bytes")?;
    if bytes.is_empty() {
        return Err(D::Error::custom("checkpoint must not be empty"));
    }
    Ok(bytes)
}

fn deserialize_change_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = deserialize_bounded_bytes(deserializer, MAX_CHANGE_BYTES, "change bytes")?;
    if bytes.is_empty() {
        return Err(D::Error::custom("change bytes must not be empty"));
    }
    Ok(bytes)
}

fn deserialize_changes<'de, D>(deserializer: D) -> Result<Vec<ChangeEnvelope>, D::Error>
where
    D: Deserializer<'de>,
{
    struct ChangesVisitor;

    impl<'de> Visitor<'de> for ChangesVisitor {
        type Value = Vec<ChangeEnvelope>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded page of change envelopes")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let capacity = sequence.size_hint().unwrap_or(0).min(MAX_CHANGES_PER_PAGE);
            let mut changes = Vec::with_capacity(capacity);
            let mut aggregate_bytes = 0usize;
            while let Some(change) = sequence.next_element::<ChangeEnvelope>()? {
                if changes.len() == MAX_CHANGES_PER_PAGE {
                    return Err(A::Error::custom("change page count exceeds limit"));
                }
                aggregate_bytes = aggregate_bytes
                    .checked_add(change.bytes.len())
                    .ok_or_else(|| A::Error::custom("change page byte count overflow"))?;
                if aggregate_bytes > MAX_PAGE_BYTES {
                    return Err(A::Error::custom("change page bytes exceed limit"));
                }
                changes.push(change);
            }
            Ok(changes)
        }
    }

    deserializer.deserialize_seq(ChangesVisitor)
}

fn deserialize_outcomes<'de, D>(deserializer: D) -> Result<Vec<PushOutcome>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_bounded_vec(deserializer, MAX_CHANGES_PER_PAGE, "push outcomes")
}

fn deserialize_bounded_bytes<'de, D>(
    deserializer: D,
    maximum: usize,
    expecting: &'static str,
) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BytesVisitor {
        maximum: usize,
        expecting: &'static str,
    }

    impl<'de> Visitor<'de> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(self.expecting)
        }

        fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_byte_buf(value.to_vec())
        }

        fn visit_borrowed_bytes<E>(self, value: &'de [u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            self.visit_bytes(value)
        }

        fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value.len() > self.maximum {
                return Err(E::invalid_length(value.len(), &self));
            }
            Ok(value)
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let capacity = sequence.size_hint().unwrap_or(0).min(self.maximum);
            let mut values = Vec::with_capacity(capacity);
            while let Some(value) = sequence.next_element()? {
                if values.len() == self.maximum {
                    return Err(A::Error::invalid_length(
                        self.maximum.saturating_add(1),
                        &self,
                    ));
                }
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_byte_buf(BytesVisitor { maximum, expecting })
}

fn deserialize_bounded_vec<'de, D, T>(
    deserializer: D,
    maximum: usize,
    expecting: &'static str,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct BoundedVecVisitor<T> {
        maximum: usize,
        expecting: &'static str,
        marker: std::marker::PhantomData<T>,
    }

    impl<'de, T> Visitor<'de> for BoundedVecVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str(self.expecting)
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let capacity = sequence.size_hint().unwrap_or(0).min(self.maximum);
            let mut values = Vec::with_capacity(capacity);
            while let Some(value) = sequence.next_element()? {
                if values.len() == self.maximum {
                    return Err(A::Error::invalid_length(
                        self.maximum.saturating_add(1),
                        &self,
                    ));
                }
                values.push(value);
            }
            Ok(values)
        }
    }

    deserializer.deserialize_seq(BoundedVecVisitor {
        maximum,
        expecting,
        marker: std::marker::PhantomData,
    })
}

#[cfg(test)]
mod tests {
    use logical_db_model::{AccountId, SpaceId};

    use super::*;

    fn envelope(replica: ReplicaId, scope: Scope) -> ChangeEnvelope {
        ChangeEnvelope {
            scope,
            document: DocumentId::new(),
            replica,
            operation: OperationId::new(),
            request: OperationDigest::new([3; 32]),
            digest: ChangeDigest::new([7; 32]),
            bytes: vec![1, 2, 3],
        }
    }

    #[test]
    fn push_rejects_cross_replica_attribution() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let sender = ReplicaId::new();
        let request = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica: sender,
            changes: vec![envelope(ReplicaId::new(), scope)],
        };
        assert_eq!(request.validate(), Err(ProtocolError::ReplicaMismatch));
    }

    #[test]
    fn pull_rejects_cross_scope_material() {
        let first = Scope::new(AccountId::new(), SpaceId::new());
        let second = Scope::new(AccountId::new(), SpaceId::new());
        let page = PullPage {
            protocol: PROTOCOL_VERSION,
            changes: vec![envelope(ReplicaId::new(), second)],
            checkpoint: Checkpoint::from_wire(vec![1]).unwrap(),
            more: false,
        };
        assert_eq!(page.validate(first), Err(ProtocolError::ScopeMismatch));
    }

    #[test]
    fn push_response_rejects_missing_and_reordered_operation_results() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let first = envelope(replica, scope);
        let second = envelope(replica, scope);
        let request = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![first.clone(), second.clone()],
        };
        let missing = PushResponse {
            protocol: PROTOCOL_VERSION,
            outcomes: vec![PushOutcome::Accepted {
                operation: first.operation,
            }],
        };
        assert_eq!(
            missing.validate(&request),
            Err(ProtocolError::OutcomeMismatch)
        );
        let reordered = PushResponse {
            protocol: PROTOCOL_VERSION,
            outcomes: vec![
                PushOutcome::Accepted {
                    operation: second.operation,
                },
                PushOutcome::Accepted {
                    operation: first.operation,
                },
            ],
        };
        assert_eq!(
            reordered.validate(&request),
            Err(ProtocolError::OutcomeMismatch)
        );
    }

    #[test]
    fn hostile_sequence_hint_cannot_preallocate_past_change_limit() {
        use serde::de::value::{Error as ValueError, SeqDeserializer};

        let oversized = std::iter::repeat_n(0u8, MAX_CHANGE_BYTES + 1);
        let deserializer = SeqDeserializer::<_, ValueError>::new(oversized);
        assert!(deserialize_change_bytes(deserializer).is_err());
    }

    #[test]
    fn checkpoint_deserialization_cannot_bypass_constructor_bounds() {
        use serde::de::value::{Error as ValueError, SeqDeserializer};

        let empty = SeqDeserializer::<_, ValueError>::new(std::iter::empty::<u8>());
        assert!(Checkpoint::deserialize(empty).is_err());
        let oversized = SeqDeserializer::<_, ValueError>::new(std::iter::repeat_n(
            0u8,
            MAX_CHECKPOINT_BYTES + 1,
        ));
        assert!(Checkpoint::deserialize(oversized).is_err());
    }

    #[test]
    fn checkpoint_and_change_bounds_accept_exact_edges_only() {
        assert_eq!(
            Checkpoint::from_wire(Vec::new()),
            Err(ProtocolError::InvalidCheckpoint)
        );
        let one = Checkpoint::from_wire(vec![9]).unwrap();
        assert_eq!(one.as_wire(), &[9]);
        let maximum = vec![7; MAX_CHECKPOINT_BYTES];
        assert_eq!(
            Checkpoint::from_wire(maximum.clone()).unwrap().as_wire(),
            maximum
        );
        assert_eq!(
            Checkpoint::from_wire(vec![0; MAX_CHECKPOINT_BYTES + 1]),
            Err(ProtocolError::InvalidCheckpoint)
        );

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let mut change = envelope(replica, scope);
        change.bytes.clear();
        assert_eq!(change.validate(), Err(ProtocolError::InvalidChangeSize(0)));
        change.bytes = vec![1];
        assert_eq!(change.validate(), Ok(()));
        change.bytes = vec![1; MAX_CHANGE_BYTES];
        assert_eq!(change.validate(), Ok(()));
        change.bytes.push(1);
        assert_eq!(
            change.validate(),
            Err(ProtocolError::InvalidChangeSize(MAX_CHANGE_BYTES + 1))
        );
    }

    #[test]
    fn every_protocol_and_page_boundary_has_an_exact_rejection_edge() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let max_changes = u16::try_from(MAX_CHANGES_PER_PAGE).unwrap();
        let max_bytes = u32::try_from(MAX_PAGE_BYTES).unwrap();
        let pull = |protocol, max_changes, max_bytes| PullRequest {
            protocol,
            scope,
            replica,
            request: OperationDigest::new([4; 32]),
            after: None,
            max_changes,
            max_bytes,
        };
        assert!(matches!(
            pull(PROTOCOL_VERSION + 1, 1, 1).validate(),
            Err(ProtocolError::UnsupportedVersion(_))
        ));
        assert_eq!(
            pull(PROTOCOL_VERSION, 0, 1).validate(),
            Err(ProtocolError::PageLimit)
        );
        assert_eq!(pull(PROTOCOL_VERSION, 1, 1).validate(), Ok(()));
        assert_eq!(
            pull(PROTOCOL_VERSION, max_changes, max_bytes).validate(),
            Ok(())
        );
        assert_eq!(
            pull(PROTOCOL_VERSION, max_changes + 1, 1).validate(),
            Err(ProtocolError::PageLimit)
        );
        assert_eq!(
            pull(PROTOCOL_VERSION, 1, 0).validate(),
            Err(ProtocolError::PageLimit)
        );
        assert_eq!(
            pull(PROTOCOL_VERSION, 1, max_bytes + 1).validate(),
            Err(ProtocolError::PageLimit)
        );

        let mut exact_bytes = envelope(replica, scope);
        exact_bytes.bytes = vec![1; MAX_CHANGE_BYTES];
        let exact_page = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![exact_bytes; MAX_PAGE_BYTES / MAX_CHANGE_BYTES],
        };
        assert_eq!(exact_page.validate(), Ok(()));
        let mut oversized_page = exact_page;
        oversized_page.changes.push(envelope(replica, scope));
        assert_eq!(oversized_page.validate(), Err(ProtocolError::PageLimit));

        let count_page = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![envelope(replica, scope); MAX_CHANGES_PER_PAGE + 1],
        };
        assert_eq!(count_page.validate(), Err(ProtocolError::PageLimit));
        let exact_count = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![envelope(replica, scope); MAX_CHANGES_PER_PAGE],
        };
        assert_eq!(exact_count.validate(), Ok(()));
    }

    #[test]
    fn bounded_deserializers_preserve_values_and_reject_the_first_excess_item() {
        use serde::de::value::{BorrowedBytesDeserializer, Error as ValueError, SeqDeserializer};

        let source = [1_u8, 2, 3];
        let bytes = deserialize_bounded_bytes(
            BorrowedBytesDeserializer::<ValueError>::new(&source),
            source.len(),
            "test bytes",
        )
        .unwrap();
        assert_eq!(bytes, source);

        let exact = SeqDeserializer::<_, ValueError>::new(0_u8..4);
        assert_eq!(
            deserialize_bounded_vec::<_, u8>(exact, 4, "test values").unwrap(),
            vec![0, 1, 2, 3]
        );
        let excessive = SeqDeserializer::<_, ValueError>::new(0_u8..5);
        assert!(deserialize_bounded_vec::<_, u8>(excessive, 4, "test values").is_err());

        let exact_bytes = SeqDeserializer::<_, ValueError>::new(0_u8..4);
        assert_eq!(
            deserialize_bounded_bytes(exact_bytes, 4, "test bytes").unwrap(),
            vec![0, 1, 2, 3]
        );
        let shorter_bytes = SeqDeserializer::<_, ValueError>::new(0_u8..3);
        assert_eq!(
            deserialize_bounded_bytes(shorter_bytes, 4, "test bytes").unwrap(),
            vec![0, 1, 2]
        );
        let excessive_bytes = SeqDeserializer::<_, ValueError>::new(0_u8..5);
        assert!(deserialize_bounded_bytes(excessive_bytes, 4, "test bytes").is_err());
    }

    #[test]
    fn cbor_wire_visitors_preserve_exact_pages_and_reject_first_excess() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let request = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![envelope(replica, scope); MAX_CHANGES_PER_PAGE],
        };
        let mut wire = Vec::new();
        ciborium::into_writer(&request, &mut wire).unwrap();
        let decoded: PushRequest = ciborium::from_reader(wire.as_slice()).unwrap();
        assert_eq!(decoded, request);
        assert_eq!(decoded.validate(), Ok(()));

        let mut excessive = request.clone();
        excessive.changes.push(envelope(replica, scope));
        wire.clear();
        ciborium::into_writer(&excessive, &mut wire).unwrap();
        assert!(ciborium::from_reader::<PushRequest, _>(wire.as_slice()).is_err());

        let response = PushResponse {
            protocol: PROTOCOL_VERSION,
            outcomes: request
                .changes
                .iter()
                .map(|change| PushOutcome::Accepted {
                    operation: change.operation,
                })
                .collect(),
        };
        wire.clear();
        ciborium::into_writer(&response, &mut wire).unwrap();
        let decoded: PushResponse = ciborium::from_reader(wire.as_slice()).unwrap();
        assert_eq!(decoded, response);

        let mut excessive = response;
        excessive.outcomes.push(PushOutcome::Accepted {
            operation: OperationId::new(),
        });
        wire.clear();
        ciborium::into_writer(&excessive, &mut wire).unwrap();
        assert!(ciborium::from_reader::<PushResponse, _>(wire.as_slice()).is_err());

        let mut exact_bytes = envelope(replica, scope);
        exact_bytes.bytes = vec![1; MAX_CHANGE_BYTES];
        let exact = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![exact_bytes; MAX_PAGE_BYTES / MAX_CHANGE_BYTES],
        };
        wire.clear();
        ciborium::into_writer(&exact, &mut wire).unwrap();
        let decoded: PushRequest = ciborium::from_reader(wire.as_slice()).unwrap();
        assert_eq!(decoded.changes.len(), exact.changes.len());
        let mut excessive = exact;
        excessive.changes.push(envelope(replica, scope));
        wire.clear();
        ciborium::into_writer(&excessive, &mut wire).unwrap();
        assert!(ciborium::from_reader::<PushRequest, _>(wire.as_slice()).is_err());
    }

    #[test]
    fn valid_push_response_and_pull_page_still_validate_their_versions() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let change = envelope(replica, scope);
        let request = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![change.clone()],
        };
        let valid = PushResponse {
            protocol: PROTOCOL_VERSION,
            outcomes: vec![PushOutcome::Duplicate {
                operation: change.operation,
            }],
        };
        assert_eq!(valid.validate(&request), Ok(()));
        assert!(matches!(
            PushResponse {
                protocol: PROTOCOL_VERSION + 1,
                ..valid
            }
            .validate(&request),
            Err(ProtocolError::UnsupportedVersion(_))
        ));
        let page = PullPage {
            protocol: PROTOCOL_VERSION + 1,
            changes: vec![change],
            checkpoint: Checkpoint::from_wire(vec![1]).unwrap(),
            more: false,
        };
        assert!(matches!(
            page.validate(scope),
            Err(ProtocolError::UnsupportedVersion(_))
        ));
    }
}
