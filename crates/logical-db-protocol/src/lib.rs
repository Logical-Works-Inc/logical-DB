//! Versioned, transport-neutral synchronization messages.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

mod action;
mod baseline;
#[cfg(test)]
mod tests;
mod wire;
pub use action::{
    PageAction, RecoveryAction, mark_consumed_action, prune_action, recover_action,
    seal_snapshot_action, write_action,
};
pub use baseline::{BaselineAccumulator, BaselineDocument, BaselinePage, BaselineRequest};

use lgwks_std::json::serde::{Deserialize, Serialize};
pub use logical_db_model::ChangeDigest;
use logical_db_model::{
    DocumentId, MAX_CHANGE_BYTES, MAX_CHANGES_PER_PAGE, MAX_PAGE_BYTES, ModelError,
    OperationDigest, OperationId, ReplicaId, Scope,
};
use thiserror::Error;

/// Current wire protocol version.
pub const PROTOCOL_VERSION: u16 = 1;
/// Maximum opaque checkpoint token length accepted from the wire.
const MAX_CHECKPOINT_BYTES: usize = 4_096;

/// Opaque server-issued progress position for exactly one account and space.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[serde(transparent)]
pub struct Checkpoint(
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "wire::deserialize_checkpoint_bytes"
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
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct ChangeEnvelope {
    /// Logical ownership scope.
    pub(crate) scope: Scope,
    /// Document receiving the change.
    pub(crate) document: DocumentId,
    /// Replica that originated the operation.
    pub(crate) replica: ReplicaId,
    /// Idempotency identity for transport retries.
    pub(crate) operation: OperationId,
    /// Canonical caller request identity used by authority and retry binding.
    pub(crate) request: OperationDigest,
    /// CRDT-native change hash.
    pub(crate) digest: ChangeDigest,
    /// Canonical Automerge change bytes.
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "wire::deserialize_change_bytes"
    )]
    pub(crate) bytes: Vec<u8>,
}

impl ChangeEnvelope {
    /// Builds one immutable change from its identities and canonical CRDT bytes;
    /// the only way to compose an envelope outside this crate.
    #[must_use]
    pub fn new(
        scope: Scope,
        document: DocumentId,
        replica: ReplicaId,
        operation: OperationId,
        request: OperationDigest,
        digest: ChangeDigest,
        bytes: Vec<u8>,
    ) -> Self {
        Self {
            scope,
            document,
            replica,
            operation,
            request,
            digest,
            bytes,
        }
    }

    /// Returns the logical ownership scope.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }

    /// Returns the document receiving the change.
    #[must_use]
    pub const fn document(&self) -> DocumentId {
        self.document
    }

    /// Returns the replica that originated the operation.
    #[must_use]
    pub const fn replica(&self) -> ReplicaId {
        self.replica
    }

    /// Returns the idempotency identity for transport retries.
    #[must_use]
    pub const fn operation(&self) -> OperationId {
        self.operation
    }

    /// Returns the canonical caller request identity.
    #[must_use]
    pub const fn request(&self) -> OperationDigest {
        self.request
    }

    /// Returns the CRDT-native change hash.
    #[must_use]
    pub const fn digest(&self) -> ChangeDigest {
        self.digest
    }

    /// Borrows the canonical Automerge change bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Takes the canonical Automerge change bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

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
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct PushRequest {
    /// Exact protocol version encoded by the client.
    pub(crate) protocol: u16,
    /// Stable sender replica.
    pub(crate) replica: ReplicaId,
    /// Ordered, independently idempotent changes.
    #[serde(deserialize_with = "wire::deserialize_changes")]
    pub(crate) changes: Vec<ChangeEnvelope>,
}

impl PushRequest {
    /// Builds a bounded upload request from its version, sender, and changes.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose requests exclusively through here.
    #[must_use]
    pub fn new(protocol: u16, replica: ReplicaId, changes: Vec<ChangeEnvelope>) -> Self {
        Self {
            protocol,
            replica,
            changes,
        }
    }

    /// Returns the protocol version encoded by the client.
    #[must_use]
    pub const fn protocol(&self) -> u16 {
        self.protocol
    }

    /// Returns the stable sender replica.
    #[must_use]
    pub const fn replica(&self) -> ReplicaId {
        self.replica
    }

    /// Borrows the ordered, independently idempotent changes.
    #[must_use]
    pub fn changes(&self) -> &[ChangeEnvelope] {
        &self.changes
    }

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
#[serde(crate = "lgwks_std::json::serde")]
#[serde(tag = "status", rename_all = "snake_case")]
#[non_exhaustive]
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
#[serde(crate = "lgwks_std::json::serde")]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
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
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct PushResponse {
    /// Exact protocol version encoded by the service.
    pub(crate) protocol: u16,
    /// One result for every request entry, in request order.
    #[serde(deserialize_with = "wire::deserialize_outcomes")]
    pub(crate) outcomes: Vec<PushOutcome>,
}

impl PushResponse {
    /// Builds a service response from its version and ordered outcomes.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose responses exclusively through here.
    #[must_use]
    pub fn new(protocol: u16, outcomes: Vec<PushOutcome>) -> Self {
        Self { protocol, outcomes }
    }

    /// Returns the protocol version encoded by the service.
    #[must_use]
    pub const fn protocol(&self) -> u16 {
        self.protocol
    }

    /// Borrows the per-change results, in request order.
    #[must_use]
    pub fn outcomes(&self) -> &[PushOutcome] {
        &self.outcomes
    }

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
            let operation = match *outcome {
                PushOutcome::Accepted { operation }
                | PushOutcome::Duplicate { operation }
                | PushOutcome::Rejected { operation, .. } => operation,
            };
            if operation != change.operation {
                return Err(ProtocolError::OutcomeMismatch);
            }
        }
        Ok(())
    }
}

/// Request for changes after an opaque durable checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
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
    /// Builds a bounded pull request from its version, scope, and page limits.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose requests exclusively through here.
    #[must_use]
    pub fn new(
        protocol: u16,
        scope: Scope,
        replica: ReplicaId,
        request: OperationDigest,
        after: Option<Checkpoint>,
        max_changes: u16,
        max_bytes: u32,
    ) -> Self {
        Self {
            protocol,
            scope,
            replica,
            request,
            after,
            max_changes,
            max_bytes,
        }
    }

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
        let max_bytes = usize::try_from(self.max_bytes).map_err(ProtocolError::Width)?;
        if self.max_bytes == 0 || max_bytes > MAX_PAGE_BYTES {
            return Err(ProtocolError::PageLimit);
        }
        Ok(())
    }
}

/// One bounded service-to-client page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
pub struct PullPage {
    /// Exact protocol version encoded by the service.
    pub(crate) protocol: u16,
    /// Changes preceding `checkpoint` in the scoped service log.
    #[serde(deserialize_with = "wire::deserialize_changes")]
    pub(crate) changes: Vec<ChangeEnvelope>,
    /// Replacement checkpoint persisted atomically with `changes`.
    pub(crate) checkpoint: Checkpoint,
    /// Whether another page is immediately available.
    pub(crate) more: bool,
}

impl PullPage {
    /// Builds one bounded service-to-client page from its changes and checkpoint.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose pages exclusively through here.
    #[must_use]
    pub fn new(
        protocol: u16,
        changes: Vec<ChangeEnvelope>,
        checkpoint: Checkpoint,
        more: bool,
    ) -> Self {
        Self {
            protocol,
            changes,
            checkpoint,
            more,
        }
    }

    /// Returns the protocol version encoded by the service.
    #[must_use]
    pub const fn protocol(&self) -> u16 {
        self.protocol
    }

    /// Borrows the changes preceding the checkpoint in the scoped service log.
    #[must_use]
    pub fn changes(&self) -> &[ChangeEnvelope] {
        &self.changes
    }

    /// Borrows the replacement checkpoint persisted atomically with the changes.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Returns whether another page is immediately available.
    #[must_use]
    pub const fn more(&self) -> bool {
        self.more
    }

    /// Takes the changes and the replacement checkpoint.
    #[must_use]
    pub fn into_parts(self) -> (Vec<ChangeEnvelope>, Checkpoint) {
        (self.changes, self.checkpoint)
    }

    /// Validates a page before any durable local mutation starts.
    ///
    /// # Errors
    ///
    /// Returns a typed [`ProtocolError`] for unsupported versions, invalid page
    /// bounds, malformed changes, or material crossing the requested scope.
    pub fn validate(&self, expected_scope: Scope) -> Result<(), ProtocolError> {
        validate_protocol(self.protocol)?;
        validate_page(&self.changes)?;
        if self.more && self.changes.is_empty() {
            return Err(ProtocolError::NonProgressPage);
        }
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
#[non_exhaustive]
pub enum ProtocolError {
    /// Sender and receiver do not implement the same protocol.
    #[error("{context}: unsupported protocol version {actual}")]
    UnsupportedVersion {
        /// Observed wire version.
        actual: u16,
        /// Protocol admission boundary.
        context: &'static str,
    },
    /// A checkpoint violated its opaque wire bounds.
    #[error("checkpoint is empty or exceeds the wire limit")]
    InvalidCheckpoint,
    /// A single change violated its encoded bound.
    #[error("change byte length {0} is outside protocol bounds")]
    InvalidChangeSize(usize),
    /// A page violated count or aggregate byte bounds.
    #[error("page exceeds protocol count or byte limits")]
    PageLimit,
    /// A sender claimed more data without advancing the page.
    #[error("page claims more data but contains no changes")]
    NonProgressPage,
    /// A push contained changes attributed to a different sender.
    #[error("change replica differs from push sender")]
    ReplicaMismatch,
    /// A pull response crossed its requested scope.
    #[error("change scope differs from pull scope")]
    ScopeMismatch,
    /// Push outcomes did not correspond one-for-one in request order.
    #[error("push outcomes do not match request operations")]
    OutcomeMismatch,
    /// A baseline descriptor violated its versioned fixed encoding.
    #[error("baseline descriptor is malformed or unsupported")]
    InvalidBaselineDescriptor,
    /// Baseline documents violated ordering, size, or payload bounds.
    #[error("baseline documents are malformed or out of bounds")]
    InvalidBaselineDocuments,
    /// Baseline documents did not reproduce the descriptor digest.
    #[error("baseline documents do not match their descriptor")]
    BaselineMismatch,
    /// A baseline descriptor could not be decoded by the model.
    #[error("baseline descriptor cannot be decoded")]
    UndecodableDescriptor(#[source] ModelError),
    /// The model refused the baseline documents while rebuilding the artifact.
    #[error("baseline documents were refused by the model")]
    RejectedDocuments(#[source] ModelError),
    /// A wire-sized integer does not fit this platform's address width.
    #[error("wire integer exceeds the platform address width")]
    Width(#[source] std::num::TryFromIntError),
}

/// Refuses any wire version other than [`PROTOCOL_VERSION`].
fn validate_protocol(version: u16) -> Result<(), ProtocolError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::UnsupportedVersion {
            actual: version,
            context: "validate_protocol",
        })
    }
}

/// Enforces per-change validity plus the page count and aggregate byte bounds.
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
