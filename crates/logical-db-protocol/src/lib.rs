//! Versioned, transport-neutral synchronization messages.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

mod action;
pub use action::{
    PageAction, mark_consumed_action, prune_action, recover_action, seal_snapshot_action,
    write_action,
};

use lgwks_std::json::serde::{
    self, Deserialize, Deserializer, Serialize,
    de::{Error as _, SeqAccess, Visitor},
};
pub use logical_db_model::ChangeDigest;
use logical_db_model::{
    DocumentId, MAX_BASELINE_BYTES, MAX_BASELINE_DOCUMENTS, MAX_BASELINE_PAGE_DOCUMENTS,
    MAX_CHANGE_BYTES, MAX_CHANGES_PER_PAGE, MAX_PAGE_BYTES, OperationDigest, OperationId,
    ReplicaId, SNAPSHOT_BASELINE_BYTES, Scope, SnapshotArtifact, SnapshotBaseline,
    SnapshotDigestBuilder, SnapshotDocument, SnapshotRecord,
};
use thiserror::Error;

/// Current wire protocol version.
pub const PROTOCOL_VERSION: u16 = 1;
const MAX_CHECKPOINT_BYTES: usize = 4_096;

/// Opaque server-issued progress position for exactly one account and space.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
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
            eprintln!("logical-db-protocol: from_wire: InvalidCheckpoint");
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
            eprintln!("logical-db-protocol: validate: InvalidChangeSize");
            return Err(ProtocolError::InvalidChangeSize(self.bytes.len()));
        }
        Ok(())
    }
}

/// Bounded client-to-service change upload.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
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
            eprintln!("logical-db-protocol: validate: ReplicaMismatch");
            return Err(ProtocolError::ReplicaMismatch);
        }
        Ok(())
    }
}

/// Per-change upload result so one invalid change cannot hide other outcomes.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
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
#[serde(crate = "lgwks_std::json::serde")]
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
#[serde(crate = "lgwks_std::json::serde")]
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
            eprintln!("logical-db-protocol: validate: OutcomeMismatch");
            return Err(ProtocolError::OutcomeMismatch);
        }
        for (outcome, change) in self.outcomes.iter().zip(&request.changes) {
            let operation = match outcome {
                PushOutcome::Accepted { operation }
                | PushOutcome::Duplicate { operation }
                | PushOutcome::Rejected { operation, .. } => operation,
            };
            if *operation != change.operation {
                eprintln!("logical-db-protocol: validate: OutcomeMismatch");
                return Err(ProtocolError::OutcomeMismatch);
            }
        }
        Ok(())
    }
}

/// Request for changes after an opaque durable checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
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
            eprintln!("logical-db-protocol: validate: PageLimit");
            return Err(ProtocolError::PageLimit);
        }
        if self.max_bytes == 0 || self.max_bytes as usize > MAX_PAGE_BYTES {
            eprintln!("logical-db-protocol: validate: PageLimit");
            return Err(ProtocolError::PageLimit);
        }
        Ok(())
    }
}

/// One bounded service-to-client page.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
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
        if self.more && self.changes.is_empty() {
            eprintln!("logical-db-protocol: validate: NonProgressPage");
            return Err(ProtocolError::NonProgressPage);
        }
        if self
            .changes
            .iter()
            .any(|change| change.scope != expected_scope)
        {
            eprintln!("logical-db-protocol: validate: ScopeMismatch");
            return Err(ProtocolError::ScopeMismatch);
        }
        Ok(())
    }
}

/// Request for the state-bearing baseline that makes checkpoint recovery possible.
///
/// A client whose checkpoint fell behind the durable retention floor cannot
/// resume from the log prefix: the changes that produced its state were
/// collected. It asks for the sealed artifact covering the recovery point
/// instead of applying a partial prefix.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
pub struct BaselineRequest {
    /// Exact protocol version encoded by the client.
    pub protocol: u16,
    /// Scope whose recovery baseline is requested.
    pub scope: Scope,
    /// Recovering replica, used for audit and policy.
    pub replica: ReplicaId,
    /// Canonical identity of this bounded request for authority binding.
    pub request: OperationDigest,
    /// Checkpoint the client held when recovery was required, when it had one.
    pub after: Option<Checkpoint>,
    /// Exclusive document cursor: the page returns documents strictly after it.
    pub after_document: Option<DocumentId>,
}

impl BaselineRequest {
    /// Validates the protocol version before service work begins.
    ///
    /// # Errors
    ///
    /// Returns a typed [`ProtocolError`] for an unsupported version.
    pub fn validate(&self) -> Result<(), ProtocolError> {
        validate_protocol(self.protocol)
    }
}

/// One document state inside a recovery baseline.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
pub struct BaselineDocument {
    /// Stable document identity.
    pub document: DocumentId,
    /// Whether the document carries a first-class tombstone.
    pub tombstone: bool,
    /// Canonical serialized document state.
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "deserialize_baseline_snapshot"
    )]
    pub snapshot: Vec<u8>,
}

/// One bounded, versioned page of a state-bearing recovery baseline.
///
/// The service sends the canonical descriptor it sealed plus an ordered slice
/// of document states and a replacement checkpoint. A single-page baseline
/// carries the whole artifact; a paged baseline advertises `more` and the
/// `next_document` cursor a client must resume from. The client refuses the
/// page unless its documents are strictly ascending, reproduce their own
/// bounds, and — across the whole sequence — the descriptor digest exactly, so
/// a truncated, reordered, or substituted artifact never mutates local state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
pub struct BaselinePage {
    /// Exact protocol version encoded by the service.
    pub protocol: u16,
    /// Canonical fixed descriptor produced by [`SnapshotBaseline::encode`].
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "deserialize_baseline_descriptor"
    )]
    pub descriptor: Vec<u8>,
    /// Strictly ascending document states through the descriptor position.
    #[serde(deserialize_with = "deserialize_baseline_documents")]
    pub documents: Vec<BaselineDocument>,
    /// Replacement checkpoint covering `descriptor` exactly.
    pub checkpoint: Checkpoint,
    /// Whether more documents follow this page for the same descriptor.
    pub more: bool,
    /// Exclusive cursor for the next page; present exactly when `more` is true.
    pub next_document: Option<DocumentId>,
}

impl BaselinePage {
    /// Validates one page of a paged baseline and returns its descriptor.
    ///
    /// Enforces the protocol version, a decodable state-bearing descriptor,
    /// strictly ascending non-empty documents within the page bounds, and the
    /// continuation contract between `more` and `next_document`. It does not
    /// require the page to carry the whole artifact; the aggregate identity is
    /// the [`BaselineAccumulator`]'s terminal check.
    ///
    /// # Errors
    ///
    /// Returns a typed [`ProtocolError`] for unsupported versions, a malformed
    /// or legacy descriptor, documents that violate ordering or size bounds, or
    /// an inconsistent continuation contract.
    pub fn validate_page(&self, expected_scope: Scope) -> Result<SnapshotBaseline, ProtocolError> {
        validate_protocol(self.protocol)?;
        let descriptor = match SnapshotBaseline::decode(&self.descriptor) {
            Ok(SnapshotRecord::State(baseline)) => baseline,
            Ok(SnapshotRecord::LegacyPosition(_)) => {
                eprintln!("logical-db-protocol: BaselinePage::validate_page: LegacyBaseline");
                return Err(ProtocolError::InvalidBaselineDescriptor);
            }
            Err(error) => {
                eprintln!("logical-db-protocol: BaselinePage::validate_page descriptor: {error}");
                return Err(ProtocolError::InvalidBaselineDescriptor);
            }
        };
        let mut builder = SnapshotDigestBuilder::new(
            expected_scope,
            descriptor.position(),
            u64::from(descriptor.document_count()),
        );
        for document in &self.documents {
            let state = SnapshotDocument {
                document: document.document,
                snapshot: document.snapshot.clone(),
                tombstone: document.tombstone,
            };
            builder.push(&state).map_err(|error| {
                eprintln!("logical-db-protocol: BaselinePage::validate_page documents: {error}");
                ProtocolError::InvalidBaselineDocuments
            })?;
        }
        let continues = match (self.more, self.next_document) {
            (true, Some(cursor)) => self.documents.last().map(|last| last.document) == Some(cursor),
            (false, None) => true,
            (true, None) | (false, Some(_)) => false,
        };
        if !continues {
            eprintln!("logical-db-protocol: BaselinePage::validate_page: NonProgressPage");
            return Err(ProtocolError::NonProgressPage);
        }
        Ok(descriptor)
    }

    /// Recomputes and verifies a whole single-page artifact before any durable
    /// local mutation.
    ///
    /// # Errors
    ///
    /// Returns a typed [`ProtocolError`] for unsupported versions, a malformed
    /// or legacy descriptor, a document list that violates ordering or size
    /// bounds, a page that advertises a continuation, or documents that do not
    /// reproduce the descriptor digest.
    pub fn validate(&self, expected_scope: Scope) -> Result<SnapshotArtifact, ProtocolError> {
        let descriptor = self.validate_page(expected_scope)?;
        if self.more {
            eprintln!("logical-db-protocol: BaselinePage::validate: BaselineMismatch");
            return Err(ProtocolError::BaselineMismatch);
        }
        if descriptor.document_count() as usize != self.documents.len() {
            eprintln!("logical-db-protocol: BaselinePage::validate: BaselineMismatch");
            return Err(ProtocolError::BaselineMismatch);
        }
        let documents = self
            .documents
            .iter()
            .map(|document| SnapshotDocument {
                document: document.document,
                snapshot: document.snapshot.clone(),
                tombstone: document.tombstone,
            })
            .collect::<Vec<_>>();
        let artifact = SnapshotArtifact::new(expected_scope, descriptor.position(), documents)
            .map_err(|error| {
                eprintln!("logical-db-protocol: BaselinePage::validate documents: {error}");
                ProtocolError::InvalidBaselineDocuments
            })?;
        if artifact.baseline != descriptor {
            eprintln!("logical-db-protocol: BaselinePage::validate: BaselineMismatch");
            return Err(ProtocolError::BaselineMismatch);
        }
        Ok(artifact)
    }
}

/// Folds validated baseline pages into one artifact.
///
/// Every page must carry the same descriptor, continue strictly after the
/// previous page, and stay within the aggregate document and byte bounds, so a
/// hostile or corrupt stream fails closed at the page it violates and never
/// grows local state without bound. The terminal page must complete the
/// descriptor's document count and digest exactly.
#[derive(Debug)]
pub struct BaselineAccumulator {
    scope: Scope,
    descriptor: SnapshotBaseline,
    documents: Vec<SnapshotDocument>,
    byte_len: usize,
    last: Option<DocumentId>,
    complete: bool,
}

impl BaselineAccumulator {
    /// Starts an accumulator for one descriptor.
    #[must_use]
    pub fn new(scope: Scope, descriptor: SnapshotBaseline) -> Self {
        Self {
            scope,
            descriptor,
            documents: Vec::new(),
            byte_len: 0,
            last: None,
            complete: false,
        }
    }

    /// Returns the descriptor this accumulator completes.
    #[must_use]
    pub const fn descriptor(&self) -> &SnapshotBaseline {
        &self.descriptor
    }

    /// Folds one page and reports whether the baseline is complete.
    ///
    /// # Errors
    ///
    /// Returns a typed [`ProtocolError`] when the page is invalid, carries a
    /// different descriptor, does not continue after the previous page, exceeds
    /// the aggregate bounds, or arrives after completion.
    pub fn push(&mut self, page: &BaselinePage) -> Result<bool, ProtocolError> {
        if self.complete {
            eprintln!("logical-db-protocol: BaselineAccumulator::push: BaselineMismatch");
            return Err(ProtocolError::BaselineMismatch);
        }
        if page.validate_page(self.scope)? != self.descriptor {
            eprintln!("logical-db-protocol: BaselineAccumulator::push: BaselineMismatch");
            return Err(ProtocolError::BaselineMismatch);
        }
        if self
            .documents
            .len()
            .checked_add(page.documents.len())
            .is_none_or(|total| total > MAX_BASELINE_DOCUMENTS)
        {
            eprintln!("logical-db-protocol: BaselineAccumulator::push: InvalidBaselineDocuments");
            return Err(ProtocolError::InvalidBaselineDocuments);
        }
        for document in &page.documents {
            if self.last.is_some_and(|last| last >= document.document) {
                eprintln!("logical-db-protocol: BaselineAccumulator::push: BaselineMismatch");
                return Err(ProtocolError::BaselineMismatch);
            }
            self.byte_len = self
                .byte_len
                .checked_add(document.snapshot.len())
                .filter(|total| *total <= MAX_BASELINE_BYTES)
                .ok_or_else(|| {
                    eprintln!(
                        "logical-db-protocol: BaselineAccumulator::push: InvalidBaselineDocuments"
                    );
                    ProtocolError::InvalidBaselineDocuments
                })?;
            self.last = Some(document.document);
            self.documents.push(SnapshotDocument {
                document: document.document,
                snapshot: document.snapshot.clone(),
                tombstone: document.tombstone,
            });
        }
        self.complete = !page.more;
        Ok(self.complete)
    }

    /// Consumes the accumulator and returns the completed artifact.
    ///
    /// # Errors
    ///
    /// Returns a typed [`ProtocolError`] when the sequence is incomplete or does
    /// not reproduce the descriptor exactly.
    pub fn finish(self) -> Result<SnapshotArtifact, ProtocolError> {
        if !self.complete {
            eprintln!("logical-db-protocol: BaselineAccumulator::finish: BaselineMismatch");
            return Err(ProtocolError::BaselineMismatch);
        }
        let artifact = SnapshotArtifact::new(
            self.scope,
            self.descriptor.position(),
            self.documents,
        )
        .map_err(|error| {
            eprintln!("logical-db-protocol: BaselineAccumulator::finish documents: {error}");
            ProtocolError::InvalidBaselineDocuments
        })?;
        if artifact.baseline != self.descriptor {
            eprintln!("logical-db-protocol: BaselineAccumulator::finish: BaselineMismatch");
            return Err(ProtocolError::BaselineMismatch);
        }
        Ok(artifact)
    }
}

/// Stable transport-neutral protocol failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
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
}

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

fn validate_page(changes: &[ChangeEnvelope]) -> Result<(), ProtocolError> {
    if changes.len() > MAX_CHANGES_PER_PAGE {
        eprintln!("logical-db-protocol: validate_page: PageLimit");
        return Err(ProtocolError::PageLimit);
    }
    let mut bytes = 0usize;
    for change in changes {
        change.validate()?;
        bytes = bytes.saturating_add(change.bytes.len());
        if bytes > MAX_PAGE_BYTES {
            eprintln!("logical-db-protocol: validate_page: PageLimit");
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
        eprintln!("logical-db-protocol: deserialize_checkpoint_bytes: custom");
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
        eprintln!("logical-db-protocol: deserialize_change_bytes: custom");
        return Err(D::Error::custom("change bytes must not be empty"));
    }
    Ok(bytes)
}

fn deserialize_baseline_snapshot<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = deserialize_bounded_bytes(deserializer, MAX_BASELINE_BYTES, "baseline document")?;
    if bytes.is_empty() {
        eprintln!("logical-db-protocol: deserialize_baseline_snapshot: custom");
        return Err(D::Error::custom("baseline document must not be empty"));
    }
    Ok(bytes)
}

fn deserialize_baseline_descriptor<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes =
        deserialize_bounded_bytes(deserializer, SNAPSHOT_BASELINE_BYTES, "baseline descriptor")?;
    if bytes.is_empty() {
        eprintln!("logical-db-protocol: deserialize_baseline_descriptor: custom");
        return Err(D::Error::custom("baseline descriptor must not be empty"));
    }
    Ok(bytes)
}

fn deserialize_baseline_documents<'de, D>(
    deserializer: D,
) -> Result<Vec<BaselineDocument>, D::Error>
where
    D: Deserializer<'de>,
{
    struct BaselineDocumentsVisitor;

    impl<'de> Visitor<'de> for BaselineDocumentsVisitor {
        type Value = Vec<BaselineDocument>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded page of baseline documents")
        }

        fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let capacity = sequence
                .size_hint()
                .unwrap_or(0)
                .min(MAX_BASELINE_PAGE_DOCUMENTS);
            let mut documents = Vec::with_capacity(capacity);
            let mut aggregate_bytes = 0usize;
            while let Some(document) = sequence.next_element::<BaselineDocument>()? {
                if documents.len() == MAX_BASELINE_PAGE_DOCUMENTS {
                    eprintln!("logical-db-protocol: visit_seq: custom");
                    return Err(A::Error::custom("baseline document count exceeds limit"));
                }
                aggregate_bytes = aggregate_bytes
                    .checked_add(document.snapshot.len())
                    .ok_or_else(|| A::Error::custom("baseline byte count overflow"))?;
                if aggregate_bytes > MAX_BASELINE_BYTES {
                    eprintln!("logical-db-protocol: visit_seq: custom");
                    return Err(A::Error::custom("baseline bytes exceed limit"));
                }
                documents.push(document);
            }
            Ok(documents)
        }
    }

    deserializer.deserialize_seq(BaselineDocumentsVisitor)
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
                    eprintln!("logical-db-protocol: visit_seq: custom");
                    return Err(A::Error::custom("change page count exceeds limit"));
                }
                aggregate_bytes = aggregate_bytes
                    .checked_add(change.bytes.len())
                    .ok_or_else(|| A::Error::custom("change page byte count overflow"))?;
                if aggregate_bytes > MAX_PAGE_BYTES {
                    eprintln!("logical-db-protocol: visit_seq: custom");
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
            if value.len() > self.maximum {
                eprintln!(
                    "logical-db-protocol: borrowed bytes exceed {}",
                    self.maximum
                );
                return Err(E::invalid_length(value.len(), &self));
            }
            Ok(value.to_vec())
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
                eprintln!("logical-db-protocol: visit_byte_buf: invalid_length");
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
                    eprintln!("logical-db-protocol: visit_seq: invalid_length");
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

    let visitor = BytesVisitor { maximum, expecting };
    deserializer.deserialize_byte_buf(visitor)
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
                    eprintln!("logical-db-protocol: visit_seq: invalid_length");
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
    use logical_db_model::{AccountId, SNAPSHOT_BASELINE_LEGACY_VERSION, SpaceId};

    use super::*;

    #[test]
    fn mixed_push_outcomes_preserve_operation_correlation() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let request = PushRequest {
            protocol: PROTOCOL_VERSION,
            replica,
            changes: vec![envelope(replica, scope), envelope(replica, scope)],
        };
        let duplicate = PushOutcome::Duplicate {
            operation: request.changes[0].operation,
        };
        let rejected = PushOutcome::Rejected {
            operation: request.changes[1].operation,
            code: RejectionCode::Forbidden,
        };
        let mut response = PushResponse {
            protocol: PROTOCOL_VERSION,
            outcomes: vec![duplicate, rejected],
        };
        assert_eq!(response.validate(&request), Ok(()));
        response.outcomes.swap(0, 1);
        assert_eq!(
            response.validate(&request),
            Err(ProtocolError::OutcomeMismatch)
        );
    }

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
    fn pull_rejects_a_non_progress_page() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let page = PullPage {
            protocol: PROTOCOL_VERSION,
            changes: Vec::new(),
            checkpoint: Checkpoint::from_wire(vec![1]).unwrap(),
            more: true,
        };
        assert_eq!(page.validate(scope), Err(ProtocolError::NonProgressPage));
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
        use lgwks_std::json::serde::de::value::{Error as ValueError, SeqDeserializer};

        let oversized = std::iter::repeat_n(0u8, MAX_CHANGE_BYTES + 1);
        let deserializer = SeqDeserializer::<_, ValueError>::new(oversized);
        assert!(deserialize_change_bytes(deserializer).is_err());
    }

    #[test]
    fn checkpoint_deserialization_cannot_bypass_constructor_bounds() {
        use lgwks_std::json::serde::de::value::{Error as ValueError, SeqDeserializer};

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
            Err(ProtocolError::UnsupportedVersion { .. })
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
        use lgwks_std::json::serde::de::value::{
            BorrowedBytesDeserializer, Error as ValueError, SeqDeserializer,
        };

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
            Err(ProtocolError::UnsupportedVersion { .. })
        ));
        let page = PullPage {
            protocol: PROTOCOL_VERSION + 1,
            changes: vec![change],
            checkpoint: Checkpoint::from_wire(vec![1]).unwrap(),
            more: false,
        };
        assert!(matches!(
            page.validate(scope),
            Err(ProtocolError::UnsupportedVersion { .. })
        ));
    }

    fn ordered_documents(snapshots: &[(Vec<u8>, bool)]) -> Vec<SnapshotDocument> {
        let mut documents = snapshots
            .iter()
            .map(|(snapshot, tombstone)| SnapshotDocument {
                document: DocumentId::new(),
                snapshot: snapshot.clone(),
                tombstone: *tombstone,
            })
            .collect::<Vec<_>>();
        documents.sort_by_key(|document| *document.document.as_uuid().as_bytes());
        documents
    }

    fn baseline_page(
        scope: Scope,
        position: u64,
        documents: Vec<SnapshotDocument>,
    ) -> BaselinePage {
        let artifact = SnapshotArtifact::new(scope, position, documents).unwrap();
        BaselinePage {
            protocol: PROTOCOL_VERSION,
            descriptor: artifact.baseline.encode().to_vec(),
            documents: artifact
                .documents
                .iter()
                .map(|document| BaselineDocument {
                    document: document.document,
                    tombstone: document.tombstone,
                    snapshot: document.snapshot.clone(),
                })
                .collect(),
            checkpoint: Checkpoint::from_wire(vec![7; 8]).unwrap(),
            more: false,
            next_document: None,
        }
    }

    #[test]
    fn baseline_page_round_trips_and_reproduces_its_descriptor() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let artifact = SnapshotArtifact::new(
            scope,
            7,
            ordered_documents(&[(vec![1, 2, 3], false), (vec![9], true)]),
        )
        .unwrap();
        let page = baseline_page(scope, 7, artifact.documents.clone());
        let encoded = lgwks_std::json::to_vec(&page).unwrap();
        let decoded: BaselinePage = lgwks_std::json::from_slice(&encoded).unwrap();
        assert_eq!(decoded.validate(scope), Ok(artifact.clone()));
        let mut wire = Vec::new();
        ciborium::into_writer(&page, &mut wire).unwrap();
        let decoded: BaselinePage = ciborium::from_reader(wire.as_slice()).unwrap();
        assert_eq!(decoded.validate(scope), Ok(artifact));
    }

    #[test]
    fn baseline_page_refuses_substituted_truncated_reordered_and_legacy_artifacts() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let page = baseline_page(
            scope,
            3,
            ordered_documents(&[(vec![1, 2, 3], false), (vec![4], false)]),
        );

        let mut substituted = page.clone();
        substituted.documents[0].snapshot = vec![9, 9, 9];
        assert_eq!(
            substituted.validate(scope),
            Err(ProtocolError::BaselineMismatch)
        );

        let mut truncated = page.clone();
        truncated.documents.pop();
        assert_eq!(
            truncated.validate(scope),
            Err(ProtocolError::BaselineMismatch)
        );

        let mut reordered = page.clone();
        reordered.documents.reverse();
        assert_eq!(
            reordered.validate(scope),
            Err(ProtocolError::InvalidBaselineDocuments)
        );

        let mut legacy = page;
        legacy.descriptor = [
            SNAPSHOT_BASELINE_LEGACY_VERSION.to_be_bytes().as_slice(),
            7_u64.to_be_bytes().as_slice(),
        ]
        .concat();
        assert_eq!(
            legacy.validate(scope),
            Err(ProtocolError::InvalidBaselineDescriptor)
        );
    }

    #[test]
    fn baseline_document_count_is_bounded_during_decode() {
        let mut documents = Vec::new();
        for _ in 0..=MAX_BASELINE_PAGE_DOCUMENTS {
            documents.push(BaselineDocument {
                document: DocumentId::new(),
                tombstone: false,
                snapshot: vec![1],
            });
        }
        let page = BaselinePage {
            protocol: PROTOCOL_VERSION,
            descriptor: SnapshotBaseline::new(
                0,
                logical_db_model::BaselineDigest::new([0; 32]),
                0,
                0,
            )
            .encode()
            .to_vec(),
            documents,
            checkpoint: Checkpoint::from_wire(vec![1]).unwrap(),
            more: false,
            next_document: None,
        };
        let encoded = lgwks_std::json::to_vec(&page).unwrap();
        assert!(lgwks_std::json::from_slice::<BaselinePage>(&encoded).is_err());
    }

    fn ordered_states(count: usize) -> Vec<SnapshotDocument> {
        let mut documents = (0..count)
            .map(|index| SnapshotDocument {
                document: DocumentId::new(),
                snapshot: vec![u8::try_from(index % 251).unwrap() + 1],
                tombstone: false,
            })
            .collect::<Vec<_>>();
        documents.sort_by_key(|document| *document.document.as_uuid().as_bytes());
        documents
    }

    fn paged_page(
        artifact: &SnapshotArtifact,
        slice: &[SnapshotDocument],
        more: bool,
    ) -> BaselinePage {
        BaselinePage {
            protocol: PROTOCOL_VERSION,
            descriptor: artifact.baseline.encode().to_vec(),
            documents: slice
                .iter()
                .map(|document| BaselineDocument {
                    document: document.document,
                    tombstone: document.tombstone,
                    snapshot: document.snapshot.clone(),
                })
                .collect(),
            checkpoint: Checkpoint::from_wire(vec![9; 8]).unwrap(),
            more,
            next_document: if more {
                slice.last().map(|document| document.document)
            } else {
                None
            },
        }
    }

    #[test]
    fn accumulator_folds_a_multi_page_baseline_and_refuses_gaps_and_shortfalls() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let documents = ordered_states(MAX_BASELINE_PAGE_DOCUMENTS + 44);
        let artifact = SnapshotArtifact::new(scope, 11, documents.clone()).unwrap();
        let (first, rest) = documents.split_at(MAX_BASELINE_PAGE_DOCUMENTS);
        let first_page = paged_page(&artifact, first, true);
        let second_page = paged_page(&artifact, rest, false);
        let mut accumulator = BaselineAccumulator::new(scope, artifact.baseline.clone());
        assert!(!accumulator.push(&first_page).unwrap());
        assert!(accumulator.push(&second_page).unwrap());
        assert_eq!(accumulator.finish().unwrap(), artifact);

        let mut gap = BaselineAccumulator::new(scope, artifact.baseline.clone());
        assert!(!gap.push(&first_page).unwrap());
        assert_eq!(gap.push(&first_page), Err(ProtocolError::BaselineMismatch));

        let mut short = BaselineAccumulator::new(scope, artifact.baseline.clone());
        assert!(!short.push(&first_page).unwrap());
        assert_eq!(short.finish(), Err(ProtocolError::BaselineMismatch));
    }
}
