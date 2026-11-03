//! Outcome and receipt types for embedded edits, deletes, and restores.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use logical_db_crdt::CrdtError;
use logical_db_model::{
    ChangeDigest, DocumentId, DocumentRevision, IndexEntry, OperationDigest, OperationId,
};
use logical_db_protocol::ChangeEnvelope;

use super::projection::BatchEditFunction;

/// MaterializedRemoteDocument
pub(crate) type MaterializedRemoteDocument = (
    DocumentId,
    Option<DocumentRevision>,
    Vec<u8>,
    Vec<IndexEntry>,
    bool,
);

/// PreparedBatchEdit
pub(crate) struct PreparedBatchEdit {
    /// Position of this edit inside the submitted batch.
    pub(crate) index: usize,
    /// Target document of the edit.
    pub(crate) document: DocumentId,
    /// Durable operation identity assigned to the edit.
    pub(crate) operation: OperationId,
    /// Caller request digest used for idempotent retry.
    pub(crate) request: OperationDigest,
    /// Native CRDT change digest of the prepared edit.
    pub(crate) digest: ChangeDigest,
    /// Verified canonical CRDT change bytes.
    pub(crate) bytes: Vec<u8>,
    /// Revision the edit expects to overwrite, when conditioned.
    pub(crate) expected_revision: Option<DocumentRevision>,
    /// Post-edit snapshot bytes retained for projection rebuild.
    pub(crate) snapshot: Vec<u8>,
    /// Index rows produced by the edit's projections.
    pub(crate) indexes: Vec<IndexEntry>,
}

/// One caller-owned document edit prepared for an atomic local batch.
#[non_exhaustive]
pub struct BatchEdit {
    /// Document receiving the edit.
    pub document: DocumentId,
    /// Retryable logical operation identity.
    pub operation: OperationId,
    /// Canonical caller request identity.
    pub request: OperationDigest,
    /// Structured edit applied to the document snapshot before commit.
    pub(crate) edit: Box<BatchEditFunction>,
}

impl BatchEdit {
    /// Creates one atomic local batch member.
    #[must_use]
    pub fn new(
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
        edit: impl for<'document> FnOnce(
            &mut logical_db_crdt::Editor<'document>,
        ) -> Result<(), CrdtError>
        + Send
        + 'static,
    ) -> Self {
        Self {
            document,
            operation,
            request,
            edit: Box::new(edit),
        }
    }
}

/// One untrusted immutable remote change entering the embedded replica.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RemoteChange {
    /// Document receiving the change.
    pub document: DocumentId,
    /// Claimed native CRDT identity, verified before commit.
    pub digest: ChangeDigest,
    /// Canonical CRDT change bytes, bounded before decoding.
    pub(crate) bytes: Vec<u8>,
}

impl RemoteChange {
    /// Builds one untrusted remote change from its claimed identity and bytes.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose remote changes through here.
    #[must_use]
    pub fn new(document: DocumentId, digest: ChangeDigest, bytes: Vec<u8>) -> Self {
        Self {
            document,
            digest,
            bytes,
        }
    }
}

/// One verified local outbox change without transport protocol coupling.
///
/// The change identity is the shared wire [`ChangeEnvelope`]: one definition
/// for the local and wire forms instead of two repeated field lists. The
/// outbox position rides alongside because it is local cursor state, not
/// wire identity. Readers name identity fields directly through [`Deref`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct PendingChange {
    /// Monotonic outbox position of this change within its scope.
    pub sequence: u64,
    /// The verified change identity shared with the wire envelope.
    pub(crate) envelope: ChangeEnvelope,
}

impl PendingChange {
    /// Borrows the verified canonical Automerge change bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.envelope.bytes
    }
}

impl std::ops::Deref for PendingChange {
    type Target = ChangeEnvelope;

    /// Dereferences to the shared wire identity so readers name fields once.
    fn deref(&self) -> &Self::Target {
        &self.envelope
    }
}

/// Idempotent result of one embedded edit request.
#[derive(Debug)]
#[non_exhaustive]
pub enum EditOutcome<T> {
    /// This call durably committed the request and carries its closure result.
    Applied(EditReceipt<T>),
    /// The exact request had already committed, so the closure was not run.
    Duplicate(DuplicateReceipt),
}

/// Result of one durable embedded edit.
#[derive(Debug)]
#[non_exhaustive]
pub struct EditReceipt<T> {
    /// Caller result computed inside the structured edit.
    pub value: T,
    /// Monotonic local outbox sequence.
    pub sequence: u64,
    /// Immutable CRDT change identity.
    pub digest: ChangeDigest,
}

/// Durable receipt returned for an exact retry without rerunning app code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DuplicateReceipt {
    /// Original monotonic local outbox sequence.
    pub sequence: u64,
    /// Original immutable CRDT change identity.
    pub digest: ChangeDigest,
}

/// Idempotent result of one embedded delete request.
#[derive(Debug)]
#[non_exhaustive]
pub enum DeleteOutcome {
    /// This call durably committed the tombstone.
    Applied(DeleteReceipt),
    /// The exact request had already committed, so no new change was staged.
    Duplicate(DuplicateReceipt),
}

/// Result of one durable embedded delete.
#[derive(Debug)]
#[non_exhaustive]
pub struct DeleteReceipt {
    /// Monotonic local outbox sequence.
    pub sequence: u64,
    /// Immutable CRDT tombstone change identity.
    pub digest: ChangeDigest,
}

/// Work durably completed by one schema migration or exact resumption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MigrationReport {
    /// Number of atomic index-replacement transactions in this invocation.
    pub batches: usize,
    /// Number of documents reprojected in this invocation.
    pub documents: usize,
}

/// Work durably completed by one versioned restore or exact resumption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RestoreReport {
    /// Number of documents carried by the export payload.
    pub documents: usize,
    /// Number of ordered history entries carried by the payload.
    pub entries: usize,
    /// Number of entries newly committed by this invocation.
    pub applied: usize,
    /// Whether the invocation resumed an interrupted restore cursor.
    pub resumed: bool,
}
