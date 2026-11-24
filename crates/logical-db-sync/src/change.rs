//! The two change shapes that cross the replica port: one local change waiting
//! for upload, and one verified remote change waiting for installation.

use logical_db_model::{ChangeDigest, DocumentId, OperationDigest, OperationId};

/// One durable local change with its private acknowledgement position.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct OutboundChange {
    /// Local sequence acknowledged only after the whole push response validates.
    pub(crate) sequence: u64,
    /// Changed document.
    pub(crate) document: DocumentId,
    /// Retry identity.
    pub(crate) operation: OperationId,
    /// Canonical caller request identity.
    pub(crate) request: OperationDigest,
    /// Native CRDT change identity.
    pub(crate) digest: ChangeDigest,
    /// Verified canonical CRDT bytes.
    pub(crate) bytes: Vec<u8>,
}

impl OutboundChange {
    /// Builds one durable local change from its acknowledgement position and payload.
    #[must_use]
    pub fn new(
        sequence: u64,
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
        digest: ChangeDigest,
        bytes: Vec<u8>,
    ) -> Self {
        Self {
            sequence,
            document,
            operation,
            request,
            digest,
            bytes,
        }
    }
}

/// One verified remote change handed to a replica for atomic installation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct InboundChange {
    /// Changed document.
    pub(crate) document: DocumentId,
    /// Native CRDT change identity.
    pub(crate) digest: ChangeDigest,
    /// Canonical CRDT bytes.
    pub(crate) bytes: Vec<u8>,
}

impl InboundChange {
    /// The changed document.
    #[must_use]
    pub const fn document(&self) -> DocumentId {
        self.document
    }

    /// The native CRDT change identity.
    #[must_use]
    pub const fn digest(&self) -> ChangeDigest {
        self.digest
    }

    /// The canonical CRDT bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
