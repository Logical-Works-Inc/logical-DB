//! Recovery baseline wire messages and the bounded page accumulator.

use lgwks_std::json::serde::{
    Deserialize, Deserializer, Serialize,
    de::{Error as _, SeqAccess, Visitor},
};
use logical_db_model::{
    DocumentId, MAX_BASELINE_BYTES, MAX_BASELINE_DOCUMENTS, MAX_BASELINE_PAGE_DOCUMENTS,
    OperationDigest, ReplicaId, SNAPSHOT_BASELINE_BYTES, Scope, SnapshotArtifact, SnapshotBaseline,
    SnapshotDigestBuilder, SnapshotDocument, SnapshotRecord,
};

use crate::{Checkpoint, ProtocolError, validate_protocol, wire::deserialize_bounded_bytes};

/// Request for the state-bearing baseline that makes checkpoint recovery possible.
///
/// A client whose checkpoint fell behind the durable retention floor cannot
/// resume from the log prefix: the changes that produced its state were
/// collected. It asks for the sealed artifact covering the recovery point
/// instead of applying a partial prefix.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(crate = "lgwks_std::json::serde")]
#[non_exhaustive]
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
    /// Builds a bounded recovery request from its scope, replica, and cursors.
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
        after_document: Option<DocumentId>,
    ) -> Self {
        Self {
            protocol,
            scope,
            replica,
            request,
            after,
            after_document,
        }
    }

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
#[non_exhaustive]
pub struct BaselineDocument {
    /// Stable document identity.
    pub(crate) document: DocumentId,
    /// Whether the document carries a first-class tombstone.
    pub(crate) tombstone: bool,
    /// Canonical serialized document state.
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "deserialize_baseline_snapshot"
    )]
    pub(crate) snapshot: Vec<u8>,
}

impl BaselineDocument {
    /// Builds one document state from its identity, tombstone flag, and bytes.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose document states through here.
    #[must_use]
    pub fn new(document: DocumentId, tombstone: bool, snapshot: Vec<u8>) -> Self {
        Self {
            document,
            tombstone,
            snapshot,
        }
    }

    /// Returns the stable document identity.
    #[must_use]
    pub const fn document(&self) -> DocumentId {
        self.document
    }

    /// Returns whether the document carries a first-class tombstone.
    #[must_use]
    pub const fn is_tombstone(&self) -> bool {
        self.tombstone
    }

    /// Borrows the canonical serialized document state.
    #[must_use]
    pub fn snapshot(&self) -> &[u8] {
        &self.snapshot
    }

    /// Copies this wire document into the model's artifact document.
    fn state(&self) -> SnapshotDocument {
        SnapshotDocument::new(self.document, self.snapshot.clone(), self.tombstone)
    }
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
#[non_exhaustive]
pub struct BaselinePage {
    /// Exact protocol version encoded by the service.
    pub(crate) protocol: u16,
    /// Canonical fixed descriptor produced by [`SnapshotBaseline::encode`].
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "deserialize_baseline_descriptor"
    )]
    pub(crate) descriptor: Vec<u8>,
    /// Strictly ascending document states through the descriptor position.
    #[serde(deserialize_with = "deserialize_baseline_documents")]
    pub(crate) documents: Vec<BaselineDocument>,
    /// Replacement checkpoint covering `descriptor` exactly.
    pub(crate) checkpoint: Checkpoint,
    /// Whether more documents follow this page for the same descriptor.
    pub(crate) more: bool,
    /// Exclusive cursor for the next page; present exactly when `more` is true.
    pub(crate) next_document: Option<DocumentId>,
}

impl BaselinePage {
    /// Builds one bounded baseline page from its descriptor, documents, and cursor.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose pages exclusively through here.
    #[must_use]
    pub fn new(
        protocol: u16,
        descriptor: Vec<u8>,
        documents: Vec<BaselineDocument>,
        checkpoint: Checkpoint,
        more: bool,
        next_document: Option<DocumentId>,
    ) -> Self {
        Self {
            protocol,
            descriptor,
            documents,
            checkpoint,
            more,
            next_document,
        }
    }

    /// Returns the protocol version encoded by the service.
    #[must_use]
    pub const fn protocol(&self) -> u16 {
        self.protocol
    }

    /// Borrows the canonical fixed descriptor bytes.
    #[must_use]
    pub fn descriptor(&self) -> &[u8] {
        &self.descriptor
    }

    /// Borrows the strictly ascending document states.
    #[must_use]
    pub fn documents(&self) -> &[BaselineDocument] {
        &self.documents
    }

    /// Borrows the replacement checkpoint covering the descriptor.
    #[must_use]
    pub const fn checkpoint(&self) -> &Checkpoint {
        &self.checkpoint
    }

    /// Returns whether more documents follow this page.
    #[must_use]
    pub const fn more(&self) -> bool {
        self.more
    }

    /// Returns the exclusive cursor for the next page.
    #[must_use]
    pub const fn next_document(&self) -> Option<DocumentId> {
        self.next_document
    }

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
        if self.documents.len() > MAX_BASELINE_PAGE_DOCUMENTS {
            return Err(ProtocolError::InvalidBaselineDocuments);
        }
        let page_bytes = self
            .documents
            .iter()
            .try_fold(0_usize, |total, document| {
                total.checked_add(document.snapshot.len())
            })
            .ok_or(ProtocolError::InvalidBaselineDocuments)?;
        if page_bytes > MAX_BASELINE_BYTES {
            return Err(ProtocolError::InvalidBaselineDocuments);
        }
        let descriptor = match SnapshotBaseline::decode(&self.descriptor) {
            Ok(SnapshotRecord::State(baseline)) => baseline,
            Ok(SnapshotRecord::LegacyPosition(_)) => {
                return Err(ProtocolError::InvalidBaselineDescriptor);
            }
            // `SnapshotRecord` is `#[non_exhaustive]`, so a future record form
            // must be refused as a foreign descriptor rather than matched.
            Ok(_) => return Err(ProtocolError::InvalidBaselineDescriptor),
            Err(error) => return Err(ProtocolError::UndecodableDescriptor(error)),
        };
        let mut builder = SnapshotDigestBuilder::new(
            expected_scope,
            descriptor.position(),
            u64::from(descriptor.document_count()),
        );
        for document in &self.documents {
            builder
                .push(&document.state())
                .map_err(ProtocolError::RejectedDocuments)?;
        }
        let continues = match (self.more, self.next_document) {
            (true, Some(cursor)) => self.documents.last().map(|last| last.document) == Some(cursor),
            (false, None) => true,
            (true, None) | (false, Some(_)) => false,
        };
        if !continues {
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
            return Err(ProtocolError::BaselineMismatch);
        }
        let document_count =
            usize::try_from(descriptor.document_count()).map_err(ProtocolError::Width)?;
        if document_count != self.documents.len() {
            return Err(ProtocolError::BaselineMismatch);
        }
        let documents = self
            .documents
            .iter()
            .map(BaselineDocument::state)
            .collect::<Vec<_>>();
        let artifact = SnapshotArtifact::new(expected_scope, descriptor.position(), documents)
            .map_err(ProtocolError::RejectedDocuments)?;
        if *artifact.baseline() != descriptor {
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
    /// Scope every folded page must match.
    scope: Scope,
    /// Descriptor each folded page must reproduce.
    descriptor: SnapshotBaseline,
    /// Documents accumulated in strictly ascending order.
    documents: Vec<SnapshotDocument>,
    /// Aggregate snapshot bytes accumulated so far.
    byte_len: usize,
    /// Exclusive identity of the last accepted document.
    last: Option<DocumentId>,
    /// Whether a terminal page has completed the artifact.
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
            return Err(ProtocolError::BaselineMismatch);
        }
        if page.validate_page(self.scope)? != self.descriptor {
            return Err(ProtocolError::BaselineMismatch);
        }
        if self
            .documents
            .len()
            .checked_add(page.documents.len())
            .is_none_or(|total| total > MAX_BASELINE_DOCUMENTS)
        {
            return Err(ProtocolError::InvalidBaselineDocuments);
        }
        for document in &page.documents {
            if self.last.is_some_and(|last| last >= document.document) {
                return Err(ProtocolError::BaselineMismatch);
            }
            self.byte_len = self
                .byte_len
                .checked_add(document.snapshot.len())
                .filter(|total| *total <= MAX_BASELINE_BYTES)
                .ok_or(ProtocolError::InvalidBaselineDocuments)?;
            self.last = Some(document.document);
            self.documents.push(document.state());
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
            return Err(ProtocolError::BaselineMismatch);
        }
        let artifact =
            SnapshotArtifact::new(self.scope, self.descriptor.position(), self.documents)
                .map_err(ProtocolError::RejectedDocuments)?;
        if *artifact.baseline() != self.descriptor {
            return Err(ProtocolError::BaselineMismatch);
        }
        Ok(artifact)
    }
}

/// Decodes one baseline document snapshot, bounding and refusing emptiness.
fn deserialize_baseline_snapshot<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = deserialize_bounded_bytes(deserializer, MAX_BASELINE_BYTES, "baseline document")?;
    if bytes.is_empty() {
        return Err(D::Error::custom("baseline document must not be empty"));
    }
    Ok(bytes)
}

/// Decodes a fixed baseline descriptor, bounding and refusing emptiness.
fn deserialize_baseline_descriptor<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes =
        deserialize_bounded_bytes(deserializer, SNAPSHOT_BASELINE_BYTES, "baseline descriptor")?;
    if bytes.is_empty() {
        return Err(D::Error::custom("baseline descriptor must not be empty"));
    }
    Ok(bytes)
}

/// Decodes a page of baseline documents, capping count and aggregate bytes.
fn deserialize_baseline_documents<'de, D>(
    deserializer: D,
) -> Result<Vec<BaselineDocument>, D::Error>
where
    D: Deserializer<'de>,
{
    /// Visitor that caps the baseline document page at its count and byte bounds.
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
            let mut documents = Vec::new();
            if let Some(hint) = sequence.size_hint() {
                documents.reserve(hint.min(MAX_BASELINE_PAGE_DOCUMENTS));
            }
            let mut aggregate_bytes = 0usize;
            while let Some(document) = sequence.next_element::<BaselineDocument>()? {
                if documents.len() == MAX_BASELINE_PAGE_DOCUMENTS {
                    return Err(A::Error::custom("baseline document count exceeds limit"));
                }
                aggregate_bytes = aggregate_bytes
                    .checked_add(document.snapshot.len())
                    .ok_or_else(|| A::Error::custom("baseline byte count overflow"))?;
                if aggregate_bytes > MAX_BASELINE_BYTES {
                    return Err(A::Error::custom("baseline bytes exceed limit"));
                }
                documents.push(document);
            }
            Ok(documents)
        }
    }

    deserializer.deserialize_seq(BaselineDocumentsVisitor)
}
