//! Store model types; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::error::StoreError;
use crate::keys::is_nonempty_bounded;
use logical_db_model::{
    ChangeDigest, DocumentId, DocumentRevision, IndexEntry, MAX_CHANGE_BYTES, OperationDigest,
    OperationId, Scope,
};

/// A validated local CRDT change without any transport-layer representation.
#[derive(Clone, Copy)]
pub struct LocalChange<'a> {
    /// Owning account and application space.
    pub(crate) scope: Scope,
    /// Document the change applies to.
    pub(crate) document: DocumentId,
    /// Retryable operation identity carried by the change.
    pub(crate) operation: OperationId,
    /// Canonical caller request identity bound to the operation.
    pub(crate) request: OperationDigest,
    /// Immutable CRDT change identity.
    pub(crate) digest: ChangeDigest,
    /// Canonical CRDT change bytes.
    pub bytes: &'a [u8],
}

impl<'a> LocalChange<'a> {
    /// Creates one bounded local change after CRDT validation.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidChangeSize`] when bytes are empty or exceed
    /// the immutable change ceiling.
    pub fn new(
        scope: Scope,
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
        digest: ChangeDigest,
        bytes: &'a [u8],
    ) -> Result<Self, StoreError> {
        if !is_nonempty_bounded(bytes.len(), MAX_CHANGE_BYTES) {
            let error = StoreError::InvalidChangeSize(bytes.len());
            return Err(error);
        }
        Ok(Self {
            scope,
            document,
            operation,
            request,
            digest,
            bytes,
        })
    }
}

/// Atomic local transition from one document state to the next.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct LocalCommit<'a> {
    /// Immutable change and its logical routing metadata.
    pub change: LocalChange<'a>,
    /// Revision observed with the input snapshot, or none for a new document.
    pub expected_revision: Option<DocumentRevision>,
    /// Complete verified Automerge snapshot after applying the change.
    pub snapshot: &'a [u8],
    /// Complete materialized index set for the successor snapshot.
    pub indexes: &'a [IndexEntry],
    /// Whether the successor snapshot carries a first-class tombstone.
    ///
    /// Tombstoned documents keep their snapshot, change, operation, outbox,
    /// and event rows; only secondary-index rows are cleared and reads
    /// report absent. The flag is monotonic: once set it is never removed.
    pub tombstone: bool,
}

impl<'a> LocalCommit<'a> {
    /// Builds one atomic local commit from a bounded change and its successor state.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose local commits through here.
    #[must_use]
    pub fn new(
        change: LocalChange<'a>,
        expected_revision: Option<DocumentRevision>,
        snapshot: &'a [u8],
        indexes: &'a [IndexEntry],
        tombstone: bool,
    ) -> Self {
        Self {
            change,
            expected_revision,
            snapshot,
            indexes,
            tombstone,
        }
    }
}

/// Latest complete snapshot and its optimistic concurrency marker.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StoredSnapshot {
    /// Complete verified Automerge snapshot bytes.
    pub(crate) bytes: Vec<u8>,
    /// Last locally committed change identity.
    pub revision: DocumentRevision,
}

impl StoredSnapshot {
    /// Borrows the complete verified snapshot bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// One immutable remote change to install with a materialized snapshot.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct RemoteChange<'a> {
    /// Document receiving the change.
    pub document: DocumentId,
    /// Content identity of canonical CRDT bytes.
    pub digest: ChangeDigest,
    /// Canonical CRDT change bytes.
    pub bytes: &'a [u8],
}

impl<'a> RemoteChange<'a> {
    /// Builds one immutable remote change for an atomic pull page.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose remote changes through here.
    #[must_use]
    pub fn new(document: DocumentId, digest: ChangeDigest, bytes: &'a [u8]) -> Self {
        Self {
            document,
            digest,
            bytes,
        }
    }
}

/// One remotely merged document transition inside an atomic pull page.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct RemoteDocument<'a> {
    /// Changed document.
    pub document: DocumentId,
    /// Revision used to derive `snapshot`.
    pub expected_revision: Option<DocumentRevision>,
    /// Complete snapshot after all page changes for this document.
    pub snapshot: &'a [u8],
    /// Complete materialized index set for the merged snapshot.
    pub indexes: &'a [IndexEntry],
    /// Whether the merged snapshot carries a first-class tombstone.
    ///
    /// The flag is monotonic: installation sets it but never clears it, so
    /// a late tombstone page converges to absent without resurrection.
    pub tombstone: bool,
}

impl<'a> RemoteDocument<'a> {
    /// Builds one remote document transition for an atomic pull page.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose remote documents through here.
    #[must_use]
    pub fn new(
        document: DocumentId,
        expected_revision: Option<DocumentRevision>,
        snapshot: &'a [u8],
        indexes: &'a [IndexEntry],
        tombstone: bool,
    ) -> Self {
        Self {
            document,
            expected_revision,
            snapshot,
            indexes,
            tombstone,
        }
    }
}

/// One stable secondary-index result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IndexHit {
    /// Matching document ordered by its portable identifier.
    pub document: DocumentId,
}

/// One bounded page of text-valued index rows with an opaque resume cursor.
///
/// Rows are `(document, text)` pairs in key order.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct TextIndexPage {
    /// Rows in key order.
    pub(crate) rows: Vec<(DocumentId, String)>,
    /// Opaque resume key; `None` when the scan is exhausted.
    pub next: Option<Vec<u8>>,
}

impl TextIndexPage {
    /// Borrows the rows in key order.
    #[must_use]
    pub fn rows(&self) -> &[(DocumentId, String)] {
        &self.rows
    }
}

/// One durable materialized-state invalidation for subscriptions and catch-up.
///
/// The two variants share one monotonic sequence, so a single cursor orders
/// document changes against whole-scope baseline installs. A baseline install
/// that carries no change of its own still advances the stream, so a
/// subscription cursor taken before it can never silently observe a replaced
/// scope as though nothing happened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ChangeEvent {
    /// A document's materialized representation changed.
    Document {
        /// Monotonic cursor within the logical scope.
        sequence: u64,
        /// Document whose materialized representation changed.
        document: DocumentId,
        /// Immutable change that caused the invalidation.
        digest: ChangeDigest,
    },
    /// A baseline advanced the scope's materialized generation.
    ///
    /// A cursor at or before this sequence must rebuild from the floor rather
    /// than replay history, because the install may have replaced state whose
    /// originating changes are absent or already pruned.
    Baseline {
        /// Monotonic cursor within the logical scope.
        sequence: u64,
        /// Monotonic generation installed at this sequence.
        generation: u64,
    },
}

impl ChangeEvent {
    /// Returns the monotonic cursor of this invalidation.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        match *self {
            Self::Document { sequence, .. } | Self::Baseline { sequence, .. } => sequence,
        }
    }

    /// Returns the invalidated document, or `None` for a baseline invalidation.
    #[must_use]
    pub fn document(&self) -> Option<DocumentId> {
        match *self {
            Self::Document { document, .. } => Some(document),
            Self::Baseline { .. } => None,
        }
    }

    /// Returns the immutable change digest, or `None` for a baseline invalidation.
    #[must_use]
    pub fn digest(&self) -> Option<ChangeDigest> {
        match *self {
            Self::Document { digest, .. } => Some(digest),
            Self::Baseline { .. } => None,
        }
    }
}

/// One immutable document input for a resumable materialized-index migration.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MigrationDocument {
    /// Document ordered by its portable identifier.
    pub document: DocumentId,
    /// Revision that must still be current when replacement indexes commit.
    pub revision: DocumentRevision,
    /// Complete verified snapshot used to compute replacement projections.
    pub(crate) snapshot: Vec<u8>,
}

impl MigrationDocument {
    /// Borrows the complete verified snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &[u8] {
        &self.snapshot
    }
}

/// One bounded migration page and the cursor it was read from.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MigrationPage {
    /// Durable cursor that must still identify the migration when applied.
    pub after: Option<DocumentId>,
    /// Ordered documents following `after`.
    pub(crate) documents: Vec<MigrationDocument>,
}

impl MigrationPage {
    /// Borrows the ordered documents following `after`.
    #[must_use]
    pub fn documents(&self) -> &[MigrationDocument] {
        &self.documents
    }
}

/// Replacement materialized rows for one migration document.
#[derive(Clone, Copy)]
#[non_exhaustive]
pub struct MigrationIndexes<'a> {
    /// Document whose complete index set is replaced.
    pub document: DocumentId,
    /// Revision observed when its snapshot was projected.
    pub expected_revision: DocumentRevision,
    /// Complete target-schema index set.
    pub indexes: &'a [IndexEntry],
}

impl<'a> MigrationIndexes<'a> {
    /// Builds one replacement materialized-index set for a migration document.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose migration index sets through here.
    #[must_use]
    pub fn new(
        document: DocumentId,
        expected_revision: DocumentRevision,
        indexes: &'a [IndexEntry],
    ) -> Self {
        Self {
            document,
            expected_revision,
            indexes,
        }
    }
}

/// One ordered commit replayed by a versioned logical export.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ExportEntry {
    /// Document changed by the replayed commit.
    pub document: DocumentId,
    /// Original retryable logical operation identity, reused for exact retry.
    pub operation: OperationId,
    /// Canonical caller request identity bound to the operation.
    pub request: OperationDigest,
    /// Immutable CRDT change identity.
    pub digest: ChangeDigest,
    /// Canonical CRDT change bytes.
    pub(crate) bytes: Vec<u8>,
}

impl ExportEntry {
    /// Borrows the canonical CRDT change bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl ExportEntry {
    /// Builds one ordered commit entry for a versioned logical export.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose export entries through here.
    #[must_use]
    pub fn new(
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
        digest: ChangeDigest,
        bytes: Vec<u8>,
    ) -> Self {
        Self {
            document,
            operation,
            request,
            digest,
            bytes,
        }
    }
}

/// One document snapshot carried by a versioned logical export.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ExportDocument {
    /// Document ordered by its portable identifier.
    pub document: DocumentId,
    /// Whether the exported snapshot carries a first-class tombstone.
    pub tombstone: bool,
    /// Complete verified snapshot after the last exported change.
    pub(crate) snapshot: Vec<u8>,
}

impl ExportDocument {
    /// Builds one document snapshot entry for a versioned logical export.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose export documents through here.
    #[must_use]
    pub fn new(document: DocumentId, tombstone: bool, snapshot: Vec<u8>) -> Self {
        Self {
            document,
            tombstone,
            snapshot,
        }
    }

    /// Borrows the complete verified snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &[u8] {
        &self.snapshot
    }
}

/// One bounded document page of a resumable logical export scan.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ExportScanPage {
    /// Durable cursor the page was read from.
    pub after: Option<DocumentId>,
    /// Ordered documents following `after`.
    pub(crate) documents: Vec<ExportDocument>,
}

impl ExportScanPage {
    /// Borrows the ordered documents following `after`.
    #[must_use]
    pub fn documents(&self) -> &[ExportDocument] {
        &self.documents
    }
}

/// Durable cursor of an interrupted versioned restore.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct RestoreCursor {
    /// Identity of the export payload the cursor belongs to.
    pub payload: [u8; 32],
    /// Index of the next history entry to replay.
    pub next: u64,
}

/// Durable result of a local commit or exact retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum CommitOutcome {
    /// A new outbox sequence was committed.
    Applied {
        /// Monotonic sequence within the logical scope.
        sequence: u64,
    },
    /// The same operation and change were committed earlier.
    Duplicate {
        /// Original monotonic sequence.
        sequence: u64,
    },
}

/// One durable unsent change reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct OutboxEntry {
    /// Monotonic sequence within the logical scope.
    pub sequence: u64,
    /// Changed document.
    pub document: DocumentId,
    /// Retryable logical operation.
    pub operation: OperationId,
    /// Immutable CRDT change identity.
    pub digest: ChangeDigest,
}

/// Durable exact-request binding for a previously committed operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CommittedOperation {
    /// Document changed by the request.
    pub document: DocumentId,
    /// Canonical caller request identity.
    pub request: OperationDigest,
    /// CRDT change produced by the request.
    pub digest: ChangeDigest,
    /// Original durable outbox sequence.
    pub sequence: u64,
}
