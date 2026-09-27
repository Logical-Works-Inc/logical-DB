//! Portable transactional storage for one embedded local replica.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{collections::BTreeSet, path::Path};

pub use logical_db_model::MAX_SNAPSHOT_BYTES;
use logical_db_model::{
    AccountId, ChangeDigest, DocumentId, DocumentRevision, IndexEntry, IndexId, IndexValue,
    MAX_CHANGE_BYTES, OperationDigest, OperationId, PeerId, ReplicaId, Scope, SpaceId,
};
use redb::{
    CommitError, Database, DatabaseError, Durability, ReadableDatabase, ReadableTable,
    SetDurabilityError, StorageError, Table, TableDefinition, TableError, TableHandle,
    TransactionError,
};
use thiserror::Error;

/// Versioned table holding every logical-store row for this file format.
const DATA: TableDefinition<&[u8], &[u8]> = TableDefinition::new("logical_data_v3");
/// Versioned magic prefix shared by every scoped storage key.
const FORMAT: &[u8] = b"LDB\x03";
/// Storage key that binds the file to one durable replica identity.
const REPLICA_KEY: &[u8] = b"LDB\x03replica";
/// Row kind for a materialized document snapshot.
const KIND_SNAPSHOT: u8 = 1;
/// Row kind for one immutable CRDT change.
const KIND_CHANGE: u8 = 2;
/// Row kind for one durable operation receipt.
const KIND_OPERATION: u8 = 3;
/// Row kind for one durable local outbox entry.
const KIND_OUTBOX: u8 = 4;
/// Row kind for the scope's highest committed outbox sequence.
const KIND_SEQUENCE: u8 = 5;
/// Row kind for one document's optimistic-concurrency revision.
const KIND_REVISION: u8 = 6;
/// Row kind for one peer's opaque synchronization checkpoint.
const KIND_CHECKPOINT: u8 = 7;
/// Row kind for the scope's global outbox acknowledgement cursor.
const KIND_OUTBOX_ACK: u8 = 8;
/// Row kind for one forward secondary-index entry.
const KIND_INDEX: u8 = 9;
/// Row kind for one reverse secondary-index entry.
const KIND_INDEX_REVERSE: u8 = 10;
/// Row kind for one scope's bound application schema.
const KIND_SCHEMA: u8 = 11;
/// Row kind for the scope's highest materialized-event sequence.
const KIND_EVENT_SEQUENCE: u8 = 12;
/// Row kind for one durable materialized-state invalidation.
const KIND_EVENT: u8 = 13;
/// Row kind for one in-progress schema migration marker.
const KIND_MIGRATION: u8 = 14;
/// Row kind for the scope's durable upstream binding.
const KIND_UPSTREAM: u8 = 15;
/// Row kind for one document's monotonic tombstone flag.
const KIND_TOMBSTONE: u8 = 16;
/// Row kind for one durable restore cursor.
const KIND_RESTORE: u8 = 17;
/// Row kind for the scope's durable baseline generation.
const KIND_BASELINE: u8 = 18;
/// Row kind for one durable baseline invalidation.
const KIND_BASELINE_EVENT: u8 = 19;
/// Row kind for one post-checkpoint change pointer, ordered by document revision.
const KIND_SNAPSHOT_DELTA: u8 = 20;
/// Row kind for the revision represented by each complete snapshot checkpoint.
const KIND_SNAPSHOT_CHECKPOINT: u8 = 21;
/// Row kind for the bounded serialized checkpoint-suffix byte count.
const KIND_SNAPSHOT_SUFFIX_BYTES: u8 = 22;
/// Magic prefix identifying a versioned logical export payload.
const EXPORT_MAGIC: [u8; 5] = [b'L', b'D', b'B', b'E', 0x01];
/// Version of the logical export payload defined in this module.
pub const EXPORT_VERSION: u16 = 1;
/// Maximum total bytes of one versioned logical export payload.
pub const MAX_EXPORT_BYTES: usize = 67_108_864;
/// Fixed export header: magic, account UUID, and space UUID.
const EXPORT_HEADER_BYTES: usize = 5 + 16 + 16;
/// Minimum export entry wire size: document, operation, request, digest, length.
const EXPORT_ENTRY_PREFIX_BYTES: usize = 16 + 16 + 32 + 32 + 4;
/// Fixed export document prefix: document UUID, tombstone flag, length.
const EXPORT_DOCUMENT_PREFIX_BYTES: usize = 16 + 1 + 4;
/// Durable restore marker: export payload identity plus next-entry cursor.
const RESTORE_MARKER_BYTES: usize = 32 + 8;
/// Maximum aggregate snapshot state retained while installing or migrating one page.
pub const MAX_MATERIALIZED_PAGE_BYTES: usize = 67_108_864;
/// Maximum immutable changes replayed after one complete snapshot checkpoint.
pub const MAX_SNAPSHOT_DELTAS: usize = 31;
/// Length of the shared scope prefix: format magic plus account and space UUIDs.
const SCOPE_PREFIX_BYTES: usize = FORMAT.len() + 16 + 16;
/// Length of one kind-prefixed sequence key: scope prefix, kind tag, and `u64`.
const SEQUENCE_KEY_BYTES: usize = SCOPE_PREFIX_BYTES + 1 + 8;
/// Maximum accepted size of one opaque peer checkpoint.
const MAX_CHECKPOINT_BYTES: usize = 4_096;
/// Maximum number of changes accepted in one remote install page.
const MAX_REMOTE_CHANGES: usize = 256;
/// Maximum aggregate change bytes accepted in one remote install page.
const MAX_REMOTE_CHANGE_BYTES: usize = 8_388_608;
/// Encoded size of one durable operation receipt.
const OPERATION_VALUE_BYTES: usize = 88;
/// Encoded size of one durable outbox entry.
const OUTBOX_VALUE_BYTES: usize = 64;
/// Maximum projectable indexes per document.
const MAX_INDEXES_PER_DOCUMENT: usize = 64;
/// Maximum encoded size of one index value key component.
const MAX_INDEX_VALUE_BYTES: usize = 4_096;
/// Encoded size of one bound application schema value.
const SCHEMA_VALUE_BYTES: usize = 36;
/// Encoded size of one schema migration marker.
const MIGRATION_VALUE_BYTES: usize = 53;
/// Encoded size of one materialized-state event value.
const EVENT_VALUE_BYTES: usize = 48;
/// Encoded size of one baseline event value.
const BASELINE_VALUE_BYTES: usize = 8;

/// A validated local CRDT change without any transport-layer representation.
#[derive(Clone, Copy)]
pub struct LocalChange<'a> {
    /// Owning account and application space.
    scope: Scope,
    /// Document the change applies to.
    document: DocumentId,
    /// Retryable operation identity carried by the change.
    operation: OperationId,
    /// Canonical caller request identity bound to the operation.
    request: OperationDigest,
    /// Immutable CRDT change identity.
    digest: ChangeDigest,
    /// Canonical CRDT change bytes.
    bytes: &'a [u8],
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
    /// Complete verified Automerge checkpoint after applying the change, or
    /// empty to append the immutable change as a bounded checkpoint suffix.
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

    /// Builds one atomic local commit that appends its immutable change to the
    /// existing checkpoint suffix. The store refuses the append once its
    /// bounded suffix is full, requiring the caller to provide a new checkpoint.
    #[must_use]
    pub fn new_incremental(
        change: LocalChange<'a>,
        expected_revision: DocumentRevision,
        indexes: &'a [IndexEntry],
        tombstone: bool,
    ) -> Self {
        Self {
            change,
            expected_revision: Some(expected_revision),
            snapshot: &[],
            indexes,
            tombstone,
        }
    }
}

/// Latest complete snapshot and its optimistic concurrency marker.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StoredSnapshot {
    /// Complete verified Automerge checkpoint bytes.
    bytes: Vec<u8>,
    /// Last locally committed change identity.
    pub revision: DocumentRevision,
    /// Ordered immutable changes after `bytes`, bounded by `MAX_SNAPSHOT_DELTAS`.
    deltas: Vec<(ChangeDigest, Vec<u8>)>,
}

impl StoredSnapshot {
    /// Builds one complete checkpoint plus its bounded ordered suffix.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose snapshots through here.
    #[must_use]
    pub fn new(
        bytes: Vec<u8>,
        revision: DocumentRevision,
        deltas: Vec<(ChangeDigest, Vec<u8>)>,
    ) -> Self {
        Self {
            bytes,
            revision,
            deltas,
        }
    }

    /// The complete verified checkpoint and its ordered suffix.
    #[must_use]
    pub fn checkpoint_and_deltas(&self) -> (&[u8], &[(ChangeDigest, Vec<u8>)]) {
        (&self.bytes, &self.deltas)
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
    /// Complete verified base checkpoint used to compute replacement projections.
    snapshot: Vec<u8>,
    /// Ordered immutable changes after the checkpoint.
    deltas: Vec<(ChangeDigest, Vec<u8>)>,
}

impl MigrationDocument {
    /// Builds one checkpoint-plus-suffix input for a migration batch.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose migration documents through here.
    #[must_use]
    pub fn new(
        document: DocumentId,
        revision: DocumentRevision,
        snapshot: Vec<u8>,
        deltas: Vec<(ChangeDigest, Vec<u8>)>,
    ) -> Self {
        Self {
            document,
            revision,
            snapshot,
            deltas,
        }
    }

    /// The complete verified base checkpoint and its ordered suffix.
    #[must_use]
    pub fn checkpoint_and_deltas(&self) -> (&[u8], &[(ChangeDigest, Vec<u8>)]) {
        (&self.snapshot, &self.deltas)
    }
}

/// One bounded migration page and the cursor it was read from.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct MigrationPage {
    /// Durable cursor that must still identify the migration when applied.
    pub after: Option<DocumentId>,
    /// Ordered documents following `after`.
    documents: Vec<MigrationDocument>,
}

impl MigrationPage {
    /// Builds one bounded migration page read from a durable cursor.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose migration pages through here.
    #[must_use]
    pub fn new(after: Option<DocumentId>, documents: Vec<MigrationDocument>) -> Self {
        Self { after, documents }
    }

    /// The ordered documents following this page's cursor.
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
    bytes: Vec<u8>,
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

    /// The canonical CRDT change bytes of this entry.
    #[must_use]
    pub fn change_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Replaces this entry's canonical change bytes, for re-encoding a
    /// deliberately altered export fixture. The bytes are not validated here;
    /// `decode_export` and the CRDT validator remain the boundary.
    pub fn replace_change_bytes(&mut self, bytes: Vec<u8>) {
        self.bytes = bytes;
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
    snapshot: Vec<u8>,
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

    /// The complete verified snapshot of this exported document.
    #[must_use]
    pub fn snapshot_bytes(&self) -> &[u8] {
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
    documents: Vec<ExportScanDocument>,
}

impl ExportScanPage {
    /// Builds one bounded export-scan page read from a durable cursor.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose export scan pages through here.
    #[must_use]
    pub fn new(after: Option<DocumentId>, documents: Vec<ExportScanDocument>) -> Self {
        Self { after, documents }
    }

    /// The ordered documents following this page's cursor.
    #[must_use]
    pub fn documents(&self) -> &[ExportScanDocument] {
        &self.documents
    }
}

/// One scoped checkpoint and bounded suffix returned by an export scan.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct ExportScanDocument {
    /// Document ordered by its portable identifier.
    pub document: DocumentId,
    /// Whether the checkpoint plus suffix carries a first-class tombstone.
    pub tombstone: bool,
    /// Complete verified base checkpoint bytes.
    snapshot: Vec<u8>,
    /// Ordered immutable changes after the checkpoint.
    deltas: Vec<(ChangeDigest, Vec<u8>)>,
}

impl ExportScanDocument {
    /// Builds one bounded checkpoint-and-suffix entry for an export scan.
    ///
    /// `#[non_exhaustive]` makes a struct expression impossible outside this
    /// crate, so external callers compose export scan documents through here.
    #[must_use]
    pub fn new(
        document: DocumentId,
        tombstone: bool,
        snapshot: Vec<u8>,
        deltas: Vec<(ChangeDigest, Vec<u8>)>,
    ) -> Self {
        Self {
            document,
            tombstone,
            snapshot,
            deltas,
        }
    }

    /// The complete verified base checkpoint and its ordered suffix.
    #[must_use]
    pub fn checkpoint_and_deltas(&self) -> (&[u8], &[(ChangeDigest, Vec<u8>)]) {
        (&self.snapshot, &self.deltas)
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

/// Cross-platform embedded store backed by redb transactions and file locking.
pub struct EmbeddedStore {
    /// Open redb database backing every durable row.
    database: Database,
    /// Durable replica identity bound to this database file.
    replica: ReplicaId,
}

impl EmbeddedStore {
    /// Opens or creates one local replica database at an explicit path.
    ///
    /// # Errors
    ///
    /// Returns a typed database, transaction, or schema error when the file
    /// cannot be opened or initialized.
    pub fn open(path: impl AsRef<Path>, replica: ReplicaId) -> Result<Self, StoreError> {
        let database = Database::create(path)?;
        {
            let transaction = database.begin_read()?;
            let tables = transaction.list_tables()?;
            let normal = tables
                .map(|table| table.name().to_owned())
                .collect::<Vec<_>>();
            let multimaps = transaction.list_multimap_tables()?.count();
            if multimaps != 0 || normal.iter().any(|name| name != DATA.name()) {
                let error = StoreError::UnsupportedSchema;
                return Err(error);
            }
        }
        let mut transaction = database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let existing_replica = {
            let replica_value = table.get(REPLICA_KEY)?;
            replica_value
                .map(|value| decode_identifier(value.value(), ReplicaId::from_durable_bytes))
                .transpose()?
        };
        match existing_replica {
            Some(existing) if existing != replica => {
                let error = StoreError::ReplicaMismatch {
                    expected: existing,
                    actual: replica,
                };
                return Err(error);
            }
            Some(_) => {}
            None => {
                let mut entries = table.iter()?;
                if entries.next().transpose()?.is_some() {
                    let error = StoreError::MissingReplicaBinding;
                    return Err(error);
                }
                table.insert(REPLICA_KEY, replica.as_uuid().as_bytes().as_slice())?;
            }
        }
        drop(table);
        transaction.commit()?;
        Ok(Self { database, replica })
    }

    /// Returns the durable replica identity bound to this database file.
    #[must_use]
    pub const fn replica(&self) -> ReplicaId {
        self.replica
    }

    /// Binds one scope to an exact application schema version and projection
    /// digest before document state is opened.
    ///
    /// # Errors
    ///
    /// Refuses a schema change or legacy scoped data without an explicit
    /// migration. A new empty scope is bound atomically.
    pub fn bind_schema(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
    ) -> Result<(), StoreError> {
        if version == 0 {
            let error = StoreError::InvalidSchemaVersion;
            return Err(error);
        }
        let key = key_schema(scope);
        let expected = schema_value(version, digest);
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let migration_marker = table.get(key_migration(scope).as_slice())?.is_some();
        if migration_marker {
            let error = StoreError::MigrationInProgress;
            return Err(error);
        }
        let existing = {
            let value = table.get(key.as_slice())?;
            value.map(|value| value.value().to_vec())
        };
        match existing.as_deref() {
            Some(existing) if existing == expected => return Ok(()),
            Some(_) => {
                let error = StoreError::SchemaMigrationRequired;
                return Err(error);
            }
            None => {
                let has_scope_entries = scope_has_records(&table, scope)?;
                if has_scope_entries {
                    let error = StoreError::SchemaMigrationRequired;
                    return Err(error);
                }
                table.insert(key.as_slice(), expected.as_slice())?;
            }
        }
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Starts or resumes an explicit schema/index migration to one exact target.
    ///
    /// A durable marker excludes local writes, remote installs, and index reads
    /// until every snapshot is reprojected and the target schema swaps in.
    ///
    /// # Errors
    ///
    /// Refuses zero versions or a different target while another migration is
    /// in progress.
    pub fn begin_schema_migration(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
    ) -> Result<(), StoreError> {
        if version == 0 {
            let error = StoreError::InvalidSchemaVersion;
            return Err(error);
        }
        let target = schema_value(version, digest);
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let key = key_migration(scope);
        let marker_value = table.get(key.as_slice())?;
        let marker = marker_value.map(|value| value.value().to_vec());
        match marker.as_deref() {
            Some(value) => {
                let (existing, _) = decode_migration(value)?;
                if existing != target {
                    let error = StoreError::MigrationTargetMismatch;
                    return Err(error);
                }
            }
            None => {
                table.insert(key.as_slice(), encode_migration(target, None).as_slice())?;
            }
        }
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Reads the next bounded snapshot page for an active schema migration.
    ///
    /// # Errors
    ///
    /// Refuses an invalid page size, inactive/different migration, malformed
    /// durable state, or a snapshot without its matching revision.
    pub fn migration_page(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
        limit: usize,
    ) -> Result<MigrationPage, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidMigrationLimit(limit);
            return Err(error);
        }
        let target = schema_value(version, digest);
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let marker_value = table.get(key_migration(scope).as_slice())?;
        let marker = marker_value.ok_or(StoreError::MigrationNotInProgress)?;
        let (existing, after) = decode_migration(marker.value())?;
        if existing != target {
            let error = StoreError::MigrationTargetMismatch;
            return Err(error);
        }
        let row_prefix = prefix(scope, KIND_SNAPSHOT);
        let end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
        let mut start = row_prefix.clone();
        if let Some(document) = after {
            start.extend_from_slice(document.as_uuid().as_bytes());
            start.push(0);
        }
        let mut documents = Vec::with_capacity(limit);
        let mut materialized_bytes = 0usize;
        let entries = table.range(start.as_slice()..end.as_slice())?;
        for entry in entries.take(limit) {
            let (key, value) = entry?;
            let snapshot_bytes = value.value().len();
            if !is_nonempty_bounded(snapshot_bytes, MAX_SNAPSHOT_BYTES) {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let document = decode_identifier(
                &key.value()[row_prefix.len()..],
                DocumentId::from_durable_bytes,
            )?;
            let revision = stored_revision(&table, scope, document)?;
            let checkpoint_revision =
                snapshot_checkpoint_revision(&table, scope, document, revision)?;
            let deltas =
                read_snapshot_deltas(&table, scope, document, checkpoint_revision, revision.get())?;
            let Some(next_bytes) =
                add_checkpoint_and_delta_bytes(materialized_bytes, snapshot_bytes, &deltas)
            else {
                if documents.is_empty() {
                    return Err(StoreError::InvalidMigrationPage);
                }
                break;
            };
            documents.push(MigrationDocument {
                document,
                revision,
                snapshot: value.value().to_vec(),
                deltas,
            });
            materialized_bytes = next_bytes;
        }
        Ok(MigrationPage { after, documents })
    }

    /// Atomically replaces all index rows for one migration page and advances
    /// its durable cursor.
    ///
    /// # Errors
    ///
    /// Refuses empty, excessive, unordered, stale, skipped, or target-mismatched
    /// batches. No cursor or index row changes on failure.
    pub fn apply_migration_page(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
        after: Option<DocumentId>,
        documents: &[MigrationIndexes<'_>],
    ) -> Result<(), StoreError> {
        if documents.is_empty() || !is_at_most(documents.len(), 256) {
            let error = StoreError::InvalidMigrationPage;
            return Err(error);
        }
        let target = schema_value(version, digest);
        let mut previous = after;
        for document in documents {
            if previous.is_some_and(|cursor| document.document <= cursor) {
                let error = StoreError::InvalidMigrationPage;
                return Err(error);
            }
            validate_indexes(document.indexes)?;
            previous = Some(document.document);
        }
        let last = previous.ok_or(StoreError::InvalidMigrationPage)?;
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let marker_key = key_migration(scope);
        let marker = {
            let marker_value = table.get(marker_key.as_slice())?;
            let marker_value = marker_value.ok_or(StoreError::MigrationNotInProgress)?;
            marker_value.value().to_vec()
        };
        let (existing, durable_after) = decode_migration(&marker)?;
        if existing != target {
            let error = StoreError::MigrationTargetMismatch;
            return Err(error);
        }
        if durable_after != after {
            let error = StoreError::StaleMigrationPage;
            return Err(error);
        }
        let row_prefix = prefix(scope, KIND_SNAPSHOT);
        let row_end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
        let mut row_start = row_prefix.clone();
        if let Some(document) = durable_after {
            row_start.extend_from_slice(document.as_uuid().as_bytes());
            row_start.push(0);
        }
        let expected_entries = table.range(row_start.as_slice()..row_end.as_slice())?;
        let expected_documents = expected_entries
            .take(documents.len())
            .map(|entry| {
                let (key, _value) = entry?;
                if key.value().len() != row_prefix.len().saturating_add(16) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                decode_identifier(
                    &key.value()[row_prefix.len()..],
                    DocumentId::from_durable_bytes,
                )
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        if expected_documents.len() != documents.len()
            || expected_documents
                .iter()
                .zip(documents)
                .any(|(expected, supplied)| *expected != supplied.document)
        {
            let error = StoreError::InvalidMigrationPage;
            return Err(error);
        }
        for document in documents {
            let revision = stored_revision(&table, scope, document.document)?;
            if revision != document.expected_revision {
                let error = StoreError::StaleDocument;
                return Err(error);
            }
            replace_indexes(&mut table, scope, document.document, document.indexes)?;
        }
        table.insert(
            marker_key.as_slice(),
            encode_migration(target, Some(last)).as_slice(),
        )?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Atomically publishes the target schema after proving no snapshots remain
    /// beyond the durable migration cursor.
    ///
    /// # Errors
    ///
    /// Refuses an incomplete, inactive, or target-mismatched migration.
    pub fn finish_schema_migration(
        &self,
        scope: Scope,
        version: u32,
        digest: [u8; 32],
    ) -> Result<(), StoreError> {
        let target = schema_value(version, digest);
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let marker_key = key_migration(scope);
        let marker = {
            let marker_value = table.get(marker_key.as_slice())?;
            let marker_value = marker_value.ok_or(StoreError::MigrationNotInProgress)?;
            marker_value.value().to_vec()
        };
        let (existing, after) = decode_migration(&marker)?;
        if existing != target {
            let error = StoreError::MigrationTargetMismatch;
            return Err(error);
        }
        let row_prefix = prefix(scope, KIND_SNAPSHOT);
        let end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
        let mut start = row_prefix;
        if let Some(document) = after {
            start.extend_from_slice(document.as_uuid().as_bytes());
            start.push(0);
        }
        let mut remaining_entries = table.range(start.as_slice()..end.as_slice())?;
        let has_remaining = remaining_entries.next().transpose()?.is_some();
        if has_remaining {
            let error = StoreError::MigrationIncomplete;
            return Err(error);
        }
        table.insert(key_schema(scope).as_slice(), target.as_slice())?;
        table.remove(marker_key.as_slice())?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Reads the next bounded document page for a versioned logical export.
    ///
    /// Documents arrive in stable identifier order with their complete
    /// snapshot and tombstone flag, so an interrupted export resumes from
    /// `after` without re-reading the prefix. Snapshots stay bounded by the
    /// same aggregate materialized budget as migration pages.
    ///
    /// # Errors
    ///
    /// Refuses an invalid page size, an active schema migration, or malformed
    /// durable state.
    pub fn export_scan(
        &self,
        scope: Scope,
        after: Option<DocumentId>,
        limit: usize,
    ) -> Result<ExportScanPage, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidExportLimit(limit);
            return Err(error);
        }
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        let row_prefix = prefix(scope, KIND_SNAPSHOT);
        let end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
        let mut start = row_prefix.clone();
        if let Some(document) = after {
            start.extend_from_slice(document.as_uuid().as_bytes());
            start.push(0);
        }
        let mut documents = Vec::new();
        let mut materialized_bytes = 0usize;
        for entry in table.range(start.as_slice()..end.as_slice())?.take(limit) {
            let (key, value) = entry?;
            if key.value().len() != row_prefix.len().saturating_add(16) {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let snapshot = value.value();
            if !is_nonempty_bounded(snapshot.len(), MAX_SNAPSHOT_BYTES) {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let document = export_document(&table, scope, key.value(), row_prefix.len(), snapshot)?;
            let Some(next_bytes) = add_checkpoint_and_delta_bytes(
                materialized_bytes,
                snapshot.len(),
                &document.deltas,
            ) else {
                if documents.is_empty() {
                    return Err(StoreError::InvalidExportSize(snapshot.len()));
                }
                break;
            };
            documents.push(document);
            materialized_bytes = next_bytes;
        }
        Ok(ExportScanPage { after, documents })
    }

    /// Reads the full ordered commit history including acknowledged entries.
    ///
    /// Unlike [`EmbeddedStore::outbox_page`], acknowledgement only advances
    /// the transport cursor and never hides history from an export: a backup
    /// must replay every commit to reproduce outbox and event cursors.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, decode, read, or table error.
    pub fn history_page(
        &self,
        scope: Scope,
        after: u64,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidOutboxLimit(limit);
            return Err(error);
        }
        if after == u64::MAX {
            return Ok(Vec::new());
        }
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let start = key_outbox(
            scope,
            after.checked_add(1).ok_or(StoreError::SequenceExhausted)?,
        );
        let end = prefix(scope, KIND_SEQUENCE);
        table
            .range(start.as_slice()..end.as_slice())?
            .take(limit)
            .map(|entry| {
                let (key, value) = entry?;
                let sequence = decode_sequence_key(key.value())?;
                decode_outbox(sequence, value.value())
            })
            .collect()
    }

    /// Atomically saves a document snapshot, immutable change, operation
    /// result, scope sequence, and outbox entry.
    ///
    /// # Errors
    ///
    /// Returns a validation or storage error. Reusing an operation identity
    /// for different change bytes returns [`StoreError::OperationConflict`].
    pub fn commit_local(&self, commit: LocalCommit<'_>) -> Result<CommitOutcome, StoreError> {
        self.commit_local_batch(std::slice::from_ref(&commit))?
            .pop()
            .ok_or(StoreError::EmptyLocalBatch)
    }

    /// Atomically saves several local document transitions in one durable
    /// transaction. Every snapshot and revision is checked before any row is
    /// written, so a stale or invalid member leaves the complete batch absent.
    ///
    /// # Errors
    ///
    /// Returns a validation, stale-read, operation, or storage error. A
    /// successful commit returns one outcome per input in input order.
    pub fn commit_local_batch(
        &self,
        commits: &[LocalCommit<'_>],
    ) -> Result<Vec<CommitOutcome>, StoreError> {
        if commits.is_empty() {
            return Ok(Vec::new());
        }
        let mut documents = BTreeSet::new();
        let mut operations = BTreeSet::new();
        for commit in commits {
            if commit.snapshot.is_empty() && commit.expected_revision.is_none() {
                return Err(StoreError::SnapshotCheckpointRequired);
            }
            if !commit.snapshot.is_empty()
                && !is_nonempty_bounded(commit.snapshot.len(), MAX_SNAPSHOT_BYTES)
            {
                let error = StoreError::InvalidSnapshotSize(commit.snapshot.len());
                return Err(error);
            }
            validate_indexes(commit.indexes)?;
            if !documents.insert(commit.change.document) {
                return Err(StoreError::DuplicateBatchDocument);
            }
            if !operations.insert(commit.change.operation) {
                return Err(StoreError::DuplicateBatchOperation);
            }
        }

        let scope = commits[0].change.scope;
        if commits.iter().any(|commit| commit.change.scope != scope) {
            return Err(StoreError::MixedBatchScope);
        }
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;

        let mut outcomes = Vec::with_capacity(commits.len());
        let mut current_revisions = Vec::with_capacity(commits.len());
        for commit in commits {
            let operation_key = key_operation(scope, commit.change.operation);
            if let Some(sequence) = existing_local_sequence(&table, commit, &operation_key)? {
                outcomes.push(Some(CommitOutcome::Duplicate { sequence }));
                current_revisions.push(None);
                continue;
            }
            let revision_key = key_revision(scope, commit.change.document);
            let current_revision = table
                .get(revision_key.as_slice())?
                .map(|value| decode_revision(value.value()))
                .transpose()?;
            if current_revision != commit.expected_revision {
                let error = StoreError::StaleDocument;
                return Err(error);
            }
            check_tombstone_transition(&table, scope, commit.change.document, commit.tombstone)?;
            outcomes.push(None);
            current_revisions.push(current_revision);
        }

        let sequence_key = key_sequence(scope);
        let mut sequence = table
            .get(sequence_key.as_slice())?
            .map(|value| decode_u64(value.value()))
            .transpose()?
            .unwrap_or(0);
        let event_sequence_key = key_event_sequence(scope);
        let mut event_sequence = table
            .get(event_sequence_key.as_slice())?
            .map(|value| decode_u64(value.value()))
            .transpose()?
            .unwrap_or(0);
        for (index, commit) in commits.iter().enumerate() {
            if outcomes[index].is_some() {
                continue;
            }
            commit_local_member(
                &mut table,
                scope,
                commit,
                current_revisions[index],
                &mut sequence,
                &mut event_sequence,
            )?;
            outcomes[index] = Some(CommitOutcome::Applied { sequence });
        }
        table.insert(sequence_key.as_slice(), sequence.to_be_bytes().as_slice())?;
        table.insert(
            event_sequence_key.as_slice(),
            event_sequence.to_be_bytes().as_slice(),
        )?;
        drop(table);
        transaction.commit()?;
        outcomes
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or(StoreError::CorruptInternalValue)
    }

    /// Loads the durable exact-request binding for an operation, when present.
    ///
    /// # Errors
    ///
    /// Returns a typed read or corruption error.
    pub fn operation(
        &self,
        scope: Scope,
        operation: OperationId,
    ) -> Result<Option<CommittedOperation>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        let key = key_operation(scope, operation);
        let committed = table
            .get(key.as_slice())?
            .map(|value| decode_operation(value.value()))
            .transpose()?;
        if let Some(committed) = committed {
            verify_operation_links(&table, scope, operation, committed)?;
        }
        Ok(committed)
    }

    /// Loads the latest complete document snapshot, when present.
    ///
    /// # Errors
    ///
    /// Returns a typed read or table error when durable state cannot be read.
    pub fn load_snapshot(
        &self,
        scope: Scope,
        document: DocumentId,
    ) -> Result<Option<StoredSnapshot>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let snapshot_key = key_snapshot(scope, document);
        let revision_key = key_revision(scope, document);
        let snapshot = table
            .get(snapshot_key.as_slice())?
            .map(|value| {
                if !is_nonempty_bounded(value.value().len(), MAX_SNAPSHOT_BYTES) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                Ok(value.value().to_vec())
            })
            .transpose()?;
        let revision = table
            .get(revision_key.as_slice())?
            .map(|value| decode_revision(value.value()))
            .transpose()?;
        match (snapshot, revision) {
            (None, None) => Ok(None),
            (Some(bytes), Some(revision)) => {
                let checkpoint_revision =
                    snapshot_checkpoint_revision(&table, scope, document, revision)?;
                let deltas = read_snapshot_deltas(
                    &table,
                    scope,
                    document,
                    checkpoint_revision,
                    revision.get(),
                )?;
                let delta_bytes = deltas
                    .iter()
                    .fold(0usize, |total, delta| total.saturating_add(delta.1.len()));
                let materialized_bytes = bytes.len().saturating_add(delta_bytes);
                if materialized_bytes > MAX_SNAPSHOT_BYTES {
                    return Err(StoreError::CorruptInternalValue);
                }
                let suffix_size = table
                    .get(key_snapshot_suffix_bytes(scope, document).as_slice())?
                    .map(|value| decode_u64(value.value()))
                    .transpose()?;
                if suffix_size
                    .is_some_and(|stored| usize::try_from(stored).ok() != Some(delta_bytes))
                    || (suffix_size.is_none() && !deltas.is_empty())
                {
                    return Err(StoreError::CorruptInternalValue);
                }
                Ok(Some(StoredSnapshot {
                    bytes,
                    revision,
                    deltas,
                }))
            }
            _ => Err(StoreError::CorruptInternalValue),
        }
    }

    /// Reads only the latest document revision, without loading its checkpoint suffix.
    ///
    /// This is the cache-validation path for a scope-bound facade. A cached
    /// document is usable only when this durable marker exactly matches it.
    ///
    /// # Errors
    ///
    /// Returns a typed read, corruption, or migration-state error.
    pub fn document_revision(
        &self,
        scope: Scope,
        document: DocumentId,
    ) -> Result<Option<DocumentRevision>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        table
            .get(key_revision(scope, document).as_slice())?
            .map(|value| decode_revision(value.value()))
            .transpose()
    }

    /// Returns whether a first-class tombstone is durably recorded.
    ///
    /// The flag is monotonic: once a local delete or a remote tombstone page
    /// commits it, neither restarts nor later non-tombstone pages clear it.
    ///
    /// # Errors
    ///
    /// Returns a typed read or table error when durable state cannot be read.
    pub fn is_tombstoned(&self, scope: Scope, document: DocumentId) -> Result<bool, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        Ok(table
            .get(key_tombstone(scope, document).as_slice())?
            .is_some_and(|value| value.value() == [1_u8]))
    }

    /// Reads a bounded ordered page from the durable local outbox.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, decode, read, or table error.
    pub fn outbox_page(
        &self,
        scope: Scope,
        after: u64,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidOutboxLimit(limit);
            return Err(error);
        }
        if after == u64::MAX {
            return Ok(Vec::new());
        }
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let acknowledged = table
            .get(key_outbox_ack(scope).as_slice())?
            .map(|value| decode_u64(value.value()))
            .transpose()?
            .unwrap_or(0);
        let start_after = after.max(acknowledged);
        if start_after == u64::MAX {
            return Ok(Vec::new());
        }
        let start = key_outbox(
            scope,
            start_after
                .checked_add(1)
                .ok_or(StoreError::SequenceExhausted)?,
        );
        let end = prefix(scope, KIND_SEQUENCE);
        table
            .range(start.as_slice()..end.as_slice())?
            .take(limit)
            .map(|entry| {
                let (key, value) = entry?;
                let sequence = decode_sequence_key(key.value())?;
                decode_outbox(sequence, value.value())
            })
            .collect()
    }

    /// Reads durable state invalidations after an exclusive subscription cursor.
    ///
    /// # Errors
    ///
    /// Returns a bound, corruption, or storage failure.
    pub fn changes_since(
        &self,
        scope: Scope,
        after: u64,
        limit: usize,
    ) -> Result<Vec<ChangeEvent>, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidEventLimit(limit);
            return Err(error);
        }
        if after == u64::MAX {
            return Ok(Vec::new());
        }
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        let mut events = Vec::new();
        let start = key_event(
            scope,
            after.checked_add(1).ok_or(StoreError::SequenceExhausted)?,
        );
        let end =
            next_prefix(&prefix(scope, KIND_EVENT)).ok_or(StoreError::CorruptInternalValue)?;
        for entry in table.range(start.as_slice()..end.as_slice())?.take(limit) {
            let (key, value) = entry?;
            let sequence = decode_sequence_key_for_kind(key.value(), KIND_EVENT)?;
            let event = decode_event(sequence, value.value())?;
            let ChangeEvent::Document {
                document, digest, ..
            } = event
            else {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            };
            let change = table
                .get(key_change(scope, document, digest).as_slice())?
                .ok_or(StoreError::CorruptInternalValue)?;
            if !is_nonempty_bounded(change.value().len(), MAX_CHANGE_BYTES) {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let snapshot = table
                .get(key_snapshot(scope, document).as_slice())?
                .ok_or(StoreError::CorruptInternalValue)?;
            if !is_nonempty_bounded(snapshot.value().len(), MAX_SNAPSHOT_BYTES) {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            let revision = table
                .get(key_revision(scope, document).as_slice())?
                .ok_or(StoreError::CorruptInternalValue)?;
            decode_revision(revision.value())?;
            events.push(event);
        }
        let start = key_baseline_event(
            scope,
            after.checked_add(1).ok_or(StoreError::SequenceExhausted)?,
        );
        let end = next_prefix(&prefix(scope, KIND_BASELINE_EVENT))
            .ok_or(StoreError::CorruptInternalValue)?;
        for entry in table.range(start.as_slice()..end.as_slice())?.take(limit) {
            let (key, value) = entry?;
            let sequence = decode_sequence_key_for_kind(key.value(), KIND_BASELINE_EVENT)?;
            events.push(decode_baseline_event(sequence, value.value())?);
        }
        events.sort_by_key(ChangeEvent::sequence);
        events.truncate(limit);
        Ok(events)
    }

    /// Reads the scope's durable baseline generation, `0` before any install.
    ///
    /// # Errors
    ///
    /// Returns a corruption or storage failure.
    pub fn baseline_generation(&self, scope: Scope) -> Result<u64, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        let generation = table
            .get(key_baseline(scope).as_slice())?
            .map(|value| decode_u64(value.value()))
            .transpose()?;
        Ok(generation.unwrap_or(0))
    }

    /// Loads one immutable change by its native digest.
    ///
    /// # Errors
    ///
    /// Returns a typed read or table error when durable state cannot be read.
    pub fn load_change(
        &self,
        scope: Scope,
        document: DocumentId,
        digest: ChangeDigest,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let key = key_change(scope, document, digest);
        table
            .get(key.as_slice())?
            .map(|value| {
                if !is_nonempty_bounded(value.value().len(), MAX_CHANGE_BYTES) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                Ok(value.value().to_vec())
            })
            .transpose()
    }

    /// Reads an exact-value secondary-index page in stable document order.
    ///
    /// # Errors
    ///
    /// Returns a validation, corruption, or storage failure. `after` is an
    /// exclusive document cursor from the preceding page.
    pub fn query_index(
        &self,
        scope: Scope,
        index: IndexId,
        value: &IndexValue,
        after: Option<DocumentId>,
        limit: usize,
    ) -> Result<Vec<IndexHit>, StoreError> {
        if !(1..=256).contains(&limit) {
            let error = StoreError::InvalidIndexLimit(limit);
            return Err(error);
        }
        let encoded = encode_index_value(value)?;
        let row_prefix = index_row_prefix(scope, index, &encoded);
        let end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
        let mut start = row_prefix.clone();
        if let Some(document) = after {
            start.extend_from_slice(document.as_uuid().as_bytes());
            start.push(0);
        }
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        table
            .range(start.as_slice()..end.as_slice())?
            .take(limit)
            .map(|entry| {
                let (key, value) = entry?;
                if key.value().len() != row_prefix.len().saturating_add(16) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                if value.value() != [1_u8] {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                let document = decode_identifier(
                    &key.value()[row_prefix.len()..],
                    DocumentId::from_durable_bytes,
                )?;
                let reverse = table
                    .get(key_index_reverse(scope, document, index).as_slice())?
                    .ok_or(StoreError::CorruptInternalValue)?;
                if reverse.value() != encoded.as_slice() {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                let snapshot = table
                    .get(key_snapshot(scope, document).as_slice())?
                    .ok_or(StoreError::CorruptInternalValue)?;
                if !is_nonempty_bounded(snapshot.value().len(), MAX_SNAPSHOT_BYTES) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                let revision = table
                    .get(key_revision(scope, document).as_slice())?
                    .ok_or(StoreError::CorruptInternalValue)?;
                decode_revision(revision.value())?;
                Ok(IndexHit { document })
            })
            .collect()
    }

    /// Advances the durable upload acknowledgement without deleting immutable
    /// operation evidence or outbox records needed to verify exact retries.
    ///
    /// # Errors
    ///
    /// Refuses a mismatched or unattributable upstream and acknowledgements
    /// beyond the highest committed local sequence.
    pub fn acknowledge_outbox(
        &self,
        scope: Scope,
        peer: PeerId,
        through: u64,
    ) -> Result<(), StoreError> {
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        bind_upstream_in_table(&mut table, scope, peer)?;
        let highest_value = table.get(key_sequence(scope).as_slice())?;
        let highest = highest_value
            .map(|value| decode_u64(value.value()))
            .transpose()?
            .unwrap_or(0);
        if through > highest {
            let error = StoreError::InvalidAcknowledgement { through, highest };
            return Err(error);
        }
        let current_value = table.get(key_outbox_ack(scope).as_slice())?;
        let current = current_value
            .map(|value| decode_u64(value.value()))
            .transpose()?
            .unwrap_or(0);
        let acknowledged = current.max(through);
        table.insert(
            key_outbox_ack(scope).as_slice(),
            acknowledged.to_be_bytes().as_slice(),
        )?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Durably binds a scope's globally acknowledged outbox to one upstream.
    ///
    /// The current wire model has one upload acknowledgement cursor per scope.
    /// Refusing a second upstream prevents an acknowledgement by one service
    /// from silently suppressing delivery to another.
    ///
    /// # Errors
    ///
    /// Returns a topology error if the scope was already bound elsewhere or
    /// legacy sync state cannot be attributed to exactly one service.
    pub fn bind_upstream(&self, scope: Scope, peer: PeerId) -> Result<(), StoreError> {
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        bind_upstream_in_table(&mut table, scope, peer)?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Loads the last atomically installed opaque checkpoint for a peer.
    ///
    /// # Errors
    ///
    /// Returns a typed storage or corruption error.
    pub fn checkpoint(&self, scope: Scope, peer: PeerId) -> Result<Option<Vec<u8>>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        table
            .get(key_checkpoint(scope, peer).as_slice())?
            .map(|value| {
                if !is_nonempty_bounded(value.value().len(), MAX_CHECKPOINT_BYTES) {
                    let error = StoreError::CorruptInternalValue;
                    return Err(error);
                }
                Ok(value.value().to_vec())
            })
            .transpose()
    }

    /// Atomically installs a verified remote page and its replacement
    /// checkpoint. Changes never enter the local outbox.
    ///
    /// # Errors
    ///
    /// Fails before commit for stale snapshots, duplicate document entries,
    /// conflicting immutable bytes, or resource-bound violations.
    pub fn install_remote_page(
        &self,
        scope: Scope,
        peer: PeerId,
        documents: &[RemoteDocument<'_>],
        changes: &[RemoteChange<'_>],
        checkpoint: &[u8],
    ) -> Result<(), StoreError> {
        self.install_remote_page_inner(scope, peer, documents, changes, checkpoint, false)
    }

    /// Installs one recovered baseline page and durably invalidates the scope.
    ///
    /// Identical to [`Self::install_remote_page`] except that, in the same
    /// transaction, it advances the scope's monotonic baseline generation and
    /// publishes a [`ChangeEvent::Baseline`] invalidation. A subscription
    /// cursor taken before this install can therefore never observe a replaced
    /// scope as though nothing happened, even when the install carries no
    /// changes of its own. No local outbox operation is manufactured and no
    /// pruned change is replayed.
    ///
    /// # Errors
    ///
    /// Returns the same validation, staleness, corruption, or storage failure
    /// as [`Self::install_remote_page`].
    pub fn install_remote_baseline(
        &self,
        scope: Scope,
        peer: PeerId,
        documents: &[RemoteDocument<'_>],
        changes: &[RemoteChange<'_>],
        checkpoint: &[u8],
    ) -> Result<(), StoreError> {
        self.install_remote_page_inner(scope, peer, documents, changes, checkpoint, true)
    }

    /// Shared body of remote-page and baseline installation.
    fn install_remote_page_inner(
        &self,
        scope: Scope,
        peer: PeerId,
        documents: &[RemoteDocument<'_>],
        changes: &[RemoteChange<'_>],
        checkpoint: &[u8],
        baseline: bool,
    ) -> Result<(), StoreError> {
        validate_remote_page(documents, changes, checkpoint)?;

        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        ensure_not_migrating(&table, scope)?;
        bind_upstream_in_table(&mut table, scope, peer)?;
        for document in documents {
            let revision_key = key_revision(scope, document.document);
            let current_value = table.get(revision_key.as_slice())?;
            let current = current_value
                .map(|value| decode_revision(value.value()))
                .transpose()?;
            if current != document.expected_revision {
                let error = StoreError::StaleDocument;
                return Err(error);
            }
        }
        let mut new_changes = Vec::new();
        for change in changes {
            let key = key_change(scope, change.document, change.digest);
            let existing = table
                .get(key.as_slice())?
                .map(|value| value.value().to_vec());
            if let Some(existing) = existing {
                if existing != change.bytes {
                    let error = StoreError::ChangeDigestConflict;
                    return Err(error);
                }
            } else {
                table.insert(key.as_slice(), change.bytes)?;
                new_changes.push((change.document, change.digest));
            }
        }
        for document in documents {
            install_remote_document(&mut table, scope, document)?;
        }
        let event_sequence_value = table.get(key_event_sequence(scope).as_slice())?;
        let mut event_sequence = event_sequence_value
            .map(|value| decode_u64(value.value()))
            .transpose()?
            .unwrap_or(0);
        for (document, digest) in new_changes {
            event_sequence = event_sequence
                .checked_add(1)
                .ok_or(StoreError::SequenceExhausted)?;
            table.insert(
                key_event(scope, event_sequence).as_slice(),
                encode_event(document, digest).as_slice(),
            )?;
        }
        if baseline {
            let generation = table
                .get(key_baseline(scope).as_slice())?
                .map(|value| decode_u64(value.value()))
                .transpose()?
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(StoreError::SequenceExhausted)?;
            event_sequence = event_sequence
                .checked_add(1)
                .ok_or(StoreError::SequenceExhausted)?;
            table.insert(
                key_baseline_event(scope, event_sequence).as_slice(),
                generation.to_be_bytes().as_slice(),
            )?;
            table.insert(
                key_baseline(scope).as_slice(),
                generation.to_be_bytes().as_slice(),
            )?;
        }
        table.insert(
            key_event_sequence(scope).as_slice(),
            event_sequence.to_be_bytes().as_slice(),
        )?;
        table.insert(key_checkpoint(scope, peer).as_slice(), checkpoint)?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Starts a versioned restore or resumes the matching interrupted one.
    ///
    /// The payload identity binds the durable cursor to one exact export:
    /// resuming with different bytes fails closed instead of interleaving two
    /// histories. The cursor survives process death; a crash between a commit
    /// and its cursor advance replays one exact-retry duplicate on retry.
    ///
    /// # Errors
    ///
    /// Refuses an active migration, a different in-progress payload, or a
    /// storage failure.
    pub fn restore_begin(
        &self,
        scope: Scope,
        payload: &[u8; 32],
    ) -> Result<RestoreCursor, StoreError> {
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let cursor = {
            let mut table = transaction.open_table(DATA)?;
            ensure_not_migrating(&table, scope)?;
            let existing = table
                .get(key_restore(scope).as_slice())?
                .map(|value| value.value().to_vec());
            match existing {
                None => {
                    let cursor = RestoreCursor {
                        payload: *payload,
                        next: 0,
                    };
                    table.insert(
                        key_restore(scope).as_slice(),
                        encode_restore_cursor(cursor).as_slice(),
                    )?;
                    cursor
                }
                Some(raw) => {
                    let existing = decode_restore_cursor(&raw)?;
                    if existing.payload != *payload {
                        let error = StoreError::RestoreInProgress;
                        return Err(error);
                    }
                    existing
                }
            }
        };
        transaction.commit()?;
        Ok(cursor)
    }

    /// Loads the durable restore cursor for a scope, when one is active.
    ///
    /// # Errors
    ///
    /// Returns a typed read or corruption error.
    pub fn restore_cursor(&self, scope: Scope) -> Result<Option<RestoreCursor>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        table
            .get(key_restore(scope).as_slice())?
            .map(|value| decode_restore_cursor(value.value()))
            .transpose()
    }

    /// Advances the durable restore cursor past one replayed history entry.
    ///
    /// Each advance is its own immediate transaction so a crash replays at
    /// most one entry, which the commit path short-circuits as an exact
    /// duplicate.
    ///
    /// # Errors
    ///
    /// Refuses a missing cursor, a foreign payload identity, or a storage
    /// failure.
    pub fn restore_advance(
        &self,
        scope: Scope,
        payload: &[u8; 32],
        next: u64,
    ) -> Result<(), StoreError> {
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let existing = {
            let existing_value = table.get(key_restore(scope).as_slice())?;
            let existing_value = existing_value.ok_or(StoreError::RestoreNotInProgress)?;
            decode_restore_cursor(existing_value.value())?
        };
        if existing.payload != *payload {
            let error = StoreError::RestoreInProgress;
            return Err(error);
        }
        table.insert(
            key_restore(scope).as_slice(),
            encode_restore_cursor(RestoreCursor {
                payload: *payload,
                next,
            })
            .as_slice(),
        )?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }

    /// Verifies every replayed document revision and clears the cursor.
    ///
    /// Verification and removal share one immediate transaction: the cursor
    /// only clears when durable state proves the full replay landed.
    ///
    /// # Errors
    ///
    /// Refuses a missing or foreign cursor, or any revision that diverges
    /// from the replayed expectation.
    pub fn restore_finish(
        &self,
        scope: Scope,
        payload: &[u8; 32],
        expected: &[(DocumentId, DocumentRevision)],
    ) -> Result<(), StoreError> {
        let mut transaction = self.database.begin_write()?;
        transaction.set_durability(Durability::Immediate)?;
        let mut table = transaction.open_table(DATA)?;
        let existing = {
            let existing_value = table.get(key_restore(scope).as_slice())?;
            let existing_value = existing_value.ok_or(StoreError::RestoreNotInProgress)?;
            decode_restore_cursor(existing_value.value())?
        };
        if existing.payload != *payload {
            let error = StoreError::RestoreInProgress;
            return Err(error);
        }
        for &(document, revision) in expected {
            let current_value = table.get(key_revision(scope, document).as_slice())?;
            let current = current_value
                .map(|value| decode_revision(value.value()))
                .transpose()?;
            if current != Some(revision) {
                let error = StoreError::RestoreIncomplete;
                return Err(error);
            }
        }
        table.remove(key_restore(scope).as_slice())?;
        drop(table);
        transaction.commit()?;
        Ok(())
    }
}

/// Stable embedded storage failures.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum StoreError {
    /// Opening or creating the database failed.
    #[error("database open failed: {0}")]
    Database(#[from] DatabaseError),
    /// Starting a read or write transaction failed.
    #[error("database transaction failed: {0}")]
    Transaction(#[from] TransactionError),
    /// Opening or using the v3 table failed.
    #[error("database table failed: {0}")]
    Table(#[from] TableError),
    /// Reading or writing a table value failed.
    #[error("database storage failed: {0}")]
    Storage(#[from] StorageError),
    /// Commit returned without proving whether the durable transition landed.
    /// Callers must retry the exact operation identity and request digest.
    #[error("database commit outcome is unknown; retry the exact request: {0}")]
    CommitOutcomeUnknown(#[from] CommitError),
    /// Selecting the required immediate durability mode failed.
    #[error("database durability configuration failed: {0}")]
    Durability(#[from] SetDurabilityError),
    /// Immutable change bytes violated the local resource bound.
    #[error("change byte length {0} is outside local bounds")]
    InvalidChangeSize(usize),
    /// A local batch had no transitions to commit.
    #[error("local commit batch is empty")]
    EmptyLocalBatch,
    /// A local batch attempted to update one document more than once.
    #[error("local commit batch contains a duplicate document")]
    DuplicateBatchDocument,
    /// A local batch attempted to reuse one operation identity.
    #[error("local commit batch contains a duplicate operation")]
    DuplicateBatchOperation,
    /// A local batch mixed ownership scopes.
    #[error("local commit batch contains multiple scopes")]
    MixedBatchScope,
    /// Complete snapshot bytes violated the local resource bound.
    #[error("snapshot byte length {0} is outside local bounds")]
    InvalidSnapshotSize(usize),
    /// The bounded incremental suffix needs a new full snapshot checkpoint.
    #[error("document checkpoint is required before another incremental change")]
    SnapshotCheckpointRequired,
    /// An operation identity was reused for a different change.
    #[error("operation identity was already committed with different bytes")]
    OperationConflict,
    /// The document changed after its input snapshot was read.
    #[error("document changed before the successor snapshot committed")]
    StaleDocument,
    /// A non-tombstone successor was attempted on a deleted document.
    ///
    /// Tombstones are monotonic: only another tombstone commit may follow.
    /// Callers holding fresh writes for a deleted document must reconcile
    /// against its conflict candidates instead of overwriting absence.
    #[error("document is deleted and only accepts further tombstones")]
    DocumentDeleted,
    /// A scope consumed every possible monotonic sequence.
    #[error("scope sequence is exhausted")]
    SequenceExhausted,
    /// A materialized document revision exhausted its numeric space.
    #[error("document revision is exhausted")]
    RevisionExhausted,
    /// A caller requested an empty or excessive outbox page.
    #[error("outbox limit {0} is outside 1..=256")]
    InvalidOutboxLimit(usize),
    /// A caller requested an empty or excessive secondary-index page.
    #[error("index query limit {0} is outside 1..=256")]
    InvalidIndexLimit(usize),
    /// A caller requested an empty or excessive change-feed page.
    #[error("change-feed limit {0} is outside 1..=256")]
    InvalidEventLimit(usize),
    /// A projection set was duplicate, excessive, or contained an oversized value.
    #[error("materialized secondary-index entries violate storage bounds")]
    InvalidIndexes,
    /// A service acknowledged changes that were never committed locally.
    #[error("outbox acknowledgement {through} exceeds committed sequence {highest}")]
    InvalidAcknowledgement {
        /// Requested inclusive acknowledgement.
        through: u64,
        /// Highest local sequence that exists.
        highest: u64,
    },
    /// One globally acknowledged outbox cannot safely target two services.
    #[error("scope is bound to upstream {configured}, not requested upstream {requested}")]
    UpstreamMismatch {
        /// Durable synchronization service identity.
        configured: PeerId,
        /// Conflicting service identity supplied by the caller.
        requested: PeerId,
    },
    /// Legacy durable checkpoints identify more than one possible upstream.
    #[error("scope has legacy checkpoints for multiple upstreams; explicit migration is required")]
    AmbiguousLegacyUpstream,
    /// A legacy global acknowledgement cannot be attributed to an upstream.
    #[error("scope has an acknowledged legacy outbox without an attributable upstream")]
    UnattributedLegacyAcknowledgement,
    /// An opaque checkpoint violated its durable resource bound.
    #[error("checkpoint byte length {0} is outside local bounds")]
    InvalidCheckpointSize(usize),
    /// A remote page violated count, aggregate byte, or document coverage rules.
    #[error("remote page violates local atomic-install bounds")]
    InvalidRemotePage,
    /// Equal change identities were presented with unequal immutable bytes.
    #[error("change digest was reused for different immutable bytes")]
    ChangeDigestConflict,
    /// Durable internal bytes did not match the v3 format.
    #[error("durable v3 internal bytes are malformed")]
    CorruptInternalValue,
    /// An existing file was opened under a different replica installation.
    #[error("database replica mismatch: expected {expected}, received {actual}")]
    ReplicaMismatch {
        /// Replica identity already bound to the file.
        expected: ReplicaId,
        /// Replica identity supplied by this open request.
        actual: ReplicaId,
    },
    /// Durable data existed without the required replica metadata.
    #[error("database contains v3 data without a durable replica binding")]
    MissingReplicaBinding,
    /// The file contains tables from another or unsupported schema version.
    #[error("database file contains an unsupported schema and requires migration")]
    UnsupportedSchema,
    /// Schema version zero is reserved and cannot identify application state.
    #[error("application schema version must be nonzero")]
    InvalidSchemaVersion,
    /// Durable state belongs to another projection/schema contract.
    #[error("application schema differs from durable state and requires migration")]
    SchemaMigrationRequired,
    /// A durable schema migration excludes writes and index-dependent reads.
    #[error("application schema migration is in progress")]
    MigrationInProgress,
    /// A caller tried to resume a migration using a different target schema.
    #[error("active schema migration belongs to a different target")]
    MigrationTargetMismatch,
    /// A migration page was requested or committed without a durable marker.
    #[error("no schema migration is in progress")]
    MigrationNotInProgress,
    /// A migration reader requested an empty or excessive page.
    #[error("migration page limit {0} is outside 1..=256")]
    InvalidMigrationLimit(usize),
    /// A migration batch was empty, excessive, unordered, or skipped a cursor.
    #[error("schema migration page violates ordering or resource bounds")]
    InvalidMigrationPage,
    /// Another worker atomically advanced the migration cursor first.
    #[error("schema migration page cursor is stale")]
    StaleMigrationPage,
    /// Target schema publication was attempted before every snapshot was rebuilt.
    #[error("schema migration still has unprocessed documents")]
    MigrationIncomplete,
    /// An export scan page was requested with an empty or excessive limit.
    #[error("export scan limit {0} is outside 1..=256")]
    InvalidExportLimit(usize),
    /// A logical export payload or one of its fields violates size bounds.
    #[error("logical export byte length {0} is outside local bounds")]
    InvalidExportSize(usize),
    /// A logical export payload declares an unsupported version.
    #[error("logical export version {0} is not supported")]
    UnknownExportVersion(u16),
    /// A logical export payload is structurally malformed.
    #[error("logical export payload is malformed")]
    InvalidExport,
    /// A second restore payload was attempted while another restore holds the scope cursor.
    #[error("a versioned restore is already in progress for this scope")]
    RestoreInProgress,
    /// A restore cursor was advanced or finalized without an active restore.
    #[error("no versioned restore is in progress for this scope")]
    RestoreNotInProgress,
    /// A restore cannot finalize because durable revisions diverge from its replay.
    #[error("versioned restore cannot finalize before its replayed revisions match")]
    RestoreIncomplete,
}

/// Writes one local commit-batch member and advances both sequence cursors.
fn commit_local_member(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    commit: &LocalCommit<'_>,
    current_revision: Option<DocumentRevision>,
    sequence: &mut u64,
    event_sequence: &mut u64,
) -> Result<(), StoreError> {
    let next_revision = match current_revision {
        Some(revision) => revision.next().ok_or(StoreError::RevisionExhausted)?,
        None => DocumentRevision::new(1),
    };
    *sequence = sequence
        .checked_add(1)
        .ok_or(StoreError::SequenceExhausted)?;
    *event_sequence = event_sequence
        .checked_add(1)
        .ok_or(StoreError::SequenceExhausted)?;
    let operation_key = key_operation(scope, commit.change.operation);
    let operation_value = encode_operation(
        commit.change.document,
        commit.change.request,
        commit.change.digest,
        *sequence,
    );
    let outbox_value = encode_outbox(commit.change);
    if commit.snapshot.is_empty() {
        let Some(current_revision) = current_revision else {
            return Err(StoreError::SnapshotCheckpointRequired);
        };
        let snapshot_key = key_snapshot(scope, commit.change.document);
        if table.get(snapshot_key.as_slice())?.is_none() {
            return Err(StoreError::SnapshotCheckpointRequired);
        }
        let delta_count = snapshot_delta_rows(table, scope, commit.change.document)?.len();
        if delta_count >= MAX_SNAPSHOT_DELTAS {
            return Err(StoreError::SnapshotCheckpointRequired);
        }
        let checkpoint = table
            .get(snapshot_key.as_slice())?
            .ok_or(StoreError::SnapshotCheckpointRequired)?;
        let checkpoint_size = checkpoint.value().len();
        drop(checkpoint);
        let suffix_key = key_snapshot_suffix_bytes(scope, commit.change.document);
        let suffix_bytes = table
            .get(suffix_key.as_slice())?
            .map(|value| decode_u64(value.value()))
            .transpose()?
            .unwrap_or(0);
        if suffix_bytes == 0 && delta_count > 0 {
            return Err(StoreError::CorruptInternalValue);
        }
        let next_suffix_bytes = usize::try_from(suffix_bytes)
            .ok()
            .and_then(|current| current.checked_add(commit.change.bytes.len()))
            .ok_or(StoreError::SnapshotCheckpointRequired)?;
        if checkpoint_size
            .checked_add(next_suffix_bytes)
            .is_none_or(|total| total > MAX_SNAPSHOT_BYTES)
        {
            return Err(StoreError::SnapshotCheckpointRequired);
        }
        let next_suffix_bytes_u64 =
            u64::try_from(next_suffix_bytes).map_err(|_| StoreError::SnapshotCheckpointRequired)?;
        table.insert(
            suffix_key.as_slice(),
            next_suffix_bytes_u64.to_be_bytes().as_slice(),
        )?;
        let checkpoint_key = key_snapshot_checkpoint(scope, commit.change.document);
        if table.get(checkpoint_key.as_slice())?.is_none() {
            table.insert(
                checkpoint_key.as_slice(),
                current_revision.get().to_be_bytes().as_slice(),
            )?;
        }
        table.insert(
            key_snapshot_delta(scope, commit.change.document, next_revision).as_slice(),
            commit.change.digest.into_bytes().as_slice(),
        )?;
    } else {
        table.insert(
            key_snapshot(scope, commit.change.document).as_slice(),
            commit.snapshot,
        )?;
        table.insert(
            key_snapshot_checkpoint(scope, commit.change.document).as_slice(),
            next_revision.get().to_be_bytes().as_slice(),
        )?;
        table.insert(
            key_snapshot_suffix_bytes(scope, commit.change.document).as_slice(),
            0_u64.to_be_bytes().as_slice(),
        )?;
        clear_snapshot_deltas(table, scope, commit.change.document)?;
    }
    table.insert(
        key_change(scope, commit.change.document, commit.change.digest).as_slice(),
        commit.change.bytes,
    )?;
    table.insert(operation_key.as_slice(), operation_value.as_slice())?;
    table.insert(
        key_outbox(scope, *sequence).as_slice(),
        outbox_value.as_slice(),
    )?;
    table.insert(
        key_revision(scope, commit.change.document).as_slice(),
        next_revision.get().to_be_bytes().as_slice(),
    )?;
    if commit.tombstone {
        write_tombstone(table, scope, commit.change.document)?;
    }
    replace_indexes(table, scope, commit.change.document, commit.indexes)?;
    table.insert(
        key_event(scope, *event_sequence).as_slice(),
        encode_event(commit.change.document, commit.change.digest).as_slice(),
    )?;
    Ok(())
}

/// Returns the sequence of an exact operation retry, or fails closed on conflict.
fn existing_local_sequence(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    commit: &LocalCommit<'_>,
    operation_key: &[u8],
) -> Result<Option<u64>, StoreError> {
    let scope = commit.change.scope;
    let stored_operation = table.get(operation_key)?;
    let Some(existing) = stored_operation else {
        return Ok(None);
    };
    let existing = decode_operation(existing.value())?;
    if existing.document == commit.change.document
        && existing.request == commit.change.request
        && existing.digest == commit.change.digest
    {
        let change_key = key_change(scope, existing.document, existing.digest);
        let existing_bytes = table
            .get(change_key.as_slice())?
            .ok_or(StoreError::CorruptInternalValue)?;
        if existing_bytes.value() == commit.change.bytes {
            return Ok(Some(existing.sequence));
        }
    }
    let error = StoreError::OperationConflict;
    Err(error)
}

/// Loads the stored revision for one document, failing closed when absent.
fn stored_revision(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    document: DocumentId,
) -> Result<DocumentRevision, StoreError> {
    let value = table.get(key_revision(scope, document).as_slice())?;
    let value = value.ok_or(StoreError::CorruptInternalValue)?;
    decode_revision(value.value())
}

/// Reports whether a scope owns any durable row.
fn scope_has_records(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
) -> Result<bool, StoreError> {
    let scope_prefix = scope_prefix(scope);
    let scope_end = next_prefix(&scope_prefix).ok_or(StoreError::CorruptInternalValue)?;
    let mut scope_entries = table.range(scope_prefix.as_slice()..scope_end.as_slice())?;
    let has_scope_entries = scope_entries.next().transpose()?.is_some();
    Ok(has_scope_entries)
}

/// Builds one export document from a snapshot row and its tombstone flag.
fn export_document(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    key: &[u8],
    prefix_length: usize,
    snapshot: &[u8],
) -> Result<ExportScanDocument, StoreError> {
    let document = decode_identifier(&key[prefix_length..], DocumentId::from_durable_bytes)?;
    let tombstone = table
        .get(key_tombstone(scope, document).as_slice())?
        .is_some_and(|value| value.value() == [1_u8]);
    let revision = stored_revision(table, scope, document)?;
    let checkpoint_revision = snapshot_checkpoint_revision(table, scope, document, revision)?;
    let deltas = read_snapshot_deltas(table, scope, document, checkpoint_revision, revision.get())?;
    Ok(ExportScanDocument {
        document,
        tombstone,
        snapshot: snapshot.to_vec(),
        deltas,
    })
}

/// Materializes one remotely merged document inside a write transaction.
fn install_remote_document(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    document: &RemoteDocument<'_>,
) -> Result<(), StoreError> {
    let next = match document.expected_revision {
        Some(revision) => revision.next().ok_or(StoreError::RevisionExhausted)?,
        None => DocumentRevision::new(1),
    };
    table.insert(
        key_snapshot(scope, document.document).as_slice(),
        document.snapshot,
    )?;
    table.insert(
        key_snapshot_checkpoint(scope, document.document).as_slice(),
        next.get().to_be_bytes().as_slice(),
    )?;
    table.insert(
        key_snapshot_suffix_bytes(scope, document.document).as_slice(),
        0_u64.to_be_bytes().as_slice(),
    )?;
    clear_snapshot_deltas(table, scope, document.document)?;
    table.insert(
        key_revision(scope, document.document).as_slice(),
        next.get().to_be_bytes().as_slice(),
    )?;
    if document.tombstone {
        write_tombstone(table, scope, document.document)?;
    }
    replace_indexes(table, scope, document.document, document.indexes)?;
    Ok(())
}

/// Checks that a stored operation receipt still matches its change and outbox rows.
fn verify_operation_links(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    operation: OperationId,
    committed: CommittedOperation,
) -> Result<(), StoreError> {
    if table
        .get(key_change(scope, committed.document, committed.digest).as_slice())?
        .is_none()
    {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let outbox = table
        .get(key_outbox(scope, committed.sequence).as_slice())?
        .ok_or(StoreError::CorruptInternalValue)?;
    let outbox = decode_outbox(committed.sequence, outbox.value())?;
    if outbox.document != committed.document
        || outbox.operation != operation
        || outbox.digest != committed.digest
    {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    Ok(())
}

/// Normalizes any durable-value decode failure into [`StoreError::CorruptInternalValue`].
///
/// The concrete decoder error is intentionally discarded: the stable store
/// error is the only information callers may act on, and it fails closed.
fn malformed_store<E: std::fmt::Debug>(cause: E) -> StoreError {
    drop(cause);
    StoreError::CorruptInternalValue
}

/// Refuses index-dependent access while a schema migration marker exists.
fn ensure_not_migrating(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
) -> Result<(), StoreError> {
    if table.get(key_migration(scope).as_slice())?.is_some() {
        Err(StoreError::MigrationInProgress)
    } else {
        Ok(())
    }
}

/// Binds or validates the scope's single durable upstream inside a write transaction.
fn bind_upstream_in_table(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    requested: PeerId,
) -> Result<(), StoreError> {
    let upstream_key = key_upstream(scope);
    if let Some(value) = table.get(upstream_key.as_slice())? {
        let configured = decode_identifier(value.value(), PeerId::from_durable_bytes)?;
        return if configured == requested {
            Ok(())
        } else {
            Err(StoreError::UpstreamMismatch {
                configured,
                requested,
            })
        };
    }

    let checkpoint_prefix = prefix(scope, KIND_CHECKPOINT);
    let checkpoint_end = next_prefix(&checkpoint_prefix).ok_or(StoreError::CorruptInternalValue)?;
    let legacy_peers = table
        .range(checkpoint_prefix.as_slice()..checkpoint_end.as_slice())?
        .take(2)
        .map(|entry| {
            let (key, value) = entry?;
            if key.value().len() != checkpoint_prefix.len().saturating_add(16)
                || !is_nonempty_bounded(value.value().len(), MAX_CHECKPOINT_BYTES)
            {
                let error = StoreError::CorruptInternalValue;
                return Err(error);
            }
            decode_identifier(
                &key.value()[checkpoint_prefix.len()..],
                PeerId::from_durable_bytes,
            )
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    let configured = match *legacy_peers.as_slice() {
        [] => {
            let acknowledged = table
                .get(key_outbox_ack(scope).as_slice())?
                .map(|value| decode_u64(value.value()))
                .transpose()?
                .unwrap_or(0);
            if acknowledged != 0 {
                let error = StoreError::UnattributedLegacyAcknowledgement;
                return Err(error);
            }
            requested
        }
        [peer] => peer,
        [_, _, ..] => {
            let error = StoreError::AmbiguousLegacyUpstream;
            return Err(error);
        }
    };
    if configured != requested {
        let error = StoreError::UpstreamMismatch {
            configured,
            requested,
        };
        return Err(error);
    }
    table.insert(
        upstream_key.as_slice(),
        configured.as_uuid().as_bytes().as_slice(),
    )?;
    Ok(())
}

/// Encodes one schema version and projection digest into its durable value.
fn schema_value(version: u32, digest: [u8; 32]) -> [u8; SCHEMA_VALUE_BYTES] {
    let mut value = [0; SCHEMA_VALUE_BYTES];
    value[..4].copy_from_slice(&version.to_be_bytes());
    value[4..].copy_from_slice(&digest);
    value
}

/// Encodes one migration marker from its target and optional cursor.
fn encode_migration(
    target: [u8; SCHEMA_VALUE_BYTES],
    after: Option<DocumentId>,
) -> [u8; MIGRATION_VALUE_BYTES] {
    let mut value = [0; MIGRATION_VALUE_BYTES];
    value[1..=SCHEMA_VALUE_BYTES].copy_from_slice(&target);
    if let Some(document) = after {
        value[0] = 1;
        value[1 + SCHEMA_VALUE_BYTES..].copy_from_slice(document.as_uuid().as_bytes());
    }
    value
}

/// Decodes and validates one migration marker.
fn decode_migration(
    bytes: &[u8],
) -> Result<([u8; SCHEMA_VALUE_BYTES], Option<DocumentId>), StoreError> {
    if bytes.len() != MIGRATION_VALUE_BYTES || bytes[0] > 1 {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let target = bytes[1..=SCHEMA_VALUE_BYTES]
        .try_into()
        .map_err(malformed_store)?;
    let cursor = match bytes[0] {
        0 if bytes[1 + SCHEMA_VALUE_BYTES..]
            .iter()
            .all(|byte| *byte == 0) =>
        {
            None
        }
        1 => Some(decode_identifier(
            &bytes[1 + SCHEMA_VALUE_BYTES..],
            DocumentId::from_durable_bytes,
        )?),
        _ => {
            let error = StoreError::CorruptInternalValue;
            return Err(error);
        }
    };
    Ok((target, cursor))
}

/// Builds the shared format-and-scope prefix for every storage key.
fn scope_prefix(scope: Scope) -> Vec<u8> {
    let mut key = Vec::with_capacity(SCOPE_PREFIX_BYTES);
    key.extend_from_slice(FORMAT);
    key.extend_from_slice(scope.account().as_uuid().as_bytes());
    key.extend_from_slice(scope.space().as_uuid().as_bytes());
    key
}

/// Builds one kind-scoped storage key prefix.
fn prefix(scope: Scope, kind: u8) -> Vec<u8> {
    let mut key = scope_prefix(scope);
    key.push(kind);
    key
}

/// Builds the snapshot row key for one document.
fn key_snapshot(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_SNAPSHOT);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the marker key for the revision materialized by a full checkpoint.
fn key_snapshot_checkpoint(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_SNAPSHOT_CHECKPOINT);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the bounded suffix-byte accounting key for one document.
fn key_snapshot_suffix_bytes(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_SNAPSHOT_SUFFIX_BYTES);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the ordered suffix prefix for one document's post-checkpoint changes.
fn key_snapshot_delta_prefix(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_SNAPSHOT_DELTA);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the suffix-index row key for one document revision.
fn key_snapshot_delta(scope: Scope, document: DocumentId, revision: DocumentRevision) -> Vec<u8> {
    let mut key = key_snapshot_delta_prefix(scope, document);
    key.extend_from_slice(&revision.get().to_be_bytes());
    key
}

/// One bounded suffix row's guarded key and value.
type SnapshotDeltaRow<'a> = (
    redb::AccessGuard<'a, &'static [u8]>,
    redb::AccessGuard<'a, &'static [u8]>,
);

/// Reads one document's bounded suffix rows, refusing a suffix longer than
/// [`MAX_SNAPSHOT_DELTAS`] so no caller can silently read or clear past the cap.
fn snapshot_delta_rows<'a, T>(
    table: &'a T,
    scope: Scope,
    document: DocumentId,
) -> Result<Vec<SnapshotDeltaRow<'a>>, StoreError>
where
    T: ReadableTable<&'static [u8], &'static [u8]>,
{
    let row_prefix = key_snapshot_delta_prefix(scope, document);
    let end = next_prefix(&row_prefix).ok_or(StoreError::CorruptInternalValue)?;
    let mut rows = Vec::new();
    for entry in table
        .range(row_prefix.as_slice()..end.as_slice())?
        .take(MAX_SNAPSHOT_DELTAS.saturating_add(1))
    {
        if rows.len() >= MAX_SNAPSHOT_DELTAS {
            return Err(StoreError::CorruptInternalValue);
        }
        rows.push(entry?);
    }
    Ok(rows)
}

/// Reads and validates the bounded immutable-change suffix after one checkpoint.
fn read_snapshot_deltas(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    document: DocumentId,
    checkpoint_revision: u64,
    latest_revision: u64,
) -> Result<Vec<(ChangeDigest, Vec<u8>)>, StoreError> {
    let row_prefix = key_snapshot_delta_prefix(scope, document);
    let mut expected_revision = checkpoint_revision;
    let mut deltas = Vec::new();
    for (key, value) in snapshot_delta_rows(table, scope, document)? {
        if key.value().len() != row_prefix.len().saturating_add(8) || value.value().len() != 32 {
            return Err(StoreError::CorruptInternalValue);
        }
        expected_revision = expected_revision
            .checked_add(1)
            .ok_or(StoreError::CorruptInternalValue)?;
        if decode_u64(&key.value()[row_prefix.len()..])? != expected_revision
            || expected_revision > latest_revision
        {
            return Err(StoreError::CorruptInternalValue);
        }
        let digest_bytes: [u8; 32] = value
            .value()
            .try_into()
            .map_err(|_| StoreError::CorruptInternalValue)?;
        let digest = ChangeDigest::new(digest_bytes);
        let change = table
            .get(key_change(scope, document, digest).as_slice())?
            .ok_or(StoreError::CorruptInternalValue)?;
        if !is_nonempty_bounded(change.value().len(), MAX_CHANGE_BYTES) {
            return Err(StoreError::CorruptInternalValue);
        }
        deltas.push((digest, change.value().to_vec()));
    }
    if expected_revision != latest_revision {
        return Err(StoreError::CorruptInternalValue);
    }
    Ok(deltas)
}

/// Returns the checkpoint revision, treating pre-suffix snapshots as current.
fn snapshot_checkpoint_revision(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    document: DocumentId,
    latest_revision: DocumentRevision,
) -> Result<u64, StoreError> {
    let checkpoint_revision = table
        .get(key_snapshot_checkpoint(scope, document).as_slice())?
        .map(|value| decode_u64(value.value()))
        .transpose()?
        .unwrap_or_else(|| latest_revision.get());
    if checkpoint_revision > latest_revision.get() {
        return Err(StoreError::CorruptInternalValue);
    }
    Ok(checkpoint_revision)
}

/// Removes one document's bounded incremental suffix during checkpointing.
fn clear_snapshot_deltas(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    document: DocumentId,
) -> Result<(), StoreError> {
    let keys = snapshot_delta_rows(table, scope, document)?
        .into_iter()
        .map(|(key, _)| key.value().to_vec())
        .collect::<Vec<_>>();
    for key in keys {
        table.remove(key.as_slice())?;
    }
    Ok(())
}

/// Builds the immutable-change row key for one document and digest.
fn key_change(scope: Scope, document: DocumentId, digest: ChangeDigest) -> Vec<u8> {
    let mut key = prefix(scope, KIND_CHANGE);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key.extend_from_slice(&digest.into_bytes());
    key
}

/// Builds the operation-receipt row key for one operation.
fn key_operation(scope: Scope, operation: OperationId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_OPERATION);
    key.extend_from_slice(operation.as_uuid().as_bytes());
    key
}

/// Builds the outbox row key for one sequence.
fn key_outbox(scope: Scope, sequence: u64) -> Vec<u8> {
    let mut key = prefix(scope, KIND_OUTBOX);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// Builds the highest-sequence row key for one scope.
fn key_sequence(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_SEQUENCE)
}

/// Builds the revision row key for one document.
fn key_revision(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_REVISION);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the checkpoint row key for one peer.
fn key_checkpoint(scope: Scope, peer: PeerId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_CHECKPOINT);
    key.extend_from_slice(peer.as_uuid().as_bytes());
    key
}

/// Builds the global outbox acknowledgement row key for one scope.
fn key_outbox_ack(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_OUTBOX_ACK)
}

/// Builds the upstream-binding row key for one scope.
fn key_upstream(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_UPSTREAM)
}

/// Builds the bound-schema row key for one scope.
fn key_schema(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_SCHEMA)
}

/// Builds the highest-event-sequence row key for one scope.
fn key_event_sequence(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_EVENT_SEQUENCE)
}

/// Builds the materialized-event row key for one sequence.
fn key_event(scope: Scope, sequence: u64) -> Vec<u8> {
    let mut key = prefix(scope, KIND_EVENT);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// Builds the baseline-generation row key for one scope.
fn key_baseline(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_BASELINE)
}

/// Builds the baseline-event row key for one sequence.
fn key_baseline_event(scope: Scope, sequence: u64) -> Vec<u8> {
    let mut key = prefix(scope, KIND_BASELINE_EVENT);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// Builds the migration-marker row key for one scope.
fn key_migration(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_MIGRATION)
}

/// Builds the tombstone-flag row key for one document.
fn key_tombstone(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_TOMBSTONE);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the restore-cursor row key for one scope.
fn key_restore(scope: Scope) -> Vec<u8> {
    prefix(scope, KIND_RESTORE)
}

/// Builds the forward index-row prefix for one encoded value.
fn index_row_prefix(scope: Scope, index: IndexId, encoded_value: &[u8]) -> Vec<u8> {
    let mut key = prefix(scope, KIND_INDEX);
    key.extend_from_slice(index.as_uuid().as_bytes());
    key.extend_from_slice(encoded_value);
    key
}

/// Builds the forward index-row key for one document.
fn key_index_row(
    scope: Scope,
    index: IndexId,
    encoded_value: &[u8],
    document: DocumentId,
) -> Vec<u8> {
    let mut key = index_row_prefix(scope, index, encoded_value);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the reverse index-row prefix for one document.
fn index_reverse_prefix(scope: Scope, document: DocumentId) -> Vec<u8> {
    let mut key = prefix(scope, KIND_INDEX_REVERSE);
    key.extend_from_slice(document.as_uuid().as_bytes());
    key
}

/// Builds the reverse index-row key for one index.
fn key_index_reverse(scope: Scope, document: DocumentId, index: IndexId) -> Vec<u8> {
    let mut key = index_reverse_prefix(scope, document);
    key.extend_from_slice(index.as_uuid().as_bytes());
    key
}

/// Refuses a non-tombstone successor for an already tombstoned document.
fn check_tombstone_transition(
    table: &Table<'_, &[u8], &[u8]>,
    scope: Scope,
    document: DocumentId,
    tombstone: bool,
) -> Result<(), StoreError> {
    if !tombstone
        && table
            .get(key_tombstone(scope, document).as_slice())?
            .is_some()
    {
        let error = StoreError::DocumentDeleted;
        return Err(error);
    }
    Ok(())
}

/// Durably records the monotonic tombstone flag for one document.
fn write_tombstone(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    document: DocumentId,
) -> Result<(), StoreError> {
    table.insert(key_tombstone(scope, document).as_slice(), [1_u8].as_slice())?;
    Ok(())
}

/// Rejects duplicate, excessive, or unencodable index projections.
fn validate_indexes(indexes: &[IndexEntry]) -> Result<(), StoreError> {
    if !is_at_most(indexes.len(), MAX_INDEXES_PER_DOCUMENT) {
        let error = StoreError::InvalidIndexes;
        return Err(error);
    }
    let mut identities = BTreeSet::new();
    for entry in indexes {
        if !identities.insert(entry.index) || encode_index_value(&entry.value).is_err() {
            let error = StoreError::InvalidIndexes;
            return Err(error);
        }
    }
    Ok(())
}

/// Replaces all forward and reverse index rows for one document.
fn replace_indexes(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    document: DocumentId,
    indexes: &[IndexEntry],
) -> Result<(), StoreError> {
    let reverse_prefix = index_reverse_prefix(scope, document);
    let reverse_end = next_prefix(&reverse_prefix).ok_or(StoreError::CorruptInternalValue)?;
    let previous = table
        .range(reverse_prefix.as_slice()..reverse_end.as_slice())?
        .map(|entry| {
            let (key, value) = entry?;
            Ok((key.value().to_vec(), value.value().to_vec()))
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    for (reverse_key, encoded_value) in previous {
        if reverse_key.len() != reverse_prefix.len().saturating_add(16) {
            let error = StoreError::CorruptInternalValue;
            return Err(error);
        }
        let index = decode_identifier(
            &reverse_key[reverse_prefix.len()..],
            IndexId::from_durable_bytes,
        )?;
        table.remove(key_index_row(scope, index, &encoded_value, document).as_slice())?;
        table.remove(reverse_key.as_slice())?;
    }
    for entry in indexes {
        let encoded = encode_index_value(&entry.value)?;
        table.insert(
            key_index_row(scope, entry.index, &encoded, document).as_slice(),
            [1_u8].as_slice(),
        )?;
        table.insert(
            key_index_reverse(scope, document, entry.index).as_slice(),
            encoded.as_slice(),
        )?;
    }
    Ok(())
}

/// Encodes one order-preserving index value, or rejects it.
fn encode_index_value(value: &IndexValue) -> Result<Vec<u8>, StoreError> {
    let mut encoded = Vec::new();
    match *value {
        IndexValue::Boolean(value) => encoded.extend_from_slice(&[1, u8::from(value)]),
        IndexValue::Integer(value) => {
            encoded.push(2);
            encoded.extend_from_slice(&(value.cast_unsigned() ^ (1_u64 << 63)).to_be_bytes());
        }
        IndexValue::Unsigned(value) => {
            encoded.push(3);
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        IndexValue::Text(ref value) => {
            encoded.push(4);
            encode_escaped(value.as_bytes(), &mut encoded);
        }
        IndexValue::Bytes(ref value) => {
            encoded.push(5);
            encode_escaped(value, &mut encoded);
        }
        IndexValue::Timestamp(value) => {
            encoded.push(6);
            encoded.extend_from_slice(&(value.cast_unsigned() ^ (1_u64 << 63)).to_be_bytes());
        }
        // `IndexValue` is `#[non_exhaustive]`, so a future projected form must
        // be refused as an unsupported encoding rather than silently skipped.
        _ => return Err(StoreError::InvalidIndexes),
    }
    if !is_at_most(encoded.len(), MAX_INDEX_VALUE_BYTES) {
        return Err(StoreError::InvalidIndexes);
    }
    Ok(encoded)
}

/// Reports whether a length is nonzero and at most a maximum.
fn is_nonempty_bounded(length: usize, maximum: usize) -> bool {
    (1..=maximum).contains(&length)
}

/// Reports whether a length is at most a maximum.
fn is_at_most(length: usize, maximum: usize) -> bool {
    length <= maximum
}

/// Adds one snapshot size to the page budget, or signals overflow.
fn add_materialized_bytes(current: usize, additional: usize) -> Option<usize> {
    current
        .checked_add(additional)
        .filter(|total| *total <= MAX_MATERIALIZED_PAGE_BYTES)
}

/// Adds the encoded base checkpoint and bounded suffix to a page byte budget.
fn add_checkpoint_and_delta_bytes(
    current: usize,
    checkpoint_bytes: usize,
    deltas: &[(ChangeDigest, Vec<u8>)],
) -> Option<usize> {
    deltas.iter().try_fold(
        add_materialized_bytes(current, checkpoint_bytes)?,
        |total, delta| add_materialized_bytes(total, delta.1.len()),
    )
}

/// Validates every count, byte, and coverage bound of one remote page.
fn validate_remote_page(
    documents: &[RemoteDocument<'_>],
    changes: &[RemoteChange<'_>],
    checkpoint: &[u8],
) -> Result<(), StoreError> {
    if !is_nonempty_bounded(checkpoint.len(), MAX_CHECKPOINT_BYTES) {
        let error = StoreError::InvalidCheckpointSize(checkpoint.len());
        return Err(error);
    }
    if !is_at_most(changes.len(), MAX_REMOTE_CHANGES) {
        let error = StoreError::InvalidRemotePage;
        return Err(error);
    }
    let mut total_change_bytes = 0usize;
    for change in changes {
        if !is_nonempty_bounded(change.bytes.len(), MAX_CHANGE_BYTES) {
            let error = StoreError::InvalidChangeSize(change.bytes.len());
            return Err(error);
        }
        total_change_bytes = total_change_bytes
            .checked_add(change.bytes.len())
            .filter(|total| is_at_most(*total, MAX_REMOTE_CHANGE_BYTES))
            .ok_or(StoreError::InvalidRemotePage)?;
    }
    let mut seen_documents = BTreeSet::new();
    let mut materialized_bytes = 0usize;
    for document in documents {
        if !is_nonempty_bounded(document.snapshot.len(), MAX_SNAPSHOT_BYTES) {
            let error = StoreError::InvalidSnapshotSize(document.snapshot.len());
            return Err(error);
        }
        materialized_bytes = add_materialized_bytes(materialized_bytes, document.snapshot.len())
            .ok_or(StoreError::InvalidRemotePage)?;
        if !seen_documents.insert(document.document) {
            let error = StoreError::InvalidRemotePage;
            return Err(error);
        }
        validate_indexes(document.indexes)?;
    }
    if changes
        .iter()
        .any(|change| !seen_documents.contains(&change.document))
    {
        let error = StoreError::InvalidRemotePage;
        return Err(error);
    }
    Ok(())
}

/// Appends one zero-escaped byte run plus its terminator.
fn encode_escaped(bytes: &[u8], output: &mut Vec<u8>) {
    for byte in bytes {
        if *byte == 0 {
            output.extend_from_slice(&[0, 0xff]);
        } else {
            output.push(*byte);
        }
    }
    output.extend_from_slice(&[0, 0]);
}

/// Returns the exclusive upper bound of one key prefix.
fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut next = prefix.to_vec();
    while let Some(last) = next.pop() {
        if last != u8::MAX {
            next.push(last.saturating_add(1));
            return Some(next);
        }
    }
    None
}

/// Encodes one durable operation receipt.
fn encode_operation(
    document: DocumentId,
    request: OperationDigest,
    digest: ChangeDigest,
    sequence: u64,
) -> [u8; OPERATION_VALUE_BYTES] {
    let mut value = [0; OPERATION_VALUE_BYTES];
    value[..16].copy_from_slice(document.as_uuid().as_bytes());
    value[16..48].copy_from_slice(&request.into_bytes());
    value[48..80].copy_from_slice(&digest.into_bytes());
    value[80..].copy_from_slice(&sequence.to_be_bytes());
    value
}

/// Decodes and validates one durable operation receipt.
fn decode_operation(bytes: &[u8]) -> Result<CommittedOperation, StoreError> {
    if bytes.len() != OPERATION_VALUE_BYTES {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let document = decode_identifier(&bytes[..16], DocumentId::from_durable_bytes)?;
    let request = bytes[16..48]
        .try_into()
        .map(OperationDigest::new)
        .map_err(malformed_store)?;
    let digest = bytes[48..80]
        .try_into()
        .map(ChangeDigest::new)
        .map_err(malformed_store)?;
    Ok(CommittedOperation {
        document,
        request,
        digest,
        sequence: decode_u64(&bytes[80..])?,
    })
}

/// Encodes one durable outbox entry.
fn encode_outbox(change: LocalChange<'_>) -> [u8; OUTBOX_VALUE_BYTES] {
    let mut value = [0; OUTBOX_VALUE_BYTES];
    value[..16].copy_from_slice(change.document.as_uuid().as_bytes());
    value[16..32].copy_from_slice(change.operation.as_uuid().as_bytes());
    value[32..].copy_from_slice(&change.digest.into_bytes());
    value
}

/// Decodes and validates one durable outbox entry.
fn decode_outbox(sequence: u64, bytes: &[u8]) -> Result<OutboxEntry, StoreError> {
    if bytes.len() != OUTBOX_VALUE_BYTES {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let document = decode_identifier(&bytes[..16], DocumentId::from_durable_bytes)?;
    let operation = decode_identifier(&bytes[16..32], OperationId::from_durable_bytes)?;
    let digest = bytes[32..]
        .try_into()
        .map(ChangeDigest::new)
        .map_err(malformed_store)?;
    Ok(OutboxEntry {
        sequence,
        document,
        operation,
        digest,
    })
}

/// Encodes one materialized-state event value.
fn encode_event(document: DocumentId, digest: ChangeDigest) -> [u8; EVENT_VALUE_BYTES] {
    let mut value = [0; EVENT_VALUE_BYTES];
    value[..16].copy_from_slice(document.as_uuid().as_bytes());
    value[16..].copy_from_slice(&digest.into_bytes());
    value
}

/// Decodes and validates one materialized-state event value.
fn decode_event(sequence: u64, bytes: &[u8]) -> Result<ChangeEvent, StoreError> {
    if bytes.len() != EVENT_VALUE_BYTES {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let document = decode_identifier(&bytes[..16], DocumentId::from_durable_bytes)?;
    let digest = bytes[16..]
        .try_into()
        .map(ChangeDigest::new)
        .map_err(malformed_store)?;
    Ok(ChangeEvent::Document {
        sequence,
        document,
        digest,
    })
}

/// Decodes and validates one baseline event value.
fn decode_baseline_event(sequence: u64, bytes: &[u8]) -> Result<ChangeEvent, StoreError> {
    if bytes.len() != BASELINE_VALUE_BYTES {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    Ok(ChangeEvent::Baseline {
        sequence,
        generation: decode_u64(bytes)?,
    })
}

/// Decodes one outbox sequence key.
fn decode_sequence_key(bytes: &[u8]) -> Result<u64, StoreError> {
    decode_sequence_key_for_kind(bytes, KIND_OUTBOX)
}

/// Decodes a sequence key after checking its kind tag.
fn decode_sequence_key_for_kind(bytes: &[u8], kind: u8) -> Result<u64, StoreError> {
    if bytes.len() != SEQUENCE_KEY_BYTES
        || bytes[..FORMAT.len()] != *FORMAT
        || bytes[SCOPE_PREFIX_BYTES] != kind
    {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    decode_u64(&bytes[bytes.len().saturating_sub(8)..])
}

/// Decodes one big-endian `u64`.
fn decode_u64(bytes: &[u8]) -> Result<u64, StoreError> {
    bytes
        .try_into()
        .map(u64::from_be_bytes)
        .map_err(malformed_store)
}

/// Decodes one document revision from its durable bytes.
fn decode_revision(bytes: &[u8]) -> Result<DocumentRevision, StoreError> {
    decode_u64(bytes).map(DocumentRevision::new)
}

/// Rehydrates a typed identifier from exactly 16 durable bytes, through the
/// model crate that owns the approved `uuid` boundary.
fn decode_identifier<T>(bytes: &[u8], wrap: fn(&[u8; 16]) -> T) -> Result<T, StoreError> {
    let raw: [u8; 16] = bytes
        .try_into()
        .map_err(|_| StoreError::CorruptInternalValue)?;
    Ok(wrap(&raw))
}

/// Encodes one versioned logical export payload.
///
/// The payload is a logical dump — ordered commit history plus document
/// snapshots with tombstone flags — never a file copy: the store refuses
/// foreign files through replica binding, so restore replays these entries
/// through the commit path instead.
///
/// # Errors
///
/// Returns [`StoreError::InvalidExportSize`] when a field or the assembled
/// payload would exceed the export budget.
pub fn encode_export(
    scope: Scope,
    entries: &[ExportEntry],
    documents: &[ExportDocument],
) -> Result<Vec<u8>, StoreError> {
    let entry_count = u32::try_from(entries.len()).map_err(malformed_store)?;
    let document_count = u32::try_from(documents.len()).map_err(malformed_store)?;
    let mut payload = Vec::new();
    push_bounded(&mut payload, &EXPORT_MAGIC)?;
    push_bounded(&mut payload, scope.account().as_uuid().as_bytes())?;
    push_bounded(&mut payload, scope.space().as_uuid().as_bytes())?;
    push_bounded(&mut payload, &entry_count.to_be_bytes())?;
    for entry in entries {
        encode_export_entry(&mut payload, entry)?;
    }
    push_bounded(&mut payload, &document_count.to_be_bytes())?;
    for document in documents {
        encode_export_document(&mut payload, document)?;
    }
    Ok(payload)
}

/// Decodes one versioned logical export payload with bounds before allocation.
///
/// Length prefixes are validated against the remaining input and the resource
/// ceilings before any buffer is allocated; unknown versions, truncation, and
/// trailing bytes fail closed.
///
/// # Errors
///
/// Returns a typed size, version, or structure failure without allocating
/// beyond the validated bounds.
pub fn decode_export(
    bytes: &[u8],
) -> Result<(Scope, Vec<ExportEntry>, Vec<ExportDocument>), StoreError> {
    if bytes.len() > MAX_EXPORT_BYTES {
        let error = StoreError::InvalidExportSize(bytes.len());
        return Err(error);
    }
    if bytes.len() < EXPORT_HEADER_BYTES {
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    if bytes[..EXPORT_MAGIC.len()] != EXPORT_MAGIC {
        if bytes[..4] == *b"LDBE" {
            let error = StoreError::UnknownExportVersion(u16::from(bytes[4]));
            return Err(error);
        }
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    let account = decode_identifier(&bytes[5..21], AccountId::from_durable_bytes)?;
    let space = decode_identifier(&bytes[21..37], SpaceId::from_durable_bytes)?;
    let scope = Scope::new(account, space);
    let mut position = EXPORT_HEADER_BYTES;
    let entry_count = usize::try_from(read_u32(bytes, &mut position)?).map_err(malformed_store)?;
    if entry_count.saturating_mul(EXPORT_ENTRY_PREFIX_BYTES) > bytes.len().saturating_sub(position)
    {
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    let mut entries = Vec::new();
    for _ in 0..entry_count {
        entries.push(decode_export_entry(bytes, &mut position)?);
    }
    let document_count =
        usize::try_from(read_u32(bytes, &mut position)?).map_err(malformed_store)?;
    if document_count.saturating_mul(EXPORT_DOCUMENT_PREFIX_BYTES)
        > bytes.len().saturating_sub(position)
    {
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    let mut documents = Vec::new();
    for _ in 0..document_count {
        documents.push(decode_export_document(bytes, &mut position)?);
    }
    if position != bytes.len() {
        let error = StoreError::InvalidExport;
        return Err(error);
    }
    Ok((scope, entries, documents))
}

/// Appends one bounded export history entry.
fn encode_export_entry(payload: &mut Vec<u8>, entry: &ExportEntry) -> Result<(), StoreError> {
    if entry.bytes.len() > MAX_CHANGE_BYTES {
        let error = StoreError::InvalidExportSize(entry.bytes.len());
        return Err(error);
    }
    push_bounded(payload, entry.document.as_uuid().as_bytes())?;
    push_bounded(payload, entry.operation.as_uuid().as_bytes())?;
    push_bounded(payload, &entry.request.into_bytes())?;
    push_bounded(payload, &entry.digest.into_bytes())?;
    let change_len = u32::try_from(entry.bytes.len()).map_err(malformed_store)?;
    push_bounded(payload, &change_len.to_be_bytes())?;
    push_bounded(payload, &entry.bytes)?;
    Ok(())
}

/// Appends one bounded export document.
fn encode_export_document(
    payload: &mut Vec<u8>,
    document: &ExportDocument,
) -> Result<(), StoreError> {
    if document.snapshot.len() > MAX_SNAPSHOT_BYTES {
        let error = StoreError::InvalidExportSize(document.snapshot.len());
        return Err(error);
    }
    push_bounded(payload, document.document.as_uuid().as_bytes())?;
    push_bounded(payload, &[u8::from(document.tombstone)])?;
    let snapshot_len = u32::try_from(document.snapshot.len()).map_err(malformed_store)?;
    push_bounded(payload, &snapshot_len.to_be_bytes())?;
    push_bounded(payload, &document.snapshot)?;
    Ok(())
}

/// Reads one export history entry, advancing the cursor.
fn decode_export_entry(bytes: &[u8], position: &mut usize) -> Result<ExportEntry, StoreError> {
    let document_bytes = read_array::<16>(bytes, position)?;
    let document = DocumentId::from_durable_bytes(&document_bytes);
    let operation_bytes = read_array::<16>(bytes, position)?;
    let operation = OperationId::from_durable_bytes(&operation_bytes);
    let request = read_array::<32>(bytes, position).map(OperationDigest::new)?;
    let digest = read_array::<32>(bytes, position).map(ChangeDigest::new)?;
    let change_len = usize::try_from(read_u32(bytes, position)?).map_err(malformed_store)?;
    if change_len > MAX_CHANGE_BYTES {
        let error = StoreError::InvalidExportSize(change_len);
        return Err(error);
    }
    let change = read_bytes(bytes, position, change_len)?;
    Ok(ExportEntry {
        document,
        operation,
        request,
        digest,
        bytes: change,
    })
}

/// Reads one export document, advancing the cursor.
fn decode_export_document(
    bytes: &[u8],
    position: &mut usize,
) -> Result<ExportDocument, StoreError> {
    let document_bytes = read_array::<16>(bytes, position)?;
    let document = DocumentId::from_durable_bytes(&document_bytes);
    let tombstone = match read_array::<1>(bytes, position)? {
        [0] => false,
        [1] => true,
        _ => {
            let error = StoreError::InvalidExport;
            return Err(error);
        }
    };
    let snapshot_len = usize::try_from(read_u32(bytes, position)?).map_err(malformed_store)?;
    if snapshot_len > MAX_SNAPSHOT_BYTES {
        let error = StoreError::InvalidExportSize(snapshot_len);
        return Err(error);
    }
    let snapshot = read_bytes(bytes, position, snapshot_len)?;
    Ok(ExportDocument {
        document,
        tombstone,
        snapshot,
    })
}

/// Derives the deterministic operation binding for a remotely installed change.
///
/// Export must replay changes that arrived through synchronization and
/// therefore carry no local operation receipt. The binding is a
/// non-cryptographic deterministic function of the document and change
/// digest, so every retry of the same restore replays the exact same
/// identities and short-circuits as duplicates. A collision with a live
/// operation fails closed through [`StoreError::OperationConflict`].
#[must_use]
pub fn restore_operation(
    document: DocumentId,
    digest: ChangeDigest,
) -> (OperationId, OperationDigest) {
    let operation_tag: &[u8] = b"logical-db-restore-op-v1";
    let request_tag: &[u8] = b"logical-db-restore-req-v1";
    let document = document.as_uuid();
    let digest = digest.into_bytes();
    let first = fnv1a64(
        0x9e37_79b9_7f4a_7c15,
        &[operation_tag, document.as_bytes(), digest.as_slice()],
    );
    let second = fnv1a64(
        0x6a09_e667_f3bc_c909,
        &[operation_tag, document.as_bytes(), digest.as_slice()],
    );
    let mut operation = [0_u8; 16];
    operation[..8].copy_from_slice(&first.to_be_bytes());
    operation[8..].copy_from_slice(&second.to_be_bytes());
    let mut request = [0_u8; 32];
    for (slot, seed) in request.chunks_mut(8).zip([
        0x510e_527f_ade6_82d1,
        0x9b05_688c_2b3e_6c1f,
        0x1f83_d9ab_fb41_bd6b,
        0x5be0_cd19_137e_2179,
    ]) {
        slot.copy_from_slice(
            &fnv1a64(seed, &[request_tag, document.as_bytes(), digest.as_slice()]).to_be_bytes(),
        );
    }
    (
        OperationId::from_durable_bytes(&operation),
        OperationDigest::new(request),
    )
}

/// Hashes one seeded FNV-1a sequence over the given byte parts.
fn fnv1a64(seed: u64, parts: &[&[u8]]) -> u64 {
    let mut hash = seed;
    for part in parts {
        for byte in *part {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    }
    hash
}

/// Appends bytes while enforcing the export byte budget.
fn push_bounded(payload: &mut Vec<u8>, bytes: &[u8]) -> Result<(), StoreError> {
    if payload.len().saturating_add(bytes.len()) > MAX_EXPORT_BYTES {
        let error = StoreError::InvalidExportSize(payload.len().saturating_add(bytes.len()));
        return Err(error);
    }
    payload.extend_from_slice(bytes);
    Ok(())
}

/// Reads a fixed-size array, advancing the cursor.
fn read_array<const LENGTH: usize>(
    bytes: &[u8],
    position: &mut usize,
) -> Result<[u8; LENGTH], StoreError> {
    let end = position
        .checked_add(LENGTH)
        .filter(|end| *end <= bytes.len())
        .ok_or(StoreError::InvalidExport)?;
    let value: [u8; LENGTH] = bytes[*position..end].try_into().map_err(malformed_store)?;
    *position = end;
    Ok(value)
}

/// Reads a length-prefixed byte run, advancing the cursor.
fn read_bytes(bytes: &[u8], position: &mut usize, length: usize) -> Result<Vec<u8>, StoreError> {
    let end = position
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or(StoreError::InvalidExport)?;
    let value = bytes[*position..end].to_vec();
    *position = end;
    Ok(value)
}

/// Reads one big-endian `u32`, advancing the cursor.
fn read_u32(bytes: &[u8], position: &mut usize) -> Result<u32, StoreError> {
    read_array(bytes, position).map(u32::from_be_bytes)
}

/// Encodes one durable restore cursor.
fn encode_restore_cursor(cursor: RestoreCursor) -> [u8; RESTORE_MARKER_BYTES] {
    let mut value = [0_u8; RESTORE_MARKER_BYTES];
    value[..32].copy_from_slice(&cursor.payload);
    value[32..].copy_from_slice(&cursor.next.to_be_bytes());
    value
}

/// Decodes and validates one durable restore cursor.
fn decode_restore_cursor(bytes: &[u8]) -> Result<RestoreCursor, StoreError> {
    if bytes.len() != RESTORE_MARKER_BYTES {
        let error = StoreError::CorruptInternalValue;
        return Err(error);
    }
    let payload: [u8; 32] = bytes[..32].try_into().map_err(malformed_store)?;
    Ok(RestoreCursor {
        payload,
        next: decode_u64(&bytes[32..])?,
    })
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fs};

    use logical_db_model::{AccountId, SpaceId};

    use super::*;

    /// Builds one unique temporary database path for a named fixture.
    fn test_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "logical-db-v3-{name}-{}.redb",
            DocumentId::unique_suffix()
        ))
    }

    /// Builds one bounded local change from deterministic fixture bytes.
    fn change(
        scope: Scope,
        document: DocumentId,
        operation: OperationId,
        request_byte: u8,
        digest_byte: u8,
    ) -> Result<LocalChange<'static>, StoreError> {
        LocalChange::new(
            scope,
            document,
            operation,
            OperationDigest::new([request_byte; 32]),
            ChangeDigest::new([digest_byte; 32]),
            b"verified-change",
        )
    }

    #[test]
    fn concurrent_scope_suffixes_do_not_cross_for_one_document_id() -> Result<(), Box<dyn Error>> {
        let path = test_path("scoped-snapshot-suffix");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope_a = Scope::new(AccountId::new(), SpaceId::new());
        let scope_b = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();

        for (scope, snapshot, request, digest) in [
            (scope_a, b"checkpoint-a".as_slice(), 1, 11),
            (scope_b, b"checkpoint-b".as_slice(), 2, 22),
        ] {
            store.commit_local(LocalCommit::new(
                change(scope, document, OperationId::new(), request, digest)?,
                None,
                snapshot,
                &[],
                false,
            ))?;
        }

        let append_suffix = |scope: Scope, request_base: u8, digest_base: u8| {
            for offset in 0_u8..8 {
                let expected = DocumentRevision::new(u64::from(offset).saturating_add(1));
                store.commit_local(LocalCommit::new_incremental(
                    change(
                        scope,
                        document,
                        OperationId::new(),
                        request_base.saturating_add(offset),
                        digest_base.saturating_add(offset),
                    )?,
                    expected,
                    &[],
                    false,
                ))?;
            }
            Ok::<(), StoreError>(())
        };
        std::thread::scope(|threads| -> Result<(), Box<dyn Error>> {
            let writer_a = threads.spawn(|| append_suffix(scope_a, 30, 40));
            let writer_b = threads.spawn(|| append_suffix(scope_b, 60, 70));
            writer_a.join().map_err(|_| "scope A writer panicked")??;
            writer_b.join().map_err(|_| "scope B writer panicked")??;
            Ok(())
        })?;

        let snapshot_a = store
            .load_snapshot(scope_a, document)?
            .ok_or("scope A checkpoint disappeared")?;
        let snapshot_b = store
            .load_snapshot(scope_b, document)?
            .ok_or("scope B checkpoint disappeared")?;
        assert_eq!(snapshot_a.bytes, b"checkpoint-a");
        assert_eq!(snapshot_b.bytes, b"checkpoint-b");
        assert_eq!(snapshot_a.deltas.len(), 8);
        assert_eq!(snapshot_b.deltas.len(), 8);
        assert_eq!(snapshot_a.deltas[0].0, ChangeDigest::new([40; 32]));
        assert_eq!(snapshot_b.deltas[0].0, ChangeDigest::new([70; 32]));
        assert_ne!(snapshot_a.deltas[0].0, snapshot_b.deltas[0].0);
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    /// Writes one raw legacy row inside its own immediate transaction.
    fn insert_legacy_value(
        store: &EmbeddedStore,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), StoreError> {
        let transaction = store.database.begin_write()?;
        {
            let mut table = transaction.open_table(DATA)?;
            table.insert(key, value)?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Applies one migration page using the fixed target schema and new index.
    fn apply_migration_documents(
        store: &EmbeddedStore,
        scope: Scope,
        index: IndexId,
        page: &MigrationPage,
    ) -> Result<(), StoreError> {
        let indexes = [IndexEntry::new(index, IndexValue::Text("new".into()))];
        let updates = page
            .documents
            .iter()
            .map(|document| MigrationIndexes {
                document: document.document,
                expected_revision: document.revision,
                indexes: &indexes,
            })
            .collect::<Vec<_>>();
        store.apply_migration_page(scope, 2, [2; 32], page.after, &updates)
    }

    /// Binds the initial schema and commits three indexed snapshot documents.
    fn seed_migration_documents(
        store: &EmbeddedStore,
        scope: Scope,
        index: IndexId,
        documents: [DocumentId; 3],
    ) -> Result<(), Box<dyn Error>> {
        store.bind_schema(scope, 1, [1; 32])?;
        for (offset, document) in documents.into_iter().enumerate() {
            store.commit_local(LocalCommit {
                change: change(
                    scope,
                    document,
                    OperationId::new(),
                    u8::try_from(offset.saturating_add(1))?,
                    u8::try_from(offset.saturating_add(11))?,
                )?,
                expected_revision: None,
                snapshot: b"snapshot",
                indexes: &[IndexEntry::new(index, IndexValue::Text("old".into()))],
                tombstone: false,
            })?;
        }
        Ok(())
    }

    #[test]
    fn commit_restart_and_outbox_are_one_atomic_state() -> Result<(), Box<dyn Error>> {
        let path = test_path("restart");
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let operation = OperationId::new();
        let change = change(scope, document, operation, 7, 9)?;
        let replica = ReplicaId::new();
        {
            let store = EmbeddedStore::open(&path, replica)?;
            assert_eq!(
                store.commit_local(LocalCommit {
                    change,
                    expected_revision: None,
                    snapshot: b"verified-snapshot",
                    indexes: &[],
                    tombstone: false,
                })?,
                CommitOutcome::Applied { sequence: 1 },
                "the first commit must apply at sequence one"
            );
        }
        {
            let store = EmbeddedStore::open(&path, replica)?;
            assert_eq!(
                store.load_snapshot(scope, document)?,
                Some(StoredSnapshot {
                    bytes: b"verified-snapshot".to_vec(),
                    revision: DocumentRevision::new(1),
                    deltas: Vec::new(),
                }),
                "the committed snapshot must survive restart"
            );
            assert_eq!(
                store.outbox_page(scope, 0, 10)?,
                vec![OutboxEntry {
                    sequence: 1,
                    document,
                    operation,
                    digest: change.digest,
                }],
                "restart must replay exactly the one outbox entry"
            );
            assert_eq!(
                store.commit_local(LocalCommit {
                    change,
                    expected_revision: Some(DocumentRevision::new(1)),
                    snapshot: b"verified-snapshot",
                    indexes: &[],
                    tombstone: false,
                })?,
                CommitOutcome::Duplicate { sequence: 1 },
                "recommit at the same revision must be a duplicate"
            );
        }
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn operation_reuse_with_different_digest_is_rejected() -> Result<(), Box<dyn Error>> {
        let path = test_path("operation-conflict");
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let operation = OperationId::new();
        let first = change(scope, document, operation, 7, 9)?;
        let second = change(scope, document, operation, 7, 8)?;
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        store.commit_local(LocalCommit {
            change: first,
            expected_revision: None,
            snapshot: b"first",
            indexes: &[],
            tombstone: false,
        })?;
        assert!(
            matches!(
                store.commit_local(LocalCommit {
                    change: second,
                    expected_revision: Some(DocumentRevision::new(1)),
                    snapshot: b"second",
                    indexes: &[],
                    tombstone: false,
                }),
                Err(StoreError::OperationConflict)
            ),
            "reusing an operation with a different digest must be rejected"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn two_successors_of_one_snapshot_cannot_overwrite_materialized_state()
    -> Result<(), Box<dyn Error>> {
        let path = test_path("stale-successor");
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let first = change(scope, document, OperationId::new(), 1, 9)?;
        let stale = change(scope, document, OperationId::new(), 2, 6)?;
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        store.commit_local(LocalCommit {
            change: first,
            expected_revision: None,
            snapshot: b"first",
            indexes: &[],
            tombstone: false,
        })?;
        assert!(
            matches!(
                store.commit_local(LocalCommit {
                    change: stale,
                    expected_revision: None,
                    snapshot: b"stale",
                    indexes: &[],
                    tombstone: false,
                }),
                Err(StoreError::StaleDocument)
            ),
            "a second successor of the same snapshot must be refused as stale"
        );
        assert_eq!(
            store.load_snapshot(scope, document)?,
            Some(StoredSnapshot {
                bytes: b"first".to_vec(),
                revision: DocumentRevision::new(1),
                deltas: Vec::new(),
            }),
            "the stale commit must leave the materialized snapshot untouched"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn replica_identity_is_durable_and_cannot_relabel_outbox_state() -> Result<(), Box<dyn Error>> {
        let path = test_path("replica-binding");
        let first = ReplicaId::new();
        let second = ReplicaId::new();
        drop(EmbeddedStore::open(&path, first)?);
        assert!(
            matches!(
                EmbeddedStore::open(&path, second),
                Err(StoreError::ReplicaMismatch {
                    expected,
                    actual
                }) if expected == first && actual == second
            ),
            "opening under a different replica identity must be refused"
        );
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn maximum_checkpoint_cannot_repeat_the_final_outbox_entry() -> Result<(), Box<dyn Error>> {
        let path = test_path("maximum-checkpoint");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        assert!(
            store.outbox_page(scope, u64::MAX, 1)?.is_empty(),
            "a checkpoint at the maximum sequence must page no entries"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn concurrent_commit_receipts_match_their_exact_outbox_entries() -> Result<(), Box<dyn Error>> {
        let path = test_path("concurrent-sequences");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let requests = (1..=16u8)
            .map(
                |byte| -> Result<(OperationId, LocalChange<'static>), StoreError> {
                    let operation = OperationId::new();
                    Ok((
                        operation,
                        change(scope, DocumentId::new(), operation, byte, byte)?,
                    ))
                },
            )
            .collect::<Result<Vec<_>, StoreError>>()?;
        let receipts = std::thread::scope(|threads| {
            let store = &store;
            requests
                .iter()
                .map(|&(operation, change)| {
                    threads.spawn(move || {
                        let outcome = store.commit_local(LocalCommit {
                            change,
                            expected_revision: None,
                            snapshot: b"concurrent-snapshot",
                            indexes: &[],
                            tombstone: false,
                        })?;
                        Ok::<_, StoreError>((operation, outcome))
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|thread| {
                    thread
                        .join()
                        .map_err(|_| StoreError::CorruptInternalValue)?
                })
                .collect::<Result<Vec<_>, StoreError>>()
        })?;
        let outbox = store
            .outbox_page(scope, 0, requests.len())?
            .into_iter()
            .map(|entry| (entry.operation, entry.sequence))
            .collect::<std::collections::BTreeMap<_, _>>();
        for (operation, outcome) in receipts {
            let sequence = match outcome {
                CommitOutcome::Applied { sequence } => sequence,
                CommitOutcome::Duplicate { sequence } => {
                    return Err(format!(
                        "unique operation {operation} reported as duplicate at {sequence}"
                    )
                    .into());
                }
            };
            assert_eq!(
                outbox.get(&operation),
                Some(&sequence),
                "every concurrent receipt must match its exact outbox entry"
            );
        }
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn unicode_and_space_path_survives_restart() -> Result<(), Box<dyn Error>> {
        let directory =
            std::env::temp_dir().join(format!("logical db 雲 {}", DocumentId::unique_suffix()));
        fs::create_dir(&directory)?;
        let path = directory.join("replica state.redb");
        let replica = ReplicaId::new();
        drop(EmbeddedStore::open(&path, replica)?);
        let reopened = EmbeddedStore::open(&path, replica)?;
        assert_eq!(
            reopened.replica(),
            replica,
            "the replica identity must survive reopen"
        );
        drop(reopened);
        fs::remove_file(path)?;
        fs::remove_dir(directory)?;
        Ok(())
    }

    #[test]
    fn corrupt_operation_record_fails_closed() -> Result<(), Box<dyn Error>> {
        let path = test_path("corrupt-operation");
        let replica = ReplicaId::new();
        let store = EmbeddedStore::open(&path, replica)?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let operation = OperationId::new();
        {
            let transaction = store.database.begin_write()?;
            {
                let mut table = transaction.open_table(DATA)?;
                table.insert(
                    key_operation(scope, operation).as_slice(),
                    b"truncated".as_slice(),
                )?;
            }
            transaction.commit()?;
        }
        assert!(
            matches!(
                store.operation(scope, operation),
                Err(StoreError::CorruptInternalValue)
            ),
            "a truncated operation record must fail closed as corrupt"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn existing_unknown_schema_is_never_opened_as_an_empty_v3_replica() -> Result<(), Box<dyn Error>>
    {
        /// A legacy table used only to prove schema refusal on open.
        const LEGACY: TableDefinition<u8, u8> = TableDefinition::new("legacy_data");

        let path = test_path("legacy-schema");
        {
            let database = Database::create(&path)?;
            let transaction = database.begin_write()?;
            transaction.open_table(LEGACY)?;
            transaction.commit()?;
        }
        assert!(
            matches!(
                EmbeddedStore::open(&path, ReplicaId::new()),
                Err(StoreError::UnsupportedSchema)
            ),
            "an unknown pre-existing schema must never open as an empty replica"
        );
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn embedded_key_axis_is_versioned_scoped_and_order_preserving() {
        let account = logical_db_model::AccountId::from_durable_bytes(&[1; 16]);
        let space = SpaceId::from_durable_bytes(&[2; 16]);
        let document = DocumentId::from_durable_bytes(&[3; 16]);
        let scope = Scope::new(account, space);
        let snapshot = key_snapshot(scope, document);
        let mut expected = FORMAT.to_vec();
        expected.extend_from_slice(account.as_uuid().as_bytes());
        expected.extend_from_slice(space.as_uuid().as_bytes());
        expected.push(KIND_SNAPSHOT);
        expected.extend_from_slice(document.as_uuid().as_bytes());
        assert_eq!(
            snapshot, expected,
            "the snapshot key must be format, scope, kind, document in order"
        );
        assert!(
            key_outbox(scope, 1) < key_outbox(scope, 2),
            "outbox keys must order by sequence"
        );
        assert_ne!(
            key_snapshot(
                Scope::new(account, SpaceId::from_durable_bytes(&[4; 16])),
                document
            ),
            snapshot,
            "snapshot keys must separate scopes"
        );
        assert_ne!(
            key_change(scope, document, ChangeDigest::new([0; 32])),
            snapshot,
            "a change key must never collide with a snapshot key"
        );
    }

    #[test]
    fn operation_lookup_rejects_a_corrupt_atomic_outbox_companion() -> Result<(), Box<dyn Error>> {
        let path = test_path("corrupt-outbox-companion");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let operation = OperationId::new();
        let change = change(scope, document, operation, 1, 2)?;
        store.commit_local(LocalCommit {
            change,
            expected_revision: None,
            snapshot: b"snapshot",
            indexes: &[],
            tombstone: false,
        })?;
        {
            let transaction = store.database.begin_write()?;
            {
                let mut table = transaction.open_table(DATA)?;
                table.insert(key_outbox(scope, 1).as_slice(), b"broken".as_slice())?;
            }
            transaction.commit()?;
        }
        assert!(
            matches!(
                store.operation(scope, operation),
                Err(StoreError::CorruptInternalValue)
            ),
            "a broken outbox companion must fail closed as corrupt"
        );

        let valid = encode_outbox(change);
        for (offset, replacement) in [
            (0_usize, DocumentId::new().as_uuid().as_bytes().to_vec()),
            (16_usize, OperationId::new().as_uuid().as_bytes().to_vec()),
            (32_usize, ChangeDigest::new([91; 32]).into_bytes().to_vec()),
        ] {
            let mut corrupted = valid;
            let end = offset.saturating_add(replacement.len());
            corrupted[offset..end].copy_from_slice(&replacement);
            let transaction = store.database.begin_write()?;
            {
                let mut table = transaction.open_table(DATA)?;
                table.insert(key_outbox(scope, 1).as_slice(), corrupted.as_slice())?;
            }
            transaction.commit()?;
            assert!(
                matches!(
                    store.operation(scope, operation),
                    Err(StoreError::CorruptInternalValue)
                ),
                "every torn outbox field must fail closed as corrupt"
            );
        }
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn schema_binding_is_exact_nonzero_and_never_adopts_existing_state()
    -> Result<(), Box<dyn Error>> {
        let path = test_path("schema-binding");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let first = Scope::new(AccountId::new(), SpaceId::new());
        assert!(
            matches!(
                store.bind_schema(first, 0, [1; 32]),
                Err(StoreError::InvalidSchemaVersion)
            ),
            "schema version zero must be refused"
        );
        assert!(
            store.bind_schema(first, 7, [1; 32]).is_ok(),
            "the first binding must succeed"
        );
        assert!(
            store.bind_schema(first, 7, [1; 32]).is_ok(),
            "rebinding the identical schema must succeed"
        );
        assert!(
            matches!(
                store.bind_schema(first, 8, [1; 32]),
                Err(StoreError::SchemaMigrationRequired)
            ),
            "a different version must require migration"
        );
        assert!(
            matches!(
                store.bind_schema(first, 7, [2; 32]),
                Err(StoreError::SchemaMigrationRequired)
            ),
            "a different fingerprint must require migration"
        );

        let second = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        store.commit_local(LocalCommit {
            change: change(second, document, OperationId::new(), 1, 2)?,
            expected_revision: None,
            snapshot: b"state-before-binding",
            indexes: &[],
            tombstone: false,
        })?;
        assert!(
            matches!(
                store.bind_schema(second, 1, [3; 32]),
                Err(StoreError::SchemaMigrationRequired)
            ),
            "existing state must never be adopted by a late binding"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn schema_migration_is_exclusive_cursor_resumable_and_atomically_published()
    -> Result<(), Box<dyn Error>> {
        let path = test_path("schema-migration");
        let replica = ReplicaId::new();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let old_index = IndexId::new();
        let new_index = IndexId::new();
        let documents = [
            DocumentId::from_fixture_u128(1),
            DocumentId::from_fixture_u128(2),
            DocumentId::from_fixture_u128(3),
        ];
        let store = EmbeddedStore::open(&path, replica)?;
        seed_migration_documents(&store, scope, old_index, documents)?;
        store.begin_schema_migration(scope, 2, [2; 32])?;
        assert!(
            matches!(
                store.bind_schema(scope, 1, [1; 32]),
                Err(StoreError::MigrationInProgress)
            ),
            "schema binding must be excluded while a migration is in progress"
        );
        assert!(
            matches!(
                store.query_index(scope, old_index, &IndexValue::Text("old".into()), None, 10),
                Err(StoreError::MigrationInProgress)
            ),
            "index reads must be excluded while a migration is in progress"
        );
        assert!(
            matches!(
                store.finish_schema_migration(scope, 2, [2; 32]),
                Err(StoreError::MigrationIncomplete)
            ),
            "finishing before the cursor completes must fail"
        );
        assert!(
            matches!(
                store.migration_page(scope, 2, [2; 32], 0),
                Err(StoreError::InvalidMigrationLimit(0))
            ),
            "a zero migration limit must be refused"
        );
        assert!(
            matches!(
                store.migration_page(scope, 3, [3; 32], 1),
                Err(StoreError::MigrationTargetMismatch)
            ),
            "a mismatched migration target must be refused"
        );
        let first = store.migration_page(scope, 2, [2; 32], 1)?;
        assert_eq!(
            first.after, None,
            "the first page starts at the cursor origin"
        );
        assert_eq!(
            first.documents[0].document, documents[0],
            "the first page must open with the first document"
        );
        apply_migration_documents(&store, scope, new_index, &first)?;
        drop(store);

        let store = EmbeddedStore::open(&path, replica)?;
        store.begin_schema_migration(scope, 2, [2; 32])?;
        assert!(
            matches!(
                store.begin_schema_migration(scope, 3, [3; 32]),
                Err(StoreError::MigrationTargetMismatch)
            ),
            "a second migration to another target must be refused"
        );
        let second = store.migration_page(scope, 2, [2; 32], 1)?;
        assert_eq!(
            second.after,
            Some(documents[0]),
            "the resumed cursor must point at the last applied document"
        );
        assert_eq!(
            second.documents[0].document, documents[1],
            "the resumed page must continue with the next document"
        );
        assert!(
            matches!(
                store.apply_migration_page(
                    scope,
                    2,
                    [2; 32],
                    None,
                    &[MigrationIndexes {
                        document: second.documents[0].document,
                        expected_revision: second.documents[0].revision,
                        indexes: &[],
                    }]
                ),
                Err(StoreError::StaleMigrationPage)
            ),
            "a stale page cursor must be refused"
        );

        let mut processed: usize = 1;
        loop {
            let page = store.migration_page(scope, 2, [2; 32], 1)?;
            if page.documents.is_empty() {
                break;
            }
            processed = processed.saturating_add(page.documents.len());
            apply_migration_documents(&store, scope, new_index, &page)?;
        }
        assert_eq!(
            processed,
            documents.len(),
            "every seeded document must be projected exactly once"
        );
        store.finish_schema_migration(scope, 2, [2; 32])?;
        store.bind_schema(scope, 2, [2; 32])?;
        assert!(
            matches!(
                store.bind_schema(scope, 1, [1; 32]),
                Err(StoreError::SchemaMigrationRequired)
            ),
            "the superseded schema must never bind again"
        );
        assert!(
            store
                .query_index(scope, old_index, &IndexValue::Text("old".into()), None, 10)?
                .is_empty(),
            "the old index must be dropped by the published migration"
        );
        assert_eq!(
            store
                .query_index(scope, new_index, &IndexValue::Text("new".into()), None, 10)?
                .len(),
            documents.len(),
            "the new index must hold every migrated document"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn migration_cursor_cannot_skip_an_unprojected_document() -> Result<(), Box<dyn Error>> {
        let path = test_path("migration-skip");
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let documents = [
            DocumentId::from_fixture_u128(1),
            DocumentId::from_fixture_u128(2),
            DocumentId::from_fixture_u128(3),
        ];
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        seed_migration_documents(&store, scope, IndexId::new(), documents)?;
        store.begin_schema_migration(scope, 2, [2; 32])?;
        let all = store.migration_page(scope, 2, [2; 32], 3)?;
        assert!(
            matches!(
                store.apply_migration_page(
                    scope,
                    2,
                    [2; 32],
                    None,
                    &[MigrationIndexes {
                        document: all.documents[2].document,
                        expected_revision: all.documents[2].revision,
                        indexes: &[],
                    }]
                ),
                Err(StoreError::InvalidMigrationPage)
            ),
            "applying a page past an unprojected document must be refused"
        );
        assert_eq!(
            store.migration_page(scope, 2, [2; 32], 1)?.after,
            None,
            "a refused apply must leave the cursor unmoved"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn accessors_acknowledgement_and_change_feed_preserve_exact_durable_values()
    -> Result<(), Box<dyn Error>> {
        let path = test_path("accessors-and-feed");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let operation = OperationId::new();
        let first = change(scope, document, operation, 4, 5)?;
        assert_eq!(
            store.load_snapshot(scope, document)?,
            None,
            "an empty store must load no snapshot"
        );
        assert_eq!(
            store.load_change(scope, document, first.digest)?,
            None,
            "an empty store must load no change"
        );
        store.commit_local(LocalCommit {
            change: first,
            expected_revision: None,
            snapshot: b"snapshot-one",
            indexes: &[],
            tombstone: false,
        })?;
        assert_eq!(
            store.load_change(scope, document, first.digest)?,
            Some(b"verified-change".to_vec()),
            "the committed change bytes must be loadable"
        );
        assert_eq!(
            store.operation(scope, operation)?,
            Some(CommittedOperation {
                document,
                request: OperationDigest::new([4; 32]),
                digest: ChangeDigest::new([5; 32]),
                sequence: 1,
            }),
            "the operation lookup must return the committed record"
        );
        assert_eq!(
            store.changes_since(scope, 0, 10)?,
            vec![ChangeEvent::Document {
                sequence: 1,
                document,
                digest: first.digest,
            }],
            "the change feed must open with the committed event"
        );
        assert!(
            store.changes_since(scope, 1, 10)?.is_empty(),
            "a feed past the last sequence must be empty"
        );
        assert!(
            store.changes_since(scope, u64::MAX, 10)?.is_empty(),
            "a feed from the maximum sequence must be empty"
        );
        assert!(
            matches!(
                store.changes_since(scope, 0, 0),
                Err(StoreError::InvalidEventLimit(0))
            ),
            "a zero event limit must be refused"
        );
        assert!(
            matches!(
                store.acknowledge_outbox(scope, peer, 2),
                Err(StoreError::InvalidAcknowledgement {
                    through: 2,
                    highest: 1
                })
            ),
            "acknowledging past the highest sequence must be refused"
        );
        store.acknowledge_outbox(scope, peer, 1)?;
        store.acknowledge_outbox(scope, peer, 0)?;
        assert!(
            store.outbox_page(scope, 0, 10)?.is_empty(),
            "a full acknowledgement must drain the outbox"
        );

        let second = change(scope, document, OperationId::new(), 6, 7)?;
        store.commit_local(LocalCommit {
            change: second,
            expected_revision: Some(DocumentRevision::new(1)),
            snapshot: b"snapshot-two",
            indexes: &[],
            tombstone: false,
        })?;
        assert_eq!(
            store.outbox_page(scope, 0, 10)?[0].sequence,
            2,
            "the second commit must occupy sequence two"
        );
        assert_eq!(
            store.changes_since(scope, 1, 10)?[0].sequence(),
            2,
            "the feed must resume at the second event"
        );
        assert!(
            matches!(
                store.outbox_page(scope, 0, 0),
                Err(StoreError::InvalidOutboxLimit(0))
            ),
            "a zero outbox limit must be refused"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn global_outbox_acknowledgement_binds_exactly_one_upstream() -> Result<(), Box<dyn Error>> {
        let path = test_path("single-upstream");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let configured = PeerId::new();
        let requested = PeerId::new();
        store.bind_upstream(scope, configured)?;
        store.bind_upstream(scope, configured)?;
        assert!(
            matches!(
                store.bind_upstream(scope, requested),
                Err(StoreError::UpstreamMismatch {
                    configured: actual,
                    requested: attempted,
                }) if actual == configured && attempted == requested
            ),
            "a second upstream identity must be refused"
        );
        assert!(
            matches!(
                store.acknowledge_outbox(scope, requested, 0),
                Err(StoreError::UpstreamMismatch { .. })
            ),
            "only the bound upstream may acknowledge"
        );
        assert!(
            matches!(
                store.install_remote_page(scope, requested, &[], &[], b"checkpoint"),
                Err(StoreError::UpstreamMismatch { .. })
            ),
            "only the bound upstream may install pages"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn legacy_checkpoint_migrates_only_to_its_attributable_upstream() -> Result<(), Box<dyn Error>>
    {
        let path = test_path("legacy-single-upstream");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let legacy = PeerId::new();
        insert_legacy_value(&store, &key_checkpoint(scope, legacy), b"legacy-checkpoint")?;
        assert!(
            matches!(
                store.bind_upstream(scope, PeerId::new()),
                Err(StoreError::UpstreamMismatch { configured, .. }) if configured == legacy
            ),
            "a foreign peer must not claim the attributable legacy checkpoint"
        );
        store.bind_upstream(scope, legacy)?;
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn ambiguous_or_unattributed_legacy_sync_state_fails_closed() -> Result<(), Box<dyn Error>> {
        let ambiguous_path = test_path("legacy-ambiguous-upstream");
        let ambiguous = EmbeddedStore::open(&ambiguous_path, ReplicaId::new())?;
        let ambiguous_scope = Scope::new(AccountId::new(), SpaceId::new());
        for peer in [PeerId::new(), PeerId::new()] {
            insert_legacy_value(
                &ambiguous,
                &key_checkpoint(ambiguous_scope, peer),
                b"checkpoint",
            )?;
        }
        assert!(
            matches!(
                ambiguous.bind_upstream(ambiguous_scope, PeerId::new()),
                Err(StoreError::AmbiguousLegacyUpstream)
            ),
            "two legacy checkpoints must fail closed as ambiguous"
        );
        drop(ambiguous);
        fs::remove_file(ambiguous_path)?;

        let ack_path = test_path("legacy-unattributed-ack");
        let acknowledged = EmbeddedStore::open(&ack_path, ReplicaId::new())?;
        let acknowledged_scope = Scope::new(AccountId::new(), SpaceId::new());
        insert_legacy_value(
            &acknowledged,
            &key_outbox_ack(acknowledged_scope),
            &1_u64.to_be_bytes(),
        )?;
        assert!(
            matches!(
                acknowledged.bind_upstream(acknowledged_scope, PeerId::new()),
                Err(StoreError::UnattributedLegacyAcknowledgement)
            ),
            "an unattributed legacy acknowledgement must fail closed"
        );
        drop(acknowledged);
        fs::remove_file(ack_path)?;

        let corrupt_path = test_path("legacy-corrupt-checkpoint");
        let corrupt = EmbeddedStore::open(&corrupt_path, ReplicaId::new())?;
        let corrupt_scope = Scope::new(AccountId::new(), SpaceId::new());
        let corrupt_peer = PeerId::new();
        insert_legacy_value(&corrupt, &key_checkpoint(corrupt_scope, corrupt_peer), b"")?;
        assert!(
            matches!(
                corrupt.bind_upstream(corrupt_scope, corrupt_peer),
                Err(StoreError::CorruptInternalValue)
            ),
            "an empty legacy checkpoint must fail closed as corrupt"
        );
        drop(corrupt);
        fs::remove_file(corrupt_path)?;
        Ok(())
    }

    #[test]
    fn index_and_event_reads_reject_orphaned_internal_rows() -> Result<(), Box<dyn Error>> {
        let path = test_path("orphaned-rows");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let index = IndexId::new();
        let value = IndexValue::Text("kind".into());
        let local = change(scope, document, OperationId::new(), 3, 4)?;
        let digest = local.digest;
        store.commit_local(LocalCommit {
            change: local,
            expected_revision: None,
            snapshot: b"snapshot",
            indexes: &[IndexEntry::new(index, value.clone())],
            tombstone: false,
        })?;
        assert_eq!(
            store.query_index(scope, index, &value, None, 1)?.len(),
            1,
            "the committed index must hold one hit"
        );
        assert_eq!(
            store.changes_since(scope, 0, 1)?.len(),
            1,
            "the feed must hold one event"
        );
        {
            let transaction = store.database.begin_write()?;
            {
                let mut table = transaction.open_table(DATA)?;
                table.remove(key_index_reverse(scope, document, index).as_slice())?;
                table.remove(key_change(scope, document, digest).as_slice())?;
            }
            transaction.commit()?;
        }
        assert!(
            matches!(
                store.query_index(scope, index, &value, None, 1),
                Err(StoreError::CorruptInternalValue)
            ),
            "an orphaned index row must fail closed"
        );
        assert!(
            matches!(
                store.changes_since(scope, 0, 1),
                Err(StoreError::CorruptInternalValue)
            ),
            "an orphaned change row must fail closed"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn secondary_indexes_replace_rows_page_stably_and_encode_order() -> Result<(), Box<dyn Error>> {
        let path = test_path("direct-index");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let index = IndexId::new();
        let first_document = DocumentId::from_fixture_u128(1);
        let second_document = DocumentId::from_fixture_u128(2);
        for (document, byte) in [(first_document, 1), (second_document, 2)] {
            store.commit_local(LocalCommit {
                change: change(scope, document, OperationId::new(), byte, byte)?,
                expected_revision: None,
                snapshot: b"indexed",
                indexes: &[IndexEntry::new(index, IndexValue::Text("note".into()))],
                tombstone: false,
            })?;
        }
        assert_eq!(
            store.query_index(scope, index, &IndexValue::Text("note".into()), None, 1)?,
            vec![IndexHit {
                document: first_document
            }],
            "the first page must start at the first document"
        );
        assert_eq!(
            store.query_index(
                scope,
                index,
                &IndexValue::Text("note".into()),
                Some(first_document),
                10,
            )?,
            vec![IndexHit {
                document: second_document
            }],
            "the continuation must yield the second document"
        );
        assert!(
            matches!(
                store.query_index(scope, index, &IndexValue::Text("note".into()), None, 0),
                Err(StoreError::InvalidIndexLimit(0))
            ),
            "a zero index limit must be refused"
        );
        store.commit_local(LocalCommit {
            change: change(scope, first_document, OperationId::new(), 3, 3)?,
            expected_revision: Some(DocumentRevision::new(1)),
            snapshot: b"reindexed",
            indexes: &[IndexEntry::new(index, IndexValue::Text("task".into()))],
            tombstone: false,
        })?;
        assert_eq!(
            store.query_index(scope, index, &IndexValue::Text("note".into()), None, 10)?,
            vec![IndexHit {
                document: second_document
            }],
            "reindexing must remove the old value row"
        );
        assert_eq!(
            store.query_index(scope, index, &IndexValue::Text("task".into()), None, 10)?,
            vec![IndexHit {
                document: first_document
            }],
            "the new value row must be queryable"
        );

        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn index_value_codecs_preserve_order_escaping_and_resource_bounds() -> Result<(), Box<dyn Error>>
    {
        assert!(
            encode_index_value(&IndexValue::Integer(-1))?
                < encode_index_value(&IndexValue::Integer(0))?,
            "negative integers must encode below zero"
        );
        assert!(
            encode_index_value(&IndexValue::Unsigned(1))?
                < encode_index_value(&IndexValue::Unsigned(2))?,
            "unsigned integers must encode in numeric order"
        );
        assert!(
            encode_index_value(&IndexValue::Timestamp(-1))?
                < encode_index_value(&IndexValue::Timestamp(0))?,
            "negative timestamps must encode below zero"
        );
        assert_ne!(
            encode_index_value(&IndexValue::Text("a\0b".into()))?,
            encode_index_value(&IndexValue::Text("a".into()))?,
            "embedded nuls must be escaped distinctly"
        );
        assert_eq!(
            next_prefix(&[1, 0xff]),
            Some(vec![2]),
            "incrementing a carry prefix must produce the next key"
        );
        assert_eq!(
            next_prefix(&[0xff]),
            None,
            "an all-ff prefix has no successor"
        );

        let index = IndexId::new();
        let duplicate = [
            IndexEntry::new(index, IndexValue::Boolean(false)),
            IndexEntry::new(index, IndexValue::Boolean(true)),
        ];
        assert!(
            matches!(
                validate_indexes(&duplicate),
                Err(StoreError::InvalidIndexes)
            ),
            "duplicate index ids must be refused"
        );
        let excessive = (0..=MAX_INDEXES_PER_DOCUMENT)
            .map(|_| IndexEntry::new(IndexId::new(), IndexValue::Boolean(true)))
            .collect::<Vec<_>>();
        assert!(
            matches!(
                validate_indexes(&excessive),
                Err(StoreError::InvalidIndexes)
            ),
            "one index past the ceiling must be refused"
        );
        assert!(
            matches!(
                encode_index_value(&IndexValue::Bytes(vec![0; MAX_INDEX_VALUE_BYTES])),
                Err(StoreError::InvalidIndexes)
            ),
            "an oversized byte value must be refused"
        );
        assert_eq!(
            encode_index_value(&IndexValue::Bytes(vec![1; 4_093]))?.len(),
            MAX_INDEX_VALUE_BYTES,
            "the exact ceiling byte length must encode"
        );
        let maximum = (0..MAX_INDEXES_PER_DOCUMENT)
            .map(|_| IndexEntry::new(IndexId::new(), IndexValue::Boolean(true)))
            .collect::<Vec<_>>();
        assert!(
            validate_indexes(&maximum).is_ok(),
            "the exact index ceiling must be accepted"
        );
        Ok(())
    }

    #[test]
    fn every_resource_ceiling_accepts_its_exact_edge_and_rejects_the_next_value() {
        assert_eq!(
            MAX_SNAPSHOT_BYTES, 16_777_216,
            "the snapshot ceiling is fixed"
        );
        assert_eq!(
            MAX_MATERIALIZED_PAGE_BYTES, 67_108_864,
            "the materialized page ceiling is fixed"
        );
        assert_eq!(
            add_materialized_bytes(MAX_MATERIALIZED_PAGE_BYTES.saturating_sub(1), 1),
            Some(MAX_MATERIALIZED_PAGE_BYTES),
            "the byte below the ceiling must still add up to it"
        );
        assert_eq!(
            add_materialized_bytes(MAX_MATERIALIZED_PAGE_BYTES, 1),
            None,
            "one byte past the ceiling must overflow to none"
        );
        assert_eq!(
            add_materialized_bytes(usize::MAX, 1),
            None,
            "usize::MAX must overflow to none"
        );
        assert_eq!(
            MAX_REMOTE_CHANGE_BYTES, 8_388_608,
            "the remote change ceiling is fixed"
        );
        for maximum in [MAX_CHANGE_BYTES, MAX_SNAPSHOT_BYTES, MAX_CHECKPOINT_BYTES] {
            assert!(
                !is_nonempty_bounded(0, maximum),
                "zero is never a valid nonempty length"
            );
            assert!(
                is_nonempty_bounded(1, maximum),
                "one is always a valid nonempty length"
            );
            assert!(
                is_nonempty_bounded(maximum, maximum),
                "the exact ceiling is valid"
            );
            assert!(
                !is_nonempty_bounded(maximum.saturating_add(1), maximum),
                "one past the ceiling is invalid"
            );
        }
        for maximum in [
            MAX_REMOTE_CHANGES,
            MAX_REMOTE_CHANGE_BYTES,
            MAX_INDEXES_PER_DOCUMENT,
            MAX_INDEX_VALUE_BYTES,
        ] {
            assert!(is_at_most(0, maximum), "zero is at most any ceiling");
            assert!(
                is_at_most(maximum, maximum),
                "the exact ceiling is at most itself"
            );
            assert!(
                !is_at_most(maximum.saturating_add(1), maximum),
                "one past the ceiling fails the bound"
            );
        }
    }

    #[test]
    fn remote_page_atomically_materializes_checkpoint_index_event_and_duplicate()
    -> Result<(), Box<dyn Error>> {
        let path = test_path("remote-page");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let document = DocumentId::new();
        let index = IndexId::new();
        let digest = ChangeDigest::new([8; 32]);
        let indexes = [IndexEntry::new(index, IndexValue::Text("remote".into()))];
        let changes = [RemoteChange {
            document,
            digest,
            bytes: b"remote-change",
        }];
        store.install_remote_page(
            scope,
            peer,
            &[RemoteDocument {
                document,
                expected_revision: None,
                snapshot: b"remote-snapshot",
                indexes: &indexes,
                tombstone: false,
            }],
            &changes,
            b"checkpoint-one",
        )?;
        assert_eq!(
            store.checkpoint(scope, peer)?,
            Some(b"checkpoint-one".to_vec()),
            "the checkpoint must materialize with the page"
        );
        assert_eq!(
            store.load_change(scope, document, digest)?,
            Some(b"remote-change".to_vec()),
            "the remote change bytes must be loadable"
        );
        assert_eq!(
            store.changes_since(scope, 0, 10)?.len(),
            1,
            "the page must emit one change event"
        );
        assert_eq!(
            store.query_index(scope, index, &IndexValue::Text("remote".into()), None, 10)?,
            vec![IndexHit { document }],
            "the page must materialize its index row"
        );
        store.install_remote_page(
            scope,
            peer,
            &[RemoteDocument {
                document,
                expected_revision: Some(DocumentRevision::new(1)),
                snapshot: b"remote-snapshot-two",
                indexes: &indexes,
                tombstone: false,
            }],
            &changes,
            b"checkpoint-two",
        )?;
        assert_eq!(
            store.changes_since(scope, 0, 10)?.len(),
            1,
            "a repeated operation must not emit a second event"
        );
        assert_eq!(
            store.checkpoint(scope, peer)?,
            Some(b"checkpoint-two".to_vec()),
            "the duplicate install must still advance the checkpoint"
        );

        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn remote_page_rejects_each_resource_and_document_coverage_violation()
    -> Result<(), Box<dyn Error>> {
        let path = test_path("remote-page-bounds");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let document = DocumentId::new();
        let digest = ChangeDigest::new([8; 32]);
        assert!(
            matches!(
                store.install_remote_page(scope, peer, &[], &[], b""),
                Err(StoreError::InvalidCheckpointSize(0))
            ),
            "an empty checkpoint must be refused"
        );
        let too_many = (0..=256)
            .map(|_| RemoteChange {
                document,
                digest,
                bytes: b"x".as_slice(),
            })
            .collect::<Vec<_>>();
        assert!(
            matches!(
                store.install_remote_page(scope, peer, &[], &too_many, b"checkpoint"),
                Err(StoreError::InvalidRemotePage)
            ),
            "one change past the remote page ceiling must be refused"
        );
        let duplicate_documents = [
            RemoteDocument {
                document,
                expected_revision: None,
                snapshot: b"x",
                indexes: &[],
                tombstone: false,
            },
            RemoteDocument {
                document,
                expected_revision: None,
                snapshot: b"y",
                indexes: &[],
                tombstone: false,
            },
        ];
        assert!(
            matches!(
                store.install_remote_page(scope, peer, &duplicate_documents, &[], b"checkpoint"),
                Err(StoreError::InvalidRemotePage)
            ),
            "a repeated document in one page must be refused"
        );
        let other = DocumentId::new();
        assert!(
            matches!(
                store.install_remote_page(
                    scope,
                    peer,
                    &[RemoteDocument {
                        document,
                        expected_revision: None,
                        snapshot: b"x",
                        indexes: &[],
                        tombstone: false,
                    }],
                    &[RemoteChange {
                        document: other,
                        digest: ChangeDigest::new([9; 32]),
                        bytes: b"x",
                    }],
                    b"checkpoint",
                ),
                Err(StoreError::InvalidRemotePage)
            ),
            "a change without its document must be refused"
        );
        assert_eq!(
            store.checkpoint(scope, peer)?,
            None,
            "every refusal must leave the checkpoint untouched"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn private_key_and_event_codecs_keep_every_axis_distinct() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let operation = OperationId::new();
        let peer = PeerId::new();
        let index = IndexId::new();
        let digest = ChangeDigest::new([1; 32]);
        let keys = [
            key_snapshot(scope, document),
            key_change(scope, document, digest),
            key_operation(scope, operation),
            key_outbox(scope, 1),
            key_sequence(scope),
            key_revision(scope, document),
            key_checkpoint(scope, peer),
            key_outbox_ack(scope),
            key_schema(scope),
            key_event_sequence(scope),
            key_event(scope, 1),
            key_migration(scope),
            key_tombstone(scope, document),
            key_restore(scope),
            key_index_row(scope, index, &[1], document),
            key_index_reverse(scope, document, index),
        ];
        assert!(
            keys.iter().all(|key| key.starts_with(&scope_prefix(scope))),
            "every private key must open with the scope prefix"
        );
        assert_eq!(
            keys.iter().collect::<BTreeSet<_>>().len(),
            keys.len(),
            "every private key must be distinct"
        );
        assert_eq!(
            decode_sequence_key(&key_outbox(scope, 2))?,
            2,
            "the outbox key must decode back to its sequence"
        );
        assert!(
            matches!(
                decode_sequence_key_for_kind(&key_event(scope, 2), KIND_OUTBOX),
                Err(StoreError::CorruptInternalValue)
            ),
            "an event key must not decode as an outbox key"
        );
        assert!(
            matches!(
                decode_sequence_key_for_kind(&[0; 3], KIND_EVENT),
                Err(StoreError::CorruptInternalValue)
            ),
            "a short key must fail closed"
        );
        let event = encode_event(document, digest);
        assert_eq!(
            decode_event(4, &event)?,
            ChangeEvent::Document {
                sequence: 4,
                document,
                digest,
            },
            "the event codec must roundtrip every axis"
        );
        assert!(
            matches!(
                decode_event(4, &[0; EVENT_VALUE_BYTES - 1]),
                Err(StoreError::CorruptInternalValue)
            ),
            "a short event must fail closed"
        );
        let target = schema_value(2, [7; 32]);
        assert_eq!(
            decode_migration(&encode_migration(target, None))?,
            (target, None),
            "a cursorless migration must roundtrip"
        );
        assert_eq!(
            decode_migration(&encode_migration(target, Some(document)))?,
            (target, Some(document)),
            "a cursor migration must roundtrip"
        );
        assert!(
            matches!(decode_migration(&[]), Err(StoreError::CorruptInternalValue)),
            "an empty migration must fail closed"
        );
        let mut invalid_flag = encode_migration(target, None);
        invalid_flag[0] = 2;
        assert!(
            matches!(
                decode_migration(&invalid_flag),
                Err(StoreError::CorruptInternalValue)
            ),
            "an unknown migration flag must fail closed"
        );
        let mut hidden_cursor = encode_migration(target, None);
        hidden_cursor[MIGRATION_VALUE_BYTES - 1] = 1;
        assert!(
            matches!(
                decode_migration(&hidden_cursor),
                Err(StoreError::CorruptInternalValue)
            ),
            "a cursor without the cursor flag must fail closed"
        );
        Ok(())
    }

    #[test]
    fn migration_pages_reject_corrupt_snapshots_and_batch_cardinality_edges()
    -> Result<(), Box<dyn Error>> {
        let path = test_path("migration-corruption");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        store.bind_schema(scope, 1, [1; 32])?;
        store.begin_schema_migration(scope, 2, [2; 32])?;
        {
            let transaction = store.database.begin_write()?;
            {
                let mut table = transaction.open_table(DATA)?;
                table.insert(key_snapshot(scope, document).as_slice(), b"".as_slice())?;
                table.insert(
                    key_revision(scope, document).as_slice(),
                    1_u64.to_be_bytes().as_slice(),
                )?;
            }
            transaction.commit()?;
        }
        assert!(
            matches!(
                store.migration_page(scope, 2, [2; 32], 1),
                Err(StoreError::CorruptInternalValue)
            ),
            "an empty corrupt snapshot must fail closed"
        );
        assert!(
            matches!(
                store.apply_migration_page(scope, 2, [2; 32], None, &[]),
                Err(StoreError::InvalidMigrationPage)
            ),
            "an empty apply must be refused"
        );
        let excessive = (0..=256_u16)
            .map(|value| MigrationIndexes {
                document: DocumentId::from_fixture_u128(u128::from(value).saturating_add(1)),
                expected_revision: DocumentRevision::new(1),
                indexes: &[],
            })
            .collect::<Vec<_>>();
        assert!(
            matches!(
                store.apply_migration_page(scope, 2, [2; 32], None, &excessive),
                Err(StoreError::InvalidMigrationPage)
            ),
            "one row past the migration batch ceiling must be refused"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn tombstone_flag_commits_restarts_and_guards_resurrection() -> Result<(), Box<dyn Error>> {
        let path = test_path("tombstone-flag");
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let replica = ReplicaId::new();
        {
            let store = EmbeddedStore::open(&path, replica)?;
            assert!(
                !store.is_tombstoned(scope, document)?,
                "a fresh document must not start tombstoned"
            );
            assert!(
                !store.is_tombstoned(scope, DocumentId::new())?,
                "an unrelated document must not be tombstoned"
            );
            store.commit_local(LocalCommit {
                change: change(scope, document, OperationId::new(), 1, 2)?,
                expected_revision: None,
                snapshot: b"snapshot",
                indexes: &[],
                tombstone: true,
            })?;
            assert!(
                store.is_tombstoned(scope, document)?,
                "the tombstone commit must mark the document deleted"
            );
            assert!(
                matches!(
                    store.commit_local(LocalCommit {
                        change: change(scope, document, OperationId::new(), 3, 4)?,
                        expected_revision: Some(DocumentRevision::new(1)),
                        snapshot: b"resurrect",
                        indexes: &[],
                        tombstone: false,
                    }),
                    Err(StoreError::DocumentDeleted)
                ),
                "resurrection of a tombstoned document must be refused"
            );
            store.commit_local(LocalCommit {
                change: change(scope, document, OperationId::new(), 5, 6)?,
                expected_revision: Some(DocumentRevision::new(1)),
                snapshot: b"snapshot-two",
                indexes: &[],
                tombstone: true,
            })?;
            assert!(
                store.is_tombstoned(scope, document)?,
                "a second tombstone commit must keep the flag set"
            );
        }
        {
            let store = EmbeddedStore::open(&path, replica)?;
            assert!(
                store.is_tombstoned(scope, document)?,
                "the tombstone flag must survive restart"
            );
            assert_eq!(
                store.outbox_page(scope, 0, 10)?.len(),
                2,
                "both tombstone commits must occupy the outbox"
            );
        }
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn remote_install_sets_tombstone_flag_monotonically() -> Result<(), Box<dyn Error>> {
        let path = test_path("tombstone-remote");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let document = DocumentId::new();
        store.install_remote_page(
            scope,
            peer,
            &[RemoteDocument {
                document,
                expected_revision: None,
                snapshot: b"snapshot",
                indexes: &[],
                tombstone: false,
            }],
            &[RemoteChange {
                document,
                digest: ChangeDigest::new([1; 32]),
                bytes: b"change-one",
            }],
            b"checkpoint-one",
        )?;
        assert!(
            !store.is_tombstoned(scope, document)?,
            "the first live install must not tombstone"
        );
        store.install_remote_page(
            scope,
            peer,
            &[RemoteDocument {
                document,
                expected_revision: Some(DocumentRevision::new(1)),
                snapshot: b"snapshot-two",
                indexes: &[],
                tombstone: true,
            }],
            &[RemoteChange {
                document,
                digest: ChangeDigest::new([2; 32]),
                bytes: b"change-two",
            }],
            b"checkpoint-two",
        )?;
        assert!(
            store.is_tombstoned(scope, document)?,
            "the remote tombstone must set the flag"
        );
        store.install_remote_page(
            scope,
            peer,
            &[RemoteDocument {
                document,
                expected_revision: Some(DocumentRevision::new(2)),
                snapshot: b"snapshot-three",
                indexes: &[],
                tombstone: false,
            }],
            &[RemoteChange {
                document,
                digest: ChangeDigest::new([3; 32]),
                bytes: b"change-three",
            }],
            b"checkpoint-three",
        )?;
        assert!(
            store.is_tombstoned(scope, document)?,
            "a later live page must not clear the tombstone flag"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn export_scan_pages_documents_in_order_with_tombstone_flags() -> Result<(), Box<dyn Error>> {
        let path = test_path("export-scan");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        store.bind_schema(scope, 1, [1; 32])?;
        let documents = [
            DocumentId::from_fixture_u128(3),
            DocumentId::from_fixture_u128(1),
            DocumentId::from_fixture_u128(2),
        ];
        store.commit_local(LocalCommit {
            change: change(scope, documents[0], OperationId::new(), 1, 11)?,
            expected_revision: None,
            snapshot: b"snapshot-three",
            indexes: &[],
            tombstone: false,
        })?;
        store.commit_local(LocalCommit {
            change: change(scope, documents[1], OperationId::new(), 2, 12)?,
            expected_revision: None,
            snapshot: b"snapshot-one",
            indexes: &[],
            tombstone: false,
        })?;
        store.commit_local(LocalCommit {
            change: change(scope, documents[2], OperationId::new(), 3, 13)?,
            expected_revision: None,
            snapshot: b"snapshot-two",
            indexes: &[],
            tombstone: true,
        })?;
        let first = store.export_scan(scope, None, 2)?;
        assert_eq!(
            first.after, None,
            "the first page starts at the cursor origin"
        );
        assert_eq!(first.documents.len(), 2, "the first page holds its limit");
        assert_eq!(
            first.documents[0].document, documents[1],
            "the scan must order by document id"
        );
        assert_eq!(
            first.documents[0].snapshot, b"snapshot-one",
            "the first page must carry the live snapshot"
        );
        assert!(
            !first.documents[0].tombstone,
            "the live document must not be flagged"
        );
        assert_eq!(
            first.documents[1].document, documents[2],
            "the second hit must be the next document"
        );
        assert_eq!(
            first.documents[1].snapshot, b"snapshot-two",
            "the tombstoned document keeps its snapshot bytes"
        );
        assert!(
            first.documents[1].tombstone,
            "the tombstoned document must be flagged"
        );
        let cursor = Some(first.documents[1].document);
        let second = store.export_scan(scope, cursor, 2)?;
        assert_eq!(
            second.after, cursor,
            "the resumed scan must echo its cursor"
        );
        assert_eq!(
            second.documents.len(),
            1,
            "the resumed page holds the remainder"
        );
        assert_eq!(
            second.documents[0].document, documents[0],
            "the resumed page must yield the last document"
        );
        assert!(!second.documents[0].tombstone, "the last document is live");
        let done = store.export_scan(scope, Some(documents[0]), 2)?;
        assert_eq!(
            done.after,
            Some(documents[0]),
            "the final page echoes the last cursor"
        );
        assert!(
            done.documents.is_empty(),
            "a past-the-end scan must be empty"
        );
        assert!(
            matches!(
                store.export_scan(scope, None, 0),
                Err(StoreError::InvalidExportLimit(0))
            ),
            "a zero export limit must be refused"
        );
        assert!(
            matches!(
                store.export_scan(scope, None, 257),
                Err(StoreError::InvalidExportLimit(257))
            ),
            "one past the export ceiling must be refused"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn history_page_keeps_acknowledged_commits_while_outbox_advances() -> Result<(), Box<dyn Error>>
    {
        let path = test_path("export-history");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let document = DocumentId::new();
        let first_operation = OperationId::new();
        let second_operation = OperationId::new();
        store.commit_local(LocalCommit {
            change: change(scope, document, first_operation, 1, 11)?,
            expected_revision: None,
            snapshot: b"snapshot-one",
            indexes: &[],
            tombstone: false,
        })?;
        store.commit_local(LocalCommit {
            change: change(scope, document, second_operation, 2, 12)?,
            expected_revision: Some(DocumentRevision::new(1)),
            snapshot: b"snapshot-two",
            indexes: &[],
            tombstone: false,
        })?;
        store.acknowledge_outbox(scope, peer, 1)?;
        let history = store.history_page(scope, 0, 256)?;
        assert_eq!(history.len(), 2, "history keeps both acknowledged commits");
        assert_eq!(
            history[0].operation, first_operation,
            "history must order by sequence"
        );
        assert_eq!(
            history[0].sequence, 1,
            "the first history row is sequence one"
        );
        assert_eq!(
            history[1].operation, second_operation,
            "the second history row carries its operation"
        );
        assert_eq!(
            history[1].sequence, 2,
            "the second history row is sequence two"
        );
        assert_eq!(
            store.outbox_page(scope, 0, 256)?.len(),
            1,
            "acknowledgement must drain the outbox to one unacked entry"
        );
        assert!(
            matches!(
                store.history_page(scope, 0, 0),
                Err(StoreError::InvalidOutboxLimit(0))
            ),
            "a zero history limit must be refused"
        );
        let remote_digest = ChangeDigest::new([21; 32]);
        store.install_remote_page(
            scope,
            peer,
            &[RemoteDocument {
                document,
                expected_revision: Some(DocumentRevision::new(2)),
                snapshot: b"snapshot-remote",
                indexes: &[],
                tombstone: false,
            }],
            &[RemoteChange {
                document,
                digest: remote_digest,
                bytes: b"remote-change",
            }],
            b"checkpoint-remote",
        )?;
        assert_eq!(
            store.load_change(scope, document, remote_digest)?,
            Some(b"remote-change".to_vec()),
            "the remote change must be loadable"
        );
        assert_eq!(
            store.history_page(scope, 0, 256)?.len(),
            2,
            "the deduplicated remote page must not extend history"
        );
        let events = store.changes_since(scope, 0, 256)?;
        assert_eq!(events.len(), 3, "the feed still records every sequence");
        assert_eq!(
            events[2].digest(),
            Some(remote_digest),
            "the remote event must close the feed"
        );
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn export_codec_round_trips_and_refuses_hostile_inputs() -> Result<(), Box<dyn Error>> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let entries = vec![ExportEntry {
            document,
            operation: OperationId::new(),
            request: OperationDigest::new([7; 32]),
            digest: ChangeDigest::new([9; 32]),
            bytes: b"change".to_vec(),
        }];
        let documents = vec![ExportDocument {
            document,
            tombstone: false,
            snapshot: b"snap".to_vec(),
        }];
        let bytes = encode_export(scope, &entries, &documents)?;
        let (decoded_scope, decoded_entries, decoded_documents) = decode_export(&bytes)?;
        assert_eq!(decoded_scope, scope, "the export must roundtrip its scope");
        assert_eq!(
            decoded_entries, entries,
            "the export must roundtrip its entries"
        );
        assert_eq!(
            decoded_documents, documents,
            "the export must roundtrip its documents"
        );
        assert!(
            decode_export(&encode_export(scope, &[], &[])?)?
                .1
                .is_empty(),
            "an empty export must roundtrip to no entries"
        );
        assert!(
            matches!(
                decode_export(&vec![0_u8; MAX_EXPORT_BYTES.saturating_add(1)]),
                Err(StoreError::InvalidExportSize(_))
            ),
            "one byte past the export ceiling must be refused"
        );
        let mut damaged = bytes.clone();
        damaged[0] ^= 0xff;
        assert!(
            matches!(decode_export(&damaged), Err(StoreError::InvalidExport)),
            "a damaged header must fail closed"
        );
        let mut future = bytes.clone();
        future[4] = 2;
        assert!(
            matches!(
                decode_export(&future),
                Err(StoreError::UnknownExportVersion(2))
            ),
            "an unknown export version must be refused"
        );
        assert!(
            matches!(
                decode_export(&bytes[..bytes.len().saturating_sub(1)]),
                Err(StoreError::InvalidExport)
            ),
            "a truncated export must fail closed"
        );
        let mut trailing = bytes;
        trailing.push(0);
        assert!(
            matches!(decode_export(&trailing), Err(StoreError::InvalidExport)),
            "a trailing byte must fail closed"
        );
        assert!(
            matches!(decode_export(&[]), Err(StoreError::InvalidExport)),
            "an empty export must fail closed"
        );
        Ok(())
    }

    #[test]
    fn restore_cursor_survives_reopen_and_finalizes_revision_checked() -> Result<(), Box<dyn Error>>
    {
        let path = test_path("restore-cursor");
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let digest_a = [7_u8; 32];
        let digest_b = [9_u8; 32];
        {
            let store = EmbeddedStore::open(&path, replica)?;
            assert_eq!(
                store.restore_cursor(scope)?,
                None,
                "a fresh store must hold no restore cursor"
            );
            assert!(
                matches!(
                    store.restore_advance(scope, &digest_a, 1),
                    Err(StoreError::RestoreNotInProgress)
                ),
                "advancing without a restore must fail"
            );
            assert!(
                matches!(
                    store.restore_finish(scope, &digest_a, &[]),
                    Err(StoreError::RestoreNotInProgress)
                ),
                "finishing without a restore must fail"
            );
            assert_eq!(
                store.restore_begin(scope, &digest_a)?,
                RestoreCursor {
                    payload: digest_a,
                    next: 0,
                },
                "begin must open the cursor at zero"
            );
            assert_eq!(
                store.restore_begin(scope, &digest_a)?.next,
                0,
                "rebeginning the same payload must restart at zero"
            );
            assert!(
                matches!(
                    store.restore_begin(scope, &digest_b),
                    Err(StoreError::RestoreInProgress)
                ),
                "beginning another payload mid-restore must fail"
            );
            store.restore_advance(scope, &digest_a, 3)?;
            assert!(
                matches!(
                    store.restore_advance(scope, &digest_b, 4),
                    Err(StoreError::RestoreInProgress)
                ),
                "advancing another payload mid-restore must fail"
            );
        }
        {
            let store = EmbeddedStore::open(&path, replica)?;
            assert_eq!(
                store.restore_cursor(scope)?,
                Some(RestoreCursor {
                    payload: digest_a,
                    next: 3,
                }),
                "the restore cursor must survive restart"
            );
            assert!(
                matches!(
                    store.restore_finish(scope, &digest_b, &[]),
                    Err(StoreError::RestoreInProgress)
                ),
                "finishing a foreign payload mid-restore must fail"
            );
            let document = DocumentId::new();
            store.commit_local(LocalCommit {
                change: change(scope, document, OperationId::new(), 1, 2)?,
                expected_revision: None,
                snapshot: b"snapshot",
                indexes: &[],
                tombstone: false,
            })?;
            assert!(
                matches!(
                    store.restore_finish(scope, &digest_a, &[(document, DocumentRevision::new(2))]),
                    Err(StoreError::RestoreIncomplete)
                ),
                "a wrong final revision must fail the restore"
            );
            assert_eq!(
                store.restore_cursor(scope)?,
                Some(RestoreCursor {
                    payload: digest_a,
                    next: 3,
                }),
                "a failed finish must leave the cursor intact"
            );
            store.restore_finish(scope, &digest_a, &[(document, DocumentRevision::new(1))])?;
            assert_eq!(
                store.restore_cursor(scope)?,
                None,
                "a successful finish must clear the cursor"
            );
        }
        fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn restore_operation_synthesis_is_deterministic_and_committable() -> Result<(), Box<dyn Error>>
    {
        let path = test_path("restore-operation");
        let store = EmbeddedStore::open(&path, ReplicaId::new())?;
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let digest = ChangeDigest::new([4; 32]);
        let (operation, request) = restore_operation(document, digest);
        assert_eq!(
            restore_operation(document, digest),
            (operation, request),
            "restore operation synthesis must be deterministic"
        );
        assert_ne!(
            restore_operation(document, ChangeDigest::new([5; 32])).0,
            operation,
            "a different digest must synthesize a different operation"
        );
        assert_ne!(
            restore_operation(DocumentId::new(), digest).0,
            operation,
            "a different document must synthesize a different operation"
        );
        store.commit_local(LocalCommit {
            change: LocalChange::new(scope, document, operation, request, digest, b"change")?,
            expected_revision: None,
            snapshot: b"snapshot",
            indexes: &[],
            tombstone: false,
        })?;
        drop(store);
        fs::remove_file(path)?;
        Ok(())
    }
}
