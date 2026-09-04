//! Embeddable local-first application API.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

pub use logical_db_crdt::{ConflictValue, Editor, Item, Object, ObjectKind, Reader, Scalar};
use logical_db_crdt::{CrdtError, Document, EncodedChange};
pub use logical_db_model::{
    AccountId, ChangeDigest, DocumentId, DocumentRevision, IndexEntry, IndexId, IndexValue,
    OperationDigest, OperationId, PeerId, ReplicaId, Scope, SpaceId,
};
pub use logical_db_platform::PlatformError;
pub use logical_db_store::{ChangeEvent, EXPORT_VERSION, MAX_EXPORT_BYTES, RestoreCursor};
use logical_db_store::{
    CommitOutcome, EmbeddedStore, ExportDocument, ExportEntry, IndexHit, LocalChange, LocalCommit,
    MAX_MATERIALIZED_PAGE_BYTES, MAX_SNAPSHOT_BYTES, MigrationIndexes,
    RemoteChange as StoredRemoteChange, RemoteDocument, StoreError, decode_export, encode_export,
    restore_operation,
};
use logical_db_sync::{
    InboundChange, OutboundChange, ReplicaPort, SyncError, synchronize as run_synchronization,
};
pub use logical_db_sync::{SyncLimits, SyncReport, Transport};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Opt-in production thin-client transport exposed by the same app facade.
#[cfg(feature = "http")]
pub mod http {
    pub use logical_db_http::{
        CredentialError, CredentialProvider, HttpError, HttpTransport, StaticBearer, Url,
    };
}

/// Types required only by hosts implementing a custom synchronization transport.
pub mod transport {
    pub use logical_db_protocol::{
        Checkpoint, PullPage, PullRequest, PushOutcome, PushRequest, PushResponse,
    };
    pub use logical_db_sync::{PullResult, Transport};
}

/// Application-owned embedded replica handle.
pub struct Database {
    store: EmbeddedStore,
    scope: Scope,
    replica: ReplicaId,
    projections: Vec<Projection>,
}

type ProjectionFunction = dyn for<'document> Fn(&mut Reader<'document>) -> Result<Option<IndexValue>, CrdtError>
    + Send
    + Sync;

/// One application-owned materialized secondary-index projection.
#[derive(Clone)]
pub struct Projection {
    index: IndexId,
    function: Arc<ProjectionFunction>,
}

impl Projection {
    /// Creates a projection with a stable schema identity.
    ///
    /// Projection code must be deterministic for the same materialized
    /// document. Changing its meaning requires a new schema migration.
    pub fn new(
        index: IndexId,
        function: impl for<'document> Fn(
            &mut Reader<'document>,
        ) -> Result<Option<IndexValue>, CrdtError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        Self {
            index,
            function: Arc::new(function),
        }
    }

    /// Returns the stable index identity.
    #[must_use]
    pub const fn index(&self) -> IndexId {
        self.index
    }
}

impl Database {
    /// Opens or creates a replica below the native per-user application-data
    /// root and creates only its private parent directories.
    ///
    /// # Errors
    ///
    /// Returns a platform-placement, directory creation, or database failure.
    pub fn open_default(
        application_id: &str,
        scope: Scope,
        replica: ReplicaId,
    ) -> Result<Self, DatabaseError> {
        Self::open_default_with_schema(application_id, scope, replica, 1, Vec::new())
    }

    /// Opens a schema-bound replica at the native per-user application-data path.
    ///
    /// # Errors
    ///
    /// Returns a placement, directory, schema, projection, or storage failure.
    pub fn open_default_with_schema(
        application_id: &str,
        scope: Scope,
        replica: ReplicaId,
        schema_version: u32,
        projections: Vec<Projection>,
    ) -> Result<Self, DatabaseError> {
        let path = logical_db_platform::default_database_path(application_id)?;
        let parent = path.parent().ok_or(DatabaseError::InvalidDefaultPath)?;
        std::fs::create_dir_all(parent)?;
        Self::open_with_schema(path, scope, replica, schema_version, projections)
    }

    /// Opens or creates a durable local replica without a daemon or network.
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::Store`] when the local database cannot open or
    /// initialize with immediate durability.
    pub fn open(
        path: impl AsRef<Path>,
        scope: Scope,
        replica: ReplicaId,
    ) -> Result<Self, DatabaseError> {
        Self::open_with_projections(path, scope, replica, Vec::new())
    }

    /// Opens a replica with its complete deterministic projection set.
    ///
    /// # Errors
    ///
    /// Refuses duplicate index identities or any embedded storage failure.
    pub fn open_with_projections(
        path: impl AsRef<Path>,
        scope: Scope,
        replica: ReplicaId,
        projections: Vec<Projection>,
    ) -> Result<Self, DatabaseError> {
        Self::open_with_schema(path, scope, replica, 1, projections)
    }

    /// Opens a replica under an exact durable application schema contract.
    ///
    /// The version and stable projection identities are hashed and bound to the
    /// scope. A changed projection contract is refused until a migration tool
    /// deliberately rewrites materialized rows.
    ///
    /// # Errors
    ///
    /// Refuses zero versions, duplicate projections, schema drift, or storage
    /// failures.
    pub fn open_with_schema(
        path: impl AsRef<Path>,
        scope: Scope,
        replica: ReplicaId,
        schema_version: u32,
        projections: Vec<Projection>,
    ) -> Result<Self, DatabaseError> {
        let digest = projection_digest(schema_version, &projections)?;
        let store = EmbeddedStore::open(path, replica)?;
        store.bind_schema(scope, schema_version, digest)?;
        Ok(Self {
            store,
            scope,
            replica,
            projections,
        })
    }

    /// Opens a replica and runs or resumes its explicit crash-safe schema/index
    /// migration before returning a usable handle.
    ///
    /// Each bounded batch projects immutable snapshots outside the write
    /// transaction, then atomically replaces complete index sets under revision
    /// checks. A crash or projection failure leaves the durable cursor ready for
    /// an exact-target retry. Normal opens and writes remain excluded until the
    /// final schema swap commits.
    ///
    /// # Errors
    ///
    /// Refuses invalid schemas, duplicate projection identities, an invalid
    /// batch size, a different in-progress target, stale/corrupt durable state,
    /// or a projection failure. The same call may safely resume after failure.
    pub fn migrate_schema(
        path: impl AsRef<Path>,
        scope: Scope,
        replica: ReplicaId,
        schema_version: u32,
        projections: Vec<Projection>,
        batch_size: usize,
    ) -> Result<(Self, MigrationReport), DatabaseError> {
        if !(1..=256).contains(&batch_size) {
            return Err(DatabaseError::InvalidMigrationBatchSize(batch_size));
        }
        let digest = projection_digest(schema_version, &projections)?;
        let store = EmbeddedStore::open(path, replica)?;
        store.begin_schema_migration(scope, schema_version, digest)?;
        let mut report = MigrationReport {
            batches: 0,
            documents: 0,
        };
        loop {
            let page = store.migration_page(scope, schema_version, digest, batch_size)?;
            if page.documents.is_empty() {
                store.finish_schema_migration(scope, schema_version, digest)?;
                break;
            }
            let prepared = page
                .documents
                .iter()
                .map(|document| {
                    let mut state = Document::load(&document.snapshot)?;
                    let indexes = project_with(&projections, &mut state)?;
                    Ok((document.document, document.revision, indexes))
                })
                .collect::<Result<Vec<_>, DatabaseError>>()?;
            let updates = prepared
                .iter()
                .map(|(document, revision, indexes)| MigrationIndexes {
                    document: *document,
                    expected_revision: *revision,
                    indexes,
                })
                .collect::<Vec<_>>();
            store.apply_migration_page(scope, schema_version, digest, page.after, &updates)?;
            report.batches += 1;
            report.documents += updates.len();
        }
        Ok((
            Self {
                store,
                scope,
                replica,
                projections,
            },
            report,
        ))
    }

    /// Returns the logical ownership scope fixed when this handle opened.
    #[must_use]
    pub const fn scope(&self) -> Scope {
        self.scope
    }

    /// Returns this durable installation's replica identity.
    #[must_use]
    pub const fn replica(&self) -> ReplicaId {
        self.replica
    }

    /// Applies one structured CRDT transaction and atomically saves its
    /// snapshot, immutable change, idempotency identity, and outbox entry.
    ///
    /// # Errors
    ///
    /// Returns a typed CRDT or storage failure. Concurrent successors of the
    /// same snapshot return [`DatabaseError::Store`] containing
    /// [`StoreError::StaleDocument`] rather than overwriting state. An exact
    /// retry returns [`EditOutcome::Duplicate`] without rerunning `edit`.
    pub fn edit<T>(
        &self,
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
        edit: impl FnOnce(&mut Editor<'_>) -> Result<T, CrdtError>,
    ) -> Result<EditOutcome<T>, DatabaseError> {
        if let Some(existing) = self.store.operation(self.scope, operation)? {
            if existing.document == document && existing.request == request {
                let bytes = self
                    .store
                    .load_change(self.scope, document, existing.digest)?
                    .ok_or(DatabaseError::MissingChange(existing.digest))?;
                EncodedChange {
                    digest: existing.digest,
                    bytes,
                }
                .validate()?;
                return Ok(EditOutcome::Duplicate(DuplicateReceipt {
                    sequence: existing.sequence,
                    digest: existing.digest,
                }));
            }
            return Err(StoreError::OperationConflict.into());
        }
        let stored = self.store.load_snapshot(self.scope, document)?;
        let expected_revision = stored.as_ref().map(|snapshot| snapshot.revision);
        let mut state = match stored {
            Some(snapshot) => Document::load(&snapshot.bytes)?,
            None => Document::new(),
        };
        if state.is_deleted()? {
            return Err(DatabaseError::DocumentDeleted);
        }
        let (value, mut changes) = state.edit(edit)?;
        if changes.len() != 1 {
            return Err(DatabaseError::ChangeCardinality(changes.len()));
        }
        let change = changes.pop().ok_or(DatabaseError::ChangeCardinality(0))?;
        let indexes = self.project(&mut state)?;
        let snapshot = state.save()?;
        let local_change = LocalChange::new(
            self.scope,
            document,
            operation,
            request,
            change.digest,
            &change.bytes,
        )?;
        let outcome = self.store.commit_local(LocalCommit {
            change: local_change,
            expected_revision,
            snapshot: &snapshot,
            indexes: &indexes,
            tombstone: false,
        })?;
        match outcome {
            CommitOutcome::Applied { sequence } => Ok(EditOutcome::Applied(EditReceipt {
                value,
                sequence,
                digest: change.digest,
                heads: state.heads(),
            })),
            CommitOutcome::Duplicate { sequence } => Ok(EditOutcome::Duplicate(DuplicateReceipt {
                sequence,
                digest: change.digest,
            })),
        }
    }

    /// Commits a first-class tombstone that converges to absent on every
    /// replica, including replicas offline at deletion time.
    ///
    /// The tombstone travels the same single-transaction discipline as an
    /// edit (snapshot, change, operation receipt, outbox, indexes, event) and
    /// carries the same operation-identity exact-retry contract: an exact
    /// retry returns [`DeleteOutcome::Duplicate`] without staging a new
    /// change, while a reused operation identity with different bytes fails
    /// closed. Deleting a never-created document still commits a tombstone so
    /// a late replica cannot resurrect the value. Tombstoned documents keep
    /// their snapshot for conflict inspection while reads report absent.
    ///
    /// # Errors
    ///
    /// Returns a typed CRDT or storage failure. Concurrent successors of the
    /// same snapshot return [`DatabaseError::Store`] containing
    /// [`StoreError::StaleDocument`] rather than overwriting state. An exact
    /// retry returns [`DeleteOutcome::Duplicate`] without staging a new
    /// change.
    pub fn delete(
        &self,
        document: DocumentId,
        operation: OperationId,
        request: OperationDigest,
    ) -> Result<DeleteOutcome, DatabaseError> {
        if let Some(existing) = self.store.operation(self.scope, operation)? {
            if existing.document == document && existing.request == request {
                let bytes = self
                    .store
                    .load_change(self.scope, document, existing.digest)?
                    .ok_or(DatabaseError::MissingChange(existing.digest))?;
                EncodedChange {
                    digest: existing.digest,
                    bytes,
                }
                .validate()?;
                return Ok(DeleteOutcome::Duplicate(DuplicateReceipt {
                    sequence: existing.sequence,
                    digest: existing.digest,
                }));
            }
            return Err(StoreError::OperationConflict.into());
        }
        let stored = self.store.load_snapshot(self.scope, document)?;
        let expected_revision = stored.as_ref().map(|snapshot| snapshot.revision);
        let mut state = match stored {
            Some(snapshot) => Document::load(&snapshot.bytes)?,
            None => Document::new(),
        };
        let mut changes = state.delete()?;
        if changes.len() != 1 {
            return Err(DatabaseError::ChangeCardinality(changes.len()));
        }
        let change = changes.pop().ok_or(DatabaseError::ChangeCardinality(0))?;
        let snapshot = state.save()?;
        let heads = state.heads();
        let local_change = LocalChange::new(
            self.scope,
            document,
            operation,
            request,
            change.digest,
            &change.bytes,
        )?;
        let outcome = self.store.commit_local(LocalCommit {
            change: local_change,
            expected_revision,
            snapshot: &snapshot,
            indexes: &[],
            tombstone: true,
        })?;
        match outcome {
            CommitOutcome::Applied { sequence } => Ok(DeleteOutcome::Applied(DeleteReceipt {
                sequence,
                digest: change.digest,
                heads,
            })),
            CommitOutcome::Duplicate { sequence } => {
                Ok(DeleteOutcome::Duplicate(DuplicateReceipt {
                    sequence,
                    digest: change.digest,
                }))
            }
        }
    }

    /// Reads a materialized document through a caller-supplied projection.
    ///
    /// # Errors
    ///
    /// Returns a typed store or CRDT error. A missing or deleted document
    /// returns `None`; conflict candidates on deleted documents stay
    /// available through [`Database::conflicts`].
    pub fn read<T>(
        &self,
        document: DocumentId,
        project: impl FnOnce(&mut Reader<'_>) -> Result<T, CrdtError>,
    ) -> Result<Option<T>, DatabaseError> {
        match self.store.load_snapshot(self.scope, document)? {
            None => Ok(None),
            Some(snapshot) => {
                let mut state = Document::load(&snapshot.bytes)?;
                if state.is_deleted()? {
                    Ok(None)
                } else {
                    project(&mut state.reader())
                        .map(Some)
                        .map_err(DatabaseError::from)
                }
            }
        }
    }

    /// Returns every causally concurrent value for a map property.
    ///
    /// Unlike [`Database::read`], inspection works on deleted documents:
    /// tombstones keep their snapshot precisely so a concurrent edit made
    /// while offline stays reconcilable after convergence.
    ///
    /// # Errors
    ///
    /// Returns a typed store or CRDT error. A never-created document returns
    /// `None`.
    pub fn conflicts(
        &self,
        document: DocumentId,
        object: &Object,
        property: &str,
    ) -> Result<Option<Vec<ConflictValue>>, DatabaseError> {
        match self.store.load_snapshot(self.scope, document)? {
            None => Ok(None),
            Some(snapshot) => {
                let mut state = Document::load(&snapshot.bytes)?;
                Ok(Some(state.reader().conflicts(object, property)?))
            }
        }
    }

    /// Returns every causally concurrent value at one list or text index.
    ///
    /// Like [`Database::conflicts`], inspection works on deleted documents
    /// and returns `None` only for never-created ones.
    ///
    /// # Errors
    ///
    /// Returns a typed store or CRDT error.
    pub fn conflicts_at(
        &self,
        document: DocumentId,
        object: &Object,
        index: usize,
    ) -> Result<Option<Vec<ConflictValue>>, DatabaseError> {
        match self.store.load_snapshot(self.scope, document)? {
            None => Ok(None),
            Some(snapshot) => {
                let mut state = Document::load(&snapshot.bytes)?;
                Ok(Some(state.reader().conflicts_at(object, index)?))
            }
        }
    }

    /// Returns whether a first-class tombstone is recorded for a document.
    ///
    /// A never-created document reports `false`; only a committed delete
    /// reports `true`.
    ///
    /// # Errors
    ///
    /// Returns a typed store or CRDT error.
    pub fn is_deleted(&self, document: DocumentId) -> Result<bool, DatabaseError> {
        match self.store.load_snapshot(self.scope, document)? {
            None => Ok(false),
            Some(snapshot) => Ok(Document::load(&snapshot.bytes)?.is_deleted()?),
        }
    }

    /// Queries an exact secondary-index value in stable document order.
    ///
    /// # Errors
    ///
    /// Returns a typed storage or cursor-bound failure.
    pub fn query_index(
        &self,
        index: IndexId,
        value: &IndexValue,
        after: Option<DocumentId>,
        limit: usize,
    ) -> Result<Vec<DocumentId>, DatabaseError> {
        self.store
            .query_index(self.scope, index, value, after, limit)
            .map(|hits| {
                hits.into_iter()
                    .map(|IndexHit { document }| document)
                    .collect()
            })
            .map_err(DatabaseError::from)
    }

    /// Reads durable document invalidations after an exclusive cursor.
    ///
    /// This is the lossless subscription primitive: a UI may use an in-process
    /// wakeup for latency, then always catch up from this committed sequence.
    ///
    /// # Errors
    ///
    /// Returns a typed bound, corruption, or storage failure.
    pub fn changes_since(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<Vec<ChangeEvent>, DatabaseError> {
        Ok(self.store.changes_since(self.scope, after, limit)?)
    }

    /// Returns a bounded ordered page of locally committed changes ready for a
    /// synchronization transport.
    ///
    /// # Errors
    ///
    /// Returns a store failure or [`DatabaseError::MissingChange`] if durable
    /// outbox metadata refers to absent change bytes.
    pub fn pending_changes(
        &self,
        after: u64,
        limit: usize,
    ) -> Result<Vec<PendingChange>, DatabaseError> {
        self.store
            .outbox_page(self.scope, after, limit)?
            .into_iter()
            .map(|entry| {
                let committed = self
                    .store
                    .operation(self.scope, entry.operation)?
                    .ok_or(DatabaseError::MissingOperation(entry.operation))?;
                let bytes = self
                    .store
                    .load_change(self.scope, entry.document, entry.digest)?
                    .ok_or(DatabaseError::MissingChange(entry.digest))?;
                EncodedChange {
                    digest: entry.digest,
                    bytes: bytes.clone(),
                }
                .validate()?;
                Ok(PendingChange {
                    sequence: entry.sequence,
                    scope: self.scope,
                    document: entry.document,
                    replica: self.replica,
                    operation: entry.operation,
                    request: committed.request,
                    digest: entry.digest,
                    bytes,
                })
            })
            .collect()
    }

    /// Atomically installs a verified remote page and its opaque peer
    /// checkpoint after CRDT merge.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, CRDT, stale-read, or durable storage error.
    /// No document or checkpoint becomes visible if any entry fails.
    pub fn install_remote_page(
        &self,
        peer: PeerId,
        changes: &[RemoteChange],
        checkpoint: &[u8],
    ) -> Result<(), DatabaseError> {
        let mut grouped: BTreeMap<DocumentId, Vec<EncodedChange>> = BTreeMap::new();
        for change in changes {
            let encoded = EncodedChange {
                digest: change.digest,
                bytes: change.bytes.clone(),
            };
            encoded.validate()?;
            grouped.entry(change.document).or_default().push(encoded);
        }

        let mut prepared = Vec::with_capacity(grouped.len());
        let mut materialized_bytes = 0usize;
        for (document, encoded) in &grouped {
            let stored = self.store.load_snapshot(self.scope, *document)?;
            let expected_revision = stored.as_ref().map(|snapshot| snapshot.revision);
            let mut state = match stored {
                Some(snapshot) => Document::load(&snapshot.bytes)?,
                None => Document::new(),
            };
            state.apply(encoded)?;
            let tombstoned = state.is_deleted()?;
            let indexes = if tombstoned {
                Vec::new()
            } else {
                self.project(&mut state)?
            };
            let snapshot = state.save()?;
            if snapshot.len() > MAX_SNAPSHOT_BYTES {
                return Err(DatabaseError::MaterializationBudgetExceeded);
            }
            materialized_bytes = materialized_bytes
                .checked_add(snapshot.len())
                .ok_or(DatabaseError::MaterializationBudgetExceeded)?;
            if materialized_bytes > MAX_MATERIALIZED_PAGE_BYTES {
                return Err(DatabaseError::MaterializationBudgetExceeded);
            }
            prepared.push((*document, expected_revision, snapshot, indexes, tombstoned));
        }

        let durable_documents = prepared
            .iter()
            .map(
                |(document, expected_revision, snapshot, indexes, tombstoned)| RemoteDocument {
                    document: *document,
                    expected_revision: *expected_revision,
                    snapshot,
                    indexes,
                    tombstone: *tombstoned,
                },
            )
            .collect::<Vec<_>>();
        let durable_changes = changes
            .iter()
            .map(|change| StoredRemoteChange {
                document: change.document,
                digest: change.digest,
                bytes: &change.bytes,
            })
            .collect::<Vec<_>>();
        self.store.install_remote_page(
            self.scope,
            peer,
            &durable_documents,
            &durable_changes,
            checkpoint,
        )?;
        Ok(())
    }

    /// Exports one versioned logical backup of this scope.
    ///
    /// The payload is a versioned logical dump — ordered commit history plus
    /// document snapshots with tombstone flags — never a file copy: the store
    /// refuses foreign files through replica binding, so restore replays
    /// these entries through the commit path instead. Entries follow the
    /// durable event order, so histories that arrived through
    /// synchronization keep their causal positions with deterministic
    /// synthesized operation bindings, and tombstones stay converged after
    /// restore. The
    /// full history, including acknowledged outbox entries, is exported so a
    /// restored replica reproduces outbox order, operation bindings, and event
    /// cursors; replayed outbox entries attribute the restoring installation
    /// as their presenter.
    ///
    /// Restored indexes are recomputed from the target handle's projections:
    /// open the restore target with the same projection set to reproduce
    /// index rows exactly.
    ///
    /// # Errors
    ///
    /// Returns a typed corruption, bound, migration, or storage failure.
    pub fn export(&self) -> Result<Vec<u8>, DatabaseError> {
        let mut bindings = BTreeMap::new();
        let mut after = 0_u64;
        loop {
            let page = self.store.history_page(self.scope, after, 256)?;
            if page.is_empty() {
                break;
            }
            for item in &page {
                let binding = self
                    .store
                    .operation(self.scope, item.operation)?
                    .ok_or(DatabaseError::MissingOperation(item.operation))?;
                bindings
                    .entry((item.document, item.digest))
                    .or_insert((item.operation, binding.request));
                after = item.sequence;
            }
            if page.len() < 256 {
                break;
            }
        }
        let mut entries = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let events = self.store.changes_since(self.scope, cursor, 256)?;
            if events.is_empty() {
                break;
            }
            let complete = events.len() < 256;
            for event in &events {
                let bytes = self
                    .store
                    .load_change(self.scope, event.document, event.digest)?
                    .ok_or(DatabaseError::MissingChange(event.digest))?;
                let (operation, request) = match bindings.get(&(event.document, event.digest)) {
                    Some(binding) => *binding,
                    None => restore_operation(event.document, event.digest),
                };
                entries.push(ExportEntry {
                    document: event.document,
                    operation,
                    request,
                    digest: event.digest,
                    bytes,
                });
                cursor = event.sequence;
            }
            if complete {
                break;
            }
        }
        let mut documents = Vec::new();
        let mut scan = None;
        loop {
            let page = self.store.export_scan(self.scope, scan, 64)?;
            if page.documents.is_empty() {
                break;
            }
            let complete = page.documents.len() < 64;
            for document in &page.documents {
                documents.push(ExportDocument {
                    document: document.document,
                    tombstone: document.tombstone,
                    snapshot: document.snapshot.clone(),
                });
                scan = Some(document.document);
            }
            if complete {
                break;
            }
        }
        Ok(encode_export(self.scope, &entries, &documents)?)
    }

    /// Restores one versioned logical backup into this scope.
    ///
    /// The payload is bounded before allocation and fully validated before
    /// the first commit: unknown versions, oversized inputs, scope mismatch,
    /// malformed structure, and corrupt change or snapshot bytes are refused
    /// without touching live state. Valid entries replay through the commit
    /// path with their original operation identities, so retrying the same
    /// payload short-circuits as exact duplicates.
    ///
    /// An interrupted restore leaves a durable cursor bound to the payload
    /// identity; retrying with the same bytes resumes from the cursor and
    /// completes, while a different payload is refused until the active one
    /// finishes. Finalization verifies every replayed revision before the
    /// cursor clears. Restore into a fresh scope; a target that already holds
    /// divergent documents fails closed at the first conflicting entry with
    /// the cursor retained for inspection.
    ///
    /// # Errors
    ///
    /// Returns a typed export, scope, CRDT, revision, cursor, or storage
    /// failure. No document, outbox, event, or checkpoint row changes unless
    /// the payload validates end to end.
    pub fn restore(&self, payload: &[u8]) -> Result<RestoreReport, DatabaseError> {
        let identity = export_identity(payload);
        let (scope, entries, documents) = decode_export(payload)?;
        if scope != self.scope {
            return Err(DatabaseError::ExportScopeMismatch);
        }
        if entries.is_empty() && documents.is_empty() {
            return Ok(RestoreReport {
                documents: 0,
                entries: 0,
                applied: 0,
                resumed: false,
            });
        }
        let documented = check_restore_coverage(&documents, &entries)?;
        let mut loaded = BTreeMap::new();
        for document in &documented {
            let revision = self
                .store
                .load_snapshot(self.scope, *document)?
                .map(|snapshot| snapshot.revision);
            loaded.insert(*document, revision);
        }
        self.check_restore_replay(&documents, &entries)?;
        if self
            .store
            .restore_cursor(self.scope)?
            .is_some_and(|cursor| cursor.payload != identity)
        {
            return Err(StoreError::RestoreInProgress.into());
        }
        let cursor = self.store.restore_begin(self.scope, &identity)?;
        let resumed = cursor.next > 0;
        let start = usize::try_from(cursor.next).map_err(|_| StoreError::RestoreIncomplete)?;
        if start > entries.len() {
            return Err(StoreError::RestoreIncomplete.into());
        }
        let mut current = loaded;
        let applied = self.replay_restore_entries(&entries, &identity, start, &mut current)?;
        let expected = current
            .into_iter()
            .map(|(document, revision)| {
                revision
                    .map(|revision| (document, revision))
                    .ok_or(StoreError::RestoreIncomplete)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.store
            .restore_finish(self.scope, &identity, &expected)?;
        Ok(RestoreReport {
            documents: documents.len(),
            entries: entries.len(),
            applied,
            resumed,
        })
    }

    fn check_restore_replay(
        &self,
        documents: &[ExportDocument],
        entries: &[ExportEntry],
    ) -> Result<(), DatabaseError> {
        let mut duplicate = BTreeSet::new();
        for entry in entries {
            if let Some(committed) = self.store.operation(self.scope, entry.operation)? {
                if committed.document != entry.document
                    || committed.request != entry.request
                    || committed.digest != entry.digest
                {
                    return Err(StoreError::OperationConflict.into());
                }
                let stored = self
                    .store
                    .load_change(self.scope, entry.document, entry.digest)?
                    .ok_or(DatabaseError::MissingChange(entry.digest))?;
                if stored != entry.bytes {
                    return Err(StoreError::OperationConflict.into());
                }
                duplicate.insert(entry.operation);
            }
        }
        let mut states: BTreeMap<DocumentId, Document> = BTreeMap::new();
        let mut last_flag = BTreeMap::new();
        for entry in entries {
            let state = states.entry(entry.document).or_default();
            state.apply(std::slice::from_ref(&EncodedChange {
                digest: entry.digest,
                bytes: entry.bytes.clone(),
            }))?;
            last_flag.insert(entry.document, state.is_deleted()?);
        }
        for document in documents {
            if last_flag.get(&document.document) != Some(&document.tombstone) {
                return Err(StoreError::InvalidExport.into());
            }
        }
        Ok(())
    }

    fn replay_restore_entries(
        &self,
        entries: &[ExportEntry],
        identity: &[u8; 32],
        start: usize,
        current: &mut BTreeMap<DocumentId, Option<DocumentRevision>>,
    ) -> Result<usize, DatabaseError> {
        let mut states: BTreeMap<DocumentId, Document> = BTreeMap::new();
        let mut applied = 0_usize;
        for (offset, entry) in entries.iter().enumerate() {
            let state = states.entry(entry.document).or_default();
            state.apply(std::slice::from_ref(&EncodedChange {
                digest: entry.digest,
                bytes: entry.bytes.clone(),
            }))?;
            if offset < start {
                continue;
            }
            let tombstoned = state.is_deleted()?;
            let indexes = if tombstoned {
                Vec::new()
            } else {
                self.project(state)?
            };
            let snapshot = state.save()?;
            let expected = current.get(&entry.document).copied().flatten();
            let change = LocalChange::new(
                self.scope,
                entry.document,
                entry.operation,
                entry.request,
                entry.digest,
                &entry.bytes,
            )?;
            let outcome = self.store.commit_local(LocalCommit {
                change,
                expected_revision: expected,
                snapshot: &snapshot,
                indexes: &indexes,
                tombstone: tombstoned,
            })?;
            if matches!(outcome, CommitOutcome::Applied { .. }) {
                let next = match expected {
                    Some(revision) => revision.next().ok_or(StoreError::RevisionExhausted)?,
                    None => DocumentRevision::new(1),
                };
                current.insert(entry.document, Some(next));
                applied += 1;
            }
            let next = u64::try_from(offset + 1).map_err(|_| StoreError::SequenceExhausted)?;
            self.store.restore_advance(self.scope, identity, next)?;
        }
        Ok(applied)
    }

    /// Loads the durable restore cursor for this scope, when one is active.
    ///
    /// A present cursor means a previous restore was interrupted; retrying
    /// [`Database::restore`] with the same payload resumes from it.
    ///
    /// # Errors
    ///
    /// Returns a typed durable read or corruption error.
    pub fn restore_cursor(&self) -> Result<Option<RestoreCursor>, DatabaseError> {
        Ok(self.store.restore_cursor(self.scope)?)
    }

    fn project(&self, state: &mut Document) -> Result<Vec<IndexEntry>, DatabaseError> {
        project_with(&self.projections, state)
    }

    /// Returns the last atomically installed opaque checkpoint for a peer.
    ///
    /// # Errors
    ///
    /// Returns a typed durable read or corruption error.
    pub fn checkpoint(&self, peer: PeerId) -> Result<Option<Vec<u8>>, DatabaseError> {
        Ok(self.store.checkpoint(self.scope, peer)?)
    }

    /// Marks all local outbox entries through an inclusive sequence as
    /// accepted by the configured synchronization service.
    ///
    /// # Errors
    ///
    /// Refuses a mismatched upstream or acknowledgements beyond the local
    /// durable sequence.
    pub fn acknowledge_uploaded(&self, peer: PeerId, through: u64) -> Result<(), DatabaseError> {
        Ok(self.store.acknowledge_outbox(self.scope, peer, through)?)
    }

    /// Runs one bounded push/pull cycle over an application-supplied transport.
    ///
    /// # Errors
    ///
    /// Preserves replica, transport, protocol, rejection, and cancellation
    /// failures. Unknown transport outcomes never acknowledge the local outbox.
    pub async fn synchronize<T: Transport>(
        &self,
        peer: PeerId,
        transport: &mut T,
        limits: SyncLimits,
        cancelled: impl FnMut() -> bool,
    ) -> Result<SyncReport, SyncError<DatabaseError, T::Error>> {
        run_synchronization(self, peer, transport, limits, cancelled).await
    }
}

impl ReplicaPort for Database {
    type Error = DatabaseError;

    fn scope(&self) -> Scope {
        Database::scope(self)
    }

    fn replica(&self) -> ReplicaId {
        Database::replica(self)
    }

    fn bind_upstream(&self, peer: PeerId) -> Result<(), Self::Error> {
        Ok(self.store.bind_upstream(self.scope, peer)?)
    }

    fn pending(&self, limit: usize) -> Result<Vec<OutboundChange>, Self::Error> {
        Database::pending_changes(self, 0, limit).map(|changes| {
            changes
                .into_iter()
                .map(|change| OutboundChange {
                    sequence: change.sequence,
                    document: change.document,
                    operation: change.operation,
                    request: change.request,
                    digest: change.digest,
                    bytes: change.bytes,
                })
                .collect()
        })
    }

    fn acknowledge(&self, peer: PeerId, through: u64) -> Result<(), Self::Error> {
        Database::acknowledge_uploaded(self, peer, through)
    }

    fn checkpoint(&self, peer: PeerId) -> Result<Option<Vec<u8>>, Self::Error> {
        Database::checkpoint(self, peer)
    }

    fn install(
        &self,
        peer: PeerId,
        changes: &[InboundChange],
        checkpoint: &[u8],
    ) -> Result<(), Self::Error> {
        let changes = changes
            .iter()
            .map(|change| RemoteChange {
                document: change.document,
                digest: change.digest,
                bytes: change.bytes.clone(),
            })
            .collect::<Vec<_>>();
        Database::install_remote_page(self, peer, &changes, checkpoint)
    }
}

/// One untrusted immutable remote change entering the embedded replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteChange {
    /// Document receiving the change.
    pub document: DocumentId,
    /// Claimed native CRDT identity, verified before commit.
    pub digest: ChangeDigest,
    /// Canonical CRDT change bytes, bounded before decoding.
    pub bytes: Vec<u8>,
}

/// One verified local outbox change without transport protocol coupling.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingChange {
    /// Private durable acknowledgement position within this scope.
    pub sequence: u64,
    /// Logical ownership scope.
    pub scope: Scope,
    /// Changed document.
    pub document: DocumentId,
    /// Durable installation that originated the change.
    pub replica: ReplicaId,
    /// Retryable logical operation identity.
    pub operation: OperationId,
    /// Canonical caller request identity bound to the operation.
    pub request: OperationDigest,
    /// Immutable CRDT change identity.
    pub digest: ChangeDigest,
    /// Verified canonical Automerge change bytes.
    pub bytes: Vec<u8>,
}

/// Idempotent result of one embedded edit request.
#[derive(Debug)]
pub enum EditOutcome<T> {
    /// This call durably committed the request and carries its closure result.
    Applied(EditReceipt<T>),
    /// The exact request had already committed, so the closure was not run.
    Duplicate(DuplicateReceipt),
}

/// Result of one durable embedded edit.
#[derive(Debug)]
pub struct EditReceipt<T> {
    /// Caller result computed inside the structured edit.
    pub value: T,
    /// Monotonic local outbox sequence.
    pub sequence: u64,
    /// Immutable CRDT change identity.
    pub digest: ChangeDigest,
    /// Causal document frontier after the edit.
    pub heads: Vec<ChangeDigest>,
}

/// Durable receipt returned for an exact retry without rerunning app code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DuplicateReceipt {
    /// Original monotonic local outbox sequence.
    pub sequence: u64,
    /// Original immutable CRDT change identity.
    pub digest: ChangeDigest,
}

/// Idempotent result of one embedded delete request.
#[derive(Debug)]
pub enum DeleteOutcome {
    /// This call durably committed the tombstone.
    Applied(DeleteReceipt),
    /// The exact request had already committed, so no new change was staged.
    Duplicate(DuplicateReceipt),
}

/// Result of one durable embedded delete.
#[derive(Debug)]
pub struct DeleteReceipt {
    /// Monotonic local outbox sequence.
    pub sequence: u64,
    /// Immutable CRDT tombstone change identity.
    pub digest: ChangeDigest,
    /// Causal document frontier after the delete.
    pub heads: Vec<ChangeDigest>,
}

/// Work durably completed by one schema migration or exact resumption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationReport {
    /// Number of atomic index-replacement transactions in this invocation.
    pub batches: usize,
    /// Number of documents reprojected in this invocation.
    pub documents: usize,
}

/// Work durably completed by one versioned restore or exact resumption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

/// Stable failures exposed by the embedded facade.
#[derive(Debug, Error)]
pub enum DatabaseError {
    /// Structured document processing failed.
    #[error("structured document failed: {0}")]
    Crdt(#[from] CrdtError),
    /// Durable local storage failed.
    #[error("local replica storage failed: {0}")]
    Store(#[from] StoreError),
    /// Native per-user placement policy failed.
    #[error("local replica placement failed: {0}")]
    Platform(#[from] PlatformError),
    /// Private replica directory creation failed.
    #[error("local replica directory failed: {0}")]
    Io(#[from] std::io::Error),
    /// Platform helper produced a path with no parent directory.
    #[error("native local replica path has no parent")]
    InvalidDefaultPath,
    /// Two projection functions claimed the same stable index identity.
    #[error("projection set contains a duplicate index identity")]
    DuplicateProjection,
    /// Schema version zero is reserved.
    #[error("application schema version must be nonzero")]
    InvalidSchemaVersion,
    /// Migration pages are bounded to keep transactions and memory predictable.
    #[error("migration batch size {0} is outside 1..=256")]
    InvalidMigrationBatchSize(usize),
    /// One atomic install would retain too much aggregate materialized state.
    #[error("materialized page exceeds the bounded in-memory installation budget")]
    MaterializationBudgetExceeded,
    /// One public edit must commit exactly one immutable causal change.
    #[error("edit produced {0} changes; expected exactly one")]
    ChangeCardinality(usize),
    /// Durable outbox metadata referenced missing immutable change bytes.
    #[error("outbox references missing change {0:?}")]
    MissingChange(ChangeDigest),
    /// Durable outbox metadata referenced a missing exact operation binding.
    #[error("outbox references missing operation {0}")]
    MissingOperation(OperationId),
    /// The document is deleted and only accepts further tombstones.
    ///
    /// Edits are refused on tombstoned documents so a fresh write can never
    /// silently overwrite converged absence; reconcile through
    /// [`Database::conflicts`] instead. Classified as
    /// [`DatabaseErrorKind::Other`].
    #[error("document is deleted and only accepts further tombstones")]
    DocumentDeleted,
    /// A versioned export was produced for a different ownership scope.
    ///
    /// Restoring a foreign scope would interleave two histories, so the
    /// payload is refused before any durable state changes. Classified as
    /// [`DatabaseErrorKind::Other`].
    #[error("logical export scope does not match this replica scope")]
    ExportScopeMismatch,
}

/// Stable recovery decision for application-facing database failures.
///
/// This keeps storage-engine variants private while allowing an embedding layer
/// to distinguish a caller bug from optimistic concurrency and an ambiguous
/// durability result. New categories may be added without exposing internals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DatabaseErrorKind {
    /// One operation identity was reused for a different canonical request.
    OperationConflict,
    /// Another successor committed first; rebuild the edit on current state.
    ConcurrentWrite,
    /// Durability is ambiguous; retry the exact operation and request identity.
    RetryExactRequest,
    /// No specialized recovery contract applies.
    Other,
}

impl DatabaseError {
    /// Classifies recovery without leaking the embedded store's error types.
    #[must_use]
    pub const fn kind(&self) -> DatabaseErrorKind {
        match self {
            Self::Store(StoreError::OperationConflict) => DatabaseErrorKind::OperationConflict,
            Self::Store(StoreError::StaleDocument) => DatabaseErrorKind::ConcurrentWrite,
            Self::Store(StoreError::CommitOutcomeUnknown(_)) => {
                DatabaseErrorKind::RetryExactRequest
            }
            _ => DatabaseErrorKind::Other,
        }
    }
}

fn check_restore_coverage(
    documents: &[ExportDocument],
    entries: &[ExportEntry],
) -> Result<BTreeSet<DocumentId>, DatabaseError> {
    let documented = documents
        .iter()
        .map(|document| document.document)
        .collect::<BTreeSet<_>>();
    let mut covered = BTreeSet::new();
    for entry in entries {
        if !documented.contains(&entry.document) {
            return Err(StoreError::InvalidExport.into());
        }
        covered.insert(entry.document);
    }
    if covered != documented {
        return Err(StoreError::InvalidExport.into());
    }
    let mut bindings = BTreeMap::new();
    for entry in entries {
        let binding = (
            entry.document,
            entry.request,
            entry.digest,
            entry.bytes.clone(),
        );
        if let Some(previous) = bindings.insert(entry.operation, binding.clone()) {
            if previous == binding {
                return Err(StoreError::InvalidExport.into());
            }
            return Err(StoreError::OperationConflict.into());
        }
    }
    for entry in entries {
        EncodedChange {
            digest: entry.digest,
            bytes: entry.bytes.clone(),
        }
        .validate()?;
    }
    for document in documents {
        let mut state = Document::load(&document.snapshot)?;
        if state.is_deleted()? != document.tombstone {
            return Err(StoreError::InvalidExport.into());
        }
    }
    Ok(documented)
}

fn export_identity(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

fn projection_digest(
    schema_version: u32,
    projections: &[Projection],
) -> Result<[u8; 32], DatabaseError> {
    if schema_version == 0 {
        return Err(DatabaseError::InvalidSchemaVersion);
    }
    let mut identities = BTreeSet::new();
    if projections
        .iter()
        .any(|projection| !identities.insert(projection.index))
    {
        return Err(DatabaseError::DuplicateProjection);
    }
    let mut digest = Sha256::new();
    digest.update(b"logical-db-schema-v1");
    digest.update(schema_version.to_be_bytes());
    for identity in identities {
        digest.update(identity.as_uuid().as_bytes());
    }
    Ok(digest.finalize().into())
}

fn project_with(
    projections: &[Projection],
    state: &mut Document,
) -> Result<Vec<IndexEntry>, DatabaseError> {
    if state.is_deleted()? {
        return Ok(Vec::new());
    }
    let mut reader = state.reader();
    projections
        .iter()
        .filter_map(|projection| match (projection.function)(&mut reader) {
            Ok(Some(value)) => Some(Ok(IndexEntry {
                index: projection.index,
                value,
            })),
            Ok(None) => None,
            Err(error) => Some(Err(DatabaseError::Crdt(error))),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, fs};

    use super::*;

    fn path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("logical-db-facade-{}.redb", uuid::Uuid::now_v7()))
    }

    fn remote_page(changes: &[PendingChange]) -> Vec<RemoteChange> {
        changes
            .iter()
            .map(|change| RemoteChange {
                document: change.document,
                digest: change.digest,
                bytes: change.bytes.clone(),
            })
            .collect()
    }

    struct AcceptingTransport;

    impl Transport for AcceptingTransport {
        type Error = Infallible;

        fn push(
            &mut self,
            request: transport::PushRequest,
        ) -> impl std::future::Future<Output = Result<transport::PushResponse, Self::Error>>
        {
            std::future::ready(Ok(transport::PushResponse {
                protocol: logical_db_protocol::PROTOCOL_VERSION,
                outcomes: request
                    .changes
                    .iter()
                    .map(|change| transport::PushOutcome::Accepted {
                        operation: change.operation,
                    })
                    .collect(),
            }))
        }

        fn pull(
            &mut self,
            _request: transport::PullRequest,
        ) -> impl std::future::Future<Output = Result<transport::PullResult, Self::Error>> {
            std::future::ready(Ok(transport::PullResult::Page(transport::PullPage {
                protocol: logical_db_protocol::PROTOCOL_VERSION,
                changes: vec![],
                checkpoint: transport::Checkpoint::from_wire(vec![9]).unwrap(),
                more: false,
            })))
        }
    }

    #[test]
    fn embedded_edit_read_restart_and_outbox_need_no_braid_or_network() {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let document = DocumentId::new();
        let operation = OperationId::new();
        let request = OperationDigest::new([1; 32]);
        {
            let database = Database::open(&path, scope, replica).unwrap();
            let outcome = database
                .edit(document, operation, request, |editor| {
                    editor.put(
                        &Object::root(),
                        "title",
                        Scalar::String("local first".into()),
                    )
                })
                .unwrap();
            let EditOutcome::Applied(receipt) = outcome else {
                panic!("first edit must apply");
            };
            assert_eq!(receipt.sequence, 1);
            assert_eq!(database.pending_changes(0, 10).unwrap().len(), 1);
        }
        {
            let database = Database::open(&path, scope, replica).unwrap();
            let title = database
                .read(document, |state| state.get(&Object::root(), "title"))
                .unwrap();
            assert_eq!(
                title,
                Some(Some(Item::Scalar(Scalar::String("local first".into()))))
            );
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn exact_retry_does_not_rerun_application_code() {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let database = Database::open(&path, scope, ReplicaId::new()).unwrap();
        let document = DocumentId::new();
        let operation = OperationId::new();
        let request = OperationDigest::new([3; 32]);
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let applied = database
            .edit(document, operation, request, |editor| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                editor.put(&Object::root(), "value", Scalar::Integer(1))
            })
            .unwrap();
        let EditOutcome::Applied(receipt) = applied else {
            panic!("first request must apply");
        };
        let duplicate = database
            .edit(document, operation, request, |_editor| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(
            duplicate,
            EditOutcome::Duplicate(DuplicateReceipt { sequence, digest })
                if sequence == receipt.sequence && digest == receipt.digest
        ));
        for (other_document, other_request) in [
            (document, OperationDigest::new([4; 32])),
            (DocumentId::new(), request),
        ] {
            let error = database
                .edit(other_document, operation, other_request, |_editor| Ok(()))
                .unwrap_err();
            assert!(matches!(
                &error,
                DatabaseError::Store(StoreError::OperationConflict)
            ));
            assert_eq!(error.kind(), DatabaseErrorKind::OperationConflict);
        }
        drop(database);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn facade_error_kinds_preserve_recovery_decisions_without_store_types() {
        assert_eq!(
            DatabaseError::Store(StoreError::StaleDocument).kind(),
            DatabaseErrorKind::ConcurrentWrite
        );
        assert_eq!(
            DatabaseError::InvalidSchemaVersion.kind(),
            DatabaseErrorKind::Other
        );
    }

    #[test]
    fn public_synchronization_delegates_every_replica_port_transition() {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let peer = PeerId::new();
        let database = Database::open(&path, scope, replica).unwrap();
        database
            .edit(
                DocumentId::new(),
                OperationId::new(),
                OperationDigest::new([42; 32]),
                |editor| editor.put(&Object::root(), "value", Scalar::Integer(1)),
            )
            .unwrap();
        assert_eq!(ReplicaPort::replica(&database), replica);
        assert_eq!(ReplicaPort::pending(&database, 10).unwrap().len(), 1);
        assert_eq!(ReplicaPort::checkpoint(&database, peer).unwrap(), None);
        let report = futures::executor::block_on(database.synchronize(
            peer,
            &mut AcceptingTransport,
            SyncLimits::default(),
            || false,
        ))
        .unwrap();
        assert_eq!(
            report,
            SyncReport {
                pushed: 1,
                pulled: 0,
                pages: 1,
                full_resync: false,
                more: false,
            }
        );
        assert!(ReplicaPort::pending(&database, 10).unwrap().is_empty());
        assert_eq!(
            ReplicaPort::checkpoint(&database, peer).unwrap(),
            Some(vec![9])
        );
        let second_peer = PeerId::new();
        assert!(matches!(
            futures::executor::block_on(database.synchronize(
                second_peer,
                &mut AcceptingTransport,
                SyncLimits::default(),
                || false,
            )),
            Err(SyncError::Replica(DatabaseError::Store(
                StoreError::UpstreamMismatch {
                    configured,
                    requested,
                }
            ))) if configured == peer && requested == second_peer
        ));
        drop(database);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn secondary_index_replaces_old_value_atomically_and_survives_restart() {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let document = DocumentId::new();
        let index = IndexId::new();
        let projection = Projection::new(index, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        });
        {
            let database =
                Database::open_with_projections(&path, scope, replica, vec![projection.clone()])
                    .unwrap();
            database
                .edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new([10; 32]),
                    |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
                )
                .unwrap();
            assert_eq!(
                database
                    .query_index(index, &IndexValue::Text("note".into()), None, 10)
                    .unwrap(),
                vec![document]
            );
            database
                .edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new([11; 32]),
                    |editor| editor.put(&Object::root(), "kind", Scalar::String("task".into())),
                )
                .unwrap();
            assert!(
                database
                    .query_index(index, &IndexValue::Text("note".into()), None, 10)
                    .unwrap()
                    .is_empty()
            );
        }
        assert!(matches!(
            Database::open(&path, scope, replica),
            Err(DatabaseError::Store(StoreError::SchemaMigrationRequired))
        ));
        let database =
            Database::open_with_projections(&path, scope, replica, vec![projection]).unwrap();
        assert_eq!(
            database
                .query_index(index, &IndexValue::Text("task".into()), None, 10)
                .unwrap(),
            vec![document]
        );
        drop(database);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn facade_schema_migration_resumes_after_projection_failure_without_mixed_indexes() {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let index = IndexId::new();
        let documents = [DocumentId::new(), DocumentId::new()];
        {
            let database = Database::open_with_schema(&path, scope, replica, 1, vec![]).unwrap();
            for (offset, document) in documents.into_iter().enumerate() {
                database
                    .edit(
                        document,
                        OperationId::new(),
                        OperationDigest::new([u8::try_from(offset + 1).unwrap(); 32]),
                        |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
                    )
                    .unwrap();
            }
        }
        assert!(matches!(
            Database::migrate_schema(&path, scope, replica, 2, vec![], 0),
            Err(DatabaseError::InvalidMigrationBatchSize(0))
        ));
        drop(Database::open_with_schema(&path, scope, replica, 1, vec![]).unwrap());

        let fail_once = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let failing = Projection::new(index, {
            let fail_once = Arc::clone(&fail_once);
            move |reader| {
                if fail_once.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    return Err(CrdtError::InvalidProperty);
                }
                Ok(match reader.get(&Object::root(), "kind")? {
                    Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                    _ => None,
                })
            }
        });
        assert!(matches!(
            Database::migrate_schema(&path, scope, replica, 2, vec![failing], 1),
            Err(DatabaseError::Crdt(CrdtError::InvalidProperty))
        ));
        assert!(matches!(
            Database::open_with_schema(&path, scope, replica, 1, vec![]),
            Err(DatabaseError::Store(StoreError::MigrationInProgress))
        ));

        let projection = Projection::new(index, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        });
        let (database, report) =
            Database::migrate_schema(&path, scope, replica, 2, vec![projection], 1).unwrap();
        assert_eq!(
            report,
            MigrationReport {
                batches: 2,
                documents: 2
            }
        );
        let hits = database
            .query_index(index, &IndexValue::Text("note".into()), None, 10)
            .unwrap();
        assert_eq!(hits.len(), documents.len());
        assert!(documents.iter().all(|document| hits.contains(document)));
        drop(database);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn deterministic_state_machine_preserves_indexes_retries_events_and_restarts() {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let index = IndexId::new();
        let projection = Projection::new(index, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        });
        let documents = (1..=8_u128)
            .map(|value| DocumentId::from_uuid(uuid::Uuid::from_u128(value)))
            .collect::<Vec<_>>();
        let mut expected = BTreeMap::new();
        let mut operations = Vec::new();
        let mut database =
            Database::open_with_schema(&path, scope, replica, 1, vec![projection.clone()]).unwrap();
        let mut random = 0x9e37_79b9_7f4a_7c15_u64;
        for step in 1..=128_u8 {
            random ^= random << 7;
            random ^= random >> 9;
            random ^= random << 8;
            let document = documents[usize::try_from(random % 8).unwrap()];
            let kind = if expected.get(&document) == Some(&"note") {
                "task"
            } else {
                "note"
            };
            let operation = OperationId::new();
            let request = OperationDigest::new([step; 32]);
            let outcome = database
                .edit(document, operation, request, |editor| {
                    editor.put(&Object::root(), "kind", Scalar::String(kind.into()))
                })
                .unwrap();
            assert!(matches!(outcome, EditOutcome::Applied(_)));
            expected.insert(document, kind);
            operations.push((document, operation, request));
            if step % 9 == 0 {
                let duplicate = database
                    .edit(document, operation, request, |_editor| {
                        Err::<(), _>(CrdtError::InvalidProperty)
                    })
                    .unwrap();
                assert!(matches!(duplicate, EditOutcome::Duplicate(_)));
            }
            for value in ["note", "task"] {
                let hits = database
                    .query_index(index, &IndexValue::Text(value.into()), None, 256)
                    .unwrap();
                let model = expected
                    .iter()
                    .filter_map(|(document, kind)| (*kind == value).then_some(*document))
                    .collect::<Vec<_>>();
                assert_eq!(hits, model);
            }
            if step % 11 == 0 {
                drop(database);
                database =
                    Database::open_with_schema(&path, scope, replica, 1, vec![projection.clone()])
                        .unwrap();
            }
        }
        assert_eq!(operations.len(), 128);
        let mut cursor = 0;
        let mut observed = 0;
        loop {
            let events = database.changes_since(cursor, 7).unwrap();
            if events.is_empty() {
                break;
            }
            for event in events {
                cursor += 1;
                observed += 1;
                assert_eq!(event.sequence, cursor);
            }
        }
        assert_eq!(observed, 128);
        drop(database);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn offline_replicas_converge_and_checkpoint_install_is_durable() {
        let first_path = path();
        let second_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let first_replica = ReplicaId::new();
        let second_replica = ReplicaId::new();
        {
            let first = Database::open(&first_path, scope, first_replica).unwrap();
            let second = Database::open(&second_path, scope, second_replica).unwrap();
            first
                .edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new([1; 32]),
                    |editor| editor.put(&Object::root(), "title", Scalar::String("A".into())),
                )
                .unwrap();
            second
                .edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new([2; 32]),
                    |editor| editor.put(&Object::root(), "count", Scalar::Integer(2)),
                )
                .unwrap();

            let first_changes = first.pending_changes(0, 10).unwrap();
            let second_changes = second.pending_changes(0, 10).unwrap();
            first
                .install_remote_page(
                    peer,
                    &second_changes
                        .iter()
                        .map(|change| RemoteChange {
                            document: change.document,
                            digest: change.digest,
                            bytes: change.bytes.clone(),
                        })
                        .collect::<Vec<_>>(),
                    b"checkpoint-a",
                )
                .unwrap();
            second
                .install_remote_page(
                    peer,
                    &first_changes
                        .iter()
                        .map(|change| RemoteChange {
                            document: change.document,
                            digest: change.digest,
                            bytes: change.bytes.clone(),
                        })
                        .collect::<Vec<_>>(),
                    b"checkpoint-b",
                )
                .unwrap();

            for database in [&first, &second] {
                assert_eq!(
                    database
                        .read(document, |reader| reader.get(&Object::root(), "title"))
                        .unwrap(),
                    Some(Some(Item::Scalar(Scalar::String("A".into()))))
                );
                assert_eq!(
                    database
                        .read(document, |reader| reader.get(&Object::root(), "count"))
                        .unwrap(),
                    Some(Some(Item::Scalar(Scalar::Integer(2))))
                );
            }
            first.acknowledge_uploaded(peer, 1).unwrap();
            assert!(first.pending_changes(0, 10).unwrap().is_empty());
        }
        let first = Database::open(&first_path, scope, first_replica).unwrap();
        assert_eq!(
            first.checkpoint(peer).unwrap(),
            Some(b"checkpoint-a".to_vec())
        );
        drop(first);
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
    }

    #[test]
    fn delete_syncs_to_absent_and_clears_indexes_on_both_replicas() {
        let first_path = path();
        let second_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let index = IndexId::new();
        let projection = Projection::new(index, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        });
        let first = Database::open_with_projections(
            &first_path,
            scope,
            ReplicaId::new(),
            vec![projection.clone()],
        )
        .unwrap();
        let second = Database::open_with_projections(
            &second_path,
            scope,
            ReplicaId::new(),
            vec![projection],
        )
        .unwrap();
        first
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
            )
            .unwrap();
        let base = first.pending_changes(0, 10).unwrap();
        second
            .install_remote_page(peer, &remote_page(&base), b"checkpoint-base")
            .unwrap();
        assert_eq!(
            second
                .query_index(index, &IndexValue::Text("note".into()), None, 10)
                .unwrap(),
            vec![document]
        );

        let outcome = first
            .delete(document, OperationId::new(), OperationDigest::new([2; 32]))
            .unwrap();
        let DeleteOutcome::Applied(receipt) = outcome else {
            panic!("first delete must apply");
        };
        assert_eq!(receipt.sequence, 2);
        assert!(!receipt.heads.is_empty());
        assert!(
            first
                .read(document, |reader| reader.get(&Object::root(), "kind"))
                .unwrap()
                .is_none()
        );
        assert!(first.is_deleted(document).unwrap());
        assert!(
            first
                .query_index(index, &IndexValue::Text("note".into()), None, 10)
                .unwrap()
                .is_empty()
        );

        let pending = first.pending_changes(0, 10).unwrap();
        assert_eq!(pending.len(), 2);
        second
            .install_remote_page(peer, &remote_page(&pending), b"checkpoint-deleted")
            .unwrap();
        for database in [&first, &second] {
            assert!(
                database
                    .read(document, |reader| reader.get(&Object::root(), "kind"))
                    .unwrap()
                    .is_none()
            );
            assert!(database.is_deleted(document).unwrap());
            assert!(
                database
                    .query_index(index, &IndexValue::Text("note".into()), None, 10)
                    .unwrap()
                    .is_empty()
            );
        }
        drop(first);
        drop(second);
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
    }

    #[test]
    fn offline_replica_and_late_joiner_converge_without_resurrection() {
        let origin_path = path();
        let offline_path = path();
        let late_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let origin = Database::open(&origin_path, scope, ReplicaId::new()).unwrap();
        let offline = Database::open(&offline_path, scope, ReplicaId::new()).unwrap();
        origin
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
            )
            .unwrap();
        let base = origin.pending_changes(0, 10).unwrap();
        offline
            .install_remote_page(peer, &remote_page(&base), b"checkpoint-base")
            .unwrap();
        origin
            .delete(document, OperationId::new(), OperationDigest::new([2; 32]))
            .unwrap();
        let tombstone_page = origin.pending_changes(0, 10).unwrap();
        offline
            .install_remote_page(peer, &remote_page(&tombstone_page), b"checkpoint-deleted")
            .unwrap();
        assert!(
            offline
                .read(document, |reader| reader.get(&Object::root(), "title"))
                .unwrap()
                .is_none()
        );
        let offline_pending = offline.pending_changes(0, 10).unwrap();
        origin
            .install_remote_page(peer, &remote_page(&offline_pending), b"checkpoint-back")
            .unwrap();
        assert!(
            origin
                .read(document, |reader| reader.get(&Object::root(), "title"))
                .unwrap()
                .is_none()
        );

        let late = Database::open(&late_path, scope, ReplicaId::new()).unwrap();
        late.install_remote_page(peer, &remote_page(&tombstone_page), b"checkpoint-late")
            .unwrap();
        assert!(
            late.read(document, |reader| reader.get(&Object::root(), "title"))
                .unwrap()
                .is_none()
        );
        assert!(late.is_deleted(document).unwrap());
        drop(origin);
        drop(offline);
        drop(late);
        fs::remove_file(origin_path).unwrap();
        fs::remove_file(offline_path).unwrap();
        fs::remove_file(late_path).unwrap();
    }

    #[test]
    fn duplicate_delete_short_circuits_exact_retry() {
        let db_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let database = Database::open(&db_path, scope, ReplicaId::new()).unwrap();
        let document = DocumentId::new();
        database
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
            )
            .unwrap();
        let operation = OperationId::new();
        let request = OperationDigest::new([2; 32]);
        let DeleteOutcome::Applied(applied) =
            database.delete(document, operation, request).unwrap()
        else {
            panic!("first delete must apply");
        };
        let duplicate = database.delete(document, operation, request).unwrap();
        assert!(matches!(
            duplicate,
            DeleteOutcome::Duplicate(DuplicateReceipt { sequence, digest })
                if sequence == applied.sequence && digest == applied.digest
        ));
        assert_eq!(database.pending_changes(0, 10).unwrap().len(), 2);
        let conflict = database
            .delete(document, operation, OperationDigest::new([3; 32]))
            .unwrap_err();
        assert!(matches!(
            &conflict,
            DatabaseError::Store(StoreError::OperationConflict)
        ));
        assert_eq!(conflict.kind(), DatabaseErrorKind::OperationConflict);
        drop(database);
        fs::remove_file(db_path).unwrap();
    }

    #[test]
    fn concurrent_edit_and_delete_surface_candidates_with_absent_visible() {
        let first_path = path();
        let second_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let first = Database::open(&first_path, scope, ReplicaId::new()).unwrap();
        let second = Database::open(&second_path, scope, ReplicaId::new()).unwrap();
        first
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("base".into())),
            )
            .unwrap();
        let base = first.pending_changes(0, 10).unwrap();
        second
            .install_remote_page(peer, &remote_page(&base), b"checkpoint-base")
            .unwrap();
        first
            .delete(document, OperationId::new(), OperationDigest::new([2; 32]))
            .unwrap();
        second
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([3; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("rival".into())),
            )
            .unwrap();
        for (source, target, checkpoint) in [
            (&first, &second, b"checkpoint-a".as_slice()),
            (&second, &first, b"checkpoint-b".as_slice()),
        ] {
            let page = source.pending_changes(0, 10).unwrap();
            target
                .install_remote_page(peer, &remote_page(&page), checkpoint)
                .unwrap();
        }
        for database in [&first, &second] {
            assert!(
                database
                    .read(document, |reader| reader.get(&Object::root(), "title"))
                    .unwrap()
                    .is_none()
            );
            assert!(database.is_deleted(document).unwrap());
            let candidates = database
                .conflicts(document, &Object::root(), "title")
                .unwrap()
                .unwrap();
            assert!(
                candidates.iter().any(
                    |candidate| candidate.value == Item::Scalar(Scalar::String("rival".into()))
                )
            );
        }
        drop(first);
        drop(second);
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
    }

    #[test]
    fn restart_between_delete_commit_and_sync_preserves_tombstone() {
        let db_path = path();
        let peer_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let replica = ReplicaId::new();
        {
            let database = Database::open(&db_path, scope, replica).unwrap();
            database
                .edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new([1; 32]),
                    |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
                )
                .unwrap();
            database
                .delete(document, OperationId::new(), OperationDigest::new([2; 32]))
                .unwrap();
        }
        let database = Database::open(&db_path, scope, replica).unwrap();
        assert!(database.is_deleted(document).unwrap());
        assert!(
            database
                .read(document, |reader| reader.get(&Object::root(), "title"))
                .unwrap()
                .is_none()
        );
        let pending = database.pending_changes(0, 10).unwrap();
        assert_eq!(pending.len(), 2);
        let target = Database::open(&peer_path, scope, ReplicaId::new()).unwrap();
        target
            .install_remote_page(peer, &remote_page(&pending), b"checkpoint-restart")
            .unwrap();
        assert!(
            target
                .read(document, |reader| reader.get(&Object::root(), "title"))
                .unwrap()
                .is_none()
        );
        assert!(target.is_deleted(document).unwrap());
        drop(database);
        drop(target);
        fs::remove_file(db_path).unwrap();
        fs::remove_file(peer_path).unwrap();
    }

    #[test]
    fn edit_after_delete_is_refused_without_leaking_store_types() {
        let db_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let database = Database::open(&db_path, scope, ReplicaId::new()).unwrap();
        let document = DocumentId::new();
        database
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
            )
            .unwrap();
        database
            .delete(document, OperationId::new(), OperationDigest::new([2; 32]))
            .unwrap();
        let error = database
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([3; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("rival".into())),
            )
            .unwrap_err();
        assert!(matches!(&error, DatabaseError::DocumentDeleted));
        assert_eq!(error.kind(), DatabaseErrorKind::Other);
        let missing = DocumentId::new();
        assert!(
            database
                .read(missing, |reader| reader.get(&Object::root(), "title"))
                .unwrap()
                .is_none()
        );
        assert!(!database.is_deleted(missing).unwrap());
        assert!(
            database
                .conflicts(missing, &Object::root(), "title")
                .unwrap()
                .is_none()
        );
        let candidates = database
            .conflicts(document, &Object::root(), "title")
            .unwrap()
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].value,
            Item::Scalar(Scalar::String("v1".into()))
        );
        drop(database);
        fs::remove_file(db_path).unwrap();
    }

    #[test]
    fn schema_migration_carries_tombstones_with_empty_indexes() {
        let db_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let document = DocumentId::new();
        let index = IndexId::new();
        let projection = Projection::new(index, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        });
        {
            let database =
                Database::open_with_schema(&db_path, scope, replica, 1, vec![projection]).unwrap();
            database
                .edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new([1; 32]),
                    |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
                )
                .unwrap();
            database
                .delete(document, OperationId::new(), OperationDigest::new([2; 32]))
                .unwrap();
        }
        let (database, report) =
            Database::migrate_schema(&db_path, scope, replica, 2, vec![], 10).unwrap();
        assert_eq!(report.documents, 1);
        assert!(database.is_deleted(document).unwrap());
        assert!(
            database
                .read(document, |reader| reader.get(&Object::root(), "kind"))
                .unwrap()
                .is_none()
        );
        drop(database);
        fs::remove_file(db_path).unwrap();
    }

    #[test]
    fn synchronization_carries_tombstones_through_push_and_checkpoint() {
        let db_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let document = DocumentId::new();
        let database = Database::open(&db_path, scope, ReplicaId::new()).unwrap();
        database
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
            )
            .unwrap();
        database
            .delete(document, OperationId::new(), OperationDigest::new([2; 32]))
            .unwrap();
        let report = futures::executor::block_on(database.synchronize(
            peer,
            &mut AcceptingTransport,
            SyncLimits::default(),
            || false,
        ))
        .unwrap();
        assert_eq!(report.pushed, 2);
        assert_eq!(report.pulled, 0);
        assert!(database.pending_changes(0, 10).unwrap().is_empty());
        assert_eq!(
            ReplicaPort::checkpoint(&database, peer).unwrap(),
            Some(vec![9])
        );
        assert!(
            database
                .read(document, |reader| reader.get(&Object::root(), "title"))
                .unwrap()
                .is_none()
        );
        assert!(database.is_deleted(document).unwrap());
        drop(database);
        fs::remove_file(db_path).unwrap();
    }

    fn kind_projection(index: IndexId) -> Projection {
        Projection::new(index, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        })
    }

    fn kind_entry(index: IndexId, kind: &str) -> Vec<IndexEntry> {
        vec![IndexEntry {
            index,
            value: IndexValue::Text(kind.into()),
        }]
    }

    fn read_kind(database: &Database, document: DocumentId) -> Option<String> {
        database
            .read(document, |reader| reader.get(&Object::root(), "kind"))
            .unwrap()
            .and_then(|item| item)
            .and_then(|item| match item {
                Item::Scalar(Scalar::String(value)) => Some(value),
                _ => None,
            })
    }

    /// Compares the durable outbox cursor content, excluding the presenting
    /// replica: a restored replica attributes replayed entries to its own
    /// installation, while sequence, document, operation, request, digest,
    /// and bytes reproduce exactly.
    fn pending_identity(
        changes: &[PendingChange],
    ) -> Vec<(
        u64,
        DocumentId,
        OperationId,
        OperationDigest,
        ChangeDigest,
        Vec<u8>,
    )> {
        changes
            .iter()
            .map(|change| {
                (
                    change.sequence,
                    change.document,
                    change.operation,
                    change.request,
                    change.digest,
                    change.bytes.clone(),
                )
            })
            .collect()
    }

    #[test]
    fn export_then_restore_reproduces_reads_indexes_and_cursors_exactly() {
        let source_path = path();
        let target_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let index = IndexId::new();
        let first = DocumentId::new();
        let second = DocumentId::new();
        let third = DocumentId::new();
        let source = Database::open_with_projections(
            &source_path,
            scope,
            ReplicaId::new(),
            vec![kind_projection(index)],
        )
        .unwrap();
        for (document, request, kind) in [
            (first, [1; 32], "note"),
            (second, [2; 32], "note"),
            (third, [3; 32], "task"),
            (first, [4; 32], "task"),
        ] {
            source
                .edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new(request),
                    |editor| editor.put(&Object::root(), "kind", Scalar::String(kind.into())),
                )
                .unwrap();
        }
        source
            .delete(second, OperationId::new(), OperationDigest::new([5; 32]))
            .unwrap();
        let source_events = source.changes_since(0, 256).unwrap();
        assert_eq!(source_events.len(), 5);
        assert!(
            source_events
                .windows(2)
                .all(|pair| pair[0].sequence + 1 == pair[1].sequence)
        );
        let source_pending = source.pending_changes(0, 256).unwrap();
        assert_eq!(source_pending.len(), 5);
        let payload = source.export().unwrap();
        assert!(!payload.is_empty());
        assert!(payload.len() <= MAX_EXPORT_BYTES);
        drop(source);

        let target = Database::open_with_projections(
            &target_path,
            scope,
            ReplicaId::new(),
            vec![kind_projection(index)],
        )
        .unwrap();
        let report = target.restore(&payload).unwrap();
        assert_eq!(
            report,
            RestoreReport {
                documents: 3,
                entries: 5,
                applied: 5,
                resumed: false,
            }
        );
        assert_eq!(read_kind(&target, first).as_deref(), Some("task"));
        assert_eq!(read_kind(&target, third).as_deref(), Some("task"));
        assert_eq!(read_kind(&target, second), None);
        assert!(target.is_deleted(second).unwrap());
        assert!(!target.is_deleted(first).unwrap());
        assert!(!target.is_deleted(third).unwrap());
        let mut tasks = target
            .query_index(index, &IndexValue::Text("task".into()), None, 10)
            .unwrap();
        tasks.sort();
        let mut expected_tasks = vec![first, third];
        expected_tasks.sort();
        assert_eq!(tasks, expected_tasks);
        assert!(
            target
                .query_index(index, &IndexValue::Text("note".into()), None, 10)
                .unwrap()
                .is_empty()
        );
        assert_eq!(target.changes_since(0, 256).unwrap(), source_events);
        let restored_pending = target.pending_changes(0, 256).unwrap();
        assert_eq!(
            pending_identity(&restored_pending),
            pending_identity(&source_pending)
        );
        assert!(
            restored_pending
                .iter()
                .all(|change| change.replica == target.replica())
        );
        assert_eq!(target.restore_cursor().unwrap(), None);
        drop(target);
        fs::remove_file(source_path).unwrap();
        fs::remove_file(target_path).unwrap();
    }

    struct RestorePrefix<'a> {
        target_path: &'a std::path::Path,
        replica: ReplicaId,
        scope: Scope,
        index: IndexId,
        payload: &'a [u8],
        identity: &'a [u8; 32],
        prefix: usize,
        first: DocumentId,
    }

    fn replay_restore_prefix(fixture: &RestorePrefix<'_>) -> usize {
        use logical_db_crdt::{Document, EncodedChange};
        use logical_db_store::{EmbeddedStore, LocalChange, LocalCommit, decode_export};

        let store = EmbeddedStore::open(fixture.target_path, fixture.replica).unwrap();
        let (_, entries, _) = decode_export(fixture.payload).unwrap();
        let replayed = entries.len();
        store
            .restore_begin(fixture.scope, fixture.identity)
            .unwrap();
        let mut states: BTreeMap<DocumentId, Document> = BTreeMap::new();
        let mut revisions: BTreeMap<DocumentId, Option<DocumentRevision>> = BTreeMap::new();
        for entry in entries.iter().take(fixture.prefix) {
            let state = states.entry(entry.document).or_default();
            state
                .apply(std::slice::from_ref(&EncodedChange {
                    digest: entry.digest,
                    bytes: entry.bytes.clone(),
                }))
                .unwrap();
            let tombstone = state.is_deleted().unwrap();
            let snapshot = state.save().unwrap();
            let expected = revisions.get(&entry.document).copied().flatten();
            let indexes = kind_entry(
                fixture.index,
                if entry.document == fixture.first {
                    "note"
                } else {
                    "task"
                },
            );
            let change = LocalChange::new(
                fixture.scope,
                entry.document,
                entry.operation,
                entry.request,
                entry.digest,
                &entry.bytes,
            )
            .unwrap();
            store
                .commit_local(LocalCommit {
                    change,
                    expected_revision: expected,
                    snapshot: &snapshot,
                    indexes: &indexes,
                    tombstone,
                })
                .unwrap();
            let next = match expected {
                Some(revision) => revision.next().unwrap(),
                None => DocumentRevision::new(1),
            };
            revisions.insert(entry.document, Some(next));
        }
        store
            .restore_advance(fixture.scope, fixture.identity, fixture.prefix as u64)
            .unwrap();
        replayed
    }

    #[test]
    fn interrupted_restore_resumes_to_completion_on_retry() {
        use sha2::{Digest, Sha256};

        const PREFIX: usize = 2;
        let source_path = path();
        let target_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let index = IndexId::new();
        let first = DocumentId::new();
        let second = DocumentId::new();
        let source = Database::open_with_projections(
            &source_path,
            scope,
            ReplicaId::new(),
            vec![kind_projection(index)],
        )
        .unwrap();
        for (document, request, kind) in [
            (first, [1; 32], "note"),
            (second, [2; 32], "task"),
            (first, [3; 32], "task"),
            (second, [4; 32], "note"),
        ] {
            source
                .edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new(request),
                    |editor| editor.put(&Object::root(), "kind", Scalar::String(kind.into())),
                )
                .unwrap();
        }
        let payload = source.export().unwrap();
        let source_pending = source.pending_changes(0, 256).unwrap();
        let source_events = source.changes_since(0, 256).unwrap();
        drop(source);
        let identity: [u8; 32] = Sha256::digest(&payload).into();

        let replica = ReplicaId::new();
        drop(
            Database::open_with_projections(
                &target_path,
                scope,
                replica,
                vec![kind_projection(index)],
            )
            .unwrap(),
        );
        let replayed = replay_restore_prefix(&RestorePrefix {
            target_path: &target_path,
            replica,
            scope,
            index,
            payload: &payload,
            identity: &identity,
            prefix: PREFIX,
            first,
        });
        assert_eq!(replayed, 4);
        let target = Database::open_with_projections(
            &target_path,
            scope,
            replica,
            vec![kind_projection(index)],
        )
        .unwrap();
        let cursor = target.restore_cursor().unwrap().unwrap();
        assert_eq!(cursor.next, PREFIX as u64);
        let report = target.restore(&payload).unwrap();
        assert_eq!(
            report,
            RestoreReport {
                documents: 2,
                entries: 4,
                applied: 2,
                resumed: true,
            }
        );
        assert_eq!(read_kind(&target, first).as_deref(), Some("task"));
        assert_eq!(read_kind(&target, second).as_deref(), Some("note"));
        let restored_pending = target.pending_changes(0, 256).unwrap();
        assert_eq!(
            pending_identity(&restored_pending),
            pending_identity(&source_pending)
        );
        assert!(
            restored_pending
                .iter()
                .all(|change| change.replica == target.replica())
        );
        assert_eq!(target.changes_since(0, 256).unwrap(), source_events);
        assert_eq!(target.restore_cursor().unwrap(), None);
        drop(target);
        fs::remove_file(source_path).unwrap();
        fs::remove_file(target_path).unwrap();
    }

    #[test]
    fn export_replays_synchronized_changes_with_synthesized_bindings() {
        use logical_db_store::restore_operation;

        let peer_path = path();
        let source_path = path();
        let target_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer_id = PeerId::new();
        let index = IndexId::new();
        let remote_edited = DocumentId::new();
        let remote_deleted = DocumentId::new();
        let local = DocumentId::new();
        let peer = Database::open_with_projections(
            &peer_path,
            scope,
            ReplicaId::new(),
            vec![kind_projection(index)],
        )
        .unwrap();
        peer.edit(
            remote_edited,
            OperationId::new(),
            OperationDigest::new([1; 32]),
            |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
        )
        .unwrap();
        peer.edit(
            remote_deleted,
            OperationId::new(),
            OperationDigest::new([2; 32]),
            |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
        )
        .unwrap();
        peer.delete(
            remote_deleted,
            OperationId::new(),
            OperationDigest::new([3; 32]),
        )
        .unwrap();
        let peer_pending = peer.pending_changes(0, 256).unwrap();
        assert_eq!(peer_pending.len(), 3);
        drop(peer);

        let source = Database::open_with_projections(
            &source_path,
            scope,
            ReplicaId::new(),
            vec![kind_projection(index)],
        )
        .unwrap();
        source
            .edit(
                local,
                OperationId::new(),
                OperationDigest::new([4; 32]),
                |editor| editor.put(&Object::root(), "kind", Scalar::String("task".into())),
            )
            .unwrap();
        source
            .install_remote_page(peer_id, &remote_page(&peer_pending), b"checkpoint-remote")
            .unwrap();
        assert!(source.is_deleted(remote_deleted).unwrap());
        let source_events = source.changes_since(0, 256).unwrap();
        assert_eq!(source_events.len(), 4);
        let payload = source.export().unwrap();
        let source_local_pending = source.pending_changes(0, 256).unwrap();
        assert_eq!(source_local_pending.len(), 1);
        drop(source);

        let target = Database::open_with_projections(
            &target_path,
            scope,
            ReplicaId::new(),
            vec![kind_projection(index)],
        )
        .unwrap();
        let report = target.restore(&payload).unwrap();
        assert_eq!(report.documents, 3);
        assert_eq!(report.entries, 4);
        assert_eq!(report.applied, 4);
        assert!(!report.resumed);
        assert_eq!(read_kind(&target, remote_edited).as_deref(), Some("note"));
        assert_eq!(read_kind(&target, local).as_deref(), Some("task"));
        assert_eq!(read_kind(&target, remote_deleted), None);
        assert!(target.is_deleted(remote_deleted).unwrap());
        assert_eq!(target.changes_since(0, 256).unwrap(), source_events);
        let restored_pending = target.pending_changes(0, 256).unwrap();
        assert_eq!(restored_pending.len(), 4);
        assert_eq!(
            pending_identity(&restored_pending[..1]),
            pending_identity(&source_local_pending)
        );
        for change in &restored_pending[1..] {
            let (operation, request) = restore_operation(change.document, change.digest);
            assert_eq!(change.operation, operation);
            assert_eq!(change.request, request);
        }
        assert_eq!(target.restore_cursor().unwrap(), None);
        drop(target);
        fs::remove_file(peer_path).unwrap();
        fs::remove_file(source_path).unwrap();
        fs::remove_file(target_path).unwrap();
    }

    #[test]
    fn corrupt_backup_is_refused_with_live_state_untouched() {
        use logical_db_store::{decode_export, encode_export};

        let source_path = path();
        let target_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let source = Database::open(&source_path, scope, ReplicaId::new()).unwrap();
        let document = DocumentId::new();
        source
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
            )
            .unwrap();
        let payload = source.export().unwrap();
        drop(source);

        let live = Database::open(&target_path, scope, ReplicaId::new()).unwrap();
        let live_document = DocumentId::new();
        live.edit(
            live_document,
            OperationId::new(),
            OperationDigest::new([9; 32]),
            |editor| editor.put(&Object::root(), "title", Scalar::String("live".into())),
        )
        .unwrap();
        let live_pending = live.pending_changes(0, 256).unwrap();
        let live_events = live.changes_since(0, 256).unwrap();
        let live_title = live
            .read(live_document, |reader| reader.get(&Object::root(), "title"))
            .unwrap();

        let (backup_scope, mut entries, documents) = decode_export(&payload).unwrap();
        entries[0].bytes[0] ^= 0xff;
        let digest_damaged = encode_export(backup_scope, &entries, &documents).unwrap();
        assert!(matches!(
            live.restore(&digest_damaged),
            Err(DatabaseError::Crdt(_))
        ));

        let mut truncated = payload.clone();
        truncated.pop();
        assert!(matches!(
            live.restore(&truncated),
            Err(DatabaseError::Store(_))
        ));

        let mut magic_damaged = payload.clone();
        magic_damaged[0] ^= 0xff;
        assert!(matches!(
            live.restore(&magic_damaged),
            Err(DatabaseError::Store(_))
        ));

        assert_eq!(
            live.read(live_document, |reader| {
                reader.get(&Object::root(), "title")
            })
            .unwrap(),
            live_title
        );
        assert_eq!(live.pending_changes(0, 256).unwrap(), live_pending);
        assert_eq!(live.changes_since(0, 256).unwrap(), live_events);
        assert_eq!(live.restore_cursor().unwrap(), None);
        drop(live);
        fs::remove_file(source_path).unwrap();
        fs::remove_file(target_path).unwrap();
    }

    #[test]
    fn oversized_and_unknown_version_payloads_refused_before_allocation() {
        let source_path = path();
        let target_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let source = Database::open(&source_path, scope, ReplicaId::new()).unwrap();
        source
            .edit(
                DocumentId::new(),
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
            )
            .unwrap();
        let payload = source.export().unwrap();
        drop(source);

        let live = Database::open(&target_path, scope, ReplicaId::new()).unwrap();
        let live_document = DocumentId::new();
        live.edit(
            live_document,
            OperationId::new(),
            OperationDigest::new([9; 32]),
            |editor| editor.put(&Object::root(), "title", Scalar::String("live".into())),
        )
        .unwrap();
        let live_pending = live.pending_changes(0, 256).unwrap();

        let oversized = vec![0xA5_u8; MAX_EXPORT_BYTES + 1];
        assert!(matches!(
            live.restore(&oversized),
            Err(DatabaseError::Store(StoreError::InvalidExportSize(_)))
        ));
        let mut future = payload.clone();
        future[4] = 2;
        assert!(matches!(
            live.restore(&future),
            Err(DatabaseError::Store(StoreError::UnknownExportVersion(2)))
        ));
        assert!(matches!(live.restore(&[]), Err(DatabaseError::Store(_))));

        assert_eq!(live.pending_changes(0, 256).unwrap(), live_pending);
        assert!(
            live.read(live_document, |reader| {
                reader.get(&Object::root(), "title")
            })
            .unwrap()
            .is_some()
        );
        assert_eq!(live.restore_cursor().unwrap(), None);
        drop(live);
        fs::remove_file(source_path).unwrap();
        fs::remove_file(target_path).unwrap();
    }

    #[test]
    fn export_includes_tombstones_and_restored_state_stays_converged() {
        use logical_db_store::decode_export;

        let source_path = path();
        let target_path = path();
        let converge_path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let document = DocumentId::new();
        let source = Database::open(&source_path, scope, ReplicaId::new()).unwrap();
        source
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "title", Scalar::String("v1".into())),
            )
            .unwrap();
        let applied = source
            .delete(document, OperationId::new(), OperationDigest::new([2; 32]))
            .unwrap();
        let DeleteOutcome::Applied(receipt) = applied else {
            panic!("delete must apply");
        };
        let payload = source.export().unwrap();
        let (_, entries, documents) = decode_export(&payload).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0].document, document);
        assert!(documents[0].tombstone);
        assert!(entries.iter().any(|entry| entry.digest == receipt.digest));
        drop(source);

        let target = Database::open(&target_path, scope, ReplicaId::new()).unwrap();
        let report = target.restore(&payload).unwrap();
        assert_eq!(report.documents, 1);
        assert_eq!(report.entries, 2);
        assert!(target.is_deleted(document).unwrap());
        assert!(
            target
                .read(document, |reader| reader.get(&Object::root(), "title"))
                .unwrap()
                .is_none()
        );
        let candidates = target
            .conflicts(document, &Object::root(), "title")
            .unwrap()
            .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].value,
            Item::Scalar(Scalar::String("v1".into()))
        );

        let converged = Database::open(&converge_path, scope, ReplicaId::new()).unwrap();
        let pending = target.pending_changes(0, 10).unwrap();
        converged
            .install_remote_page(peer, &remote_page(&pending), b"checkpoint-converged")
            .unwrap();
        assert!(
            converged
                .read(document, |reader| reader.get(&Object::root(), "title"))
                .unwrap()
                .is_none()
        );
        assert!(converged.is_deleted(document).unwrap());
        drop(target);
        drop(converged);
        fs::remove_file(source_path).unwrap();
        fs::remove_file(target_path).unwrap();
        fs::remove_file(converge_path).unwrap();
    }
}
