//! Durable schema binding, projection, and crash-safe migration.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::collections::BTreeMap;
use std::path::Path;

use lgwks_std::hash::Hasher;
use logical_db_crdt::Document;
use logical_db_model::{DocumentId, DocumentRevision, IndexEntry, ReplicaId, Scope};
use logical_db_store::{EmbeddedStore, MigrationDocument, MigrationIndexes, MigrationPage};

use super::db::Database;
use super::error::DatabaseError;
use super::outcomes::MigrationReport;
use super::projection::Projection;

impl Database {
    /// Opens a replica under an exact durable application schema contract.
    ///
    /// The version and stable `(index, semantics)` projection identities are
    /// hashed and bound to the scope. A changed projection contract is refused
    /// until a migration tool deliberately rewrites materialized rows.
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
            profile: logical_db_model::CapabilityProfile::embedded_local(),
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
            let applied = migrate_schema_batch(
                &store,
                &projections,
                scope,
                schema_version,
                digest,
                batch_size,
            )?;
            let Some(document_count) = applied else { break };
            report.batches = report.batches.saturating_add(1);
            report.documents = report.documents.saturating_add(document_count);
        }
        Ok((
            Self {
                store,
                scope,
                replica,
                projections,
                profile: logical_db_model::CapabilityProfile::embedded_local(),
            },
            report,
        ))
    }
}

/// Projection digest
pub(crate) fn projection_digest(
    schema_version: u32,
    projections: &[Projection],
) -> Result<[u8; 32], DatabaseError> {
    if schema_version == 0 {
        return Err(DatabaseError::InvalidSchemaVersion);
    }
    let mut identities = BTreeMap::new();
    for projection in projections {
        if identities
            .insert(projection.index, projection.semantics)
            .is_some()
        {
            return Err(DatabaseError::DuplicateProjection);
        }
    }
    let mut digest = Hasher::new();
    // v2 binds each index identity to its declared semantics. A v1 binding
    // recorded no projection semantics, so it cannot be assumed to match and
    // fails closed into an explicit migration instead.
    digest.update(b"logical-db-schema-v2");
    digest.update(&schema_version.to_be_bytes());
    for (index, semantics) in identities {
        digest.update(index.as_uuid().as_bytes());
        semantics.encode_into(&mut digest);
    }
    Ok(*digest.finalize().as_bytes())
}

/// Project with
pub(crate) fn project_with(
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
            Ok(Some(value)) => Some(Ok(IndexEntry::new(projection.index, value))),
            Ok(None) => None,
            Err(error) => Some(Err(DatabaseError::Crdt(error))),
        })
        .collect()
}

/// Prepare migration documents
fn prepare_migration_documents(
    projections: &[Projection],
    documents: &[MigrationDocument],
) -> Result<Vec<(DocumentId, DocumentRevision, Vec<IndexEntry>)>, DatabaseError> {
    let mut prepared = Vec::with_capacity(documents.len());
    for document in documents {
        let mut state = Document::load(document.snapshot())?;
        let indexes = project_with(projections, &mut state)?;
        prepared.push((document.document, document.revision, indexes));
    }
    Ok(prepared)
}

/// Migrate schema batch
fn migrate_schema_batch(
    store: &EmbeddedStore,
    projections: &[Projection],
    scope: Scope,
    schema_version: u32,
    digest: [u8; 32],
    batch_size: usize,
) -> Result<Option<usize>, DatabaseError> {
    let page: MigrationPage = store.migration_page(scope, schema_version, digest, batch_size)?;
    if page.documents().is_empty() {
        store.finish_schema_migration(scope, schema_version, digest)?;
        return Ok(None);
    }
    let prepared = prepare_migration_documents(projections, page.documents())?;
    let updates = prepared
        .iter()
        .map(|&(document, revision, ref indexes)| {
            MigrationIndexes::new(document, revision, indexes)
        })
        .collect::<Vec<_>>();
    store.apply_migration_page(scope, schema_version, digest, page.after, &updates)?;
    Ok(Some(updates.len()))
}
