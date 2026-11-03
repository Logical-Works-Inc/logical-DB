//! Replica open paths and the library-managed identity.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::path::Path;

use logical_db_model::{ReplicaId, Scope};

use super::db::Database;
use super::error::DatabaseError;
use super::projection::Projection;

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
    pub(crate) fn open_default_with_schema(
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
    pub(crate) fn open_with_projections(
        path: impl AsRef<Path>,
        scope: Scope,
        replica: ReplicaId,
        projections: Vec<Projection>,
    ) -> Result<Self, DatabaseError> {
        Self::open_with_schema(path, scope, replica, 1, projections)
    }
}
