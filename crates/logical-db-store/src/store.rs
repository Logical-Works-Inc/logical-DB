//! Embedded store struct and open guards; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::decode_uuid;
use crate::error::StoreError;
use crate::keys::{DATA, REPLICA_KEY, key_migration, key_tombstone};
use logical_db_model::{DocumentId, ReplicaId, Scope};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, Table, TableHandle};
use std::path::Path;

/// Cross-platform embedded store backed by redb transactions and file locking.
pub struct EmbeddedStore {
    /// Open redb database backing every durable row.
    pub(crate) database: Database,
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
                .map(|value| decode_uuid(value.value()).map(ReplicaId::from_uuid))
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
}

/// Refuses index-dependent access while a schema migration marker exists.
pub(crate) fn ensure_not_migrating(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
) -> Result<(), StoreError> {
    if table.get(key_migration(scope).as_slice())?.is_some() {
        Err(StoreError::MigrationInProgress)
    } else {
        Ok(())
    }
}

/// Refuses a non-tombstone successor for an already tombstoned document.
pub(crate) fn check_tombstone_transition(
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
pub(crate) fn write_tombstone(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    document: DocumentId,
) -> Result<(), StoreError> {
    table.insert(key_tombstone(scope, document).as_slice(), [1_u8].as_slice())?;
    Ok(())
}
