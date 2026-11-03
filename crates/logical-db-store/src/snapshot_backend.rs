//! Dedicated redb snapshot backend (#119 row 4).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! [`RedbSnapshotStore`] implements [`super::snapshot_envelope::SnapshotStore`]
//! over its own redb file, separate from the replica file — mirroring how the
//! kernel backend is a dedicated store beside the tape. All identity fields
//! are fixed-width, so the NUL-injection ambiguity class the kernel defends
//! against cannot be constructed here.

use logical_db_model::{IndexId, Scope};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use super::snapshot_envelope::{
    SnapshotEnvelope, SnapshotEnvelopeError, SnapshotMetadata, SnapshotStore,
};

/// Table holding every snapshot record: key to envelope bytes.
const SNAPS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("projection_snapshots_v1");
/// Table holding one latest pointer per scope and projection.
const LATEST: TableDefinition<&[u8], &[u8]> = TableDefinition::new("projection_latest_v1");

/// Key for one snapshot record: fixed-width scope and projection plus hash.
fn snapshot_key(scope: Scope, projection: IndexId, hash: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + 1 + 16 + 1 + 16 + 1 + 32);
    key.extend_from_slice(scope.account().as_uuid().as_bytes().as_slice());
    key.push(0x00);
    key.extend_from_slice(scope.space().as_uuid().as_bytes().as_slice());
    key.push(0x00);
    key.extend_from_slice(projection.as_uuid().as_bytes().as_slice());
    key.push(0x00);
    key.extend_from_slice(hash);
    key
}

/// Key for the latest pointer of a scope and projection.
fn latest_key(scope: Scope, projection: IndexId) -> Vec<u8> {
    let mut key = Vec::with_capacity(16 + 1 + 16 + 1 + 16 + 9);
    key.extend_from_slice(scope.account().as_uuid().as_bytes().as_slice());
    key.push(0x01);
    key.extend_from_slice(scope.space().as_uuid().as_bytes().as_slice());
    key.push(0x01);
    key.extend_from_slice(projection.as_uuid().as_bytes().as_slice());
    key.extend_from_slice(b"::latest");
    key
}

/// Prefix covering every snapshot record of a scope and projection.
fn snapshot_prefix(scope: Scope, projection: IndexId) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(16 + 1 + 16 + 1 + 16 + 1);
    prefix.extend_from_slice(scope.account().as_uuid().as_bytes().as_slice());
    prefix.push(0x00);
    prefix.extend_from_slice(scope.space().as_uuid().as_bytes().as_slice());
    prefix.push(0x00);
    prefix.extend_from_slice(projection.as_uuid().as_bytes().as_slice());
    prefix.push(0x00);
    prefix
}

/// Computes the exclusive upper bound of a key prefix.
fn next_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for byte in end.iter_mut().rev() {
        if *byte < 0xFF {
            *byte = byte.saturating_add(1);
            return Some(end);
        }
    }
    None
}

/// Maximum snapshot records one listing returns. Past the ceiling the
/// listing is refused instead of returning a partial page.
const MAX_SNAPSHOT_LISTING: usize = 4096;

/// Snapshot backend over a dedicated redb file.
#[derive(Debug)]
pub struct RedbSnapshotStore {
    /// The dedicated snapshot database.
    database: Database,
}

impl RedbSnapshotStore {
    /// Opens or creates the snapshot store at `path`, creating both tables
    /// so cold-start reads see empty tables instead of missing ones.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotEnvelopeError::BackendFailed`] when the file cannot
    /// be opened.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, SnapshotEnvelopeError> {
        let database =
            Database::create(path).map_err(|error| SnapshotEnvelopeError::BackendFailed {
                at: "open snapshot store",
                reason: error.to_string(),
            })?;
        let failed = |reason: String| SnapshotEnvelopeError::BackendFailed {
            at: "create snapshot tables",
            reason,
        };
        let transaction = database
            .begin_write()
            .map_err(|error| failed(error.to_string()))?;
        {
            transaction
                .open_table(SNAPS)
                .map_err(|error| failed(error.to_string()))?;
            transaction
                .open_table(LATEST)
                .map_err(|error| failed(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| failed(error.to_string()))?;
        Ok(Self { database })
    }

    /// Resolves the latest pointer to its content hash, or `None` cold-start.
    fn resolve_latest_hash(
        &self,
        scope: Scope,
        projection: IndexId,
    ) -> Result<Option<[u8; 32]>, SnapshotEnvelopeError> {
        let failed = |reason: String| SnapshotEnvelopeError::BackendFailed {
            at: "read latest pointer",
            reason,
        };
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| failed(error.to_string()))?;
        let table = transaction
            .open_table(LATEST)
            .map_err(|error| failed(error.to_string()))?;
        let key = latest_key(scope, projection);
        let Some(record) = table
            .get(key.as_slice())
            .map_err(|error| failed(error.to_string()))?
        else {
            return Ok(None);
        };
        if record.value().len() != 32 {
            return Err(SnapshotEnvelopeError::Tampered {
                at: "resolve_latest_hash",
                reason: format!(
                    "index pointer is not 32 bytes: found {}",
                    record.value().len()
                ),
            });
        }
        let mut hash = [0_u8; 32];
        hash.copy_from_slice(record.value());
        Ok(Some(hash))
    }
}

impl SnapshotStore for RedbSnapshotStore {
    fn save_snapshot(&self, snapshot: &SnapshotEnvelope) -> Result<(), SnapshotEnvelopeError> {
        snapshot.check_version()?;
        let (hash_bytes, encoded) = snapshot.serialize_for_storage()?;
        let failed = |reason: String| SnapshotEnvelopeError::BackendFailed {
            at: "save snapshot",
            reason,
        };
        let transaction = self
            .database
            .begin_write()
            .map_err(|error| failed(error.to_string()))?;
        {
            let mut snaps = transaction
                .open_table(SNAPS)
                .map_err(|error| failed(error.to_string()))?;
            let scope = Scope::new(snapshot.account, snapshot.space);
            snaps
                .insert(
                    snapshot_key(scope, snapshot.projection, &snapshot.hash).as_slice(),
                    encoded.as_slice(),
                )
                .map_err(|error| failed(error.to_string()))?;
            let mut latest = transaction
                .open_table(LATEST)
                .map_err(|error| failed(error.to_string()))?;
            latest
                .insert(
                    latest_key(scope, snapshot.projection).as_slice(),
                    hash_bytes.as_slice(),
                )
                .map_err(|error| failed(error.to_string()))?;
        }
        transaction
            .commit()
            .map_err(|error| failed(error.to_string()))?;
        Ok(())
    }

    fn load_latest_snapshot(
        &self,
        scope: Scope,
        projection: IndexId,
    ) -> Result<Option<SnapshotEnvelope>, SnapshotEnvelopeError> {
        let Some(hash) = self.resolve_latest_hash(scope, projection)? else {
            return Ok(None);
        };
        let key = snapshot_key(scope, projection, &hash);
        let failed = |reason: String| SnapshotEnvelopeError::BackendFailed {
            at: "load snapshot record",
            reason,
        };
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| failed(error.to_string()))?;
        let Some(record) = transaction
            .open_table(SNAPS)
            .map_err(|error| failed(error.to_string()))?
            .get(key.as_slice())
            .map_err(|error| failed(error.to_string()))?
        else {
            return Err(SnapshotEnvelopeError::Tampered {
                at: "load_latest_snapshot",
                reason: String::from("index points to a non-existent snapshot"),
            });
        };
        SnapshotEnvelope::deserialize_from_storage(&hash, record.value()).map(Some)
    }

    fn list_snapshots(
        &self,
        scope: Scope,
        projection: IndexId,
    ) -> Result<Vec<SnapshotMetadata>, SnapshotEnvelopeError> {
        let failed = |reason: String| SnapshotEnvelopeError::BackendFailed {
            at: "list snapshots",
            reason,
        };
        let transaction = self
            .database
            .begin_read()
            .map_err(|error| failed(error.to_string()))?;
        let table = transaction
            .open_table(SNAPS)
            .map_err(|error| failed(error.to_string()))?;
        let prefix = snapshot_prefix(scope, projection);
        let end = next_prefix(&prefix).ok_or_else(|| {
            SnapshotEnvelopeError::Malformed(String::from("snapshot prefix overflow"))
        })?;
        let mut entries = Vec::new();
        for item in table
            .range(prefix.as_slice()..end.as_slice())
            .map_err(|error| failed(error.to_string()))?
        {
            if entries.len() == MAX_SNAPSHOT_LISTING {
                return Err(SnapshotEnvelopeError::BackendFailed {
                    at: "list snapshots",
                    reason: format!("snapshot listing exceeds {MAX_SNAPSHOT_LISTING} records"),
                });
            }
            let (key, value) = item.map_err(|error| failed(error.to_string()))?;
            let Some(hash_start) = key.value().len().checked_sub(32) else {
                return Err(SnapshotEnvelopeError::Malformed(String::from(
                    "snapshot key shorter than hash",
                )));
            };
            let envelope = SnapshotEnvelope::deserialize_from_storage(
                &key.value()[hash_start..],
                value.value(),
            )?;
            let expected = snapshot_key(scope, projection, &envelope.hash);
            if key.value() != expected.as_slice() {
                return Err(SnapshotEnvelopeError::Tampered {
                    at: "list_snapshots",
                    reason: String::from("snapshot identity does not match storage key"),
                });
            }
            if envelope.account != scope.account() || envelope.space != scope.space() {
                continue;
            }
            entries.push(SnapshotMetadata::new(
                envelope.account,
                envelope.space,
                envelope.projection,
                envelope.hash,
                envelope.created_at_secs,
                envelope.last_row,
            ));
        }
        entries.sort_by_key(|entry| (std::cmp::Reverse(entry.created_at_secs), entry.hash));
        Ok(entries)
    }

    fn delete_snapshot(
        &self,
        scope: Scope,
        projection: IndexId,
        hash: &[u8; 32],
    ) -> Result<bool, SnapshotEnvelopeError> {
        let failed = |reason: String| SnapshotEnvelopeError::BackendFailed {
            at: "delete snapshot",
            reason,
        };
        let transaction = self
            .database
            .begin_write()
            .map_err(|error| failed(error.to_string()))?;
        let removed = {
            let mut snaps = transaction
                .open_table(SNAPS)
                .map_err(|error| failed(error.to_string()))?;
            let key = snapshot_key(scope, projection, hash);
            let existed = snaps
                .get(key.as_slice())
                .map_err(|error| failed(error.to_string()))?
                .is_some();
            if !existed {
                return Ok(false);
            }
            snaps
                .remove(key.as_slice())
                .map_err(|error| failed(error.to_string()))?;
            let mut latest = transaction
                .open_table(LATEST)
                .map_err(|error| failed(error.to_string()))?;
            let index_key = latest_key(scope, projection);
            let clears_pointer = latest
                .get(index_key.as_slice())
                .map_err(|error| failed(error.to_string()))?
                .is_some_and(|pointer| pointer.value() == hash);
            if clears_pointer {
                latest
                    .remove(index_key.as_slice())
                    .map_err(|error| failed(error.to_string()))?;
            }
            true
        };
        transaction
            .commit()
            .map_err(|error| failed(error.to_string()))?;
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::super::snapshot_envelope::{SnapshotEnvelope, SnapshotSeal};
    use super::*;
    use logical_db_model::{AccountId, IndexId, SpaceId};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// One unique temporary snapshot store path for a named fixture.
    fn store_path(name: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let sequence = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "ldb-snapstore-{name}-{}-{sequence}.redb",
            std::process::id()
        ))
    }

    /// Builds one sealed envelope over a blob.
    fn sealed(blob: Vec<u8>) -> Result<SnapshotEnvelope, SnapshotEnvelopeError> {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        SnapshotEnvelope::seal(SnapshotSeal {
            scope,
            projection: IndexId::new(),
            semantics: [3_u8; 32],
            checkpoint: [1_u8; 32],
            last_row: [2_u8; 32],
            last_seq: 7,
            state: blob,
            created_at_secs: 1_700_000_000,
        })
    }

    #[test]
    fn backend_save_and_load() -> Result<(), Box<dyn std::error::Error>> {
        let path = store_path("save-load");
        let store = RedbSnapshotStore::open(&path)?;
        let envelope = sealed(vec![3, 4, 5, 6, 7])?;
        let scope = Scope::new(envelope.account, envelope.space);
        store.save_snapshot(&envelope)?;
        let loaded = store
            .load_latest_snapshot(scope, envelope.projection)?
            .ok_or("snapshot should exist")?;
        assert_eq!(loaded, envelope, "load must return the saved envelope");
        loaded.verify()?;
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn backend_missing_returns_none() -> Result<(), Box<dyn std::error::Error>> {
        let path = store_path("missing");
        let store = RedbSnapshotStore::open(&path)?;
        let loaded = store
            .load_latest_snapshot(Scope::new(AccountId::new(), SpaceId::new()), IndexId::new())?;
        assert!(loaded.is_none(), "cold start must answer none");
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn backend_unsealed_rejected_at_save() -> Result<(), Box<dyn std::error::Error>> {
        let path = store_path("unsealed");
        let store = RedbSnapshotStore::open(&path)?;
        let mut envelope = sealed(vec![1])?;
        envelope.state.push(9);
        let error = store.save_snapshot(&envelope);
        assert!(
            error.is_err(),
            "a mutated-after-seal envelope must never persist, got {error:?}"
        );
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn backend_tenant_isolation() -> Result<(), Box<dyn std::error::Error>> {
        let path = store_path("isolation");
        let store = RedbSnapshotStore::open(&path)?;
        let first = sealed(vec![10])?;
        let mut second = sealed(vec![20])?;
        second.account = AccountId::new();
        second.hash = second.compute_hash()?;
        store.save_snapshot(&first)?;
        store.save_snapshot(&second)?;

        let scope = Scope::new(first.account, first.space);
        let loaded = store
            .load_latest_snapshot(scope, first.projection)?
            .ok_or("first snapshot should exist")?;
        assert_eq!(loaded.state, vec![10], "each scope sees only its snapshot");
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn backend_delete_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let path = store_path("delete");
        let store = RedbSnapshotStore::open(&path)?;
        let envelope = sealed(vec![3, 4, 5])?;
        let scope = Scope::new(envelope.account, envelope.space);
        store.save_snapshot(&envelope)?;
        let deleted = store.delete_snapshot(scope, envelope.projection, &envelope.hash)?;
        assert!(deleted, "delete must report the removal");
        let after = store.load_latest_snapshot(scope, envelope.projection)?;
        assert!(after.is_none(), "the snapshot must be gone after delete");
        std::fs::remove_file(&path)?;
        Ok(())
    }
}
