//! Materialized reads, conflicts, index queries, and subscriptions.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use logical_db_crdt::{CrdtError, Document, Reader};
use logical_db_model::{DocumentId, IndexId, IndexValue};
use logical_db_store::{ChangeEvent, IndexHit};

use super::db::Database;
use super::error::DatabaseError;

impl Database {
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
        Ok(self
            .read_with_revision(document, project)?
            .map(|(_, value)| value))
    }

    /// Reads a materialized document and returns its durable document revision.
    ///
    /// The revision can be supplied to [`Database::edit_if_revision`] to make
    /// a read-modify-clear operation refuse a concurrent replacement.
    ///
    /// # Errors
    ///
    /// Returns a typed store or CRDT error. A missing or deleted document
    /// returns `None`.
    pub fn read_with_revision<T>(
        &self,
        document: DocumentId,
        project: impl FnOnce(&mut Reader<'_>) -> Result<T, CrdtError>,
    ) -> Result<Option<(logical_db_model::DocumentRevision, T)>, DatabaseError> {
        match self.store.load_snapshot(self.scope, document)? {
            None => Ok(None),
            Some(snapshot) => {
                let mut state = Document::load(snapshot.bytes())?;
                if state.is_deleted()? {
                    Ok(None)
                } else {
                    project(&mut state.reader())
                        .map(|value| Some((snapshot.revision, value)))
                        .map_err(DatabaseError::from)
                }
            }
        }
    }

    /// Returns every causally concurrent value for a map property.
    ///
    /// Unlike [`Database::read`], inspection works on deleted documents:
    /// tombstones keep their snapshot precisely so a concurrent edit made
    /// while offline stays reconcilable after convergence. Test-only: no
    /// shipped path inspects conflicts directly, so it stays out of the
    /// library surface until an external caller needs it.
    ///
    /// # Errors
    ///
    /// Returns a typed store or CRDT error. A never-created document returns
    /// `None`.
    #[cfg(test)]
    pub(crate) fn conflicts(
        &self,
        document: DocumentId,
        object: &logical_db_crdt::Object,
        property: &str,
    ) -> Result<Option<Vec<logical_db_crdt::ConflictValue>>, DatabaseError> {
        match self.store.load_snapshot(self.scope, document)? {
            None => Ok(None),
            Some(snapshot) => {
                let mut state = Document::load(snapshot.bytes())?;
                Ok(Some(state.reader().conflicts(object, property)?))
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
            Some(snapshot) => Ok(Document::load(snapshot.bytes())?.is_deleted()?),
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
                    .map(|IndexHit { document, .. }| document)
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
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs};

    use logical_db_crdt::{Item, Object, Scalar};
    use logical_db_model::{
        AccountId, IndexValue, OperationDigest, OperationId, ReplicaId, Scope, SpaceId,
    };

    use super::*;
    use crate::identity::decode_identity_id;
    use crate::outcomes::EditOutcome;
    use crate::projection::{Projection, ProjectionSemantics};
    use crate::test_helpers::path;

    #[test]
    fn deterministic_state_machine_preserves_indexes_retries_events_and_restarts()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = path();
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let index = IndexId::new();
        let projection = Projection::new(index, ProjectionSemantics::V1, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        });
        let mut documents = Vec::new();
        for value in 1..=8_u128 {
            documents.push(DocumentId::from_uuid(decode_identity_id(
                &value.to_be_bytes(),
            )?));
        }
        let mut expected = BTreeMap::new();
        let mut operations = Vec::new();
        let mut database =
            Database::open_with_schema(&path, scope, replica, 1, vec![projection.clone()])?;
        let mut random = 0x9e37_79b9_7f4a_7c15_u64;
        for step in 1..=128_u8 {
            random ^= random << 7;
            random ^= random >> 9;
            random ^= random << 8;
            let document = documents[usize::try_from(random % 8)?];
            let kind = if expected.get(&document) == Some(&"note") {
                "task"
            } else {
                "note"
            };
            let operation = OperationId::new();
            let request = OperationDigest::new([step; 32]);
            let outcome = database.edit(document, operation, request, |editor| {
                editor.put(&Object::root(), "kind", Scalar::String(kind.into()))
            })?;
            assert!(matches!(outcome, EditOutcome::Applied(_)));
            expected.insert(document, kind);
            operations.push((document, operation, request));
            if step % 9 == 0 {
                let duplicate = database.edit(document, operation, request, |_editor| {
                    Err::<(), _>(CrdtError::InvalidProperty)
                })?;
                assert!(matches!(duplicate, EditOutcome::Duplicate(_)));
            }
            for value in ["note", "task"] {
                let hits =
                    database.query_index(index, &IndexValue::Text(value.into()), None, 256)?;
                let model = expected
                    .iter()
                    .filter_map(|(document, kind)| (*kind == value).then_some(*document))
                    .collect::<Vec<_>>();
                assert_eq!(hits, model);
            }
            if step % 11 == 0 {
                drop(database);
                database =
                    Database::open_with_schema(&path, scope, replica, 1, vec![projection.clone()])?;
            }
        }
        assert_eq!(operations.len(), 128);
        let mut cursor = 0;
        let mut observed = 0;
        loop {
            let events = database.changes_since(cursor, 7)?;
            if events.is_empty() {
                break;
            }
            for event in events {
                cursor += 1;
                observed += 1;
                assert_eq!(event.sequence(), cursor);
            }
        }
        assert_eq!(observed, 128);
        drop(database);
        fs::remove_file(path)?;
        Ok(())
    }
}
