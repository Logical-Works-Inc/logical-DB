//! Restore cursor implementation; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::{decode_revision, decode_u64, malformed_store};
use crate::error::StoreError;
use crate::keys::{DATA, RESTORE_MARKER_BYTES, key_restore, key_revision};
use crate::store::{EmbeddedStore, ensure_not_migrating};
use crate::types::RestoreCursor;
use logical_db_model::{DocumentId, DocumentRevision, Scope};
use redb::{Durability, ReadableDatabase, ReadableTable, WriteTransaction};

/// Opens an immediate restore write transaction with the active cursor.
///
/// The cursor check and the mutation share one transaction so a crash
/// replays at most one entry; both advance and finish load it the same way.
fn restore_write_cursor(
    store: &EmbeddedStore,
    scope: Scope,
    payload: &[u8; 32],
) -> Result<(WriteTransaction, RestoreCursor), StoreError> {
    let mut transaction = store.database.begin_write()?;
    transaction.set_durability(Durability::Immediate)?;
    let existing = {
        let table = transaction.open_table(DATA)?;
        let existing_value = table.get(key_restore(scope).as_slice())?;
        let existing_value = existing_value.ok_or(StoreError::RestoreNotInProgress)?;
        decode_restore_cursor(existing_value.value())?
    };
    if existing.payload != *payload {
        return Err(StoreError::RestoreInProgress);
    }
    Ok((transaction, existing))
}

impl EmbeddedStore {
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
        let (transaction, _) = restore_write_cursor(self, scope, payload)?;
        let mut table = transaction.open_table(DATA)?;
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
        let (transaction, _) = restore_write_cursor(self, scope, payload)?;
        let mut table = transaction.open_table(DATA)?;
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

/// Encodes one durable restore cursor.
pub(crate) fn encode_restore_cursor(cursor: RestoreCursor) -> [u8; RESTORE_MARKER_BYTES] {
    let mut value = [0_u8; RESTORE_MARKER_BYTES];
    value[..32].copy_from_slice(&cursor.payload);
    value[32..].copy_from_slice(&cursor.next.to_be_bytes());
    value
}

/// Decodes and validates one durable restore cursor.
pub(crate) fn decode_restore_cursor(bytes: &[u8]) -> Result<RestoreCursor, StoreError> {
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
