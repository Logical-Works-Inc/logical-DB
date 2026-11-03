//! Outbox cursor implementation; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::{collect_outbox_page, decode_u64, decode_uuid};
use crate::error::StoreError;
use crate::keys::{
    DATA, KIND_CHECKPOINT, KIND_SEQUENCE, MAX_CHECKPOINT_BYTES, is_nonempty_bounded,
    key_checkpoint, key_outbox, key_outbox_ack, key_sequence, key_upstream, next_prefix, prefix,
};
use crate::store::{EmbeddedStore, ensure_not_migrating};
use crate::types::OutboxEntry;
use logical_db_model::{PeerId, Scope};
use redb::{Durability, ReadableDatabase, ReadableTable, Table};

/// Loads one durable sequence counter, starting a fresh scope at zero.
///
/// The outbox acknowledgement, the committed sequence, and the legacy
/// migration paths all read counters that are absent before the first
/// write, so the missing-key default exists once instead of once per reader.
fn load_counter(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    key: &[u8],
) -> Result<u64, StoreError> {
    let mut value = 0;
    if let Some(next) = table
        .get(key)?
        .map(|value| decode_u64(value.value()))
        .transpose()?
    {
        value = next;
    }
    Ok(value)
}

impl EmbeddedStore {
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
        crate::error::require_page_limit(limit, StoreError::InvalidOutboxLimit)?;
        if after == u64::MAX {
            return Ok(Vec::new());
        }
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        let acknowledged = load_counter(&table, key_outbox_ack(scope).as_slice())?;
        let start_after = after.max(acknowledged);
        collect_outbox_history(&table, scope, start_after, limit)
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
        let highest = load_counter(&table, key_sequence(scope).as_slice())?;
        if through > highest {
            let error = StoreError::InvalidAcknowledgement { through, highest };
            return Err(error);
        }
        let current = load_counter(&table, key_outbox_ack(scope).as_slice())?;
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

    /// Reads one bounded outbox-ordered history page without acknowledgement
    /// filtering, so an export replays every commit while the transport
    /// cursor only advances past acknowledged ones.
    ///
    /// # Errors
    ///
    /// Returns a typed validation, decode, read, or table error.
    pub(crate) fn outbox_history_page(
        &self,
        scope: Scope,
        after: u64,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, StoreError> {
        crate::error::require_page_limit(limit, StoreError::InvalidOutboxLimit)?;
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(DATA)?;
        collect_outbox_history(&table, scope, after, limit)
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
}

/// Shared tail of every outbox-ordered page: empty on a saturated cursor,
/// otherwise one bounded collect over the sequence-keyed outbox range.
/// History and acknowledgement-filtered readers share this tail instead of
/// repeating the cursor guard and range construction.
fn collect_outbox_history(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    scope: Scope,
    after: u64,
    limit: usize,
) -> Result<Vec<OutboxEntry>, StoreError> {
    if after == u64::MAX {
        return Ok(Vec::new());
    }
    let start = key_outbox(
        scope,
        after.checked_add(1).ok_or(StoreError::SequenceExhausted)?,
    );
    let end = prefix(scope, KIND_SEQUENCE);
    collect_outbox_page(table, start.as_slice(), end.as_slice(), limit)
}

/// Binds or validates the scope's single durable upstream inside a write transaction.
pub(crate) fn bind_upstream_in_table(
    table: &mut Table<'_, &[u8], &[u8]>,
    scope: Scope,
    requested: PeerId,
) -> Result<(), StoreError> {
    let upstream_key = key_upstream(scope);
    if let Some(value) = table.get(upstream_key.as_slice())? {
        let configured = decode_uuid(value.value()).map(PeerId::from_uuid)?;
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
            decode_uuid(&key.value()[checkpoint_prefix.len()..]).map(PeerId::from_uuid)
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    let configured = match *legacy_peers.as_slice() {
        [] => {
            let acknowledged = load_counter(table, key_outbox_ack(scope).as_slice())?;
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
