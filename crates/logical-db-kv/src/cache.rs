//! Public cache operations: open, put, get, delete, scan, recency, eviction, sync.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Mutex, atomic::AtomicU64},
    time::Duration,
};

use logical_db::{ChangeEvent, OperationId, RemoteChange};

use crate::{
    BatchPut, BlobReference, CacheEntry, Database, DocumentId, KV_SCHEMA_VERSION, KvCache, KvError,
    MAX_BATCH_PUT, MAX_PAGE_LIMIT, MAX_TIERED_VALUE_BYTES, MigrationReport, NamespaceCheckpoints,
    PeerId, ReplicaId, Scope, TIER_THRESHOLD_BYTES,
    blob::{blob_dir_for, content_digest},
    envelope::{Decoded, SlotValue, kv_projections},
    request::{
        batch_request_digest, batch_sequence, checkpoint_map_error, document_for, expiry_for,
        now_ms, operation_started_at_ms, seed_sequence, validate_key, validate_namespace,
    },
};

impl KvCache {
    /// Opens or creates the cache in `namespace` with the schema-bound
    /// expiry and recency projections.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidNamespace`] for a bad namespace, or
    /// [`KvError::Database`] when the replica cannot open or its bound
    /// schema differs (migrate through [`KvCache::migrate_schema`]).
    pub fn open(
        path: impl AsRef<Path>,
        scope: Scope,
        replica: ReplicaId,
        namespace: &str,
    ) -> Result<Self, KvError> {
        validate_namespace(namespace)?;
        let database = Database::open_with_schema(
            path.as_ref(),
            scope,
            replica,
            KV_SCHEMA_VERSION,
            kv_projections()?,
        )?;
        Self::assemble(database, namespace, path.as_ref())
    }

    /// Wraps an opened replica with the cache's namespace, recency seed, and
    /// blob directory.
    fn assemble(database: Database, namespace: &str, path: &Path) -> Result<Self, KvError> {
        Ok(Self {
            database,
            namespace: namespace.to_owned(),
            sequence: AtomicU64::new(seed_sequence()?),
            blob_dir: blob_dir_for(path),
            blob_lock: Mutex::new(()),
        })
    }

    /// Installs the cache projections on an existing replica through the
    /// bounded resumable schema-migration path, preserving every document.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidNamespace`] for a bad namespace, or
    /// [`KvError::Database`] for an invalid batch size, a projection
    /// failure, or any durable storage failure.
    pub fn migrate_schema(
        path: impl AsRef<Path>,
        scope: Scope,
        replica: ReplicaId,
        namespace: &str,
        batch_size: usize,
    ) -> Result<(Self, MigrationReport), KvError> {
        validate_namespace(namespace)?;
        let (database, report) = Database::migrate_schema(
            path.as_ref(),
            scope,
            replica,
            KV_SCHEMA_VERSION,
            kv_projections()?,
            batch_size,
        )?;
        Ok((Self::assemble(database, namespace, path.as_ref())?, report))
    }

    /// Returns the namespace this cache reads and writes.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the embedded replica below this cache for sync orchestration
    /// and index inspection.
    #[must_use]
    pub const fn database(&self) -> &Database {
        &self.database
    }

    /// Stores `value` under `key` with an optional time-to-live, generating
    /// the document and operation identity internally.
    ///
    /// A `None` TTL never expires. Re-putting a key replaces its value,
    /// expiry, and recency through one atomic edit. Values at or under
    /// [`TIER_THRESHOLD_BYTES`] stay inline; larger values tier to the blob
    /// store with only a sidecar reference inside the bounded change.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidKey`] for a bad key,
    /// [`KvError::BlobTooLarge`] before committing anything when the value
    /// exceeds [`MAX_TIERED_VALUE_BYTES`], or [`KvError::Database`] and
    /// [`KvError::BlobIo`] for durable failures.
    pub fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KvError> {
        validate_key(key)?;
        if value.len() > MAX_TIERED_VALUE_BYTES {
            return Err(KvError::BlobTooLarge {
                actual: value.len(),
                maximum: MAX_TIERED_VALUE_BYTES,
            });
        }
        let _blob_guard = self.lock_blobs();
        let now = now_ms()?;
        self.put_absolute(key, value, expiry_for(ttl, now), self.next_sequence(now))?;
        Ok(())
    }

    /// Stores a batch of keys atomically: every entry validates and materializes
    /// before one durable transaction commits all snapshots, indexes, and
    /// outbox entries. Blob bytes are staged before that transaction, so a
    /// failed commit can leave only unreferenced files for [`Self::sweep_blobs`]
    /// to reclaim.
    ///
    /// Duplicate keys within one batch resolve last-wins in batch order.
    /// A concurrent document successor makes the complete batch fail with no
    /// partial durable application; the caller can retry after reconciling the
    /// stale input. This convenience method generates a fresh operation
    /// identity; use [`KvCache::put_many_with_operation`] to retry an
    /// ambiguous result exactly.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidBatchSize`] for an empty or oversized batch,
    /// [`KvError::InvalidKey`] or [`KvError::BlobTooLarge`] before committing
    /// anything when any entry is invalid, or [`KvError::Database`] and
    /// [`KvError::BlobIo`] when a commit fails.
    pub fn put_many(&self, entries: &[BatchPut]) -> Result<(), KvError> {
        self.put_many_with_operation(OperationId::new(), entries)
    }

    /// Stores a batch using a caller-owned retry identity.
    ///
    /// Reusing `operation` with the same entries returns the original durable
    /// result without appending another change. The `UUIDv7` operation timestamp
    /// anchors relative TTLs and recency across exact retries. Reusing the
    /// identity with different entries is rejected as an operation conflict.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidOperationId`] for a non-UUIDv7 operation,
    /// [`KvError::InvalidBatchSize`] for an empty or oversized batch,
    /// [`KvError::InvalidKey`] or [`KvError::BlobTooLarge`] before committing
    /// anything when any entry is invalid, or [`KvError::Database`] and
    /// [`KvError::BlobIo`] when a commit or blob operation fails.
    pub fn put_many_with_operation(
        &self,
        operation: OperationId,
        entries: &[BatchPut],
    ) -> Result<(), KvError> {
        if entries.is_empty() || entries.len() > MAX_BATCH_PUT {
            return Err(KvError::InvalidBatchSize(entries.len()));
        }
        let started_at_ms = operation_started_at_ms(&operation)?;
        for entry in entries {
            validate_key(&entry.key)?;
            if entry.value.len() > MAX_TIERED_VALUE_BYTES {
                return Err(KvError::BlobTooLarge {
                    actual: entry.value.len(),
                    maximum: MAX_TIERED_VALUE_BYTES,
                });
            }
        }
        let _blob_guard = self.lock_blobs();
        let mut last_positions = BTreeMap::new();
        for (index, entry) in entries.iter().enumerate() {
            last_positions.insert(entry.key.as_str(), index);
        }
        let selected: Vec<&BatchPut> = entries
            .iter()
            .enumerate()
            .filter_map(|(index, entry)| {
                (last_positions.get(entry.key.as_str()) == Some(&index)).then_some(entry)
            })
            .collect();
        let now = started_at_ms;
        self.reserve_batch_sequences(started_at_ms, selected.len())?;
        let batch_digest = batch_request_digest(&self.namespace, entries, now)?;
        let mut edits = Vec::with_capacity(selected.len());
        for (index, entry) in selected.iter().enumerate() {
            let expires_at_ms = expiry_for(entry.ttl, now);
            let sequence = batch_sequence(started_at_ms, index)?;
            let (_slot, edit) = self.prepare_batch_member(
                entry,
                expires_at_ms,
                sequence,
                operation,
                index,
                &batch_digest,
            )?;
            edits.push(edit);
        }
        for entry in &selected {
            if entry.value.len() > TIER_THRESHOLD_BYTES {
                let digest = content_digest(&entry.value);
                self.write_blob_file(&digest, &entry.value)?;
            }
        }
        let outcomes = self.database.edit_many(edits)?;
        if outcomes.len() != selected.len() {
            return Err(KvError::CorruptEnvelope);
        }
        Ok(())
    }

    /// Reads the live value stored under `key`.
    ///
    /// Expired entries read as absent with no eviction call required, as do
    /// missing, deleted, and never-created keys. Tiered values stream from
    /// the blob store and verify against their content address; a missing or
    /// diverged blob fails loudly instead of returning partial bytes.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidKey`] for a bad key,
    /// [`KvError::UnsupportedEnvelopeVersion`] when the slot was written by
    /// a newer cache, [`KvError::CorruptEnvelope`] when the slot carries
    /// cache markers that do not decode, [`KvError::MissingBlob`] or
    /// [`KvError::CorruptBlob`] when tiered bytes are absent or diverged, or
    /// [`KvError::Database`] and [`KvError::BlobIo`] for durable failures.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        validate_key(key)?;
        let _blob_guard = self.lock_blobs();
        let document = document_for(&self.namespace, key)?;
        let now = now_ms()?;
        match self.decode_slot(document)? {
            Decoded::Absent => Ok(None),
            Decoded::Corrupt => Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => Err(KvError::UnsupportedEnvelopeVersion { actual }),
            Decoded::Found(envelope) => {
                if envelope.namespace != self.namespace || envelope.key != key {
                    return Ok(None);
                }
                if envelope.expires_at_ms <= now {
                    return Ok(None);
                }
                match envelope.value {
                    SlotValue::Inline(value) => Ok(Some(value)),
                    SlotValue::Tiered { digest, size } => {
                        Ok(Some(self.resolve_tiered_locked(&digest, size)?))
                    }
                }
            }
        }
    }

    /// Clears the slot under `key`, reporting whether a live entry was
    /// removed. Clearing a missing or already-absent key writes nothing.
    ///
    /// The slot keeps its document identity, so the key stays reusable for
    /// later puts (see the deletion rule in the module documentation). A
    /// tiered slot's blob file becomes eligible for collection after the
    /// clearing edit commits; call [`KvCache::sweep_blobs`] in a quiescent
    /// maintenance window to reclaim it.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidKey`] for a bad key, or [`KvError::Database`]
    /// and [`KvError::BlobIo`] for durable and blob-store failures.
    pub fn delete(&self, key: &str) -> Result<bool, KvError> {
        validate_key(key)?;
        let _blob_guard = self.lock_blobs();
        let document = document_for(&self.namespace, key)?;
        let outcome = self.remove_reference(document, key)?;
        Ok(outcome.live)
    }

    /// Scans live entries whose keys start with `prefix` in key order,
    /// resuming after the `after` cursor for pagination.
    ///
    /// The scan pages the durable change log in bounded pages and reads
    /// each candidate through the live replica; undecodable or foreign
    /// slots are skipped while point reads stay strict. Tiered values
    /// resolve from the blob store, so a missing or diverged blob fails the
    /// scan loudly instead of returning partial bytes.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidKey`] for an oversized cursor,
    /// [`KvError::InvalidScanLimit`] for a page outside `1..=256`,
    /// [`KvError::MissingBlob`] or [`KvError::CorruptBlob`] when a scanned
    /// tiered value's bytes are absent or diverged, or [`KvError::Database`]
    /// and [`KvError::BlobIo`] for durable failures.
    pub fn scan(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CacheEntry>, KvError> {
        if limit == 0 || limit > MAX_PAGE_LIMIT {
            return Err(KvError::InvalidScanLimit(limit));
        }
        if after.is_some_and(|cursor| cursor.len() > logical_db_model::MAX_NAME_BYTES) {
            return Err(KvError::InvalidKey);
        }
        let _blob_guard = self.lock_blobs();
        let now = now_ms()?;
        let mut entries = Vec::new();
        for document in self.snapshot_documents()? {
            let Decoded::Found(envelope) = self.decode_slot(document)? else {
                continue;
            };
            if envelope.namespace != self.namespace || !envelope.key.starts_with(prefix) {
                continue;
            }
            if envelope.expires_at_ms <= now {
                continue;
            }
            if after.is_some_and(|cursor| envelope.key.as_str() <= cursor) {
                continue;
            }
            let value = match envelope.value {
                SlotValue::Inline(value) => value,
                SlotValue::Tiered { digest, size } => self.resolve_tiered_locked(&digest, size)?,
            };
            entries.push(CacheEntry {
                key: envelope.key,
                value,
            });
        }
        entries.sort_by(|left, right| left.key.cmp(&right.key));
        entries.truncate(limit);
        Ok(entries)
    }

    /// Returns up to `limit` live entries whose keys start with `prefix`,
    /// newest observed first.
    ///
    /// This is the reverse catch-up path for cold devices: after syncing,
    /// read the freshest keys immediately through `recent`, then backfill the
    /// full key-ordered set through [`KvCache::scan`]. Newest means most
    /// recently observed by this replica — local commits in commit order and
    /// synced changes in install order — which is a display heuristic, never
    /// a consistency primitive.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidScanLimit`] for a page outside `1..=256`,
    /// [`KvError::MissingBlob`] or [`KvError::CorruptBlob`] when a tiered
    /// value's bytes are absent or diverged, or [`KvError::Database`] and
    /// [`KvError::BlobIo`] for durable failures.
    pub fn recent(&self, prefix: &str, limit: usize) -> Result<Vec<CacheEntry>, KvError> {
        if limit == 0 || limit > MAX_PAGE_LIMIT {
            return Err(KvError::InvalidScanLimit(limit));
        }
        let _blob_guard = self.lock_blobs();
        let now = now_ms()?;
        let mut observed: BTreeMap<DocumentId, u64> = BTreeMap::new();
        let mut after = 0_u64;
        loop {
            let events = self.database.changes_since(after, MAX_PAGE_LIMIT)?;
            if events.is_empty() {
                break;
            }
            let complete = events.len() < MAX_PAGE_LIMIT;
            for event in &events {
                if let &ChangeEvent::Document {
                    ref document,
                    sequence,
                    ..
                } = event
                {
                    observed.insert(*document, sequence);
                }
                after = event.sequence();
            }
            if complete {
                break;
            }
        }
        let mut ranked: Vec<(u64, CacheEntry)> = Vec::new();
        for (document, sequence) in &observed {
            let Decoded::Found(envelope) = self.decode_slot(*document)? else {
                continue;
            };
            if envelope.namespace != self.namespace || !envelope.key.starts_with(prefix) {
                continue;
            }
            if envelope.expires_at_ms <= now {
                continue;
            }
            let value = match envelope.value {
                SlotValue::Inline(value) => value,
                SlotValue::Tiered { digest, size } => self.resolve_tiered_locked(&digest, size)?,
            };
            ranked.push((
                *sequence,
                CacheEntry {
                    key: envelope.key,
                    value,
                },
            ));
        }
        ranked.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| left.1.key.cmp(&right.1.key))
        });
        ranked.truncate(limit);
        Ok(ranked.into_iter().map(|(_, entry)| entry).collect())
    }

    /// Deletes up to `limit` expired slots in this namespace, reporting how
    /// many were reclaimed. Reclaimed tiered blobs become eligible for a later
    /// [`KvCache::sweep_blobs`] maintenance pass.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidEvictionLimit`] for a batch outside
    /// `1..=256`, or [`KvError::Database`] and [`KvError::BlobIo`] for durable
    /// and blob-store failures.
    pub fn evict_expired(&self, limit: usize) -> Result<usize, KvError> {
        if limit == 0 || limit > MAX_PAGE_LIMIT {
            return Err(KvError::InvalidEvictionLimit(limit));
        }
        let _blob_guard = self.lock_blobs();
        let now = now_ms()?;
        let mut cleared = 0_usize;
        for document in self.snapshot_documents()? {
            if cleared >= limit {
                break;
            }
            let Decoded::Found(envelope) = self.decode_slot(document)? else {
                continue;
            };
            if envelope.namespace != self.namespace || envelope.expires_at_ms > now {
                continue;
            }
            let outcome =
                self.remove_reference_at(document, &envelope.key, Some(envelope.revision))?;
            if !outcome.cleared {
                continue;
            }
            cleared = cleared.saturating_add(1);
        }
        Ok(cleared)
    }

    /// Classifies a slot document for namespace sync filtering.
    ///
    /// Returns the namespace recorded in the slot envelope, or `None` for
    /// missing, cleared, and non-cache documents. Expiry is ignored: an
    /// expired slot still belongs to its namespace for sync purposes, and
    /// reads keep enforcing expiry. Foreign slots never error; only slots
    /// carrying undecodable cache markers do.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::UnsupportedEnvelopeVersion`] when the slot was
    /// written by a newer cache, [`KvError::CorruptEnvelope`] when the slot
    /// carries cache markers that do not decode, or [`KvError::Database`]
    /// for a durable failure.
    pub fn document_namespace(&self, document: DocumentId) -> Result<Option<String>, KvError> {
        match self.decode_slot(document)? {
            Decoded::Found(envelope) => Ok(Some(envelope.namespace)),
            Decoded::Absent => Ok(None),
            Decoded::Corrupt => Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => Err(KvError::UnsupportedEnvelopeVersion { actual }),
        }
    }

    /// Loads the multiplexed namespace checkpoint map from the durable
    /// checkpoint slot bound to `peer`, or an empty map when nothing was
    /// stored yet.
    ///
    /// The slot stays the single per-peer binding; namespaces partition state
    /// within it. A slot holding a legacy global cursor fails closed so the
    /// two sync shapes are never silently mixed on one peer.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::CorruptCheckpointMap`] (or the versioned and sized
    /// variants) when the slot does not hold a namespace map, or
    /// [`KvError::Database`] for a durable failure.
    pub fn namespace_checkpoints(&self, peer: PeerId) -> Result<NamespaceCheckpoints, KvError> {
        match self.database.checkpoint(peer)? {
            None => Ok(NamespaceCheckpoints::new()),
            Some(bytes) => {
                NamespaceCheckpoints::decode(&bytes).map_err(|error| checkpoint_map_error(&error))
            }
        }
    }

    /// Stores the multiplexed namespace checkpoint map into the durable
    /// checkpoint slot bound to `peer`, atomically and without touching any
    /// document.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::CheckpointMapTooLarge`] when the map would not fit
    /// the slot, or [`KvError::Database`] for a durable failure.
    pub fn store_namespace_checkpoints(
        &self,
        peer: PeerId,
        map: &NamespaceCheckpoints,
    ) -> Result<(), KvError> {
        let encoded = map.encode().map_err(|error| checkpoint_map_error(&error))?;
        let changes: &[RemoteChange] = &[];
        self.database.install_remote_page(peer, changes, &encoded)?;
        Ok(())
    }

    /// Returns the tiered content address for `key`, or `None` for missing,
    /// cleared, foreign, and inline slots.
    ///
    /// Expiry is ignored like [`KvCache::document_namespace`]: an expired
    /// tiered slot still names its bytes for sync purposes while reads keep
    /// enforcing expiry. Only slots carrying undecodable cache markers
    /// error; every other non-tiered slot reports `None`.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidKey`] for a bad key,
    /// [`KvError::UnsupportedEnvelopeVersion`] when the slot was written by
    /// a newer cache, [`KvError::CorruptEnvelope`] when the slot carries
    /// cache markers that do not decode, or [`KvError::Database`] for a
    /// durable failure.
    pub fn blob_reference(&self, key: &str) -> Result<Option<BlobReference>, KvError> {
        validate_key(key)?;
        let document = document_for(&self.namespace, key)?;
        match self.decode_slot(document)? {
            Decoded::Found(envelope)
                if envelope.namespace == self.namespace && envelope.key == key =>
            {
                match envelope.value {
                    SlotValue::Tiered { digest, size } => Ok(Some(BlobReference { digest, size })),
                    SlotValue::Inline(_) => Ok(None),
                }
            }
            Decoded::Found(_) | Decoded::Absent => Ok(None),
            Decoded::Corrupt => Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => Err(KvError::UnsupportedEnvelopeVersion { actual }),
        }
    }
}
