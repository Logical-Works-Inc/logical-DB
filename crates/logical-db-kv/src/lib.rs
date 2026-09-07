//! Deterministic key-value cache with time-to-live and bounded eviction.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! [`KvCache`] sits above the public [`Database`] facade and touches only
//! [`Database`] plus `logical-db-model` types, so the one-way crate layering
//! the verifier enforces stays intact. Applications get one-line puts, gets,
//! deletes, and prefix scans without inventing document identity, operation
//! identity, or projection plumbing.
//!
//! # Key mapping (stable forever)
//!
//! A cache key maps to exactly one [`DocumentId`]:
//!
//! ```text
//! sha256("logical-db-kv/document/v1" || 0x00 || u64be(len(namespace))
//!         || namespace || 0x00 || u64be(len(key)) || key)
//! ```
//!
//! The first sixteen digest bytes become the UUID with the version nibble set
//! to `8` (custom) and the RFC 4122 variant bits set. The namespace is part
//! of the hash, so two caches in different namespaces never share a slot even
//! when their keys are identical. This mapping never changes: stored slots
//! address their documents by it across restarts, migrations, and sync.
//!
//! # Envelope layout (stable from the first shipped shape)
//!
//! Each slot document is a root map carrying a versioned envelope:
//!
//! ```text
//! kv_v   Unsigned  envelope layout version (1 on this writer)
//! kv_key String    original key, for prefix scans
//! kv_ns  String    namespace, isolating caches sharing one scope
//! kv_val Bytes     opaque value bytes
//! kv_exp Timestamp expiry in milliseconds since the Unix epoch (i64::MAX = never)
//! kv_seq Unsigned  writer-local recency sequence
//! ```
//!
//! The version field ships from day one. A newer reader decodes an older
//! envelope by defaulting absent fields (`kv_exp` defaults to never expire,
//! `kv_seq` defaults to zero) and refuses an envelope whose version is newer
//! than it understands instead of guessing.
//!
//! # Expiry and eviction
//!
//! Reads are expiry-aware: [`KvCache::get`] and [`KvCache::scan`] report an
//! entry whose expiry has passed as absent with no eviction call required.
//! [`KvCache::evict_expired`] reclaims expired slots in bounded batches so
//! dead entries stop costing storage and scan pages.
//!
//! # Deletion rule
//!
//! Deletes clear the slot through the same single-edit discipline as puts:
//! the value is removed and the expiry is zeroed, so the slot keeps its
//! [`DocumentId`] and the key stays reusable. Tombstones
//! ([`Database::delete`]) are deliberately not used here: a tombstone retires
//! document identity forever to make absence converge, which would
//! permanently burn the cache key for later puts.
//!
//! # Indexes
//!
//! [`kv_projections`] declares the two schema-bound secondary indexes every
//! cache scope carries: [`EXPIRY_INDEX`] (`Timestamp` expiry, defaulting to
//! never expire for older envelopes) and [`RECENCY_INDEX`] (`Unsigned`
//! recency, defaulting to zero). Slots without a live value project no rows.
//! The projection functions never fail a commit: unexpected shapes project
//! nothing, while point reads stay strict (see [`KvError`]).
//!
//! # Namespace sync domains
//!
//! Namespaces compose with the key mapping instead of forking it: every slot
//! already carries its namespace, so [`KvCache::document_namespace`]
//! classifies any pulled change for a sync driver, [`KvCache::put_many`]
//! keeps multi-key namespace writes atomic, [`KvCache::recent`] gives cold
//! devices the newest keys first before [`KvCache::scan`] backfills, and
//! [`KvCache::namespace_checkpoints`] with
//! [`KvCache::store_namespace_checkpoints`] persists per-namespace cursors in
//! the one durable checkpoint slot bound to the upstream.

#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub use logical_db::{Database, DatabaseError, MigrationReport, Projection};
use logical_db::{EditOutcome, Item, Object, OperationDigest, OperationId, RemoteChange, Scalar};
use logical_db_model::MAX_CHANGE_BYTES;
pub use logical_db_model::{
    AccountId, DocumentId, IndexId, IndexValue, NamespaceCheckpoint, NamespaceCheckpoints, PeerId,
    ReplicaId, Scope, SpaceId,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

/// Application schema version binding the cache projections to a scope.
pub const KV_SCHEMA_VERSION: u32 = 1;
/// Envelope layout version written by this cache.
pub const ENVELOPE_VERSION: u64 = 1;
/// Largest cache value accepted, leaving headroom for change framing and the
/// remaining envelope fields under [`MAX_CHANGE_BYTES`].
pub const MAX_VALUE_BYTES: usize = MAX_CHANGE_BYTES - 65_536;
/// Largest page accepted by [`KvCache::scan`] and [`KvCache::evict_expired`],
/// mirroring the facade's bounded pages.
pub const MAX_PAGE_LIMIT: usize = 256;
/// Largest atomic batch accepted by [`KvCache::put_many`].
///
/// The batch snapshots prior values for rollback, so the bound caps transient
/// compensation memory as well as per-call outbox churn.
pub const MAX_BATCH_PUT: usize = 32;
/// Expiry meaning "never expires".
pub const NEVER_EXPIRES_AT_MS: i64 = i64::MAX;

/// Secondary index over slot expiry (`Timestamp` milliseconds).
pub const EXPIRY_INDEX: IndexId = IndexId::from_uuid(Uuid::from_bytes(*b"ldb-kv-expiry-01"));
/// Secondary index over slot recency (`Unsigned` writer sequence).
pub const RECENCY_INDEX: IndexId = IndexId::from_uuid(Uuid::from_bytes(*b"ldb-kv-recency-1"));

/// Root-map property carrying the envelope layout version.
pub const FIELD_VERSION: &str = "kv_v";
/// Root-map property carrying the original cache key.
pub const FIELD_KEY: &str = "kv_key";
/// Root-map property carrying the owning namespace.
pub const FIELD_NAMESPACE: &str = "kv_ns";
/// Root-map property carrying the opaque value bytes.
pub const FIELD_VALUE: &str = "kv_val";
/// Root-map property carrying the expiry timestamp.
pub const FIELD_EXPIRY: &str = "kv_exp";
/// Root-map property carrying the writer-local recency sequence.
pub const FIELD_SEQUENCE: &str = "kv_seq";

/// One live cache entry returned by [`KvCache::scan`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheEntry {
    /// Original cache key.
    pub key: String,
    /// Opaque value bytes as stored.
    pub value: Vec<u8>,
}

/// One entry of an atomic [`KvCache::put_many`] batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchPut {
    /// Cache key, validated exactly like [`KvCache::put`].
    pub key: String,
    /// Opaque value bytes, bounded exactly like [`KvCache::put`].
    pub value: Vec<u8>,
    /// Optional time-to-live; `None` never expires.
    pub ttl: Option<Duration>,
}

/// Stable failures exposed by the cache facade.
#[derive(Debug, Error)]
pub enum KvError {
    /// The embedded replica below the cache failed.
    #[error("cache storage failed: {0}")]
    Database(#[from] DatabaseError),
    /// A cache key or scan cursor was empty or exceeded 512 UTF-8 bytes.
    #[error("cache key is empty or exceeds 512 UTF-8 bytes")]
    InvalidKey,
    /// A cache namespace was empty or exceeded 512 UTF-8 bytes.
    #[error("cache namespace is empty or exceeds 512 UTF-8 bytes")]
    InvalidNamespace,
    /// A value would not fit the bounded change envelope.
    #[error("cache value length {actual} exceeds maximum {maximum}")]
    ValueTooLarge {
        /// Observed value byte count.
        actual: usize,
        /// Accepted value byte count.
        maximum: usize,
    },
    /// A prefix-scan page fell outside `1..=256`.
    #[error("scan limit {0} is outside 1..=256")]
    InvalidScanLimit(usize),
    /// A batch fell outside `1..=32`.
    #[error("batch size {0} is outside 1..=32")]
    InvalidBatchSize(usize),
    /// A namespace checkpoint map was malformed.
    #[error("namespace checkpoint map is malformed")]
    CorruptCheckpointMap,
    /// A namespace checkpoint map used a foreign layout version.
    #[error("namespace checkpoint map version {actual} is unsupported")]
    UnsupportedCheckpointMapVersion {
        /// Observed map layout version.
        actual: u16,
    },
    /// A namespace checkpoint map would not fit the durable slot.
    #[error("namespace checkpoint map length {actual} exceeds maximum {maximum}")]
    CheckpointMapTooLarge {
        /// Observed encoded byte count.
        actual: usize,
        /// Accepted encoded byte count.
        maximum: usize,
    },
    /// An eviction batch fell outside `1..=256`.
    #[error("eviction limit {0} is outside 1..=256")]
    InvalidEvictionLimit(usize),
    /// An envelope was written by a newer cache than this reader.
    #[error("cache envelope version {actual} is unsupported")]
    UnsupportedEnvelopeVersion {
        /// Observed envelope layout version.
        actual: u64,
    },
    /// A slot carried cache markers that do not decode.
    #[error("cache envelope is malformed")]
    CorruptEnvelope,
}

/// Deterministic key-value cache with TTL above one embedded replica.
pub struct KvCache {
    database: Database,
    namespace: String,
    sequence: AtomicU64,
}

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
        let database =
            Database::open_with_schema(path, scope, replica, KV_SCHEMA_VERSION, kv_projections())?;
        Ok(Self {
            database,
            namespace: namespace.to_owned(),
            sequence: AtomicU64::new(seed_sequence()),
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
            path,
            scope,
            replica,
            KV_SCHEMA_VERSION,
            kv_projections(),
            batch_size,
        )?;
        Ok((
            Self {
                database,
                namespace: namespace.to_owned(),
                sequence: AtomicU64::new(seed_sequence()),
            },
            report,
        ))
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
    /// expiry, and recency through one atomic edit.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidKey`] for a bad key,
    /// [`KvError::ValueTooLarge`] before committing anything when the value
    /// would not fit the bounded change envelope, or [`KvError::Database`]
    /// for a durable failure.
    pub fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<(), KvError> {
        validate_key(key)?;
        if value.len() > MAX_VALUE_BYTES {
            eprintln!("logical-db-kv: cache value exceeds {MAX_VALUE_BYTES} byte limit");
            return Err(KvError::ValueTooLarge {
                actual: value.len(),
                maximum: MAX_VALUE_BYTES,
            });
        }
        let now = now_ms();
        self.put_absolute(key, value, expiry_for(ttl, now), self.next_sequence(now))
    }

    /// Stores a batch of keys atomically: every entry validates before the
    /// first commit, and a commit failure restores the prior values of the
    /// entries this call already applied before reporting the error.
    ///
    /// Duplicate keys within one batch resolve last-wins in batch order.
    /// Retrying a failed batch converges: restoration returns slots to their
    /// pre-batch values, so the retry applies cleanly with fresh identities.
    /// A rollback that itself fails still reports the original error; the
    /// batch then needs an explicit retry to converge.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidBatchSize`] for an empty or oversized batch,
    /// [`KvError::InvalidKey`] or [`KvError::ValueTooLarge`] before committing
    /// anything when any entry is invalid, or [`KvError::Database`] when a
    /// commit fails.
    pub fn put_many(&self, entries: &[BatchPut]) -> Result<(), KvError> {
        if entries.is_empty() || entries.len() > MAX_BATCH_PUT {
            eprintln!("logical-db-kv: invalid batch count {}", entries.len());
            return Err(KvError::InvalidBatchSize(entries.len()));
        }
        for entry in entries {
            validate_key(&entry.key)?;
            if entry.value.len() > MAX_VALUE_BYTES {
                eprintln!("logical-db-kv: cache value exceeds {MAX_VALUE_BYTES} byte limit");
                return Err(KvError::ValueTooLarge {
                    actual: entry.value.len(),
                    maximum: MAX_VALUE_BYTES,
                });
            }
        }
        let now = now_ms();
        let mut priors = Vec::with_capacity(entries.len());
        for entry in entries {
            priors.push(self.live_prior(&entry.key, now)?);
        }
        for (applied, entry) in entries.iter().enumerate() {
            let expires_at_ms = expiry_for(entry.ttl, now);
            let sequence = self.next_sequence(now);
            if let Err(error) = self.put_absolute(&entry.key, &entry.value, expires_at_ms, sequence)
            {
                self.rollback(&entries[..applied], &priors[..applied]);
                eprintln!("logical-db-kv: batch write failed after {applied} entries: {error}");
                return Err(error);
            }
        }
        Ok(())
    }

    /// Reads the live value stored under `key`.
    ///
    /// Expired entries read as absent with no eviction call required, as do
    /// missing, deleted, and never-created keys.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidKey`] for a bad key,
    /// [`KvError::UnsupportedEnvelopeVersion`] when the slot was written by
    /// a newer cache, [`KvError::CorruptEnvelope`] when the slot carries
    /// cache markers that do not decode, or [`KvError::Database`] for a
    /// durable failure.
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        validate_key(key)?;
        let document = document_for(&self.namespace, key);
        let now = now_ms();
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
                Ok(Some(envelope.value))
            }
        }
    }

    /// Clears the slot under `key`, reporting whether a live entry was
    /// removed. Clearing a missing or already-absent key writes nothing.
    ///
    /// The slot keeps its document identity, so the key stays reusable for
    /// later puts (see the deletion rule in the module documentation).
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidKey`] for a bad key or
    /// [`KvError::Database`] for a durable failure.
    pub fn delete(&self, key: &str) -> Result<bool, KvError> {
        validate_key(key)?;
        let document = document_for(&self.namespace, key);
        let now = now_ms();
        match self.decode_slot(document)? {
            Decoded::Found(envelope)
                if envelope.namespace == self.namespace && envelope.key == key =>
            {
                let live = envelope.expires_at_ms > now;
                self.clear_slot(document, key)?;
                Ok(live)
            }
            Decoded::Absent | Decoded::Found(_) => Ok(false),
            Decoded::Corrupt => Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => Err(KvError::UnsupportedEnvelopeVersion { actual }),
        }
    }

    /// Scans live entries whose keys start with `prefix` in key order,
    /// resuming after the `after` cursor for pagination.
    ///
    /// The scan pages the durable change log in bounded pages and reads
    /// each candidate through the live replica; undecodable or foreign
    /// slots are skipped while point reads stay strict.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidKey`] for an oversized cursor,
    /// [`KvError::InvalidScanLimit`] for a page outside `1..=256`, or
    /// [`KvError::Database`] for a durable failure.
    pub fn scan(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<CacheEntry>, KvError> {
        if limit == 0 || limit > MAX_PAGE_LIMIT {
            eprintln!("logical-db-kv: invalid scan limit {limit}");
            return Err(KvError::InvalidScanLimit(limit));
        }
        if after.is_some_and(|cursor| cursor.len() > logical_db_model::MAX_NAME_BYTES) {
            eprintln!("logical-db-kv: invalid key or cursor length");
            return Err(KvError::InvalidKey);
        }
        let now = now_ms();
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
            entries.push(CacheEntry {
                key: envelope.key,
                value: envelope.value,
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
    /// Returns [`KvError::InvalidScanLimit`] for a page outside `1..=256`, or
    /// [`KvError::Database`] for a durable failure.
    pub fn recent(&self, prefix: &str, limit: usize) -> Result<Vec<CacheEntry>, KvError> {
        if limit == 0 || limit > MAX_PAGE_LIMIT {
            eprintln!("logical-db-kv: invalid scan limit {limit}");
            return Err(KvError::InvalidScanLimit(limit));
        }
        let now = now_ms();
        let mut observed: BTreeMap<DocumentId, u64> = BTreeMap::new();
        let mut after = 0_u64;
        loop {
            let events = self.database.changes_since(after, MAX_PAGE_LIMIT)?;
            if events.is_empty() {
                break;
            }
            let complete = events.len() < MAX_PAGE_LIMIT;
            for event in &events {
                observed.insert(event.document, event.sequence);
                after = event.sequence;
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
            ranked.push((
                *sequence,
                CacheEntry {
                    key: envelope.key,
                    value: envelope.value,
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
    /// many were reclaimed.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidEvictionLimit`] for a batch outside
    /// `1..=256`, or [`KvError::Database`] for a durable failure.
    pub fn evict_expired(&self, limit: usize) -> Result<usize, KvError> {
        if limit == 0 || limit > MAX_PAGE_LIMIT {
            eprintln!("logical-db-kv: invalid eviction limit {limit}");
            return Err(KvError::InvalidEvictionLimit(limit));
        }
        let now = now_ms();
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
            self.clear_slot(document, &envelope.key)?;
            cleared += 1;
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

    fn put_absolute(
        &self,
        key: &str,
        value: &[u8],
        expires_at_ms: i64,
        sequence: u64,
    ) -> Result<(), KvError> {
        let document = document_for(&self.namespace, key);
        let operation = OperationId::new();
        let digest = put_digest(
            &operation,
            &self.namespace,
            key,
            value,
            expires_at_ms,
            sequence,
        );
        let stored_key = key.to_owned();
        let stored_value = value.to_vec();
        let stored_namespace = self.namespace.clone();
        match self.database.edit(document, operation, digest, |editor| {
            editor.put(
                &Object::root(),
                FIELD_VERSION,
                Scalar::Unsigned(ENVELOPE_VERSION),
            )?;
            editor.put(&Object::root(), FIELD_KEY, Scalar::String(stored_key))?;
            editor.put(
                &Object::root(),
                FIELD_NAMESPACE,
                Scalar::String(stored_namespace),
            )?;
            editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(stored_value))?;
            editor.put(
                &Object::root(),
                FIELD_EXPIRY,
                Scalar::Timestamp(expires_at_ms),
            )?;
            editor.put(&Object::root(), FIELD_SEQUENCE, Scalar::Unsigned(sequence))?;
            Ok(())
        })? {
            EditOutcome::Applied(_) | EditOutcome::Duplicate(_) => Ok(()),
        }
    }

    fn live_prior(&self, key: &str, now: i64) -> Result<Option<Prior>, KvError> {
        let document = document_for(&self.namespace, key);
        match self.decode_slot(document)? {
            Decoded::Found(envelope)
                if envelope.namespace == self.namespace
                    && envelope.key == key
                    && envelope.expires_at_ms > now =>
            {
                Ok(Some(Prior {
                    value: envelope.value,
                    expires_at_ms: envelope.expires_at_ms,
                }))
            }
            Decoded::Found(_) | Decoded::Absent => Ok(None),
            Decoded::Corrupt => Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => Err(KvError::UnsupportedEnvelopeVersion { actual }),
        }
    }

    fn rollback(&self, entries: &[BatchPut], priors: &[Option<Prior>]) {
        for (entry, prior) in entries.iter().zip(priors.iter()).rev() {
            match prior {
                Some(value) => {
                    if let Err(error) = self.put_absolute(
                        &entry.key,
                        &value.value,
                        value.expires_at_ms,
                        self.next_sequence(now_ms()),
                    ) {
                        eprintln!("logical-db-kv: rollback prior value failed: {error}");
                    }
                }
                None => {
                    if let Err(error) = self.clear_if_live(&entry.key) {
                        eprintln!("logical-db-kv: rollback newly inserted value failed: {error}");
                    }
                }
            }
        }
    }

    fn clear_if_live(&self, key: &str) -> Result<(), KvError> {
        let document = document_for(&self.namespace, key);
        let now = now_ms();
        match self.decode_slot(document)? {
            Decoded::Found(envelope)
                if envelope.namespace == self.namespace
                    && envelope.key == key
                    && envelope.expires_at_ms > now =>
            {
                self.clear_slot(document, key)
            }
            Decoded::Found(_) | Decoded::Absent => Ok(()),
            Decoded::Corrupt => Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => Err(KvError::UnsupportedEnvelopeVersion { actual }),
        }
    }

    fn decode_slot(&self, document: DocumentId) -> Result<Decoded, KvError> {
        let decoded = self.database.read(document, |reader| {
            let key = match reader.get(&Object::root(), FIELD_KEY)? {
                None => return Ok(Decoded::Absent),
                Some(Item::Scalar(Scalar::String(key))) => key,
                Some(_) => return Ok(Decoded::Corrupt),
            };
            let namespace = match reader.get(&Object::root(), FIELD_NAMESPACE)? {
                None => return Ok(Decoded::Absent),
                Some(Item::Scalar(Scalar::String(namespace))) => namespace,
                Some(_) => return Ok(Decoded::Corrupt),
            };
            let value = match reader.get(&Object::root(), FIELD_VALUE)? {
                None => return Ok(Decoded::Absent),
                Some(Item::Scalar(Scalar::Bytes(value))) => value,
                Some(_) => return Ok(Decoded::Corrupt),
            };
            let version = match reader.get(&Object::root(), FIELD_VERSION)? {
                None => 1,
                Some(Item::Scalar(Scalar::Unsigned(version))) => version,
                Some(Item::Scalar(Scalar::Integer(version))) => {
                    let Ok(version) = u64::try_from(version) else {
                        return Ok(Decoded::Corrupt);
                    };
                    version
                }
                Some(_) => return Ok(Decoded::Corrupt),
            };
            if version == 0 {
                return Ok(Decoded::Corrupt);
            }
            if version > ENVELOPE_VERSION {
                return Ok(Decoded::Unsupported(version));
            }
            let expires_at_ms = match reader.get(&Object::root(), FIELD_EXPIRY)? {
                None => NEVER_EXPIRES_AT_MS,
                Some(Item::Scalar(Scalar::Timestamp(millis) | Scalar::Integer(millis))) => millis,
                Some(_) => return Ok(Decoded::Corrupt),
            };
            let _sequence = match reader.get(&Object::root(), FIELD_SEQUENCE)? {
                None => 0,
                Some(Item::Scalar(Scalar::Unsigned(sequence))) => sequence,
                Some(Item::Scalar(Scalar::Integer(sequence))) => {
                    let Ok(sequence) = u64::try_from(sequence) else {
                        return Ok(Decoded::Corrupt);
                    };
                    sequence
                }
                Some(_) => return Ok(Decoded::Corrupt),
            };
            Ok(Decoded::Found(Envelope {
                key,
                namespace,
                value,
                expires_at_ms,
            }))
        })?;
        Ok(decoded.unwrap_or(Decoded::Absent))
    }

    fn clear_slot(&self, document: DocumentId, key: &str) -> Result<(), KvError> {
        let sequence = self.next_sequence(now_ms());
        let operation = OperationId::new();
        let digest = clear_digest(&operation, &self.namespace, key);
        match self.database.edit(document, operation, digest, |editor| {
            editor.delete(&Object::root(), FIELD_VALUE)?;
            editor.put(&Object::root(), FIELD_EXPIRY, Scalar::Timestamp(0))?;
            editor.put(&Object::root(), FIELD_SEQUENCE, Scalar::Unsigned(sequence))?;
            Ok(())
        })? {
            EditOutcome::Applied(_) | EditOutcome::Duplicate(_) => Ok(()),
        }
    }

    fn snapshot_documents(&self) -> Result<BTreeSet<DocumentId>, KvError> {
        let mut documents = BTreeSet::new();
        let mut after = 0_u64;
        loop {
            let events = self.database.changes_since(after, MAX_PAGE_LIMIT)?;
            if events.is_empty() {
                break;
            }
            let complete = events.len() < MAX_PAGE_LIMIT;
            for event in &events {
                documents.insert(event.document);
                after = event.sequence;
            }
            if complete {
                break;
            }
        }
        Ok(documents)
    }

    fn next_sequence(&self, now: i64) -> u64 {
        let floor = u64::try_from(now.max(0)).unwrap_or(0);
        self.sequence.fetch_max(floor, Ordering::Relaxed);
        self.sequence
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1)
    }
}

/// Returns the schema-bound expiry and recency projections every cache scope
/// carries. The functions never fail a commit: slots without a live value
/// project no rows, missing expiry defaults to never expire, missing
/// recency defaults to zero, and unexpected shapes project nothing.
#[must_use]
pub fn kv_projections() -> Vec<Projection> {
    vec![
        Projection::new(EXPIRY_INDEX, |reader| {
            if !matches!(
                reader.get(&Object::root(), FIELD_VALUE)?,
                Some(Item::Scalar(Scalar::Bytes(_)))
            ) {
                return Ok(None);
            }
            Ok(match reader.get(&Object::root(), FIELD_EXPIRY)? {
                None => Some(IndexValue::Timestamp(NEVER_EXPIRES_AT_MS)),
                Some(Item::Scalar(Scalar::Timestamp(millis) | Scalar::Integer(millis))) => {
                    Some(IndexValue::Timestamp(millis))
                }
                Some(_) => None,
            })
        }),
        Projection::new(RECENCY_INDEX, |reader| {
            if !matches!(
                reader.get(&Object::root(), FIELD_VALUE)?,
                Some(Item::Scalar(Scalar::Bytes(_)))
            ) {
                return Ok(None);
            }
            Ok(match reader.get(&Object::root(), FIELD_SEQUENCE)? {
                None => Some(IndexValue::Unsigned(0)),
                Some(Item::Scalar(Scalar::Unsigned(sequence))) => {
                    Some(IndexValue::Unsigned(sequence))
                }
                Some(Item::Scalar(Scalar::Integer(sequence))) => {
                    Some(IndexValue::Unsigned(u64::try_from(sequence).unwrap_or(0)))
                }
                Some(_) => None,
            })
        }),
    ]
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Envelope {
    key: String,
    namespace: String,
    value: Vec<u8>,
    expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Prior {
    value: Vec<u8>,
    expires_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Decoded {
    Absent,
    Corrupt,
    Unsupported(u64),
    Found(Envelope),
}

fn validate_key(key: &str) -> Result<(), KvError> {
    if key.is_empty() || key.len() > logical_db_model::MAX_NAME_BYTES {
        eprintln!("logical-db-kv: invalid key or cursor length");
        return Err(KvError::InvalidKey);
    }
    Ok(())
}

fn validate_namespace(namespace: &str) -> Result<(), KvError> {
    if namespace.is_empty() || namespace.len() > logical_db_model::MAX_NAME_BYTES {
        eprintln!(
            "logical-db-kv: invalid namespace length {}",
            namespace.len()
        );
        return Err(KvError::InvalidNamespace);
    }
    Ok(())
}

fn checkpoint_map_error(error: &logical_db_model::ModelError) -> KvError {
    match error {
        logical_db_model::ModelError::UnsupportedNamespaceCheckpointVersion { actual } => {
            KvError::UnsupportedCheckpointMapVersion { actual: *actual }
        }
        logical_db_model::ModelError::NamespaceCheckpointMapTooLarge { actual, maximum } => {
            KvError::CheckpointMapTooLarge {
                actual: *actual,
                maximum: *maximum,
            }
        }
        _ => KvError::CorruptCheckpointMap,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

fn seed_sequence() -> u64 {
    u64::try_from(now_ms().max(0)).unwrap_or(0)
}

fn expiry_for(ttl: Option<Duration>, now: i64) -> i64 {
    match ttl {
        None => NEVER_EXPIRES_AT_MS,
        Some(ttl) => {
            let millis = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
            let delta = i64::try_from(millis).unwrap_or(i64::MAX);
            now.checked_add(delta).unwrap_or(NEVER_EXPIRES_AT_MS)
        }
    }
}

fn put_digest(
    operation: &OperationId,
    namespace: &str,
    key: &str,
    value: &[u8],
    expires_at_ms: i64,
    sequence: u64,
) -> OperationDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"logical-db-kv/put/v1");
    hasher.update([0]);
    hasher.update(operation.as_uuid().as_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update([0]);
    hasher.update(key.as_bytes());
    hasher.update([0]);
    hasher.update(value);
    hasher.update(expires_at_ms.to_be_bytes());
    hasher.update(sequence.to_be_bytes());
    OperationDigest::new(hasher.finalize().into())
}

fn clear_digest(operation: &OperationId, namespace: &str, key: &str) -> OperationDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"logical-db-kv/clear/v1");
    hasher.update([0]);
    hasher.update(operation.as_uuid().as_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update([0]);
    hasher.update(key.as_bytes());
    OperationDigest::new(hasher.finalize().into())
}

fn document_for(namespace: &str, key: &str) -> DocumentId {
    let namespace_length = u64::try_from(namespace.len()).unwrap_or(u64::MAX);
    let key_length = u64::try_from(key.len()).unwrap_or(u64::MAX);
    let mut hasher = Sha256::new();
    hasher.update(b"logical-db-kv/document/v1");
    hasher.update([0]);
    hasher.update(namespace_length.to_be_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update([0]);
    hasher.update(key_length.to_be_bytes());
    hasher.update(key.as_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0F) | 0x80;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    DocumentId::from_uuid(Uuid::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> Scope {
        Scope::new(AccountId::new(), SpaceId::new())
    }

    fn path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("logical-db-kv-{tag}-{}.redb", Uuid::now_v7()))
    }

    fn open_cache(path: &std::path::Path, namespace: &str) -> KvCache {
        KvCache::open(path, scope(), ReplicaId::new(), namespace).unwrap()
    }

    fn cleanup(path: &std::path::Path) {
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn round_trip_put_get_delete_and_prefix_scan_without_caller_identity() {
        let db_path = path("roundtrip");
        let cache = open_cache(&db_path, "sessions");
        cache
            .put("user:ada", b"1", Some(Duration::from_secs(60)))
            .unwrap();
        cache
            .put("user:grace", b"2", Some(Duration::from_secs(60)))
            .unwrap();
        cache
            .put("config:theme", b"dark", Some(Duration::from_secs(60)))
            .unwrap();
        assert_eq!(cache.get("user:ada").unwrap(), Some(b"1".to_vec()));
        let page = cache.scan("user:", None, 10).unwrap();
        assert_eq!(
            page,
            vec![
                CacheEntry {
                    key: "user:ada".into(),
                    value: b"1".to_vec()
                },
                CacheEntry {
                    key: "user:grace".into(),
                    value: b"2".to_vec()
                },
            ]
        );
        assert!(cache.delete("user:ada").unwrap());
        assert_eq!(cache.get("user:ada").unwrap(), None);
        assert!(!cache.delete("user:ada").unwrap());
        assert!(!cache.delete("missing").unwrap());
        let page = cache.scan("user:", None, 10).unwrap();
        assert_eq!(
            page,
            vec![CacheEntry {
                key: "user:grace".into(),
                value: b"2".to_vec()
            }]
        );
        let first = cache.scan("", None, 1).unwrap();
        assert_eq!(first.len(), 1);
        let rest = cache.scan("", Some(&first[0].key), 10).unwrap();
        assert!(
            rest.iter()
                .all(|entry| entry.key.as_str() > first[0].key.as_str())
        );
        assert_eq!(first.len() + rest.len(), 2);
        cleanup(&db_path);
    }

    #[test]
    fn expired_entries_read_absent_and_evict_in_bounded_batches() {
        let db_path = path("expiry");
        let cache = open_cache(&db_path, "sessions");
        cache.put("gone:1", b"a", Some(Duration::ZERO)).unwrap();
        cache.put("gone:2", b"b", Some(Duration::ZERO)).unwrap();
        cache.put("gone:3", b"c", Some(Duration::ZERO)).unwrap();
        cache
            .put("live", b"z", Some(Duration::from_secs(60)))
            .unwrap();
        assert_eq!(cache.get("gone:1").unwrap(), None);
        assert_eq!(cache.get("live").unwrap(), Some(b"z".to_vec()));
        assert_eq!(cache.evict_expired(2).unwrap(), 2);
        assert_eq!(cache.evict_expired(10).unwrap(), 1);
        assert_eq!(cache.evict_expired(10).unwrap(), 0);
        assert_eq!(cache.get("live").unwrap(), Some(b"z".to_vec()));
        assert!(cache.scan("gone:", None, 10).unwrap().is_empty());
        cleanup(&db_path);
    }

    #[test]
    fn older_envelope_reads_with_new_fields_defaulted_and_newer_refused() {
        let db_path = path("envelope");
        let scope = scope();
        let replica = ReplicaId::new();
        {
            let raw = Database::open(&db_path, scope, replica).unwrap();
            let document = document_for("sessions", "legacy");
            raw.edit(
                document,
                OperationId::new(),
                OperationDigest::new([11; 32]),
                |editor| {
                    editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(1))?;
                    editor.put(&Object::root(), FIELD_KEY, Scalar::String("legacy".into()))?;
                    editor.put(
                        &Object::root(),
                        FIELD_NAMESPACE,
                        Scalar::String("sessions".into()),
                    )?;
                    editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(b"old".to_vec()))?;
                    Ok(())
                },
            )
            .unwrap();
            let future = document_for("sessions", "future");
            raw.edit(
                future,
                OperationId::new(),
                OperationDigest::new([12; 32]),
                |editor| {
                    editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(99))?;
                    editor.put(&Object::root(), FIELD_KEY, Scalar::String("future".into()))?;
                    editor.put(
                        &Object::root(),
                        FIELD_NAMESPACE,
                        Scalar::String("sessions".into()),
                    )?;
                    editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(b"new".to_vec()))?;
                    Ok(())
                },
            )
            .unwrap();
        }
        let (cache, report) =
            KvCache::migrate_schema(&db_path, scope, replica, "sessions", 16).unwrap();
        assert_eq!(report.documents, 2);
        assert_eq!(cache.get("legacy").unwrap(), Some(b"old".to_vec()));
        let stored = cache
            .database()
            .query_index(
                EXPIRY_INDEX,
                &IndexValue::Timestamp(NEVER_EXPIRES_AT_MS),
                None,
                10,
            )
            .unwrap();
        assert!(stored.contains(&document_for("sessions", "legacy")));
        let error = cache.get("future").unwrap_err();
        assert!(matches!(
            error,
            KvError::UnsupportedEnvelopeVersion { actual: 99 }
        ));
        cleanup(&db_path);
    }

    #[test]
    fn near_bound_values_refused_before_commit_and_max_fits() {
        let db_path = path("bounds");
        let cache = open_cache(&db_path, "blobs");
        let oversize = vec![7_u8; MAX_VALUE_BYTES + 1];
        let error = cache
            .put("big", &oversize, Some(Duration::from_secs(60)))
            .unwrap_err();
        assert!(matches!(
            error,
            KvError::ValueTooLarge { actual, maximum }
            if actual == MAX_VALUE_BYTES + 1 && maximum == MAX_VALUE_BYTES
        ));
        assert_eq!(cache.get("big").unwrap(), None);
        let exact = vec![9_u8; MAX_VALUE_BYTES];
        cache
            .put("edge", &exact, Some(Duration::from_secs(60)))
            .unwrap();
        assert_eq!(cache.get("edge").unwrap(), Some(exact));
        cleanup(&db_path);
    }

    #[test]
    fn schema_migration_preserves_documents_and_installs_indexes() {
        let db_path = path("migrate");
        let scope = scope();
        let replica = ReplicaId::new();
        {
            let raw = Database::open(&db_path, scope, replica).unwrap();
            raw.edit(
                DocumentId::new(),
                OperationId::new(),
                OperationDigest::new([21; 32]),
                |editor| editor.put(&Object::root(), "note", Scalar::String("plain".into())),
            )
            .unwrap();
        }
        let (cache, _) = KvCache::migrate_schema(&db_path, scope, replica, "sessions", 16).unwrap();
        cache.put("forever", b"v", None).unwrap();
        assert_eq!(cache.get("forever").unwrap(), Some(b"v".to_vec()));
        let hits = cache
            .database()
            .query_index(
                EXPIRY_INDEX,
                &IndexValue::Timestamp(NEVER_EXPIRES_AT_MS),
                None,
                10,
            )
            .unwrap();
        assert_eq!(hits, vec![document_for("sessions", "forever")]);
        cleanup(&db_path);
    }

    #[test]
    fn namespaces_never_share_slots_and_inputs_are_validated() {
        let db_path = path("namespaces");
        let scope = scope();
        let replica = ReplicaId::new();
        {
            let first = KvCache::open(&db_path, scope, replica, "one").unwrap();
            first
                .put("shared", b"1", Some(Duration::from_secs(60)))
                .unwrap();
            assert_eq!(first.get("shared").unwrap(), Some(b"1".to_vec()));
        }
        {
            let second = KvCache::open(&db_path, scope, replica, "two").unwrap();
            assert_eq!(second.get("shared").unwrap(), None);
            second
                .put("shared", b"2", Some(Duration::from_secs(60)))
                .unwrap();
            assert_eq!(second.get("shared").unwrap(), Some(b"2".to_vec()));
        }
        {
            let first = KvCache::open(&db_path, scope, replica, "one").unwrap();
            assert_eq!(first.get("shared").unwrap(), Some(b"1".to_vec()));
            assert!(matches!(first.get("").unwrap_err(), KvError::InvalidKey));
            assert!(matches!(
                first.scan("", None, 0).unwrap_err(),
                KvError::InvalidScanLimit(0)
            ));
            assert!(matches!(
                first.evict_expired(0).unwrap_err(),
                KvError::InvalidEvictionLimit(0)
            ));
        }
        assert!(matches!(
            KvCache::open(&db_path, scope, replica, ""),
            Err(KvError::InvalidNamespace)
        ));
        cleanup(&db_path);
    }

    #[test]
    fn batch_put_applies_every_entry_atomically() {
        let db_path = path("batch-apply");
        let cache = open_cache(&db_path, "sessions");
        cache
            .put_many(&[
                BatchPut {
                    key: "a".into(),
                    value: b"1".to_vec(),
                    ttl: Some(Duration::from_secs(60)),
                },
                BatchPut {
                    key: "b".into(),
                    value: b"2".to_vec(),
                    ttl: None,
                },
                BatchPut {
                    key: "c".into(),
                    value: b"3".to_vec(),
                    ttl: Some(Duration::from_secs(60)),
                },
            ])
            .unwrap();
        assert_eq!(cache.get("a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(cache.get("b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(cache.get("c").unwrap(), Some(b"3".to_vec()));
        cleanup(&db_path);
    }

    #[test]
    fn batch_put_validation_failure_applies_nothing() {
        let db_path = path("batch-atomic");
        let cache = open_cache(&db_path, "sessions");
        cache
            .put("keep", b"v0", Some(Duration::from_secs(60)))
            .unwrap();
        let oversize = vec![7_u8; MAX_VALUE_BYTES + 1];
        let error = cache
            .put_many(&[
                BatchPut {
                    key: "first".into(),
                    value: b"1".to_vec(),
                    ttl: Some(Duration::from_secs(60)),
                },
                BatchPut {
                    key: "keep".into(),
                    value: b"replaced".to_vec(),
                    ttl: Some(Duration::from_secs(60)),
                },
                BatchPut {
                    key: "big".into(),
                    value: oversize,
                    ttl: Some(Duration::from_secs(60)),
                },
                BatchPut {
                    key: "last".into(),
                    value: b"z".to_vec(),
                    ttl: Some(Duration::from_secs(60)),
                },
            ])
            .unwrap_err();
        assert!(matches!(
            error,
            KvError::ValueTooLarge { actual, maximum }
            if actual == MAX_VALUE_BYTES + 1 && maximum == MAX_VALUE_BYTES
        ));
        assert_eq!(cache.get("first").unwrap(), None);
        assert_eq!(cache.get("last").unwrap(), None);
        assert_eq!(cache.get("big").unwrap(), None);
        assert_eq!(cache.get("keep").unwrap(), Some(b"v0".to_vec()));
        cleanup(&db_path);
    }

    #[test]
    fn batch_put_rejects_empty_and_oversize_batches() {
        let db_path = path("batch-bounds");
        let cache = open_cache(&db_path, "sessions");
        assert!(matches!(
            cache.put_many(&[]).unwrap_err(),
            KvError::InvalidBatchSize(0)
        ));
        let oversize: Vec<BatchPut> = (0..=MAX_BATCH_PUT)
            .map(|offset| BatchPut {
                key: format!("k{offset}"),
                value: b"v".to_vec(),
                ttl: None,
            })
            .collect();
        assert!(matches!(
            cache.put_many(&oversize).unwrap_err(),
            KvError::InvalidBatchSize(actual) if actual == MAX_BATCH_PUT + 1
        ));
        assert_eq!(cache.get("k0").unwrap(), None);
        cleanup(&db_path);
    }

    #[test]
    fn fresh_device_catches_up_newest_first_then_backfills() {
        let writer_path = path("catchup-a");
        let reader_path = path("catchup-b");
        let scope = scope();
        let peer = logical_db_model::PeerId::new();
        {
            let writer = KvCache::open(&writer_path, scope, ReplicaId::new(), "sessions").unwrap();
            for key in ["k1", "k2", "k3", "k4", "k5"] {
                writer
                    .put(key, key.as_bytes(), Some(Duration::from_secs(60)))
                    .unwrap();
            }
            let pending = writer.database().pending_changes(0, 64).unwrap();
            let remote: Vec<logical_db::RemoteChange> = pending
                .into_iter()
                .map(|change| logical_db::RemoteChange {
                    document: change.document,
                    digest: change.digest,
                    bytes: change.bytes,
                })
                .collect();
            assert_eq!(remote.len(), 5);
            let reader = KvCache::open(&reader_path, scope, ReplicaId::new(), "sessions").unwrap();
            reader
                .database()
                .install_remote_page(peer, &remote, b"cursor-1")
                .unwrap();
            let newest = reader.recent("", 2).unwrap();
            assert_eq!(
                newest,
                vec![
                    CacheEntry {
                        key: "k5".into(),
                        value: b"k5".to_vec(),
                    },
                    CacheEntry {
                        key: "k4".into(),
                        value: b"k4".to_vec(),
                    },
                ]
            );
            let backfill = reader.scan("", None, 10).unwrap();
            assert_eq!(
                backfill
                    .iter()
                    .map(|entry| entry.key.as_str())
                    .collect::<Vec<_>>(),
                vec!["k1", "k2", "k3", "k4", "k5"]
            );
            let full_recent = reader.recent("", 10).unwrap();
            assert_eq!(
                full_recent
                    .iter()
                    .map(|entry| entry.key.as_str())
                    .collect::<Vec<_>>(),
                vec!["k5", "k4", "k3", "k2", "k1"]
            );
        }
        cleanup(&writer_path);
        cleanup(&reader_path);
    }

    #[test]
    fn namespace_checkpoint_map_round_trips_through_the_durable_slot() {
        let db_path = path("ns-slot");
        let scope = scope();
        let peer = logical_db_model::PeerId::new();
        let cache = KvCache::open(&db_path, scope, ReplicaId::new(), "sessions").unwrap();
        assert!(cache.namespace_checkpoints(peer).unwrap().is_empty());
        let mut map = logical_db_model::NamespaceCheckpoints::new();
        map.insert(
            "hot",
            logical_db_model::NamespaceCheckpoint::new(vec![1, 2], 1_000).unwrap(),
        )
        .unwrap();
        map.insert(
            "cold",
            logical_db_model::NamespaceCheckpoint::new(vec![3], 2_000).unwrap(),
        )
        .unwrap();
        cache.store_namespace_checkpoints(peer, &map).unwrap();
        let loaded = cache.namespace_checkpoints(peer).unwrap();
        assert_eq!(loaded.get("hot").unwrap().cursor(), &[1, 2]);
        assert_eq!(loaded.get("cold").unwrap().cursor(), &[3]);
        let mut advanced = loaded.clone();
        advanced
            .insert(
                "hot",
                logical_db_model::NamespaceCheckpoint::new(vec![9], 3_000).unwrap(),
            )
            .unwrap();
        cache.store_namespace_checkpoints(peer, &advanced).unwrap();
        let reloaded = cache.namespace_checkpoints(peer).unwrap();
        assert_eq!(reloaded.get("hot").unwrap().cursor(), &[9]);
        assert_eq!(reloaded.get("cold").unwrap().cursor(), &[3]);
        cleanup(&db_path);
    }

    #[test]
    fn document_namespace_classifies_slots_for_sync_filtering() {
        let db_path = path("ns-classify");
        let scope = scope();
        let replica = ReplicaId::new();
        {
            let first = KvCache::open(&db_path, scope, replica, "one").unwrap();
            first
                .put("shared", b"1", Some(Duration::from_secs(60)))
                .unwrap();
            assert_eq!(
                first
                    .document_namespace(document_for("one", "shared"))
                    .unwrap(),
                Some("one".to_owned())
            );
            assert_eq!(
                first
                    .document_namespace(document_for("two", "shared"))
                    .unwrap(),
                None
            );
            assert_eq!(first.document_namespace(DocumentId::new()).unwrap(), None);
        }
        {
            let second = KvCache::open(&db_path, scope, replica, "two").unwrap();
            second
                .put("shared", b"2", Some(Duration::from_secs(60)))
                .unwrap();
            assert_eq!(
                second
                    .document_namespace(document_for("two", "shared"))
                    .unwrap(),
                Some("two".to_owned())
            );
            assert_eq!(
                second
                    .document_namespace(document_for("one", "shared"))
                    .unwrap(),
                Some("one".to_owned())
            );
        }
        cleanup(&db_path);
    }
}
