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
//! blake3("logical-db-kv/document/v1" || 0x00 || u64be(len(namespace))
//!        || namespace || 0x00 || u64be(len(key)) || key)
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
//! kv_v   Unsigned  envelope layout version (1 inline, 2 tiered)
//! kv_key String    original key, for prefix scans
//! kv_ns  String    namespace, isolating caches sharing one scope
//! kv_val Bytes     opaque value bytes (inline values only)
//! kv_blob Bytes    32-byte SHA-256 content address (tiered values only)
//! kv_size Unsigned total value byte count (tiered values only)
//! kv_exp Timestamp expiry in milliseconds since the Unix epoch (i64::MAX = never)
//! kv_seq Unsigned  writer-local recency sequence
//! ```
//!
//! The version field ships from day one. A newer reader decodes an older
//! envelope by defaulting absent fields (`kv_exp` defaults to never expire,
//! `kv_seq` defaults to zero) and refuses an envelope whose version is newer
//! than it understands instead of guessing. Version 1 never carries sidecar
//! markers: a version-1 slot with `kv_blob` markers is corrupt, and a slot
//! mixing `kv_val` with sidecar markers is corrupt, so a tiered slot written
//! by this cache reads as absent (never as partial bytes) under a reader
//! that predates tiering.
//!
//! # Large-value tiering
//!
//! Values at or under [`TIER_THRESHOLD_BYTES`] stay inline exactly as before,
//! so small values never touch the blob path and keep reading under older
//! readers. Larger values (up to [`MAX_TIERED_VALUE_BYTES`]) split metadata
//! from bytes: the bounded change carries only the sidecar reference
//! (`kv_blob` content address plus `kv_size`), while the bytes live in a
//! content-addressed blob file beside the replica. The blob directory is the
//! replica path with `.blobs` appended, so per-OS placement inherits the
//! platform-resolved replica path with no new placement policy.
//!
//! Ordering keeps the two stores consistent without a distributed commit.
//! Writes persist blob bytes before committing the referencing envelope, so
//! a crash leaves at most an unreferenced file. Deletes and evictions commit
//! the clearing edit first and leave the old blob eligible for garbage
//! collection. [`KvCache::sweep_blobs`] removes every unreferenced blob file and
//! staging copy during a quiescent maintenance window, so a concurrent writer
//! can never lose a blob between reference publication and collection. Blob
//! files are shared by content address: a file is removed only when no live slot
//! in any namespace references it.
//!
//! Tiered bytes cross replicas in chunks of at most [`MAX_BLOB_CHUNK_BYTES`]
//! through [`KvCache::read_blob_chunk`] and [`KvCache::write_blob_chunk`],
//! so every transfer unit fits the same bounded-change discipline as a sync
//! page. A tiered read whose bytes are absent or diverge from the content
//! address fails with [`KvError::MissingBlob`] or [`KvError::CorruptBlob`]
//! instead of returning partial bytes.
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
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use lgwks_std::hash::Hasher;
pub use logical_db::{
    BatchEdit, Database, DatabaseError, MigrationReport, Projection, ProjectionSemantics,
};
use logical_db::{
    ChangeEvent, EditOutcome, Item, Object, OperationDigest, OperationId, RemoteChange, Scalar,
};
use logical_db_model::MAX_CHANGE_BYTES;
pub use logical_db_model::{
    AccountId, DocumentId, DocumentRevision, IndexId, IndexValue, NamespaceCheckpoint,
    NamespaceCheckpoints, PeerId, ReplicaId, Scope, SpaceId,
};
use thiserror::Error;
use uuid::Uuid;

/// Application schema version binding the cache projections to a scope.
pub const KV_SCHEMA_VERSION: u32 = 1;
/// Semantic version of [`kv_projections`]. It is separate from
/// [`KV_SCHEMA_VERSION`] so a change to either projection function's output for
/// any slot forces [`KvCache::migrate_schema`] instead of reopening a stale
/// index. Bump it whenever the projection logic changes.
pub const KV_PROJECTION_SEMANTICS: ProjectionSemantics = ProjectionSemantics::V1;
/// Newest envelope layout version this cache writes and reads: `1` for
/// inline values, `2` for tiered values carrying a sidecar reference.
/// Inline values keep version `1` so readers that predate tiering keep
/// reading every small value unchanged.
pub const ENVELOPE_VERSION: u64 = 2;
/// Largest value kept inline in the envelope. Larger values tier to the
/// blob store with only a sidecar reference inside the bounded change.
pub const TIER_THRESHOLD_BYTES: usize = 256 * 1024;
/// Largest cache value accepted at any tier. The bound is checked before
/// any allocation, file creation, or commit, and matches the snapshot
/// materialization budget the substrate already tolerates.
pub const MAX_TIERED_VALUE_BYTES: usize = 16_777_216;
/// Largest blob chunk moved by one [`KvCache::read_blob_chunk`] or
/// [`KvCache::write_blob_chunk`] call. Every transfer unit fits the bounded
/// change, so chunked sync transfer pages exactly like sync pages.
pub const MAX_BLOB_CHUNK_BYTES: usize = MAX_CHANGE_BYTES;
/// Largest cache value accepted inline, leaving headroom for change framing
/// and the remaining envelope fields under [`MAX_CHANGE_BYTES`]. Values
/// above [`TIER_THRESHOLD_BYTES`] tier instead of using this headroom.
pub const MAX_VALUE_BYTES: usize = MAX_CHANGE_BYTES - 65_536;

const _: () = assert!(
    TIER_THRESHOLD_BYTES < MAX_VALUE_BYTES,
    "the tier threshold must leave room for the inline envelope headroom"
);
/// Stream buffer used while hashing a tiered blob file.
const BLOB_HASH_BUFFER_BYTES: usize = 64 * 1024;
/// Largest page accepted by [`KvCache::scan`] and [`KvCache::evict_expired`],
/// mirroring the facade's bounded pages.
pub const MAX_PAGE_LIMIT: usize = 256;
/// Largest atomic batch accepted by [`KvCache::put_many`].
///
/// The bound caps transient materialization and per-call outbox churn.
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
/// Root-map property carrying the 32-byte SHA-256 content address of a
/// tiered value whose bytes live in the blob store.
pub const FIELD_BLOB: &str = "kv_blob";
/// Root-map property carrying the total byte count of a tiered value.
pub const FIELD_SIZE: &str = "kv_size";
/// Root-map property carrying the expiry timestamp.
pub const FIELD_EXPIRY: &str = "kv_exp";
/// Root-map property carrying the writer-local recency sequence.
pub const FIELD_SEQUENCE: &str = "kv_seq";

/// One live cache entry returned by [`KvCache::scan`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct CacheEntry {
    /// Original cache key.
    pub key: String,
    /// Opaque value bytes as stored.
    pub value: Vec<u8>,
}

/// Content address of a tiered value, returned by
/// [`KvCache::blob_reference`] for chunked sync transfer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct BlobReference {
    /// SHA-256 of the complete value bytes; also the blob file name.
    pub digest: [u8; 32],
    /// Total value byte count.
    pub size: u64,
}

/// One entry of an atomic [`KvCache::put_many`] batch.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
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
#[non_exhaustive]
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
    /// A value would not fit the blob store, checked before any allocation,
    /// file creation, or commit.
    #[error("cache value length {actual} exceeds maximum {maximum}")]
    BlobTooLarge {
        /// Observed value byte count.
        actual: usize,
        /// Accepted value byte count.
        maximum: usize,
    },
    /// A tiered value's bytes are absent from the blob store. The sidecar
    /// reference is intact, so the read fails instead of returning partial
    /// bytes.
    #[error("cache blob {digest} is missing from the blob store")]
    MissingBlob {
        /// Lowercase hex SHA-256 content address.
        digest: String,
    },
    /// A tiered value's bytes diverge from their content address (wrong
    /// length or hash). The read fails instead of returning partial bytes.
    #[error("cache blob {digest} does not match its content address")]
    CorruptBlob {
        /// Lowercase hex SHA-256 content address.
        digest: String,
    },
    /// A blob chunk exceeded [`MAX_BLOB_CHUNK_BYTES`].
    #[error("blob chunk length {0} exceeds the page-bounded maximum")]
    InvalidChunkLength(usize),
    /// A blob chunk range fell outside its declared total.
    #[error("blob chunk offset {0} falls outside its declared total")]
    InvalidChunkOffset(u64),
    /// The blob store directory or a blob file could not be read or written.
    #[error("cache blob store failed: {0}")]
    BlobIo(#[from] std::io::Error),
    /// A prefix-scan page fell outside `1..=256`.
    #[error("scan limit {0} is outside 1..=256")]
    InvalidScanLimit(usize),
    /// A batch fell outside `1..=32`.
    #[error("batch size {0} is outside 1..=32")]
    InvalidBatchSize(usize),
    /// A caller-owned batch operation did not carry a `UUIDv7` timestamp.
    #[error("batch operation identity must be a UUIDv7")]
    InvalidOperationId,
    /// A namespace checkpoint map was malformed.
    #[error("namespace checkpoint map is malformed")]
    CorruptCheckpointMap,
    /// The replica's scope was rewritten by a baseline install, whose recovered
    /// documents carry no events. Maintenance that reclaims by the
    /// event-derived document set fails closed instead of reclaiming bytes that
    /// a recovered document still references.
    #[error("replica scope was rewritten by a baseline install")]
    ScopeRewritten,
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
#[non_exhaustive]
pub struct KvCache {
    /// Embedded replica backing this cache.
    database: Database,
    /// Cache namespace prefixing every key.
    namespace: String,
    /// Monotonic sequence stamped on every committed mutation.
    sequence: AtomicU64,
    /// Directory holding tiered blob files for this cache.
    blob_dir: PathBuf,
    /// Serializes tiered blob create/delete against concurrent readers.
    blob_lock: Mutex<()>,
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
        let database = Database::open_with_schema(
            path.as_ref(),
            scope,
            replica,
            KV_SCHEMA_VERSION,
            kv_projections(),
        )?;
        Ok(Self {
            database,
            namespace: namespace.to_owned(),
            sequence: AtomicU64::new(seed_sequence()),
            blob_dir: blob_dir_for(path.as_ref()),
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
            kv_projections(),
            batch_size,
        )?;
        Ok((
            Self {
                database,
                namespace: namespace.to_owned(),
                sequence: AtomicU64::new(seed_sequence()),
                blob_dir: blob_dir_for(path.as_ref()),
                blob_lock: Mutex::new(()),
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
        let now = now_ms();
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
        let document = document_for(&self.namespace, key);
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
        let document = document_for(&self.namespace, key);
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

    /// Reads up to `len` blob bytes at `offset` for content address
    /// `digest`, validating the bounded blob stream before returning one
    /// page-bounded chunk.
    ///
    /// Pair with [`KvCache::write_blob_chunk`] to move tiered bytes between
    /// replicas after the sidecar reference syncs through the existing
    /// change pages.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidChunkLength`] when `len` exceeds
    /// [`MAX_BLOB_CHUNK_BYTES`], [`KvError::InvalidChunkOffset`] when the
    /// range falls outside the stored file, [`KvError::MissingBlob`] when
    /// no file exists for `digest`, [`KvError::CorruptBlob`] when the file
    /// shrinks mid-read, or [`KvError::BlobIo`] for other I/O failures.
    pub fn read_blob_chunk(
        &self,
        digest: &[u8; 32],
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, KvError> {
        if len > MAX_BLOB_CHUNK_BYTES {
            return Err(KvError::InvalidChunkLength(len));
        }
        let _blob_guard = self.lock_blobs();
        let path = self.blob_path(digest);
        let stored = fs::metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                KvError::MissingBlob {
                    digest: hex_digest(digest),
                }
            } else {
                KvError::BlobIo(error)
            }
        })?;
        let stored_len = stored.len();
        let tiered_max = u64::try_from(MAX_TIERED_VALUE_BYTES).unwrap_or(u64::MAX);
        if stored_len > tiered_max || !Self::blob_valid_path(&path, digest, stored_len)? {
            return Err(KvError::CorruptBlob {
                digest: hex_digest(digest),
            });
        }
        let want = u64::try_from(len).unwrap_or(u64::MAX);
        let end = offset
            .checked_add(want)
            .ok_or(KvError::InvalidChunkOffset(offset))?;
        if end > stored_len {
            return Err(KvError::InvalidChunkOffset(offset));
        }
        let mut file = File::open(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                KvError::MissingBlob {
                    digest: hex_digest(digest),
                }
            } else {
                KvError::BlobIo(error)
            }
        })?;
        file.seek(SeekFrom::Start(offset))
            .map_err(KvError::BlobIo)?;
        let mut chunk = vec![0_u8; len];
        file.read_exact(&mut chunk).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                KvError::CorruptBlob {
                    digest: hex_digest(digest),
                }
            } else {
                KvError::BlobIo(error)
            }
        })?;
        Ok(chunk)
    }

    /// Writes `chunk` at `offset` into the staging blob file for content
    /// address `digest`, whose complete length is `total`.
    ///
    /// Every bound is validated before allocation or I/O: the total against
    /// [`MAX_TIERED_VALUE_BYTES`], the chunk against
    /// [`MAX_BLOB_CHUNK_BYTES`], and the range against the total. Reports
    /// whether the file reached its declared total; a completed file is
    /// hash-verified before reporting success, so a diverged transfer fails
    /// here instead of poisoning a later read. Partial files are never
    /// trusted by reads until length and hash both verify.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::BlobTooLarge`] when `total` exceeds
    /// [`MAX_TIERED_VALUE_BYTES`], [`KvError::InvalidChunkLength`] for an
    /// oversized chunk, [`KvError::InvalidChunkOffset`] when the range
    /// falls outside `total`, [`KvError::CorruptBlob`] when a completed
    /// file diverges from `digest`, or [`KvError::BlobIo`] for I/O
    /// failures.
    pub fn write_blob_chunk(
        &self,
        digest: &[u8; 32],
        total: u64,
        offset: u64,
        chunk: &[u8],
    ) -> Result<bool, KvError> {
        let tiered_max = u64::try_from(MAX_TIERED_VALUE_BYTES).unwrap_or(u64::MAX);
        if total > tiered_max {
            return Err(KvError::BlobTooLarge {
                actual: usize::try_from(total).unwrap_or(usize::MAX),
                maximum: MAX_TIERED_VALUE_BYTES,
            });
        }
        if chunk.len() > MAX_BLOB_CHUNK_BYTES {
            return Err(KvError::InvalidChunkLength(chunk.len()));
        }
        let width = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        let end = offset
            .checked_add(width)
            .ok_or(KvError::InvalidChunkOffset(offset))?;
        if end > total {
            return Err(KvError::InvalidChunkOffset(offset));
        }
        let _blob_guard = self.lock_blobs();
        fs::create_dir_all(&self.blob_dir)?;
        let path = self.blob_staging_path(digest, total);
        let mut file = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(chunk)?;
        file.sync_all()?;
        drop(file);
        let stored = fs::metadata(&path)?.len();
        if stored != total {
            return Ok(false);
        }
        if !Self::blob_valid_path(&path, digest, total)? {
            return Err(KvError::CorruptBlob {
                digest: hex_digest(digest),
            });
        }
        self.install_blob(&path, digest, total)?;
        Ok(true)
    }

    /// Removes every unreferenced blob file, reporting how many were
    /// reclaimed: orphan bytes from a crash between reference removal and
    /// blob collection, superseded import staging, and leftover temporary
    /// files. Files still referenced by any live slot in any namespace are
    /// kept, including expired-but-not-yet-evicted tiered slots.
    ///
    /// Call after crash recovery or startup during a quiescent maintenance
    /// window with no concurrent cache writer, reader, or sync importer on any
    /// handle or process; staging files only become referenced once their
    /// sidecar envelope commits.
    ///
    /// # Errors
    ///
    /// Returns envelope decoding failures, or [`KvError::BlobIo`] for
    /// directory and file failures.
    pub fn sweep_blobs(&self) -> Result<usize, KvError> {
        let _blob_guard = self.lock_blobs();
        let live = self.blob_references_locked()?;
        let entries = match fs::read_dir(&self.blob_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(KvError::BlobIo(error)),
        };
        let mut removed = 0_usize;
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let orphan = if let Some((stem, suffix)) = name.split_once(".tmp-") {
                let Some(digest) = hex_to_digest(stem) else {
                    continue;
                };
                if suffix.starts_with("gc-") {
                    self.gc_staging_is_orphan(&entry.path(), &digest, &live)?
                } else if !live.contains(&digest) {
                    true
                } else if let Ok(total) = suffix.parse::<u64>() {
                    let complete = fs::metadata(entry.path())?.len() == total;
                    complete && !Self::blob_valid_path(&entry.path(), &digest, total)?
                } else {
                    true
                }
            } else if is_blob_name(&name) {
                match hex_to_digest(&name) {
                    Some(digest) => !live.contains(&digest),
                    None => false,
                }
            } else {
                continue;
            };
            if orphan {
                match fs::remove_file(entry.path()) {
                    Ok(()) => removed = removed.saturating_add(1),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(KvError::BlobIo(error)),
                }
            }
        }
        Ok(removed)
    }

    /// Returns whether a staging blob should be discarded as orphaned.
    fn gc_staging_is_orphan(
        &self,
        path: &Path,
        digest: &[u8; 32],
        live: &BTreeSet<[u8; 32]>,
    ) -> Result<bool, KvError> {
        if !live.contains(digest) {
            return Ok(true);
        }
        let size = fs::metadata(path)?.len();
        if !Self::blob_valid_path(path, digest, size)? {
            return Ok(true);
        }
        let canonical = self.blob_path(digest);
        match fs::metadata(&canonical) {
            Ok(metadata) if Self::blob_valid_path(&canonical, digest, metadata.len())? => Ok(true),
            Ok(_) => {
                self.install_blob(path, digest, size)?;
                Ok(false)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.install_blob(path, digest, size)?;
                Ok(false)
            }
            Err(error) => Err(KvError::BlobIo(error)),
        }
    }

    /// Builds the inline or tiered slot and the batch edit for one entry.
    fn prepare_batch_member(
        &self,
        entry: &BatchPut,
        expires_at_ms: i64,
        sequence: u64,
        batch_operation: OperationId,
        index: usize,
        batch_digest: &OperationDigest,
    ) -> Result<(SlotValue, BatchEdit), KvError> {
        let slot = if entry.value.len() > TIER_THRESHOLD_BYTES {
            let digest = content_digest(&entry.value);
            self.write_blob_file(&digest, &entry.value)?;
            SlotValue::Tiered {
                digest,
                size: u64::try_from(entry.value.len()).unwrap_or(u64::MAX),
            }
        } else {
            SlotValue::Inline(entry.value.clone())
        };
        let operation = batch_member_operation(batch_operation, index);
        let request = match slot {
            SlotValue::Inline(ref value) => put_digest(
                &operation,
                &self.namespace,
                &entry.key,
                value,
                expires_at_ms,
                sequence,
            ),
            SlotValue::Tiered { ref digest, size } => tiered_put_digest(
                &operation,
                &self.namespace,
                &entry.key,
                digest,
                size,
                expires_at_ms,
                sequence,
            ),
        };
        let request = bind_batch_request(request, batch_digest);
        let document = document_for(&self.namespace, &entry.key);
        let stored_key = entry.key.clone();
        let stored_namespace = self.namespace.clone();
        let stored_slot = slot.clone();
        let edit = BatchEdit::new(document, operation, request, move |editor| {
            editor.delete(&Object::root(), FIELD_VALUE)?;
            editor.delete(&Object::root(), FIELD_BLOB)?;
            editor.delete(&Object::root(), FIELD_SIZE)?;
            match stored_slot {
                SlotValue::Inline(ref value) => {
                    editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(1))?;
                    editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(value.clone()))?;
                }
                SlotValue::Tiered { ref digest, size } => {
                    editor.put(
                        &Object::root(),
                        FIELD_VERSION,
                        Scalar::Unsigned(ENVELOPE_VERSION),
                    )?;
                    editor.put(&Object::root(), FIELD_BLOB, Scalar::Bytes(digest.to_vec()))?;
                    editor.put(&Object::root(), FIELD_SIZE, Scalar::Unsigned(size))?;
                }
            }
            editor.put(&Object::root(), FIELD_KEY, Scalar::String(stored_key))?;
            editor.put(
                &Object::root(),
                FIELD_NAMESPACE,
                Scalar::String(stored_namespace),
            )?;
            editor.put(
                &Object::root(),
                FIELD_EXPIRY,
                Scalar::Timestamp(expires_at_ms),
            )?;
            editor.put(&Object::root(), FIELD_SEQUENCE, Scalar::Unsigned(sequence))?;
            Ok(())
        });
        Ok((slot, edit))
    }

    /// Writes a value with an explicit expiry and sequence.
    fn put_absolute(
        &self,
        key: &str,
        value: &[u8],
        expires_at_ms: i64,
        sequence: u64,
    ) -> Result<(), KvError> {
        if value.len() > TIER_THRESHOLD_BYTES {
            let digest = content_digest(value);
            self.write_blob_file(&digest, value)?;
            let size = u64::try_from(value.len()).unwrap_or(u64::MAX);
            self.put_envelope(
                key,
                &SlotValue::Tiered { digest, size },
                expires_at_ms,
                sequence,
            )?;
            return Ok(());
        }
        self.put_envelope(
            key,
            &SlotValue::Inline(value.to_vec()),
            expires_at_ms,
            sequence,
        )
    }

    /// Persists a decoded slot envelope into the database document.
    fn put_envelope(
        &self,
        key: &str,
        slot: &SlotValue,
        expires_at_ms: i64,
        sequence: u64,
    ) -> Result<(), KvError> {
        let document = document_for(&self.namespace, key);
        match self.decode_slot(document)? {
            Decoded::Found(envelope)
                if envelope.namespace == self.namespace && envelope.key == key => {}
            Decoded::Found(_) | Decoded::Absent => {}
            Decoded::Corrupt => return Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => {
                return Err(KvError::UnsupportedEnvelopeVersion { actual });
            }
        }
        let operation = OperationId::new();
        let digest = match *slot {
            SlotValue::Inline(ref value) => put_digest(
                &operation,
                &self.namespace,
                key,
                value,
                expires_at_ms,
                sequence,
            ),
            SlotValue::Tiered { ref digest, size } => tiered_put_digest(
                &operation,
                &self.namespace,
                key,
                digest,
                size,
                expires_at_ms,
                sequence,
            ),
        };
        let stored_key = key.to_owned();
        let stored_namespace = self.namespace.clone();
        let stored_slot = slot.clone();
        match self.database.edit(document, operation, digest, |editor| {
            editor.delete(&Object::root(), FIELD_VALUE)?;
            editor.delete(&Object::root(), FIELD_BLOB)?;
            editor.delete(&Object::root(), FIELD_SIZE)?;
            match stored_slot {
                SlotValue::Inline(ref value) => {
                    editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(1))?;
                    editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(value.clone()))?;
                }
                SlotValue::Tiered { ref digest, size } => {
                    editor.put(
                        &Object::root(),
                        FIELD_VERSION,
                        Scalar::Unsigned(ENVELOPE_VERSION),
                    )?;
                    editor.put(&Object::root(), FIELD_BLOB, Scalar::Bytes(digest.to_vec()))?;
                    editor.put(&Object::root(), FIELD_SIZE, Scalar::Unsigned(size))?;
                }
            }
            editor.put(&Object::root(), FIELD_KEY, Scalar::String(stored_key))?;
            editor.put(
                &Object::root(),
                FIELD_NAMESPACE,
                Scalar::String(stored_namespace),
            )?;
            editor.put(
                &Object::root(),
                FIELD_EXPIRY,
                Scalar::Timestamp(expires_at_ms),
            )?;
            editor.put(&Object::root(), FIELD_SEQUENCE, Scalar::Unsigned(sequence))?;
            Ok(())
        })? {
            EditOutcome::Applied(_) | EditOutcome::Duplicate(_) => {}
            // `EditOutcome` is non_exhaustive: a future outcome is not a
            // successful slot write, so this fails closed.
            _ => return Err(KvError::CorruptEnvelope),
        }
        Ok(())
    }

    /// Reads and validates the envelope stored under a document id.
    fn decode_slot(&self, document: DocumentId) -> Result<Decoded, KvError> {
        let decoded = self.database.read_with_revision(document, |reader| {
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
            if key.is_empty()
                || key.len() > logical_db_model::MAX_NAME_BYTES
                || namespace.is_empty()
                || namespace.len() > logical_db_model::MAX_NAME_BYTES
            {
                return Ok(Decoded::Corrupt);
            }
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
            let inline = match reader.get(&Object::root(), FIELD_VALUE)? {
                None => None,
                Some(Item::Scalar(Scalar::Bytes(value))) => Some(value),
                Some(_) => return Ok(Decoded::Corrupt),
            };
            let blob = match reader.get(&Object::root(), FIELD_BLOB)? {
                None => None,
                Some(Item::Scalar(Scalar::Bytes(blob))) => Some(blob),
                Some(_) => return Ok(Decoded::Corrupt),
            };
            let size = match reader.get(&Object::root(), FIELD_SIZE)? {
                None => None,
                Some(Item::Scalar(Scalar::Unsigned(size))) => Some(size),
                Some(Item::Scalar(Scalar::Integer(size))) => {
                    let Ok(size) = u64::try_from(size) else {
                        return Ok(Decoded::Corrupt);
                    };
                    Some(size)
                }
                Some(_) => return Ok(Decoded::Corrupt),
            };
            let value = match (inline, blob, size) {
                (Some(value), None, None) if version == 1 && value.len() <= MAX_VALUE_BYTES => {
                    SlotValue::Inline(value)
                }
                (None, Some(blob), Some(size))
                    if version == ENVELOPE_VERSION
                        && blob.len() == 32
                        && size > u64::try_from(TIER_THRESHOLD_BYTES).unwrap_or(u64::MAX)
                        && size <= u64::try_from(MAX_TIERED_VALUE_BYTES).unwrap_or(u64::MAX) =>
                {
                    let Ok(digest) = <[u8; 32]>::try_from(blob) else {
                        return Ok(Decoded::Corrupt);
                    };
                    SlotValue::Tiered { digest, size }
                }
                (None, None, None) => return Ok(Decoded::Absent),
                _ => return Ok(Decoded::Corrupt),
            };
            let Some((expires_at_ms, sequence)) = Self::decode_expiry_and_sequence(reader)? else {
                return Ok(Decoded::Corrupt);
            };
            Ok(Decoded::Found(Envelope {
                key,
                namespace,
                value,
                expires_at_ms,
                sequence,
                revision: DocumentRevision::new(1),
            }))
        })?;
        let Some((revision, decoded)) = decoded else {
            return Ok(Decoded::Absent);
        };
        Ok(match decoded {
            Decoded::Found(mut envelope) => {
                envelope.revision = revision;
                Decoded::Found(envelope)
            }
            decoded => decoded,
        })
    }

    /// Decodes expiry and sequence fields from a reader.
    fn decode_expiry_and_sequence(
        reader: &mut logical_db::Reader<'_>,
    ) -> Result<Option<(i64, u64)>, logical_db::CrdtError> {
        let expires_at_ms = match reader.get(&Object::root(), FIELD_EXPIRY)? {
            None => NEVER_EXPIRES_AT_MS,
            Some(Item::Scalar(Scalar::Timestamp(millis) | Scalar::Integer(millis))) => millis,
            Some(_) => return Ok(None),
        };
        let sequence = match reader.get(&Object::root(), FIELD_SEQUENCE)? {
            None => 0,
            Some(Item::Scalar(Scalar::Unsigned(sequence))) => sequence,
            Some(Item::Scalar(Scalar::Integer(sequence))) => {
                let Ok(sequence) = u64::try_from(sequence) else {
                    return Ok(None);
                };
                sequence
            }
            Some(_) => return Ok(None),
        };
        Ok(Some((expires_at_ms, sequence)))
    }

    /// Clears a reference if the stored key and namespace match.
    fn remove_reference(&self, document: DocumentId, key: &str) -> Result<ClearOutcome, KvError> {
        let expected_revision = match self.decode_slot(document)? {
            Decoded::Found(envelope)
                if envelope.namespace == self.namespace && envelope.key == key =>
            {
                Some(envelope.revision)
            }
            Decoded::Found(_) | Decoded::Absent => None,
            Decoded::Corrupt => return Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => {
                return Err(KvError::UnsupportedEnvelopeVersion { actual });
            }
        };
        self.remove_reference_at(document, key, expected_revision)
    }

    /// Clears a reference when an optional revision precondition holds.
    fn remove_reference_at(
        &self,
        document: DocumentId,
        key: &str,
        expected_revision: Option<DocumentRevision>,
    ) -> Result<ClearOutcome, KvError> {
        let now = now_ms();
        match self.decode_slot(document)? {
            Decoded::Found(envelope)
                if envelope.namespace == self.namespace && envelope.key == key =>
            {
                if expected_revision.is_some_and(|expected| envelope.revision != expected) {
                    return Ok(ClearOutcome {
                        live: false,
                        cleared: false,
                    });
                }
                let live = envelope.expires_at_ms > now;
                let sequence = self.next_sequence(now);
                let operation = OperationId::new();
                let digest = clear_digest(&operation, &self.namespace, key);
                let clear = |editor: &mut logical_db::Editor<'_>| {
                    editor.delete(&Object::root(), FIELD_VALUE)?;
                    editor.delete(&Object::root(), FIELD_BLOB)?;
                    editor.delete(&Object::root(), FIELD_SIZE)?;
                    editor.put(&Object::root(), FIELD_EXPIRY, Scalar::Timestamp(0))?;
                    editor.put(&Object::root(), FIELD_SEQUENCE, Scalar::Unsigned(sequence))?;
                    Ok(())
                };
                let result = match expected_revision {
                    Some(revision) => self
                        .database
                        .edit_if_revision(document, revision, operation, digest, clear),
                    None => self.database.edit(document, operation, digest, clear),
                }?;
                match result {
                    EditOutcome::Applied(_) | EditOutcome::Duplicate(_) => Ok(ClearOutcome {
                        live,
                        cleared: true,
                    }),
                    // `EditOutcome` is non_exhaustive: fail closed.
                    _ => Err(KvError::CorruptEnvelope),
                }
            }
            Decoded::Found(_) | Decoded::Absent => Ok(ClearOutcome {
                live: false,
                cleared: false,
            }),
            Decoded::Corrupt => Err(KvError::CorruptEnvelope),
            Decoded::Unsupported(actual) => Err(KvError::UnsupportedEnvelopeVersion { actual }),
        }
    }

    /// Collects digests of every live tiered blob reference under the lock.
    fn blob_references_locked(&self) -> Result<BTreeSet<[u8; 32]>, KvError> {
        let mut live = BTreeSet::new();
        for document in self.snapshot_documents()? {
            match self.decode_slot(document)? {
                Decoded::Found(Envelope {
                    value: SlotValue::Tiered { digest, .. },
                    ..
                }) => {
                    live.insert(digest);
                }
                Decoded::Found(_) | Decoded::Absent => {}
                Decoded::Corrupt => return Err(KvError::CorruptEnvelope),
                Decoded::Unsupported(actual) => {
                    return Err(KvError::UnsupportedEnvelopeVersion { actual });
                }
            }
        }
        Ok(live)
    }

    /// Returns the canonical path for a blob digest.
    fn blob_path(&self, digest: &[u8; 32]) -> PathBuf {
        self.blob_dir.join(hex_digest(digest))
    }

    /// Returns the staging path for a blob of the given total size.
    fn blob_staging_path(&self, digest: &[u8; 32], total: u64) -> PathBuf {
        self.blob_dir
            .join(format!("{}.tmp-{total}", hex_digest(digest)))
    }

    /// Writes blob bytes to a staged file and installs them atomically.
    fn write_blob_file(&self, digest: &[u8; 32], bytes: &[u8]) -> Result<(), KvError> {
        let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
        if self.blob_valid(digest, size)? {
            return Ok(());
        }
        fs::create_dir_all(&self.blob_dir)?;
        let staged = self.blob_dir.join(format!(
            "{}.tmp-{}",
            hex_digest(digest),
            Uuid::now_v7().hyphenated()
        ));
        let mut file = File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&staged)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        self.install_blob(&staged, digest, size)?;
        Ok(())
    }

    /// Returns whether the canonical blob file matches digest and size.
    fn blob_valid(&self, digest: &[u8; 32], size: u64) -> Result<bool, KvError> {
        Self::blob_valid_path(&self.blob_path(digest), digest, size)
    }

    /// Hashes a path and checks it against the expected digest and size.
    fn blob_valid_path(path: &Path, digest: &[u8; 32], size: u64) -> Result<bool, KvError> {
        let stored = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(KvError::BlobIo(error)),
        };
        if stored.len() != size {
            return Ok(false);
        }
        if size > u64::try_from(MAX_TIERED_VALUE_BYTES).unwrap_or(u64::MAX) {
            return Ok(false);
        }
        let mut file = File::open(path)?;
        let mut hasher = Hasher::new();
        let mut buffer = vec![0_u8; BLOB_HASH_BUFFER_BYTES].into_boxed_slice();
        let mut remaining = size;
        while remaining > 0 {
            let width = usize::try_from(remaining)
                .unwrap_or(BLOB_HASH_BUFFER_BYTES)
                .min(buffer.len());
            match file.read_exact(&mut buffer[..width]) {
                Ok(()) => {
                    hasher.update(&buffer[..width]);
                }
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(error) => return Err(KvError::BlobIo(error)),
            }
            remaining = remaining.saturating_sub(u64::try_from(width).unwrap_or(0));
        }
        Ok(*hasher.finalize().as_bytes() == *digest)
    }

    /// Renames a staged blob into place, resolving existing targets.
    fn install_blob(&self, source: &Path, digest: &[u8; 32], size: u64) -> Result<(), KvError> {
        let target = self.blob_path(digest);
        match fs::rename(source, &target) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if Self::blob_valid_path(&target, digest, size)? {
                    match fs::remove_file(source) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(KvError::BlobIo(error)),
                    }
                    return Ok(());
                }
                match fs::remove_file(&target) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(KvError::BlobIo(error)),
                }
                fs::rename(source, target)?;
                Ok(())
            }
            Err(error) => Err(KvError::BlobIo(error)),
        }
    }

    /// Acquires the blob lock, recovering from a poisoned mutex.
    fn lock_blobs(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.blob_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Reads, verifies, and returns a tiered blob under the blob lock.
    fn resolve_tiered_locked(&self, digest: &[u8; 32], size: u64) -> Result<Vec<u8>, KvError> {
        let address = hex_digest(digest);
        let tiered_max = u64::try_from(MAX_TIERED_VALUE_BYTES).unwrap_or(u64::MAX);
        if size > tiered_max {
            return Err(KvError::CorruptBlob { digest: address });
        }
        let path = self.blob_path(digest);
        let stored = fs::metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                KvError::MissingBlob {
                    digest: address.clone(),
                }
            } else {
                KvError::BlobIo(error)
            }
        })?;
        if stored.len() != size {
            return Err(KvError::CorruptBlob { digest: address });
        }
        let bytes = fs::read(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                KvError::MissingBlob {
                    digest: address.clone(),
                }
            } else {
                KvError::BlobIo(error)
            }
        })?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != size
            || content_digest(&bytes) != *digest
        {
            return Err(KvError::CorruptBlob { digest: address });
        }
        Ok(bytes)
    }

    /// Pages change history to collect every scoped document id.
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
                let &ChangeEvent::Document {
                    ref document,
                    sequence,
                    ..
                } = event
                else {
                    return Err(KvError::ScopeRewritten);
                };
                documents.insert(*document);
                after = sequence;
            }
            if complete {
                break;
            }
        }
        Ok(documents)
    }

    /// Advances the recency sequence past a wall-clock floor and returns it.
    fn next_sequence(&self, now: i64) -> u64 {
        let floor = u64::try_from(now.max(0)).unwrap_or(0);
        self.sequence.fetch_max(floor, Ordering::SeqCst);
        self.sequence
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1)
    }

    /// Reserves recency sequences covering every member of a batch.
    fn reserve_batch_sequences(&self, started_at_ms: i64, count: usize) -> Result<(), KvError> {
        let last_index = count.checked_sub(1).ok_or(KvError::InvalidBatchSize(0))?;
        let last_sequence = batch_sequence(started_at_ms, last_index)?;
        self.sequence.fetch_max(last_sequence, Ordering::SeqCst);
        Ok(())
    }
}

/// Returns the schema-bound expiry and recency projections every cache scope
/// carries. The functions never fail a commit: slots without a live value
/// project no rows, missing expiry defaults to never expire, missing
/// recency defaults to zero, and unexpected shapes project nothing. A tiered
/// slot is live exactly when it carries a well-formed 32-byte content
/// address; point reads stay strict (see [`KvError`]).
#[must_use]
pub fn kv_projections() -> Vec<Projection> {
    vec![
        Projection::new(EXPIRY_INDEX, KV_PROJECTION_SEMANTICS, |reader| {
            if !projection_has_live_value(reader)? {
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
        Projection::new(RECENCY_INDEX, KV_PROJECTION_SEMANTICS, |reader| {
            if !projection_has_live_value(reader)? {
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

/// Returns whether a reader describes a well-formed live slot value.
fn projection_has_live_value(
    reader: &mut logical_db::Reader<'_>,
) -> Result<bool, logical_db::CrdtError> {
    let Some(Item::Scalar(Scalar::String(key))) = reader.get(&Object::root(), FIELD_KEY)? else {
        return Ok(false);
    };
    let Some(Item::Scalar(Scalar::String(namespace))) =
        reader.get(&Object::root(), FIELD_NAMESPACE)?
    else {
        return Ok(false);
    };
    if key.is_empty()
        || key.len() > logical_db_model::MAX_NAME_BYTES
        || namespace.is_empty()
        || namespace.len() > logical_db_model::MAX_NAME_BYTES
    {
        return Ok(false);
    }
    let version = match reader.get(&Object::root(), FIELD_VERSION)? {
        None => 1,
        Some(Item::Scalar(Scalar::Unsigned(version))) => version,
        Some(Item::Scalar(Scalar::Integer(version))) => {
            let Ok(version) = u64::try_from(version) else {
                return Ok(false);
            };
            version
        }
        Some(_) => return Ok(false),
    };
    let inline_len = match reader.get(&Object::root(), FIELD_VALUE)? {
        None => None,
        Some(Item::Scalar(Scalar::Bytes(value))) => Some(value.len()),
        Some(_) => return Ok(false),
    };
    let blob_len = match reader.get(&Object::root(), FIELD_BLOB)? {
        None => None,
        Some(Item::Scalar(Scalar::Bytes(blob))) => Some(blob.len()),
        Some(_) => return Ok(false),
    };
    let size = match reader.get(&Object::root(), FIELD_SIZE)? {
        None => None,
        Some(Item::Scalar(Scalar::Unsigned(size))) => Some(size),
        Some(Item::Scalar(Scalar::Integer(size))) => u64::try_from(size).ok(),
        Some(_) => return Ok(false),
    };
    match version {
        1 => Ok(inline_len.is_some_and(|len| len <= MAX_VALUE_BYTES)
            && blob_len.is_none()
            && size.is_none()),
        ENVELOPE_VERSION => {
            let tiered_max = u64::try_from(MAX_TIERED_VALUE_BYTES).unwrap_or(u64::MAX);
            let threshold = u64::try_from(TIER_THRESHOLD_BYTES).unwrap_or(u64::MAX);
            Ok(blob_len == Some(32)
                && inline_len.is_none()
                && size.is_some_and(|size| size > threshold && size <= tiered_max))
        }
        _ => Ok(false),
    }
}

/// Decoded identity, payload, and metadata for one KV document slot.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Envelope {
    /// Logical key stored in the envelope.
    key: String,
    /// Namespace the envelope belongs to.
    namespace: String,
    /// Inline bytes or a tiered content address.
    value: SlotValue,
    /// Absolute expiry in Unix milliseconds, or never-expires sentinel.
    expires_at_ms: i64,
    /// Monotonic recency sequence assigned at write time.
    sequence: u64,
    /// Document revision observed while decoding.
    revision: DocumentRevision,
}

/// How a KV value is stored: inline bytes or a tiered blob reference.
#[derive(Clone, Debug, Eq, PartialEq)]
enum SlotValue {
    /// Value kept entirely inside the document.
    Inline(Vec<u8>),
    /// Content address and byte length of an external blob.
    Tiered {
        /// Blake3 content digest of the blob file.
        digest: [u8; 32],
        /// Exact blob length in bytes.
        size: u64,
    },
}

/// Result of clearing a reference: liveness before clear and whether it cleared.
struct ClearOutcome {
    /// Whether the value was unexpired when the clear was attempted.
    live: bool,
    /// Whether a clear edit was applied to the document.
    cleared: bool,
}

/// Outcome of decoding a document slot during point reads.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Decoded {
    /// Document has no KV envelope fields.
    Absent,
    /// Envelope fields are present but malformed.
    Corrupt,
    /// Envelope declares a newer format version than this crate understands.
    Unsupported(u64),
    /// Envelope decoded successfully.
    Found(Envelope),
}

/// Validates a key against empty and max-name bounds.
fn validate_key(key: &str) -> Result<(), KvError> {
    if key.is_empty() || key.len() > logical_db_model::MAX_NAME_BYTES {
        return Err(KvError::InvalidKey);
    }
    Ok(())
}

/// Validates a namespace against empty and max-name bounds.
fn validate_namespace(namespace: &str) -> Result<(), KvError> {
    if namespace.is_empty() || namespace.len() > logical_db_model::MAX_NAME_BYTES {
        return Err(KvError::InvalidNamespace);
    }
    Ok(())
}

/// Maps model checkpoint-map errors onto the KV error surface.
fn checkpoint_map_error(error: &logical_db_model::ModelError) -> KvError {
    match *error {
        logical_db_model::ModelError::UnsupportedNamespaceCheckpointVersion { actual } => {
            KvError::UnsupportedCheckpointMapVersion { actual }
        }
        logical_db_model::ModelError::NamespaceCheckpointMapTooLarge { actual, maximum } => {
            KvError::CheckpointMapTooLarge { actual, maximum }
        }
        _ => KvError::CorruptCheckpointMap,
    }
}

/// Returns the current Unix time in milliseconds, zero on clock failure.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Seeds a recency sequence from the current wall clock.
fn seed_sequence() -> u64 {
    u64::try_from(now_ms().max(0)).unwrap_or(0)
}

/// Converts an optional TTL into an absolute expiry timestamp.
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

/// Computes the content digest used as a blob's canonical address.
fn content_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

/// Encodes a digest as lowercase hex for blob file names.
fn hex_digest(digest: &[u8; 32]) -> String {
    lgwks_std::hex::encode(digest)
}

/// Parses a lowercase hex blob name into a 32-byte digest.
fn hex_to_digest(name: &str) -> Option<[u8; 32]> {
    if name.len() != 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    lgwks_std::hex::decode(name).ok()?.try_into().ok()
}

/// Returns whether a name is a well-formed hex blob digest.
fn is_blob_name(name: &str) -> bool {
    hex_to_digest(name).is_some()
}

/// Returns the sibling blob directory for a cache database path.
fn blob_dir_for(path: &Path) -> PathBuf {
    let mut dir = path.as_os_str().to_owned();
    dir.push(".blobs");
    PathBuf::from(dir)
}

/// Extracts the wall-clock start time encoded in a UUIDv7 operation id.
fn operation_started_at_ms(operation: &OperationId) -> Result<i64, KvError> {
    let uuid = operation.as_uuid();
    if uuid.get_version_num() != 7 {
        return Err(KvError::InvalidOperationId);
    }
    let Some(timestamp) = uuid.get_timestamp() else {
        return Err(KvError::InvalidOperationId);
    };
    let (seconds, nanos) = timestamp.to_unix();
    seconds
        .checked_mul(1_000)
        .and_then(|millis| millis.checked_add(u64::from(nanos.checked_div(1_000_000)?)))
        .and_then(|millis| i64::try_from(millis).ok())
        .ok_or(KvError::InvalidOperationId)
}

/// Derives a unique recency sequence for one index within a batch.
fn batch_sequence(started_at_ms: i64, index: usize) -> Result<u64, KvError> {
    let started_at_ms = u64::try_from(started_at_ms).map_err(|_| KvError::InvalidOperationId)?;
    let stride = u64::try_from(MAX_BATCH_PUT + 1).unwrap_or(u64::MAX);
    started_at_ms
        .checked_mul(stride)
        .and_then(|base| base.checked_add(u64::try_from(index).ok()?))
        .ok_or(KvError::InvalidOperationId)
}

/// Derives a deterministic operation id for one member of a batch.
fn batch_member_operation(batch: OperationId, index: usize) -> OperationId {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/batch-member/v1");
    hasher.update(&[0]);
    hasher.update(batch.as_uuid().as_bytes());
    hasher.update(&u64::try_from(index).unwrap_or(u64::MAX).to_be_bytes());
    let digest: [u8; 32] = *hasher.finalize().as_bytes();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0F) | 0x80;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    OperationId::from_uuid(Uuid::from_bytes(bytes))
}

/// Hashes a full batch request for idempotent apply and deduplication.
fn batch_request_digest(
    namespace: &str,
    entries: &[BatchPut],
    started_at_ms: i64,
) -> Result<OperationDigest, KvError> {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/batch-request/v1");
    hasher.update(
        &u64::try_from(namespace.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(namespace.as_bytes());
    hasher.update(
        &u64::try_from(entries.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    for (index, entry) in entries.iter().enumerate() {
        let sequence = batch_sequence(started_at_ms, index)?;
        let expires_at_ms = expiry_for(entry.ttl, started_at_ms);
        hasher.update(
            &u64::try_from(entry.key.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        hasher.update(entry.key.as_bytes());
        hasher.update(
            &u64::try_from(entry.value.len())
                .unwrap_or(u64::MAX)
                .to_be_bytes(),
        );
        hasher.update(&entry.value);
        hasher.update(&expires_at_ms.to_be_bytes());
        hasher.update(&sequence.to_be_bytes());
    }
    Ok(OperationDigest::new(*hasher.finalize().as_bytes()))
}

/// Binds a member request digest to its parent batch request digest.
fn bind_batch_request(request: OperationDigest, batch_digest: &OperationDigest) -> OperationDigest {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/batch-member-request/v1");
    hasher.update(&request.into_bytes());
    hasher.update(&batch_digest.into_bytes());
    OperationDigest::new(*hasher.finalize().as_bytes())
}

/// Builds the request digest for an inline put operation.
fn put_digest(
    operation: &OperationId,
    namespace: &str,
    key: &str,
    value: &[u8],
    expires_at_ms: i64,
    sequence: u64,
) -> OperationDigest {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/put/v1");
    hasher.update(&[0]);
    hasher.update(operation.as_uuid().as_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update(&[0]);
    hasher.update(key.as_bytes());
    hasher.update(&[0]);
    hasher.update(value);
    hasher.update(&expires_at_ms.to_be_bytes());
    hasher.update(&sequence.to_be_bytes());
    OperationDigest::new(*hasher.finalize().as_bytes())
}

/// Builds the request digest for a tiered put operation.
fn tiered_put_digest(
    operation: &OperationId,
    namespace: &str,
    key: &str,
    digest: &[u8; 32],
    size: u64,
    expires_at_ms: i64,
    sequence: u64,
) -> OperationDigest {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/tiered-put/v1");
    hasher.update(&[0]);
    hasher.update(operation.as_uuid().as_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update(&[0]);
    hasher.update(key.as_bytes());
    hasher.update(&[0]);
    hasher.update(digest);
    hasher.update(&size.to_be_bytes());
    hasher.update(&expires_at_ms.to_be_bytes());
    hasher.update(&sequence.to_be_bytes());
    OperationDigest::new(*hasher.finalize().as_bytes())
}

/// Builds the request digest for a clear/delete operation.
fn clear_digest(operation: &OperationId, namespace: &str, key: &str) -> OperationDigest {
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/clear/v1");
    hasher.update(&[0]);
    hasher.update(operation.as_uuid().as_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update(&[0]);
    hasher.update(key.as_bytes());
    OperationDigest::new(*hasher.finalize().as_bytes())
}

/// Derives the deterministic document id for a namespace and key pair.
fn document_for(namespace: &str, key: &str) -> DocumentId {
    let namespace_length = u64::try_from(namespace.len()).unwrap_or(u64::MAX);
    let key_length = u64::try_from(key.len()).unwrap_or(u64::MAX);
    let mut hasher = Hasher::new();
    hasher.update(b"logical-db-kv/document/v1");
    hasher.update(&[0]);
    hasher.update(&namespace_length.to_be_bytes());
    hasher.update(namespace.as_bytes());
    hasher.update(&[0]);
    hasher.update(&key_length.to_be_bytes());
    hasher.update(key.as_bytes());
    let digest: [u8; 32] = *hasher.finalize().as_bytes();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0F) | 0x80;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    DocumentId::from_uuid(Uuid::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use logical_db::CrdtError;

    fn scope() -> Scope {
        Scope::new(AccountId::new(), SpaceId::new())
    }

    fn path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("logical-db-kv-{tag}-{}.redb", Uuid::now_v7()))
    }

    fn open_cache(
        path: &std::path::Path,
        namespace: &str,
    ) -> Result<KvCache, Box<dyn std::error::Error>> {
        Ok(KvCache::open(path, scope(), ReplicaId::new(), namespace)?)
    }

    fn cleanup(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn round_trip_put_get_delete_and_prefix_scan_without_caller_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("roundtrip");
        let cache = open_cache(&db_path, "sessions")?;
        cache.put("user:ada", b"1", Some(Duration::from_secs(60)))?;
        cache.put("user:grace", b"2", Some(Duration::from_secs(60)))?;
        cache.put("config:theme", b"dark", Some(Duration::from_secs(60)))?;
        assert_eq!(cache.get("user:ada")?, Some(b"1".to_vec()));
        let page = cache.scan("user:", None, 10)?;
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
        assert!(cache.delete("user:ada")?);
        assert_eq!(cache.get("user:ada")?, None);
        assert!(!cache.delete("user:ada")?);
        assert!(!cache.delete("missing")?);
        let page = cache.scan("user:", None, 10)?;
        assert_eq!(
            page,
            vec![CacheEntry {
                key: "user:grace".into(),
                value: b"2".to_vec()
            }]
        );
        let first = cache.scan("", None, 1)?;
        assert_eq!(first.len(), 1);
        let rest = cache.scan("", Some(&first[0].key), 10)?;
        assert!(
            rest.iter()
                .all(|entry| entry.key.as_str() > first[0].key.as_str())
        );
        assert_eq!(first.len() + rest.len(), 2);
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn expired_entries_read_absent_and_evict_in_bounded_batches()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("expiry");
        let cache = open_cache(&db_path, "sessions")?;
        cache.put("gone:1", b"a", Some(Duration::ZERO))?;
        cache.put("gone:2", b"b", Some(Duration::ZERO))?;
        cache.put("gone:3", b"c", Some(Duration::ZERO))?;
        cache.put("live", b"z", Some(Duration::from_secs(60)))?;
        assert_eq!(cache.get("gone:1")?, None);
        assert_eq!(cache.get("live")?, Some(b"z".to_vec()));
        assert_eq!(cache.evict_expired(2)?, 2);
        assert_eq!(cache.evict_expired(10)?, 1);
        assert_eq!(cache.evict_expired(10)?, 0);
        assert_eq!(cache.get("live")?, Some(b"z".to_vec()));
        assert!(cache.scan("gone:", None, 10)?.is_empty());
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn eviction_revision_guard_preserves_a_concurrent_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("eviction-revision");
        let scope = scope();
        let replica = ReplicaId::new();
        let (document, revision) = {
            let first = KvCache::open(&db_path, scope, replica, "sessions")?;
            first.put("key", b"old", None)?;
            let document = document_for("sessions", "key");
            let revision = match first.decode_slot(document)? {
                Decoded::Found(envelope) => envelope.revision,
                _ => return Err(std::io::Error::other("expected stored envelope").into()),
            };
            (document, revision)
        };
        {
            let second = KvCache::open(&db_path, scope, replica, "sessions")?;
            second.put("key", b"new", None)?;
        }
        let first = KvCache::open(&db_path, scope, replica, "sessions")?;
        let outcome = first.remove_reference_at(document, "key", Some(revision))?;
        assert!(!outcome.cleared);
        assert_eq!(first.get("key")?, Some(b"new".to_vec()));
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn older_envelope_reads_with_new_fields_defaulted_and_newer_refused()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("envelope");
        let scope = scope();
        let replica = ReplicaId::new();
        {
            let raw = Database::open(&db_path, scope, replica)?;
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
            )?;
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
            )?;
        }
        let (cache, report) = KvCache::migrate_schema(&db_path, scope, replica, "sessions", 16)?;
        assert_eq!(report.documents, 2);
        assert_eq!(cache.get("legacy")?, Some(b"old".to_vec()));
        let stored = cache.database().query_index(
            EXPIRY_INDEX,
            &IndexValue::Timestamp(NEVER_EXPIRES_AT_MS),
            None,
            10,
        )?;
        assert_eq!(stored, vec![document_for("sessions", "legacy")]);
        let Err(error) = cache.get("future") else {
            return Err("expected unsupported envelope version".into());
        };
        assert!(matches!(
            error,
            KvError::UnsupportedEnvelopeVersion { actual: 99 }
        ));
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn tier_boundary_values_round_trip_and_huge_refused_before_commit()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("bounds");
        let cache = open_cache(&db_path, "blobs")?;
        let inline = vec![7_u8; TIER_THRESHOLD_BYTES];
        cache.put("inline", &inline, Some(Duration::from_secs(60)))?;
        assert_eq!(cache.get("inline")?, Some(inline));
        assert_eq!(cache.blob_reference("inline")?, None);
        let tiered = vec![9_u8; MAX_VALUE_BYTES + 1];
        cache.put("tiered", &tiered, Some(Duration::from_secs(60)))?;
        assert_eq!(cache.get("tiered")?, Some(tiered));
        assert!(cache.blob_reference("tiered")?.is_some());
        let huge = vec![1_u8; MAX_TIERED_VALUE_BYTES + 1];
        let Err(error) = cache.put("huge", &huge, Some(Duration::from_secs(60))) else {
            return Err("expected blob too large".into());
        };
        assert!(matches!(
            error,
            KvError::BlobTooLarge { actual, maximum }
            if actual == MAX_TIERED_VALUE_BYTES + 1 && maximum == MAX_TIERED_VALUE_BYTES
        ));
        assert_eq!(cache.get("huge")?, None);
        let stored: Vec<_> = std::fs::read_dir(&cache.blob_dir)?.collect();
        assert_eq!(
            stored.len(),
            1,
            "the refused value must leave no blob file behind"
        );
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn schema_migration_preserves_documents_and_installs_indexes()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("migrate");
        let scope = scope();
        let replica = ReplicaId::new();
        {
            let raw = Database::open(&db_path, scope, replica)?;
            raw.edit(
                DocumentId::new(),
                OperationId::new(),
                OperationDigest::new([21; 32]),
                |editor| editor.put(&Object::root(), "note", Scalar::String("plain".into())),
            )?;
        }
        let (cache, _) = KvCache::migrate_schema(&db_path, scope, replica, "sessions", 16)?;
        cache.put("forever", b"v", None)?;
        assert_eq!(cache.get("forever")?, Some(b"v".to_vec()));
        let hits = cache.database().query_index(
            EXPIRY_INDEX,
            &IndexValue::Timestamp(NEVER_EXPIRES_AT_MS),
            None,
            10,
        )?;
        assert_eq!(hits, vec![document_for("sessions", "forever")]);
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn namespaces_never_share_slots_and_inputs_are_validated()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("namespaces");
        let scope = scope();
        let replica = ReplicaId::new();
        {
            let first = KvCache::open(&db_path, scope, replica, "one")?;
            first.put("shared", b"1", Some(Duration::from_secs(60)))?;
            assert_eq!(first.get("shared")?, Some(b"1".to_vec()));
        }
        {
            let second = KvCache::open(&db_path, scope, replica, "two")?;
            assert_eq!(second.get("shared")?, None);
            second.put("shared", b"2", Some(Duration::from_secs(60)))?;
            assert_eq!(second.get("shared")?, Some(b"2".to_vec()));
        }
        {
            let first = KvCache::open(&db_path, scope, replica, "one")?;
            assert_eq!(first.get("shared")?, Some(b"1".to_vec()));
            let Err(error) = first.get("") else {
                return Err("expected invalid key".into());
            };
            assert!(matches!(error, KvError::InvalidKey));
            let Err(error) = first.scan("", None, 0) else {
                return Err("expected invalid scan limit".into());
            };
            assert!(matches!(error, KvError::InvalidScanLimit(0)));
            let Err(error) = first.evict_expired(0) else {
                return Err("expected invalid eviction limit".into());
            };
            assert!(matches!(error, KvError::InvalidEvictionLimit(0)));
        }
        assert!(matches!(
            KvCache::open(&db_path, scope, replica, ""),
            Err(KvError::InvalidNamespace)
        ));
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn batch_put_applies_every_entry_atomically() -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("batch-apply");
        let cache = open_cache(&db_path, "sessions")?;
        cache.put_many(&[
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
        ])?;
        assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
        assert_eq!(cache.get("b")?, Some(b"2".to_vec()));
        assert_eq!(cache.get("c")?, Some(b"3".to_vec()));
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn batch_put_validation_failure_applies_nothing() -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("batch-atomic");
        let cache = open_cache(&db_path, "sessions")?;
        cache.put("keep", b"v0", Some(Duration::from_secs(60)))?;
        let oversize = vec![7_u8; MAX_TIERED_VALUE_BYTES + 1];
        let Err(error) = cache.put_many(&[
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
        ]) else {
            return Err("expected blob too large".into());
        };
        assert!(matches!(
            error,
            KvError::BlobTooLarge { actual, maximum }
            if actual == MAX_TIERED_VALUE_BYTES + 1 && maximum == MAX_TIERED_VALUE_BYTES
        ));
        assert_eq!(cache.get("first")?, None);
        assert_eq!(cache.get("last")?, None);
        assert_eq!(cache.get("big")?, None);
        assert_eq!(cache.get("keep")?, Some(b"v0".to_vec()));
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn batch_put_rejects_empty_and_oversize_batches() -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("batch-bounds");
        let cache = open_cache(&db_path, "sessions")?;
        let Err(error) = cache.put_many(&[]) else {
            return Err("expected invalid batch size".into());
        };
        assert!(matches!(error, KvError::InvalidBatchSize(0)));
        let oversize: Vec<BatchPut> = (0..=MAX_BATCH_PUT)
            .map(|offset| BatchPut {
                key: format!("k{offset}"),
                value: b"v".to_vec(),
                ttl: None,
            })
            .collect();
        let Err(error) = cache.put_many(&oversize) else {
            return Err("expected invalid batch size".into());
        };
        assert!(matches!(
            error,
            KvError::InvalidBatchSize(actual) if actual == MAX_BATCH_PUT + 1
        ));
        assert_eq!(cache.get("k0")?, None);
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn batch_put_with_operation_retries_without_appending_changes()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("batch-retry");
        let cache = open_cache(&db_path, "sessions")?;
        let operation = OperationId::new();
        let entries = [
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
        ];
        cache.put_many_with_operation(operation, &entries)?;
        let changes = cache.database().changes_since(0, MAX_PAGE_LIMIT)?.len();
        cache.put_many_with_operation(operation, &entries)?;
        assert_eq!(
            cache.database().changes_since(0, MAX_PAGE_LIMIT)?.len(),
            changes
        );
        assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
        assert_eq!(cache.get("b")?, Some(b"2".to_vec()));
        let extended = [
            entries[0].clone(),
            entries[1].clone(),
            BatchPut {
                key: "c".into(),
                value: b"3".to_vec(),
                ttl: None,
            },
        ];
        assert!(cache.put_many_with_operation(operation, &extended).is_err());
        assert_eq!(cache.get("c")?, None);
        let overwritten = [
            BatchPut {
                key: "a".into(),
                value: b"ignored-before-final".to_vec(),
                ttl: Some(Duration::from_secs(60)),
            },
            entries[0].clone(),
            entries[1].clone(),
        ];
        assert!(
            cache
                .put_many_with_operation(operation, &overwritten)
                .is_err()
        );
        assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
        assert_eq!(cache.get("b")?, Some(b"2".to_vec()));
        assert!(
            cache
                .put_many_with_operation(
                    operation,
                    &[BatchPut {
                        key: "a".into(),
                        value: b"different".to_vec(),
                        ttl: Some(Duration::from_secs(60)),
                    }],
                )
                .is_err()
        );
        assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn batch_reserves_recency_before_a_following_put() -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("batch-sequence");
        let cache = open_cache(&db_path, "sessions")?;
        let batch_operation = OperationId::new();
        cache.put_many_with_operation(
            batch_operation,
            &[BatchPut {
                key: "batch".into(),
                value: b"batch-value".to_vec(),
                ttl: None,
            }],
        )?;
        let batch_sequence = match cache.decode_slot(document_for("sessions", "batch"))? {
            Decoded::Found(envelope) => envelope.sequence,
            _ => {
                return Err(std::io::Error::other("batch envelope must be present").into());
            }
        };
        cache.put("following", b"following-value", None)?;
        let following_sequence = match cache.decode_slot(document_for("sessions", "following"))? {
            Decoded::Found(envelope) => envelope.sequence,
            _ => {
                return Err(std::io::Error::other("following envelope must be present").into());
            }
        };
        assert!(following_sequence > batch_sequence);
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn batch_put_with_operation_rejects_non_uuidv7_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("batch-operation-id");
        let cache = open_cache(&db_path, "sessions")?;
        let Err(error) = cache.put_many_with_operation(
            OperationId::from_uuid(Uuid::nil()),
            &[BatchPut {
                key: "key".into(),
                value: b"value".to_vec(),
                ttl: None,
            }],
        ) else {
            return Err("expected invalid operation id".into());
        };
        assert!(matches!(error, KvError::InvalidOperationId));
        assert_eq!(cache.get("key")?, None);
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn fresh_device_catches_up_newest_first_then_backfills()
    -> Result<(), Box<dyn std::error::Error>> {
        let writer_path = path("catchup-a");
        let reader_path = path("catchup-b");
        let scope = scope();
        let peer = logical_db_model::PeerId::new();
        {
            let writer = KvCache::open(&writer_path, scope, ReplicaId::new(), "sessions")?;
            for key in ["k1", "k2", "k3", "k4", "k5"] {
                writer.put(key, key.as_bytes(), Some(Duration::from_secs(60)))?;
            }
            let pending = writer.database().pending_changes(0, 64)?;
            let remote: Vec<logical_db::RemoteChange> = pending
                .into_iter()
                .map(|change| {
                    logical_db::RemoteChange::new(change.document, change.digest, change.bytes)
                })
                .collect();
            assert_eq!(remote.len(), 5);
            let reader = KvCache::open(&reader_path, scope, ReplicaId::new(), "sessions")?;
            reader
                .database()
                .install_remote_page(peer, &remote, b"cursor-1")?;
            let newest = reader.recent("", 2)?;
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
            let backfill = reader.scan("", None, 10)?;
            assert_eq!(
                backfill
                    .iter()
                    .map(|entry| entry.key.as_str())
                    .collect::<Vec<_>>(),
                vec!["k1", "k2", "k3", "k4", "k5"]
            );
            let full_recent = reader.recent("", 10)?;
            assert_eq!(
                full_recent
                    .iter()
                    .map(|entry| entry.key.as_str())
                    .collect::<Vec<_>>(),
                vec!["k5", "k4", "k3", "k2", "k1"]
            );
        }
        cleanup(&writer_path)?;
        cleanup(&reader_path)?;
        Ok(())
    }

    #[test]
    fn namespace_checkpoint_map_round_trips_through_the_durable_slot()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("ns-slot");
        let scope = scope();
        let peer = logical_db_model::PeerId::new();
        let cache = KvCache::open(&db_path, scope, ReplicaId::new(), "sessions")?;
        assert!(cache.namespace_checkpoints(peer)?.is_empty());
        let mut map = logical_db_model::NamespaceCheckpoints::new();
        map.insert(
            "hot",
            logical_db_model::NamespaceCheckpoint::new(vec![1, 2], 1_000)?,
        )?;
        map.insert(
            "cold",
            logical_db_model::NamespaceCheckpoint::new(vec![3], 2_000)?,
        )?;
        cache.store_namespace_checkpoints(peer, &map)?;
        let loaded = cache.namespace_checkpoints(peer)?;
        assert_eq!(loaded.get("hot").ok_or("expected Some")?.cursor(), &[1, 2]);
        assert_eq!(loaded.get("cold").ok_or("expected Some")?.cursor(), &[3]);
        let mut advanced = loaded;
        advanced.insert(
            "hot",
            logical_db_model::NamespaceCheckpoint::new(vec![9], 3_000)?,
        )?;
        cache.store_namespace_checkpoints(peer, &advanced)?;
        let reloaded = cache.namespace_checkpoints(peer)?;
        assert_eq!(reloaded.get("hot").ok_or("expected Some")?.cursor(), &[9]);
        assert_eq!(reloaded.get("cold").ok_or("expected Some")?.cursor(), &[3]);
        cleanup(&db_path)?;
        Ok(())
    }

    fn cleanup_tree(
        db: &std::path::Path,
        blobs: &std::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if blobs.exists() {
            std::fs::remove_dir_all(blobs)?;
        }
        if db.exists() {
            std::fs::remove_file(db)?;
        }
        Ok(())
    }

    fn deterministic_bytes(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| {
                let scaled = index
                    .checked_mul(31)
                    .and_then(|partial| partial.checked_add(7));
                u8::try_from(scaled.unwrap_or(0).checked_rem(251).unwrap_or(0)).unwrap_or(0)
            })
            .collect()
    }

    fn install_all(
        writer: &KvCache,
        reader: &KvCache,
        peer: PeerId,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let pending = writer.database().pending_changes(0, 64)?;
        let remote: Vec<logical_db::RemoteChange> = pending
            .into_iter()
            .map(|change| {
                logical_db::RemoteChange::new(change.document, change.digest, change.bytes)
            })
            .collect();
        let count = remote.len();
        reader
            .database()
            .install_remote_page(peer, &remote, b"cursor-u8")?;
        Ok(count)
    }

    #[test]
    fn tiered_value_round_trips_byte_identical_on_two_synced_replicas()
    -> Result<(), Box<dyn std::error::Error>> {
        let writer_path = path("tier-sync-a");
        let reader_path = path("tier-sync-b");
        let scope = scope();
        let peer = PeerId::new();
        let writer = KvCache::open(&writer_path, scope, ReplicaId::new(), "media")?;
        let reader = KvCache::open(&reader_path, scope, ReplicaId::new(), "media")?;
        let big = deterministic_bytes(3 * MAX_BLOB_CHUNK_BYTES + 12_345);
        writer.put("big", &big, None)?;
        assert_eq!(writer.get("big")?, Some(big.clone()));
        let pending = writer.database().pending_changes(0, 64)?;
        assert_eq!(pending.len(), 1);
        for change in &pending {
            assert!(
                change.bytes.len() < TIER_THRESHOLD_BYTES,
                "sidecar reference must stay inside the bounded change"
            );
        }
        install_all(&writer, &reader, peer)?;
        let reference = reader
            .blob_reference("big")?
            .ok_or("expected tiered blob reference")?;
        assert_eq!(reference.size, u64::try_from(big.len()).unwrap_or(u64::MAX));
        assert!(
            matches!(reader.get("big"), Err(KvError::MissingBlob { .. })),
            "reference without bytes must fail loudly, never partial"
        );
        let mut offset = 0_u64;
        let mut chunks = 0_u32;
        while offset < reference.size {
            let want = usize::try_from(reference.size - offset)?.min(MAX_BLOB_CHUNK_BYTES);
            let bytes = writer.read_blob_chunk(&reference.digest, offset, want)?;
            assert!(!bytes.is_empty() && bytes.len() <= MAX_BLOB_CHUNK_BYTES);
            let complete =
                reader.write_blob_chunk(&reference.digest, reference.size, offset, &bytes)?;
            offset = offset.saturating_add(u64::try_from(bytes.len()).unwrap_or(0));
            chunks += 1;
            assert_eq!(complete, offset == reference.size);
        }
        assert!(chunks > 1, "large value must travel in multiple chunks");
        assert_eq!(reader.get("big")?, Some(big));
        cleanup_tree(&writer_path, &writer.blob_dir)?;
        cleanup_tree(&reader_path, &reader.blob_dir)?;
        Ok(())
    }

    #[test]
    fn small_values_never_touch_the_blob_path() -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-small");
        let cache = open_cache(&db_path, "sessions")?;
        cache.put("a", b"1", Some(Duration::from_secs(60)))?;
        cache.put("empty-ok", b"x", None)?;
        assert_eq!(cache.get("a")?, Some(b"1".to_vec()));
        assert_eq!(cache.scan("", None, 10)?.len(), 2);
        assert_eq!(cache.recent("", 10)?.len(), 2);
        assert_eq!(cache.blob_reference("a")?, None);
        assert_eq!(cache.evict_expired(10)?, 0);
        assert!(
            !cache.blob_dir.exists(),
            "small values must never create the blob directory"
        );
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn delete_and_evict_remove_reference_and_tiered_bytes() -> Result<(), Box<dyn std::error::Error>>
    {
        let db_path = path("tier-delete");
        let cache = open_cache(&db_path, "media")?;
        let first = deterministic_bytes(TIER_THRESHOLD_BYTES + 1_000);
        cache.put("gone", &first, None)?;
        let digest = cache
            .blob_reference("gone")?
            .ok_or("expected tiered blob reference")?
            .digest;
        assert!(cache.blob_path(&digest).exists());
        assert!(cache.delete("gone")?);
        assert_eq!(cache.get("gone")?, None);
        assert_eq!(cache.sweep_blobs()?, 1);
        assert!(
            !cache.blob_path(&digest).exists(),
            "sweep must remove tiered bytes after the reference commit"
        );
        cache.put("old", &first, Some(Duration::ZERO))?;
        let old_digest = cache
            .blob_reference("old")?
            .ok_or("expected tiered blob reference")?
            .digest;
        assert_eq!(cache.get("old")?, None);
        assert_eq!(cache.evict_expired(10)?, 1);
        assert_eq!(cache.sweep_blobs()?, 1);
        assert!(!cache.blob_path(&old_digest).exists());
        let shared = deterministic_bytes(TIER_THRESHOLD_BYTES + 7);
        cache.put("s1", &shared, None)?;
        cache.put("s2", &shared, None)?;
        let shared_digest = cache
            .blob_reference("s1")?
            .ok_or("expected tiered blob reference")?
            .digest;
        assert_eq!(
            cache
                .blob_reference("s2")?
                .ok_or("expected tiered blob reference")?
                .digest,
            shared_digest
        );
        assert!(cache.delete("s1")?);
        assert!(
            cache.blob_path(&shared_digest).exists(),
            "shared bytes survive while a live reference remains"
        );
        assert_eq!(cache.get("s2")?, Some(shared));
        assert!(cache.delete("s2")?);
        assert_eq!(cache.sweep_blobs()?, 1);
        assert!(!cache.blob_path(&shared_digest).exists());
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn crash_between_reference_removal_and_blob_deletion_recovered_by_sweep()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-crash");
        let scope = scope();
        let replica = ReplicaId::new();
        let digest = {
            let cache = KvCache::open(&db_path, scope, replica, "media")?;
            let big = deterministic_bytes(TIER_THRESHOLD_BYTES + 511);
            cache.put("big", &big, None)?;
            let digest = cache
                .blob_reference("big")?
                .ok_or("expected tiered blob reference")?
                .digest;
            let document = document_for("media", "big");
            let outcome = cache.remove_reference(document, "big")?;
            assert!(outcome.live);
            let blobs = cache.blob_dir.clone();
            drop(cache);
            assert!(
                blobs.join(hex_digest(&digest)).exists(),
                "crashed delete leaves an orphan blob file"
            );
            digest
        };
        {
            let cache = KvCache::open(&db_path, scope, replica, "media")?;
            assert_eq!(cache.get("big")?, None);
            assert!(cache.blob_path(&digest).exists());
            assert_eq!(cache.sweep_blobs()?, 1);
            assert!(!cache.blob_path(&digest).exists());
            assert_eq!(cache.sweep_blobs()?, 0);
            let blobs = cache.blob_dir.clone();
            drop(cache);
            cleanup_tree(&db_path, &blobs)?;
        }
        Ok(())
    }

    #[test]
    fn sweep_promotes_referenced_gc_staging_back_to_canonical_storage()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-gc-republish");
        let cache = open_cache(&db_path, "media")?;
        let value = deterministic_bytes(TIER_THRESHOLD_BYTES + 512);
        cache.put("old", &value, None)?;
        let digest = cache
            .blob_reference("old")?
            .ok_or("expected tiered blob reference")?
            .digest;
        assert!(cache.delete("old")?);
        let staged = cache
            .blob_dir
            .join(format!("{}.tmp-gc-test", hex_digest(&digest)));
        std::fs::rename(cache.blob_path(&digest), &staged)?;
        assert!(!cache.blob_path(&digest).exists());

        let document = document_for("media", "new");
        cache.database().edit(
            document,
            OperationId::new(),
            OperationDigest::new([78; 32]),
            |editor| {
                editor.put(
                    &Object::root(),
                    FIELD_VERSION,
                    Scalar::Unsigned(ENVELOPE_VERSION),
                )?;
                editor.put(&Object::root(), FIELD_KEY, Scalar::String("new".into()))?;
                editor.put(
                    &Object::root(),
                    FIELD_NAMESPACE,
                    Scalar::String("media".into()),
                )?;
                editor.put(&Object::root(), FIELD_BLOB, Scalar::Bytes(digest.to_vec()))?;
                editor.put(
                    &Object::root(),
                    FIELD_SIZE,
                    Scalar::Unsigned(
                        u64::try_from(value.len())
                            .map_err(|_error| CrdtError::InvalidChangeSize(value.len()))?,
                    ),
                )?;
                Ok(())
            },
        )?;
        assert_eq!(cache.sweep_blobs()?, 0);
        assert_eq!(cache.get("new")?, Some(value));
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn missing_or_damaged_blob_fails_read_loudly_without_partial_bytes()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-loud");
        let cache = open_cache(&db_path, "media")?;
        let big = deterministic_bytes(TIER_THRESHOLD_BYTES + 999);
        cache.put("big", &big, None)?;
        let digest = cache
            .blob_reference("big")?
            .ok_or("expected tiered blob reference")?
            .digest;
        let file = cache.blob_path(&digest);
        std::fs::remove_file(&file)?;
        assert!(matches!(cache.get("big"), Err(KvError::MissingBlob { .. })));
        assert!(matches!(
            cache.scan("", None, 10),
            Err(KvError::MissingBlob { .. })
        ));
        cache.put("big", &big, None)?;
        let mut damaged = std::fs::read(&file)?;
        damaged[100] ^= 0xFF;
        std::fs::write(&file, &damaged)?;
        assert!(matches!(cache.get("big"), Err(KvError::CorruptBlob { .. })));
        assert!(matches!(
            cache.read_blob_chunk(&digest, 0, 1),
            Err(KvError::CorruptBlob { .. })
        ));
        cache.put("big", &big, None)?;
        let truncated = std::fs::read(&file)?;
        let half = truncated.len().checked_div(2).unwrap_or(0);
        std::fs::write(&file, &truncated[..half])?;
        assert!(matches!(cache.get("big"), Err(KvError::CorruptBlob { .. })));
        assert!(cache.blob_reference("big")?.is_some());
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn invalid_chunk_never_mutates_an_existing_canonical_blob()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-chunk-isolation");
        let cache = open_cache(&db_path, "media")?;
        let original = deterministic_bytes(TIER_THRESHOLD_BYTES + 64);
        cache.put("big", &original, None)?;
        let reference = cache
            .blob_reference("big")?
            .ok_or("expected tiered blob reference")?;
        let invalid = vec![0_u8; original.len()];
        assert!(matches!(
            cache.write_blob_chunk(&reference.digest, reference.size, 0, &invalid),
            Err(KvError::CorruptBlob { .. })
        ));
        assert_eq!(cache.get("big")?, Some(original));
        assert!(cache.blob_path(&reference.digest).exists());
        assert_eq!(cache.sweep_blobs()?, 1);
        assert!(cache.blob_path(&reference.digest).exists());
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn sweep_refuses_unsupported_and_corrupt_cache_envelopes()
    -> Result<(), Box<dyn std::error::Error>> {
        for (tag, version, blob) in [("unsupported", 99_u64, None), ("corrupt", 2, Some(vec![1]))] {
            let db_path = path(tag);
            let cache = open_cache(&db_path, "media")?;
            let document = document_for("media", "bad");
            cache.database().edit(
                document,
                OperationId::new(),
                OperationDigest::new([41; 32]),
                |editor| {
                    editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(version))?;
                    editor.put(&Object::root(), FIELD_KEY, Scalar::String("bad".into()))?;
                    editor.put(
                        &Object::root(),
                        FIELD_NAMESPACE,
                        Scalar::String("media".into()),
                    )?;
                    if let Some(blob) = blob {
                        editor.put(&Object::root(), FIELD_BLOB, Scalar::Bytes(blob))?;
                        editor.put(&Object::root(), FIELD_SIZE, Scalar::Unsigned(1))?;
                    } else {
                        editor.put(
                            &Object::root(),
                            FIELD_VALUE,
                            Scalar::Bytes(b"future".to_vec()),
                        )?;
                    }
                    Ok(())
                },
            )?;
            let Err(error) = cache.sweep_blobs() else {
                return Err("expected sweep error".into());
            };
            if version == 99 {
                assert!(matches!(
                    error,
                    KvError::UnsupportedEnvelopeVersion { actual: 99 }
                ));
            } else {
                assert!(matches!(error, KvError::CorruptEnvelope));
            }
            cleanup_tree(&db_path, &cache.blob_dir.clone())?;
        }
        Ok(())
    }

    #[test]
    fn point_reads_refuse_malformed_tiered_shapes() -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-shape");
        let cache = open_cache(&db_path, "media")?;
        for (key, digest, size) in [("inline", None, None), ("small", Some([8; 32]), Some(1))] {
            let document = document_for("media", key);
            cache.database().edit(
                document,
                OperationId::new(),
                OperationDigest::new([42; 32]),
                |editor| {
                    editor.put(&Object::root(), FIELD_VERSION, Scalar::Unsigned(2))?;
                    editor.put(&Object::root(), FIELD_KEY, Scalar::String(key.into()))?;
                    editor.put(
                        &Object::root(),
                        FIELD_NAMESPACE,
                        Scalar::String("media".into()),
                    )?;
                    match (digest, size) {
                        (None, None) => {
                            editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(vec![1]))?;
                        }
                        (Some(digest), Some(size)) => {
                            editor.put(
                                &Object::root(),
                                FIELD_BLOB,
                                Scalar::Bytes(digest.to_vec()),
                            )?;
                            editor.put(&Object::root(), FIELD_SIZE, Scalar::Unsigned(size))?;
                        }
                        _ => return Err(CrdtError::InvalidChangeSize(0)),
                    }
                    Ok(())
                },
            )?;
            assert!(matches!(cache.get(key), Err(KvError::CorruptEnvelope)));
        }
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn point_reads_refuse_unbounded_envelope_axes() -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("envelope-bounds");
        let cache = open_cache(&db_path, "media")?;
        let oversized = "x".repeat(logical_db_model::MAX_NAME_BYTES + 1);
        let malformed = vec![
            (
                document_for("media", "empty-key"),
                String::new(),
                "media".to_owned(),
            ),
            (
                document_for("media", "oversized-key"),
                oversized,
                "media".to_owned(),
            ),
            (
                document_for("media", "empty-namespace"),
                "key".to_owned(),
                String::new(),
            ),
        ];
        for (index, (document, key, namespace)) in malformed.into_iter().enumerate() {
            let request_byte = 50_u8 + u8::try_from(index)?;
            cache.database().edit(
                document,
                OperationId::new(),
                OperationDigest::new([request_byte; 32]),
                move |editor| {
                    editor.put(&Object::root(), FIELD_KEY, Scalar::String(key))?;
                    editor.put(&Object::root(), FIELD_NAMESPACE, Scalar::String(namespace))?;
                    editor.put(&Object::root(), FIELD_VALUE, Scalar::Bytes(vec![1]))?;
                    Ok(())
                },
            )?;
            assert!(matches!(
                cache.document_namespace(document),
                Err(KvError::CorruptEnvelope)
            ));
        }
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn values_beyond_inline_bound_tier_and_huge_values_refused_before_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-bounds");
        let cache = open_cache(&db_path, "media")?;
        let large = deterministic_bytes(MAX_VALUE_BYTES + 1);
        cache.put("large", &large, None)?;
        assert_eq!(cache.get("large")?, Some(large));
        assert!(cache.blob_reference("large")?.is_some());
        let blobs = cache.blob_dir.clone();
        assert!(blobs.exists());
        let before = std::fs::read_dir(&blobs)?.count();
        let huge = vec![7_u8; MAX_TIERED_VALUE_BYTES + 1];
        let Err(error) = cache.put("huge", &huge, None) else {
            return Err("expected blob too large".into());
        };
        assert!(matches!(
            error,
            KvError::BlobTooLarge { actual, maximum }
            if actual == MAX_TIERED_VALUE_BYTES + 1 && maximum == MAX_TIERED_VALUE_BYTES
        ));
        assert_eq!(cache.get("huge")?, None);
        assert_eq!(std::fs::read_dir(&blobs)?.count(), before);
        drop(cache);
        cleanup_tree(&db_path, &blobs)?;
        Ok(())
    }

    #[test]
    fn tiered_overwrite_and_batch_reclaim_displaced_bytes_on_sweep()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-overwrite");
        let cache = open_cache(&db_path, "media")?;
        let first = deterministic_bytes(TIER_THRESHOLD_BYTES + 11);
        let second = deterministic_bytes(TIER_THRESHOLD_BYTES + 12);
        cache.put("k", &first, None)?;
        let first_digest = cache
            .blob_reference("k")?
            .ok_or("expected tiered blob reference")?
            .digest;
        cache.put("k", &second, None)?;
        assert!(cache.blob_path(&first_digest).exists());
        assert_eq!(cache.sweep_blobs()?, 1);
        assert!(!cache.blob_path(&first_digest).exists());
        assert_eq!(cache.get("k")?, Some(second));
        cache.put_many(&[
            BatchPut {
                key: "b1".into(),
                value: deterministic_bytes(TIER_THRESHOLD_BYTES + 21),
                ttl: None,
            },
            BatchPut {
                key: "b2".into(),
                value: b"small".to_vec(),
                ttl: Some(Duration::from_secs(60)),
            },
        ])?;
        assert!(cache.blob_reference("b1")?.is_some());
        assert_eq!(cache.blob_reference("b2")?, None);
        assert_eq!(
            cache.get("b1")?.ok_or("expected b1 value")?.len(),
            TIER_THRESHOLD_BYTES + 21
        );
        let keep = deterministic_bytes(TIER_THRESHOLD_BYTES + 31);
        cache.put("keep", &keep, None)?;
        let keep_digest = cache
            .blob_reference("keep")?
            .ok_or("expected tiered blob reference")?
            .digest;
        let replacement = deterministic_bytes(TIER_THRESHOLD_BYTES + 32);
        let Err(error) = cache.put_many(&[
            BatchPut {
                key: "keep".into(),
                value: replacement,
                ttl: None,
            },
            BatchPut {
                key: "bad".into(),
                value: vec![0_u8; MAX_TIERED_VALUE_BYTES + 1],
                ttl: None,
            },
        ]) else {
            return Err("expected blob too large".into());
        };
        assert!(matches!(error, KvError::BlobTooLarge { .. }));
        assert_eq!(cache.get("keep")?, Some(keep));
        assert!(cache.blob_path(&keep_digest).exists());
        assert_eq!(cache.get("bad")?, None);
        let expired = deterministic_bytes(TIER_THRESHOLD_BYTES + 33);
        cache.put("expired", &expired, Some(Duration::ZERO))?;
        let expired_digest = cache
            .blob_reference("expired")?
            .ok_or("expected tiered blob reference")?
            .digest;
        cache.put_many(&[BatchPut {
            key: "expired".into(),
            value: b"replaced".to_vec(),
            ttl: None,
        }])?;
        assert!(cache.blob_path(&expired_digest).exists());
        assert_eq!(cache.sweep_blobs()?, 1);
        assert!(!cache.blob_path(&expired_digest).exists());
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn concurrent_writers_do_not_expose_partial_tiered_values()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-concurrent");
        let cache = std::sync::Arc::new(open_cache(&db_path, "media")?);
        let workers: Vec<_> = (0..6)
            .map(|index| {
                let cache = std::sync::Arc::clone(&cache);
                std::thread::Builder::new().spawn(move || {
                    let value = deterministic_bytes(TIER_THRESHOLD_BYTES + index + 1);
                    let mut result = cache.put("race", &value, None);
                    for _ in 0..8 {
                        if result.is_ok() {
                            break;
                        }
                        if !matches!(result, Err(KvError::Database(_))) {
                            break;
                        }
                        std::thread::yield_now();
                        result = cache.put("race", &value, None);
                    }
                    result
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        for worker in workers {
            match worker.join() {
                Ok(result) => result?,
                Err(_) => return Err("concurrent writer panicked".into()),
            }
        }
        let value = cache.get("race")?.ok_or("expected race value")?;
        assert!(value.len() > TIER_THRESHOLD_BYTES);
        let blobs = cache.blob_dir.clone();
        drop(cache);
        cleanup_tree(&db_path, &blobs)?;
        Ok(())
    }

    #[test]
    fn blob_chunk_transfer_rejects_out_of_bounds_before_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("tier-chunks");
        let cache = open_cache(&db_path, "media")?;
        let value = deterministic_bytes(TIER_THRESHOLD_BYTES + 64);
        cache.put("v", &value, None)?;
        let reference = cache
            .blob_reference("v")?
            .ok_or("expected tiered blob reference")?;
        let oversize = vec![0_u8; MAX_BLOB_CHUNK_BYTES + 1];
        let Err(error) = cache.read_blob_chunk(&reference.digest, 0, oversize.len()) else {
            return Err("expected invalid chunk length".into());
        };
        assert!(matches!(error, KvError::InvalidChunkLength(_)));
        let Err(error) = cache.write_blob_chunk(&reference.digest, reference.size, 0, &oversize)
        else {
            return Err("expected invalid chunk length".into());
        };
        assert!(matches!(error, KvError::InvalidChunkLength(_)));
        let Err(error) =
            cache.write_blob_chunk(&reference.digest, reference.size, reference.size, b"x")
        else {
            return Err("expected invalid chunk offset".into());
        };
        assert!(matches!(error, KvError::InvalidChunkOffset(_)));
        let Err(error) = cache.read_blob_chunk(&[9_u8; 32], 0, 8) else {
            return Err("expected missing blob".into());
        };
        assert!(matches!(error, KvError::MissingBlob { .. }));
        let foreign = [7_u8; 32];
        let Err(error) = cache.write_blob_chunk(&foreign, 3, 0, b"xyz") else {
            return Err("expected corrupt blob".into());
        };
        assert!(matches!(error, KvError::CorruptBlob { .. }));
        assert_eq!(
            cache.sweep_blobs()?,
            1,
            "a completed-but-diverged staging file is an unreferenced orphan"
        );
        assert!(!cache.blob_path(&foreign).exists());
        let payload = b"hello-chunked-world";
        let staged = content_digest(payload);
        let total = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let complete = cache.write_blob_chunk(&staged, total, 0, &payload[..5])?;
        assert!(!complete);
        let complete = cache.write_blob_chunk(&staged, total, 5, &payload[5..])?;
        assert!(complete);
        assert_eq!(cache.read_blob_chunk(&staged, 0, payload.len())?, payload);
        std::fs::remove_file(cache.blob_path(&staged))?;
        cleanup_tree(&db_path, &cache.blob_dir)?;
        Ok(())
    }

    #[test]
    fn document_namespace_classifies_slots_for_sync_filtering()
    -> Result<(), Box<dyn std::error::Error>> {
        let db_path = path("ns-classify");
        let scope = scope();
        let replica = ReplicaId::new();
        {
            let first = KvCache::open(&db_path, scope, replica, "one")?;
            first.put("shared", b"1", Some(Duration::from_secs(60)))?;
            assert_eq!(
                first.document_namespace(document_for("one", "shared"))?,
                Some("one".to_owned())
            );
            assert_eq!(
                first.document_namespace(document_for("two", "shared"))?,
                None
            );
            assert_eq!(first.document_namespace(DocumentId::new())?, None);
        }
        {
            let second = KvCache::open(&db_path, scope, replica, "two")?;
            second.put("shared", b"2", Some(Duration::from_secs(60)))?;
            assert_eq!(
                second.document_namespace(document_for("two", "shared"))?,
                Some("two".to_owned())
            );
            assert_eq!(
                second.document_namespace(document_for("one", "shared"))?,
                Some("one".to_owned())
            );
        }
        cleanup(&db_path)?;
        Ok(())
    }

    #[test]
    fn blob_names_are_canonical_lowercase_hex_and_reject_every_near_miss()
    -> Result<(), Box<dyn std::error::Error>> {
        let canonical = hex_digest(&[0xab; 32]);
        assert_eq!(canonical.len(), 64);
        assert_eq!(hex_to_digest(&canonical), Some([0xab; 32]));
        assert!(is_blob_name(&canonical));
        let short = canonical[..63].to_owned();
        let long = format!("{canonical}0");
        let mixed_case = canonical.replace('a', "A");
        let invalid_digit = format!("z{}", &canonical[1..]);
        assert_eq!(hex_to_digest(&short), None);
        assert_eq!(hex_to_digest(&long), None);
        assert_eq!(hex_to_digest(&mixed_case), None);
        assert_eq!(hex_to_digest(&invalid_digit), None);
        assert!(!is_blob_name(&short));
        assert!(!is_blob_name(&long));
        assert!(!is_blob_name(&mixed_case));
        assert!(!is_blob_name(&invalid_digit));
        Ok(())
    }
}
