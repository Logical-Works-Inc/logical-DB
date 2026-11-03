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
//! cache scope carries: the expiry index `ldb-kv-expiry-01` (`Timestamp`
//! expiry, defaulting to never expire for older envelopes) and the recency
//! index `ldb-kv-recency-1` (`Unsigned` recency, defaulting to zero). Slots without a live value project no rows.
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
    path::PathBuf,
    sync::{Mutex, atomic::AtomicU64},
    time::Duration,
};

use lgwks_std::hash::Hasher;
pub use logical_db::{
    BatchEdit, Database, DatabaseError, MigrationReport, Projection, ProjectionSemantics,
};
use logical_db_model::MAX_CHANGE_BYTES;
pub use logical_db_model::{
    AccountId, DocumentId, DocumentRevision, IndexId, IndexValue, NamespaceCheckpoint,
    NamespaceCheckpoints, PeerId, ReplicaId, Scope, SpaceId,
};
use thiserror::Error;

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

/// Identity bytes of the secondary index over slot expiry (`Timestamp`
/// milliseconds). Fixed forever: stored index rows address it.
const EXPIRY_INDEX_BYTES: [u8; 16] = *b"ldb-kv-expiry-01";
/// Identity bytes of the secondary index over slot recency (`Unsigned` writer
/// sequence). Fixed forever: stored index rows address it.
const RECENCY_INDEX_BYTES: [u8; 16] = *b"ldb-kv-recency-1";

/// Secondary index over slot expiry (`Timestamp` milliseconds).
pub(crate) fn expiry_index() -> Result<IndexId, KvError> {
    Ok(logical_db::decode_identity_id(&EXPIRY_INDEX_BYTES)?)
}

/// Secondary index over slot recency (`Unsigned` writer sequence).
pub(crate) fn recency_index() -> Result<IndexId, KvError> {
    Ok(logical_db::decode_identity_id(&RECENCY_INDEX_BYTES)?)
}

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
    pub(crate) key: String,
    /// Opaque value bytes as stored.
    pub(crate) value: Vec<u8>,
}

impl CacheEntry {
    /// Returns the original cache key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the opaque value bytes as stored.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
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
    pub(crate) key: String,
    /// Opaque value bytes, bounded exactly like [`KvCache::put`].
    pub(crate) value: Vec<u8>,
    /// Optional time-to-live; `None` never expires.
    pub(crate) ttl: Option<Duration>,
}

impl BatchPut {
    /// Builds one batch entry; validation happens when the batch is put, so
    /// a bad key or oversized value fails the whole batch before any commit.
    #[must_use]
    pub fn new(key: impl Into<String>, value: Vec<u8>, ttl: Option<Duration>) -> Self {
        Self {
            key: key.into(),
            value,
            ttl,
        }
    }
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
    /// The system clock read before the Unix epoch, so no expiry or recency
    /// could be stamped.
    #[error("system clock reads before the Unix epoch")]
    ClockBeforeEpoch(#[from] std::time::SystemTimeError),
    /// The system clock read past `i64` milliseconds since the epoch.
    #[error("system clock is past the representable Unix millisecond range")]
    ClockOutOfRange(#[source] std::num::TryFromIntError),
    /// An in-memory length, index or timestamp did not fit the integer width
    /// a digest frame or the blob store records, so it is refused rather than
    /// recorded as a stand-in.
    #[error("a length or index does not fit its recorded integer width")]
    WidthOverflow(#[from] std::num::TryFromIntError),
    /// No unique blob staging name could be drawn (clock before the epoch or
    /// unreadable OS entropy), so the write fails instead of reusing a name.
    #[error("blob staging name could not be generated")]
    StagingName(#[source] lgwks_std::id::V7Error),
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

/// Shapes the first sixteen digest bytes of `hasher` into the custom
/// (version 8) RFC 4122 UUID layout the key mapping fixes forever, and
/// builds the requested identity from them.
fn custom_id<T>(hasher: Hasher) -> Result<T, KvError>
where
    T: for<'de> lgwks_std::json::Deserialize<'de>,
{
    let digest: [u8; 32] = *hasher.finalize().as_bytes();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0F) | 0x80;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    Ok(logical_db::decode_identity_id(&bytes)?)
}

/// Returns a unique time-ordered suffix for one blob staging file.
///
/// A clock before 1970 or unreadable OS entropy fails the write instead of
/// reusing a name another writer could be staging.
fn staging_suffix() -> Result<String, KvError> {
    lgwks_std::id::Uuid::new_v7()
        .map(|id| id.to_string())
        .map_err(KvError::StagingName)
}

mod blob;
mod cache;
mod envelope;
mod request;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_blob;
