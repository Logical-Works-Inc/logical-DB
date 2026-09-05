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

#![forbid(unsafe_code)]

use std::{
    collections::BTreeSet,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub use logical_db::{Database, DatabaseError, MigrationReport, Projection};
use logical_db::{EditOutcome, Item, Object, OperationDigest, OperationId, Scalar};
use logical_db_model::MAX_CHANGE_BYTES;
pub use logical_db_model::{AccountId, DocumentId, IndexId, IndexValue, ReplicaId, Scope, SpaceId};
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
            return Err(KvError::ValueTooLarge {
                actual: value.len(),
                maximum: MAX_VALUE_BYTES,
            });
        }
        let document = document_for(&self.namespace, key);
        let now = now_ms();
        let expires_at_ms = expiry_for(ttl, now);
        let sequence = self.next_sequence(now);
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
            return Err(KvError::InvalidScanLimit(limit));
        }
        if after.is_some_and(|cursor| cursor.len() > logical_db_model::MAX_NAME_BYTES) {
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

    /// Deletes up to `limit` expired slots in this namespace, reporting how
    /// many were reclaimed.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidEvictionLimit`] for a batch outside
    /// `1..=256`, or [`KvError::Database`] for a durable failure.
    pub fn evict_expired(&self, limit: usize) -> Result<usize, KvError> {
        if limit == 0 || limit > MAX_PAGE_LIMIT {
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
enum Decoded {
    Absent,
    Corrupt,
    Unsupported(u64),
    Found(Envelope),
}

fn validate_key(key: &str) -> Result<(), KvError> {
    if key.is_empty() || key.len() > logical_db_model::MAX_NAME_BYTES {
        return Err(KvError::InvalidKey);
    }
    Ok(())
}

fn validate_namespace(namespace: &str) -> Result<(), KvError> {
    if namespace.is_empty() || namespace.len() > logical_db_model::MAX_NAME_BYTES {
        return Err(KvError::InvalidNamespace);
    }
    Ok(())
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
}
