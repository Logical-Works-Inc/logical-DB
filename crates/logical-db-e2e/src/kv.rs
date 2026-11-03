//! Key-value cache journey: one device caches app state, a second device
//! catches up newest-first and pulls a tiered blob in bounded chunks, and the
//! first device reclaims expired and deleted bytes.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Every step goes through the public `logical_db_kv` surface, the same calls
//! an application makes. Failures return a typed [`KvJourneyError`] naming
//! the step that diverged, so the shipped `kv_journey` binary and the e2e
//! test report the same cause.

use std::path::Path;
use std::time::Duration;

use logical_db::{OperationId, RemoteChange};
use logical_db_kv::{
    AccountId, BatchPut, Database, KvCache, KvError, MAX_BLOB_CHUNK_BYTES, NamespaceCheckpoint,
    NamespaceCheckpoints, PeerId, ReplicaId, Scope, SpaceId, TIER_THRESHOLD_BYTES,
};

/// Namespace every journey cache writes.
const NAMESPACE: &str = "sessions";
/// Largest page the journey reads.
const PAGE: usize = 256;

/// Why a cache journey step failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum KvJourneyError {
    /// The cache returned an error (boxed: the facade error is large).
    Cache(Box<KvError>),
    /// The facade below the cache returned an error.
    Database(Box<logical_db::DatabaseError>),
    /// A namespace checkpoint was refused by the model.
    Checkpoint(logical_db_model::ModelError),
    /// A length did not fit its integer width.
    Width(std::num::TryFromIntError),
    /// The receipt or a replica directory could not be written.
    Io(std::io::Error),
    /// The cache answered, but not what the journey's user would see.
    Diverged(&'static str),
}

impl std::fmt::Display for KvJourneyError {
    /// Names the failing layer and its cause.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Self::Cache(ref error) => write!(formatter, "cache: {error}"),
            Self::Database(ref error) => write!(formatter, "database: {error}"),
            Self::Checkpoint(ref error) => write!(formatter, "checkpoint: {error}"),
            Self::Width(ref error) => write!(formatter, "width: {error}"),
            Self::Io(ref error) => write!(formatter, "io: {error}"),
            Self::Diverged(step) => write!(formatter, "diverged: {step}"),
        }
    }
}

impl std::error::Error for KvJourneyError {
    /// Exposes the underlying error, if any.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match *self {
            Self::Cache(ref error) => Some(error.as_ref()),
            Self::Database(ref error) => Some(error.as_ref()),
            Self::Checkpoint(ref error) => Some(error),
            Self::Width(ref error) => Some(error),
            Self::Io(ref error) => Some(error),
            Self::Diverged(_) => None,
        }
    }
}

impl From<KvError> for KvJourneyError {
    /// Wraps a cache error.
    fn from(error: KvError) -> Self {
        Self::Cache(Box::new(error))
    }
}

impl From<logical_db::DatabaseError> for KvJourneyError {
    /// Wraps a facade error.
    fn from(error: logical_db::DatabaseError) -> Self {
        Self::Database(Box::new(error))
    }
}

impl From<logical_db_model::ModelError> for KvJourneyError {
    /// Wraps a checkpoint model error.
    fn from(error: logical_db_model::ModelError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<std::num::TryFromIntError> for KvJourneyError {
    /// Wraps an integer width error.
    fn from(error: std::num::TryFromIntError) -> Self {
        Self::Width(error)
    }
}

impl From<std::io::Error> for KvJourneyError {
    /// Wraps a filesystem error.
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Fails the journey with `step` unless `holds`.
fn ensure(holds: bool, step: &'static str) -> Result<(), KvJourneyError> {
    if holds {
        Ok(())
    } else {
        Err(KvJourneyError::Diverged(step))
    }
}

/// Runs the whole cache journey with replicas under `dir` and returns one
/// receipt line per step the user would observe.
///
/// # Errors
///
/// Returns [`KvJourneyError`] naming the first step whose result differs
/// from what the user expects.
pub fn kv_cache_journey(dir: &Path) -> Result<Vec<&'static str>, KvJourneyError> {
    let mut receipt = Vec::new();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let writer = KvCache::open(dir.join("writer.redb"), scope, ReplicaId::new(), NAMESPACE)?;
    ensure(writer.namespace() == NAMESPACE, "writer namespace")?;

    writer.put("profile", b"ada", None)?;
    writer.put("draft", b"gone", Some(Duration::ZERO))?;
    ensure(
        writer.get("profile")?.as_deref() == Some(&b"ada"[..]),
        "put then get",
    )?;
    ensure(writer.get("draft")?.is_none(), "expired entry reads absent")?;
    receipt.push("put, get and expiry-aware read: pass");

    writer.put_many(&[
        BatchPut::new("k1", b"one".to_vec(), None),
        BatchPut::new("k2", b"two".to_vec(), None),
    ])?;
    let retry = OperationId::new();
    let third = [BatchPut::new("k3", b"three".to_vec(), None)];
    writer.put_many_with_operation(retry, &third)?;
    let before = writer.database().changes_since(0, PAGE)?.len();
    writer.put_many_with_operation(retry, &third)?;
    ensure(
        writer.database().changes_since(0, PAGE)?.len() == before,
        "exact batch retry appends nothing",
    )?;
    let page = writer.scan("k", None, 10)?;
    let keys: Vec<&str> = page.iter().map(logical_db_kv::CacheEntry::key).collect();
    ensure(keys == ["k1", "k2", "k3"], "prefix scan in key order")?;
    ensure(
        page.first().map(logical_db_kv::CacheEntry::value) == Some(&b"one"[..]),
        "scan carries values",
    )?;
    receipt.push("atomic batch, idempotent retry and prefix scan: pass");

    let size = TIER_THRESHOLD_BYTES
        .checked_add(MAX_BLOB_CHUNK_BYTES)
        .ok_or(KvJourneyError::Diverged("tiered size overflows"))?;
    let avatar = vec![0x5A_u8; size];
    writer.put("avatar", &avatar, None)?;
    ensure(
        writer.evict_expired(10)? == 1,
        "eviction reclaims the expired draft",
    )?;
    ensure(writer.delete("profile")?, "delete removes a live key")?;
    ensure(writer.get("profile")?.is_none(), "deleted key reads absent")?;
    receipt.push("tiered put, eviction and delete: pass");

    let reader = KvCache::open(dir.join("reader.redb"), scope, ReplicaId::new(), NAMESPACE)?;
    let peer = PeerId::new();
    let pending = writer.database().pending_changes(0, PAGE)?;
    let page: Vec<RemoteChange> = pending
        .iter()
        .map(|change| RemoteChange::new(change.document, change.digest, change.bytes().to_vec()))
        .collect();
    let mut checkpoints = NamespaceCheckpoints::new();
    checkpoints.insert(NAMESPACE, NamespaceCheckpoint::new(vec![1], 1_000)?)?;
    reader
        .database()
        .install_remote_page(peer, &page, &checkpoints.encode()?)?;
    ensure(
        reader.namespace_checkpoints(peer)? == checkpoints,
        "the page's namespace cursor commits with its changes",
    )?;
    checkpoints.insert(NAMESPACE, NamespaceCheckpoint::new(vec![2], 2_000)?)?;
    reader.store_namespace_checkpoints(peer, &checkpoints)?;
    ensure(
        reader.namespace_checkpoints(peer)? == checkpoints,
        "namespace cursor survives the durable slot",
    )?;
    let newest = reader.recent("k", 1)?;
    ensure(
        newest.first().map(logical_db_kv::CacheEntry::key) == Some("k3"),
        "cold device reads the newest key first",
    )?;
    let classified = pending
        .iter()
        .map(|change| reader.document_namespace(change.document))
        .collect::<Result<Vec<_>, _>>()?;
    ensure(
        classified
            .iter()
            .flatten()
            .all(|namespace| namespace == NAMESPACE),
        "every synced slot classifies into its namespace",
    )?;
    receipt.push("sync, namespace cursor and newest-first catch-up: pass");

    let reference = reader
        .blob_reference("avatar")?
        .ok_or(KvJourneyError::Diverged(
            "synced tiered slot names its blob",
        ))?;
    ensure(
        matches!(reader.get("avatar"), Err(KvError::MissingBlob { .. })),
        "a reference without bytes fails loudly",
    )?;
    let mut offset = 0_u64;
    while offset < reference.size {
        let left = usize::try_from(reference.size.saturating_sub(offset))?;
        let chunk =
            writer.read_blob_chunk(&reference.digest, offset, left.min(MAX_BLOB_CHUNK_BYTES))?;
        reader.write_blob_chunk(&reference.digest, reference.size, offset, &chunk)?;
        offset = offset.saturating_add(u64::try_from(chunk.len())?);
    }
    ensure(
        reader.get("avatar")?.as_deref() == Some(avatar.as_slice()),
        "chunked blob reads whole",
    )?;
    receipt.push("bounded chunked blob transfer: pass");

    ensure(writer.delete("avatar")?, "delete the tiered key")?;
    ensure(
        writer.sweep_blobs()? == 1,
        "sweep reclaims the unreferenced blob",
    )?;
    receipt.push("blob sweep after delete: pass");

    let legacy = dir.join("legacy.redb");
    let replica = ReplicaId::new();
    drop(Database::open(&legacy, scope, replica)?);
    let (migrated, report) = KvCache::migrate_schema(&legacy, scope, replica, NAMESPACE, 16)?;
    ensure(report.documents == 0, "empty replica migrates no documents")?;
    migrated.put("after-migration", b"ok", None)?;
    ensure(
        migrated.get("after-migration")?.as_deref() == Some(&b"ok"[..]),
        "migrated replica serves the cache",
    )?;
    receipt.push("schema migration onto an existing replica: pass");
    Ok(receipt)
}
