//! Embedded transactional driver and compatibility FDB-shaped primitives.
//!
//! [`LogicalDb`] runs in-process over memory or redb and is the zero-daemon
//! deployment profile. The native FoundationDB driver is [`crate::admitted::OutboardDb`]
//! behind the `fdb` feature. Both are reached through the admitted hybrid API;
//! a configured cluster path is rejected here rather than silently opening an
//! embedded database.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use redb::{ReadableDatabase, ReadableTable};

use crate::port::{ClockPort, DurableKv, KvPort};
use crate::{KernelError, Result};

// Keep the pinned binding in this feature graph; admitted::OutboardDb owns the
// actual client calls and the crate owns the unsafe C-client boundary.
#[cfg(feature = "fdb")]
use foundationdb as _;

/// FoundationDB's hard affected-data ceiling. This is decimal, not 10 MiB.
pub const MAX_TRANSACTION_BYTES: usize = 10_000_000;
/// FoundationDB's hard encoded-key ceiling, shared by both providers.
pub const MAX_KEY_BYTES: usize = 10_000;
/// FoundationDB's hard single-value ceiling, shared by both providers.
pub const MAX_VALUE_BYTES: usize = 100_000;
pub const MAX_TRANSACTION_KEYS: usize = 100_000;
pub const DEFAULT_TRANSACTION_TIMEOUT_MS: u64 = 5_000;
const MAX_RETRY_ATTEMPTS: usize = 5;
const VERSIONSTAMP_PLACEHOLDER_LEN: usize = 10;

#[derive(Debug, Clone)]
pub struct DatabaseOptions {
    pub cluster_file: Option<String>,
    pub transaction_timeout_ms: u64,
    pub transaction_size_limit_bytes: usize,
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        Self {
            cluster_file: None,
            transaction_timeout_ms: DEFAULT_TRANSACTION_TIMEOUT_MS,
            transaction_size_limit_bytes: MAX_TRANSACTION_BYTES,
        }
    }
}

/// Compatibility name for callers written before ADR-003. A cluster path is
/// never silently treated as an embedded database; see [`LogicalDb::open`].
pub type FdbConfig = DatabaseOptions;

// ── Atomic op ──────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq)]
enum AtomicOpKind {
    Add,
    And,
    Or,
    Max,
    Min,
    AppendIfFits,
}

#[derive(Clone, Debug)]
struct AtomicOp {
    key: Vec<u8>,
    param: Vec<u8>,
    kind: AtomicOpKind,
}

#[derive(Clone, Debug)]
struct VersionstampedWrite {
    key: Vec<u8>,
    value: Vec<u8>,
    is_key: bool,
}

// ── Watch ──────────────────────────────────────────────────────

/// Watch handle — fires when the watched key is committed by another
/// transaction. In the shim this is a oneshot channel driven by
/// `LogicalDb::try_commit` notifying `watches` waiters.
pub struct Watch {
    key: Vec<u8>,
    rx: tokio::sync::oneshot::Receiver<()>,
}

impl Watch {
    /// Wait until the watched key changes or the watch is cancelled.
    /// Returns `Ok(())` on change, `Err(WatchCancelled)` on cancel.
    pub async fn wait(self) -> Result<()> {
        self.rx.await.map_err(|_| KernelError::WatchCancelled)
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

// ── LogicalDb ──────────────────────────────────────────────────

type WatchSenders = Vec<tokio::sync::oneshot::Sender<()>>;
type WatchMap = HashMap<Vec<u8>, WatchSenders>;
type SnapshotMap = BTreeMap<Vec<u8>, Vec<u8>>;

pub struct LogicalDb {
    config: DatabaseOptions,
    pub(crate) backend: Arc<Mutex<EmbeddedBackend>>,
    #[allow(clippy::type_complexity)]
    pub(crate) watches: Arc<Mutex<WatchMap>>,
}

impl LogicalDb {
    /// In-memory processing profile. Supplying an FDB cluster path is an
    /// error: embedded execution must never masquerade as an outboard client.
    pub fn open(config: DatabaseOptions) -> Result<Self> {
        validate_database_options(&config)?;
        reject_outboard_config(&config)?;
        let backend = EmbeddedBackend::new_mem_shim();
        Ok(Self {
            config,
            backend: Arc::new(Mutex::new(backend)),
            watches: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// File-backed durable open — same crash story as `EmbeddedBackend::open`
    /// (redb transaction per commit). Used for local-dev durability
    /// rehearsal without a live fdbserver.
    pub fn open_at(path: impl AsRef<Path>, config: DatabaseOptions) -> Result<Self> {
        validate_database_options(&config)?;
        reject_outboard_config(&config)?;
        let backend = EmbeddedBackend::open(path)?;
        // Validate durable metadata before exposing the handle. Corruption is
        // not a fresh database and must never be reset to version zero.
        backend.committed_version()?;
        Ok(Self {
            config,
            backend: Arc::new(Mutex::new(backend)),
            watches: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn config(&self) -> &DatabaseOptions {
        &self.config
    }

    /// Embedded transaction with bounded optimistic retries.
    pub async fn transact<F, T>(&self, mut f: F) -> Result<T>
    where
        F: FnMut(&mut FdbTransaction) -> Result<T>,
    {
        let mut attempts = 0usize;
        loop {
            let (read_version, snapshot) = self.versioned_snapshot()?;
            let mut tx = FdbTransaction::new_with_snapshot(
                self.config.transaction_size_limit_bytes,
                self.config.transaction_timeout_ms,
                read_version,
                snapshot,
            );
            let result = f(&mut tx);
            match result {
                Ok(val) => match self.try_commit(tx).await {
                    Ok(_) => return Ok(val),
                    Err(KernelError::TransactionConflict) if attempts < MAX_RETRY_ATTEMPTS => {
                        attempts += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempts as u64))
                            .await;
                        continue;
                    }
                    Err(e) => return Err(e),
                },
                Err(KernelError::TransactionConflict) if attempts < MAX_RETRY_ATTEMPTS => {
                    attempts += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(5 * attempts as u64)).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Synchronous transact shim for backward compat — runs a sync
    /// closure in a transaction and commits.
    pub async fn transact_sync<F, T>(&self, f: F) -> Result<T>
    where
        F: FnMut(&mut FdbTransaction) -> Result<T>,
    {
        self.transact(f).await
    }

    /// Commit `tx` and return the exact version represented by the result.
    /// A mutating transaction returns its newly committed version. A
    /// read-only transaction returns its original read version because it
    /// creates no new durable version.
    pub(crate) async fn try_commit(&self, mut tx: FdbTransaction) -> Result<u64> {
        // Timeout check at commit boundary as well.
        let elapsed = tx.start.elapsed().as_millis() as u64;
        if elapsed > tx.timeout_ms {
            return Err(KernelError::TransactionTimedOut {
                elapsed_ms: elapsed,
                limit_ms: tx.timeout_ms,
            });
        }
        // The backend lock is the single serialization boundary for reading
        // the committed version, conflict validation, and durable commit.
        // Releasing it between these operations lets two stale writers pass
        // the same check and both commit.
        let mut be = self.lock_backend()?;
        let current_committed = be.committed_version()?;
        if tx.requires_exact_read_version && current_committed != tx.read_version {
            return Err(KernelError::StaleJustification {
                expected: current_committed,
                got: tx.read_version,
            });
        }
        let current_map = be.snapshot_map()?;
        if current_committed != tx.read_version && !tx.read_conflict_ranges.is_empty() {
            for (r_begin, r_end) in &tx.read_conflict_ranges {
                if tx
                    .backend_snapshot
                    .range(r_begin.clone()..r_end.clone())
                    .ne(current_map.range(r_begin.clone()..r_end.clone()))
                {
                    return Err(KernelError::TransactionConflict);
                }
            }
        }

        // Resolve atomic ops into buffer
        for op in std::mem::take(&mut tx.atomic_ops) {
            let existing = tx
                .buffer
                .get(&op.key)
                .and_then(|v| v.as_ref())
                // Atomic operations linearize at commit and therefore resolve
                // against current committed state, not the stale read image.
                .or_else(|| current_map.get(&op.key))
                .cloned();
            let new_val = apply_atomic_op(&op, existing.as_deref())?;
            // atomic ops count as a key+bytes against limits (check)
            let add = op.key.len() + new_val.len();
            if tx.total_bytes + add > tx.size_limit_bytes {
                return Err(KernelError::TransactionTooLarge {
                    size: tx.total_bytes + add,
                    limit: tx.size_limit_bytes,
                });
            }
            tx.buffer.insert(op.key, Some(new_val));
        }

        // A range clear applies to the state at serialization time, not only
        // to keys visible in the transaction's original snapshot. Preserve a
        // later point write in the same transaction by filling only keys that
        // do not already have a buffered final operation.
        for (begin, end) in &tx.clear_ranges {
            let current_keys: Vec<Vec<u8>> = current_map
                .range(begin.clone()..end.clone())
                .map(|(key, _)| key.clone())
                .collect();
            for key in current_keys {
                if !tx.buffer.contains_key(&key) {
                    tx.check_limits(key.len())?;
                    tx.total_bytes += key.len();
                    tx.total_keys += 1;
                    tx.buffer.insert(key, None);
                }
            }
        }

        // Resolve versionstamped writes
        let versionstamped_writes = std::mem::take(&mut tx.versionstamped_writes);
        let mut next_version = None;
        if !versionstamped_writes.is_empty() {
            let version = current_committed
                .checked_add(1)
                .ok_or_else(|| KernelError::Backend("committed version overflow".into()))?;
            next_version = Some(version);
            for vs in versionstamped_writes {
                let (k, v) = resolve_versionstamp(vs, version)?;
                tx.buffer.insert(k, Some(v));
            }
        }

        // Collect entries for apply_batch: buffer Some -> put, None -> delete.
        // For shim we emulate clear_range already expanded into per-key deletes
        // in buffer; versionstamped and atomic already resolved.
        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut deletes: Vec<Vec<u8>> = Vec::new();
        for (k, v) in tx.buffer {
            match v {
                Some(val) => puts.push((k, val)),
                None => deletes.push(k),
            }
        }

        let has_writes = !puts.is_empty() || !deletes.is_empty();
        let committed_version = if has_writes {
            match next_version {
                Some(version) => version,
                None => current_committed
                    .checked_add(1)
                    .ok_or_else(|| KernelError::Backend("committed version overflow".into()))?,
            }
        } else {
            tx.read_version
        };
        // Acquire watch state before the durable mutation so poison cannot
        // turn a successful commit into an error response.
        let mut watches = if has_writes || !tx.watches.is_empty() {
            Some(self.lock_watches()?)
        } else {
            None
        };

        if has_writes {
            let watches = watches
                .as_mut()
                .ok_or_else(|| KernelError::Backend("write watch state unavailable".into()))?;
            be.apply_transaction(&puts, &deletes, committed_version)?;

            // Notify watches only after durability succeeds.
            let mut to_notify: Vec<Vec<u8>> = Vec::new();
            for (k, _) in &puts {
                if watches.contains_key(k) {
                    to_notify.push(k.clone());
                }
            }
            for k in &deletes {
                if watches.contains_key(k) {
                    to_notify.push(k.clone());
                }
            }
            for k in to_notify {
                if let Some(senders) = watches.remove(&k) {
                    for s in senders {
                        let _ = s.send(());
                    }
                }
            }
        }

        // Persist watch registrations
        if !tx.watches.is_empty() {
            let wmap = watches.as_mut().ok_or_else(|| {
                KernelError::Backend("watch registration state unavailable".into())
            })?;
            for (key, sender) in tx.watches {
                wmap.entry(key).or_default().push(sender);
            }
        }

        Ok(committed_version)
    }

    pub fn read_version(&self) -> Result<u64> {
        self.committed_version()
    }

    pub fn committed_version(&self) -> Result<u64> {
        self.lock_backend()?.committed_version()
    }

    pub(crate) fn versioned_snapshot(&self) -> Result<(u64, SnapshotMap)> {
        let backend = self.lock_backend()?;
        Ok((backend.committed_version()?, backend.snapshot_map()?))
    }

    pub(crate) fn lock_backend(&self) -> Result<MutexGuard<'_, EmbeddedBackend>> {
        self.backend
            .lock()
            .map_err(|_| KernelError::StateLockPoisoned { lock: "backend" })
    }

    /// Try to enter the embedded transaction boundary without an unbounded
    /// thread-blocking mutex wait. Admitted orchestration applies the configured
    /// deadline while the transaction is still safe to abandon.
    pub(crate) fn try_lock_backend(&self) -> Result<Option<MutexGuard<'_, EmbeddedBackend>>> {
        match self.backend.try_lock() {
            Ok(backend) => Ok(Some(backend)),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Poisoned(_)) => {
                Err(KernelError::StateLockPoisoned { lock: "backend" })
            }
        }
    }

    fn lock_watches(&self) -> Result<MutexGuard<'_, WatchMap>> {
        self.watches
            .lock()
            .map_err(|_| KernelError::StateLockPoisoned { lock: "watches" })
    }
}

fn reject_outboard_config(config: &DatabaseOptions) -> Result<()> {
    if config.cluster_file.is_some() {
        Err(KernelError::Backend(
            "cluster_file requires the native FoundationDB client profile; embedded open refuses fallback"
                .into(),
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_database_options(config: &DatabaseOptions) -> Result<()> {
    if config.transaction_timeout_ms == 0 {
        return Err(KernelError::ConfigurationInvalid(
            "transaction_timeout_ms must be greater than zero",
        ));
    }
    if config.transaction_timeout_ms > i32::MAX as u64 {
        return Err(KernelError::ConfigurationInvalid(
            "transaction_timeout_ms must fit FoundationDB's signed option field",
        ));
    }
    if config.transaction_size_limit_bytes == 0 {
        return Err(KernelError::ConfigurationInvalid(
            "transaction_size_limit_bytes must be greater than zero",
        ));
    }
    if config.transaction_size_limit_bytes > MAX_TRANSACTION_BYTES {
        return Err(KernelError::TransactionTooLarge {
            size: config.transaction_size_limit_bytes,
            limit: MAX_TRANSACTION_BYTES,
        });
    }
    Ok(())
}

fn apply_atomic_op(op: &AtomicOp, existing: Option<&[u8]>) -> Result<Vec<u8>> {
    match op.kind {
        AtomicOpKind::Add => {
            if op.param.len() != 8 {
                return Err(KernelError::AtomicOpViolation(
                    "add operand must be 8 bytes LE i64".into(),
                ));
            }
            let add = i64::from_le_bytes(op.param.as_slice().try_into().unwrap());
            let cur = if let Some(v) = existing {
                if v.len() != 8 {
                    return Err(KernelError::AtomicOpViolation(
                        "existing value for add must be 8 bytes".into(),
                    ));
                }
                i64::from_le_bytes(v.try_into().unwrap())
            } else {
                0
            };
            Ok((cur.wrapping_add(add)).to_le_bytes().to_vec())
        }
        AtomicOpKind::And => {
            let cur = existing.unwrap_or(&[]);
            let mut out = cur.to_vec();
            if out.len() < op.param.len() {
                out.resize(op.param.len(), 0);
            }
            for (i, b) in op.param.iter().enumerate() {
                out[i] &= b;
            }
            Ok(out)
        }
        AtomicOpKind::Or => {
            let cur = existing.unwrap_or(&[]);
            let mut out = cur.to_vec();
            if out.len() < op.param.len() {
                out.resize(op.param.len(), 0);
            }
            for (i, b) in op.param.iter().enumerate() {
                out[i] |= b;
            }
            Ok(out)
        }
        AtomicOpKind::Max => {
            let cur = existing.unwrap_or(&[]);
            if cur.is_empty() {
                return Ok(op.param.clone());
            }
            if op.param.len() == 8 && cur.len() == 8 {
                let a = i64::from_le_bytes(cur.try_into().unwrap());
                let b = i64::from_le_bytes(op.param.as_slice().try_into().unwrap());
                return Ok(a.max(b).to_le_bytes().to_vec());
            }
            if op.param.as_slice() > cur {
                Ok(op.param.clone())
            } else {
                Ok(cur.to_vec())
            }
        }
        AtomicOpKind::Min => {
            let cur = existing.unwrap_or(&[]);
            if cur.is_empty() {
                return Ok(op.param.clone());
            }
            if op.param.len() == 8 && cur.len() == 8 {
                let a = i64::from_le_bytes(cur.try_into().unwrap());
                let b = i64::from_le_bytes(op.param.as_slice().try_into().unwrap());
                return Ok(a.min(b).to_le_bytes().to_vec());
            }
            if op.param.as_slice() < cur {
                Ok(op.param.clone())
            } else {
                Ok(cur.to_vec())
            }
        }
        AtomicOpKind::AppendIfFits => {
            let cur = existing.unwrap_or(&[]);
            let mut out = cur.to_vec();
            if out.len() + op.param.len() > MAX_TRANSACTION_BYTES {
                return Err(KernelError::TransactionTooLarge {
                    size: out.len() + op.param.len(),
                    limit: MAX_TRANSACTION_BYTES,
                });
            }
            out.extend_from_slice(&op.param);
            Ok(out)
        }
    }
}

fn resolve_versionstamp(vs: VersionstampedWrite, version: u64) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut key = vs.key;
    let mut value = vs.value;
    let stamp = versionstamp_bytes(version);
    if vs.is_key {
        let pos = key
            .windows(VERSIONSTAMP_PLACEHOLDER_LEN)
            .position(|w| w == [0xFF; 10]);
        if let Some(idx) = pos {
            key.splice(idx..idx + VERSIONSTAMP_PLACEHOLDER_LEN, stamp.clone());
        } else if key.len() >= VERSIONSTAMP_PLACEHOLDER_LEN && key[key.len() - 10..] == [0u8; 10] {
            key.splice(key.len() - 10.., stamp.clone());
        } else {
            return Err(KernelError::Backend(
                "versionstamped key missing 10-byte placeholder".into(),
            ));
        }
    } else {
        let pos = value
            .windows(VERSIONSTAMP_PLACEHOLDER_LEN)
            .position(|w| w == [0xFF; 10]);
        if let Some(idx) = pos {
            value.splice(idx..idx + VERSIONSTAMP_PLACEHOLDER_LEN, stamp);
        } else if value.len() >= VERSIONSTAMP_PLACEHOLDER_LEN
            && value[value.len() - 10..] == [0u8; 10]
        {
            value.splice(value.len() - 10.., stamp);
        } else {
            return Err(KernelError::Backend(
                "versionstamped value missing 10-byte placeholder".into(),
            ));
        }
    }
    Ok((key, value))
}

fn versionstamp_bytes(version: u64) -> Vec<u8> {
    let mut out = version.to_be_bytes().to_vec();
    out.extend_from_slice(&[0u8, 0u8]);
    out
}

// ── FdbTransaction ─────────────────────────────────────────────

pub struct FdbTransaction {
    size_limit_bytes: usize,
    timeout_ms: u64,
    start: std::time::Instant,
    read_version: u64,
    total_bytes: usize,
    total_keys: usize,
    buffer: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    read_conflict_ranges: Vec<(Vec<u8>, Vec<u8>)>,
    write_conflict_ranges: Vec<(Vec<u8>, Vec<u8>)>,
    clear_ranges: Vec<(Vec<u8>, Vec<u8>)>,
    requires_exact_read_version: bool,
    watches: Vec<(Vec<u8>, tokio::sync::oneshot::Sender<()>)>,
    atomic_ops: Vec<AtomicOp>,
    versionstamped_writes: Vec<VersionstampedWrite>,
    backend_snapshot: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl FdbTransaction {
    pub fn new(size_limit_bytes: usize, timeout_ms: u64) -> Self {
        Self::new_with_snapshot(size_limit_bytes, timeout_ms, 0, BTreeMap::new())
    }

    pub(crate) fn new_with_snapshot(
        size_limit_bytes: usize,
        timeout_ms: u64,
        read_version: u64,
        backend_snapshot: BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> Self {
        Self {
            size_limit_bytes,
            timeout_ms,
            start: std::time::Instant::now(),
            read_version,
            total_bytes: 0,
            total_keys: 0,
            buffer: BTreeMap::new(),
            read_conflict_ranges: Vec::new(),
            write_conflict_ranges: Vec::new(),
            clear_ranges: Vec::new(),
            requires_exact_read_version: false,
            watches: Vec::new(),
            atomic_ops: Vec::new(),
            versionstamped_writes: Vec::new(),
            backend_snapshot,
        }
    }

    fn check_limits(&self, additional_bytes: usize) -> Result<()> {
        let elapsed = self.start.elapsed().as_millis() as u64;
        if elapsed > self.timeout_ms {
            return Err(KernelError::TransactionTimedOut {
                elapsed_ms: elapsed,
                limit_ms: self.timeout_ms,
            });
        }
        if self.total_bytes + additional_bytes > self.size_limit_bytes {
            return Err(KernelError::TransactionTooLarge {
                size: self.total_bytes + additional_bytes,
                limit: self.size_limit_bytes,
            });
        }
        if self.total_keys + 1 > MAX_TRANSACTION_KEYS {
            return Err(KernelError::TransactionTooLarge {
                size: self.total_keys + 1,
                limit: MAX_TRANSACTION_KEYS,
            });
        }
        Ok(())
    }

    fn check_timeout(&self) -> Result<()> {
        let elapsed = self.start.elapsed().as_millis() as u64;
        if elapsed > self.timeout_ms {
            return Err(KernelError::TransactionTimedOut {
                elapsed_ms: elapsed,
                limit_ms: self.timeout_ms,
            });
        }
        Ok(())
    }

    // ── Core KV ─────────────────────────────────────────────

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_timeout()?;
        if let Some(v) = self.buffer.get(key) {
            return Ok(v.clone());
        }
        Ok(self.backend_snapshot.get(key).cloned())
    }

    pub fn get_snapshot(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.check_timeout()?;
        Ok(self.backend_snapshot.get(key).cloned())
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        validate_key(&key)?;
        validate_value(&value)?;
        let additional = key.len() + value.len();
        self.check_limits(additional)?;
        self.total_bytes += additional;
        self.total_keys += 1;
        self.buffer.insert(key, Some(value));
        Ok(())
    }

    pub fn clear(&mut self, key: &[u8]) -> Result<()> {
        validate_key(key)?;
        self.check_limits(key.len())?;
        self.total_bytes += key.len();
        self.total_keys += 1;
        self.buffer.insert(key.to_vec(), None);
        Ok(())
    }

    pub fn clear_range(&mut self, begin: &[u8], end: &[u8]) -> Result<()> {
        self.check_timeout()?;
        if begin > end {
            return Err(KernelError::Backend(
                "clear range begin must not exceed end".into(),
            ));
        }
        // Record write conflict range and expand deletes for snapshot keys in range
        self.write_conflict_ranges
            .push((begin.to_vec(), end.to_vec()));
        self.clear_ranges.push((begin.to_vec(), end.to_vec()));
        // Estimate cost: number of keys in range * average key len (charge per delete)
        let keys_in_range: Vec<Vec<u8>> = self
            .backend_snapshot
            .range(begin.to_vec()..end.to_vec())
            .map(|(k, _)| k.clone())
            .chain(
                self.buffer
                    .range(begin.to_vec()..end.to_vec())
                    .map(|(k, _)| k.clone()),
            )
            .collect();
        for k in keys_in_range {
            self.check_limits(k.len())?;
            self.total_bytes += k.len();
            self.total_keys += 1;
            self.buffer.insert(k, None);
        }
        // Also record an empty sentinel so clear_range on empty range still counts as a write
        if self.buffer.is_empty() {
            // no-op
        }
        Ok(())
    }

    // ── Range ───────────────────────────────────────────────

    pub fn get_range(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.get_range_with_bounds(
            Bound::Included(prefix),
            Bound::Excluded(&fdb_exclusive_upper_bound(prefix).unwrap_or(vec![0xFF; 1])),
            None,
            false,
            false,
        )
    }

    pub fn get_range_snapshot(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_timeout()?;
        let mut out = Vec::new();
        let upper = fdb_exclusive_upper_bound(prefix);
        for (k, v) in &self.backend_snapshot {
            if k.starts_with(prefix) {
                if let Some(end) = &upper {
                    if k >= end {
                        continue;
                    }
                }
                out.push((k.clone(), v.clone()));
            }
        }
        Ok(out)
    }

    pub fn get_range_with_bounds(
        &self,
        begin: Bound<&[u8]>,
        end: Bound<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
        snapshot: bool,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.check_timeout()?;
        // Merge backend_snapshot + buffer (buffer shadows)
        let mut merged = BTreeMap::new();
        let source: &BTreeMap<Vec<u8>, Vec<u8>> = &self.backend_snapshot;
        for (k, v) in source {
            let in_begin = match &begin {
                Bound::Included(b) => k.as_slice() >= *b,
                Bound::Excluded(b) => k.as_slice() > *b,
                Bound::Unbounded => true,
            };
            let in_end = match &end {
                Bound::Included(b) => k.as_slice() <= *b,
                Bound::Excluded(b) => k.as_slice() < *b,
                Bound::Unbounded => true,
            };
            if in_begin && in_end {
                merged.insert(k.clone(), Some(v.clone()));
            }
        }
        if !snapshot {
            for (k, v) in &self.buffer {
                let in_begin = match &begin {
                    Bound::Included(b) => k.as_slice() >= *b,
                    Bound::Excluded(b) => k.as_slice() > *b,
                    Bound::Unbounded => true,
                };
                let in_end = match &end {
                    Bound::Included(b) => k.as_slice() <= *b,
                    Bound::Excluded(b) => k.as_slice() < *b,
                    Bound::Unbounded => true,
                };
                if in_begin && in_end {
                    merged.insert(k.clone(), v.clone());
                }
            }
        }
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect();
        if reverse {
            out.reverse();
        }
        if let Some(lim) = limit {
            out.truncate(lim);
        }
        Ok(out)
    }

    // ── Conflict ranges ─────────────────────────────────────

    pub fn add_read_conflict_range(&mut self, begin: Vec<u8>, end: Vec<u8>) -> Result<()> {
        self.check_limits(0)?;
        self.read_conflict_ranges.push((begin, end));
        Ok(())
    }

    pub fn add_write_conflict_range(&mut self, begin: Vec<u8>, end: Vec<u8>) -> Result<()> {
        self.check_limits(0)?;
        self.write_conflict_ranges.push((begin, end));
        Ok(())
    }

    pub fn add_conflict_range(&mut self, begin: Vec<u8>, end: Vec<u8>) -> Result<()> {
        self.add_read_conflict_range(begin.clone(), end.clone())?;
        self.add_write_conflict_range(begin, end)
    }

    // ── Versions ────────────────────────────────────────────

    pub fn read_version(&self) -> Result<u64> {
        self.check_timeout()?;
        Ok(self.read_version)
    }

    pub fn committed_version(&self) -> Result<u64> {
        self.check_timeout()?;
        Ok(self.read_version)
    }

    pub fn get_read_version(&self) -> Result<u64> {
        self.read_version()
    }

    pub(crate) fn require_exact_read_version(&mut self) {
        self.requires_exact_read_version = true;
    }

    // ── Versionstamp ────────────────────────────────────────

    pub fn set_versionstamped_key(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        validate_key(&key)?;
        validate_value(&value)?;
        if !key.windows(10).any(|w| w == [0xFF; 10] || w == [0u8; 10]) {
            return Err(KernelError::Backend(
                "versionstamped key must contain 10-byte placeholder".into(),
            ));
        }
        let add = key.len() + value.len();
        self.check_limits(add)?;
        self.total_bytes += add;
        self.total_keys += 1;
        self.versionstamped_writes.push(VersionstampedWrite {
            key,
            value,
            is_key: true,
        });
        Ok(())
    }

    pub fn set_versionstamped_value(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        validate_key(&key)?;
        // The four-byte offset suffix is client metadata and is not stored.
        if value.len().saturating_sub(4) > MAX_VALUE_BYTES {
            return Err(KernelError::ValueTooLarge {
                len: value.len().saturating_sub(4),
                limit: MAX_VALUE_BYTES,
            });
        }
        if !value.windows(10).any(|w| w == [0xFF; 10] || w == [0u8; 10]) {
            return Err(KernelError::Backend(
                "versionstamped value must contain 10-byte placeholder".into(),
            ));
        }
        let add = key.len() + value.len();
        self.check_limits(add)?;
        self.total_bytes += add;
        self.total_keys += 1;
        self.versionstamped_writes.push(VersionstampedWrite {
            key,
            value,
            is_key: false,
        });
        Ok(())
    }

    // ── Watches ─────────────────────────────────────────────

    pub fn watch(&mut self, key: &[u8]) -> Watch {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.watches.push((key.to_vec(), tx));
        Watch {
            key: key.to_vec(),
            rx,
        }
    }

    // ── Atomic ops ──────────────────────────────────────────

    pub fn atomic_add(&mut self, key: Vec<u8>, param: Vec<u8>) -> Result<()> {
        if param.len() != 8 {
            return Err(KernelError::AtomicOpViolation(
                "add param must be 8 bytes LE i64".into(),
            ));
        }
        self.check_limits(key.len() + param.len())?;
        self.total_bytes += key.len() + param.len();
        self.total_keys += 1;
        self.atomic_ops.push(AtomicOp {
            key,
            param,
            kind: AtomicOpKind::Add,
        });
        Ok(())
    }

    pub fn atomic_and(&mut self, key: Vec<u8>, param: Vec<u8>) -> Result<()> {
        self.check_limits(key.len() + param.len())?;
        self.total_bytes += key.len() + param.len();
        self.total_keys += 1;
        self.atomic_ops.push(AtomicOp {
            key,
            param,
            kind: AtomicOpKind::And,
        });
        Ok(())
    }

    pub fn atomic_or(&mut self, key: Vec<u8>, param: Vec<u8>) -> Result<()> {
        self.check_limits(key.len() + param.len())?;
        self.total_bytes += key.len() + param.len();
        self.total_keys += 1;
        self.atomic_ops.push(AtomicOp {
            key,
            param,
            kind: AtomicOpKind::Or,
        });
        Ok(())
    }

    pub fn atomic_max(&mut self, key: Vec<u8>, param: Vec<u8>) -> Result<()> {
        self.check_limits(key.len() + param.len())?;
        self.total_bytes += key.len() + param.len();
        self.total_keys += 1;
        self.atomic_ops.push(AtomicOp {
            key,
            param,
            kind: AtomicOpKind::Max,
        });
        Ok(())
    }

    pub fn atomic_min(&mut self, key: Vec<u8>, param: Vec<u8>) -> Result<()> {
        self.check_limits(key.len() + param.len())?;
        self.total_bytes += key.len() + param.len();
        self.total_keys += 1;
        self.atomic_ops.push(AtomicOp {
            key,
            param,
            kind: AtomicOpKind::Min,
        });
        Ok(())
    }

    pub fn atomic_append_if_fits(&mut self, key: Vec<u8>, param: Vec<u8>) -> Result<()> {
        self.check_limits(key.len() + param.len())?;
        self.total_bytes += key.len() + param.len();
        self.total_keys += 1;
        self.atomic_ops.push(AtomicOp {
            key,
            param,
            kind: AtomicOpKind::AppendIfFits,
        });
        Ok(())
    }

    // ── Snapshot flag ───────────────────────────────────────

    pub fn set_snapshot(&mut self, _snapshot: bool) {
        // Shim: snapshot reads are via get_snapshot/get_range_snapshot.
        // The flag is retained for API parity with the real binding.
    }
}

pub struct FdbClock {
    last: u64,
}

impl FdbClock {
    pub fn new() -> Self {
        Self { last: 0 }
    }

    pub fn advance_to(&mut self, version: u64) -> Result<()> {
        if version < self.last {
            return Err(KernelError::TsRegression {
                last: self.last,
                got: version,
            });
        }
        self.last = version;
        Ok(())
    }
}

impl Default for FdbClock {
    fn default() -> Self {
        Self::new()
    }
}

impl ClockPort for FdbClock {
    fn now(&self) -> u64 {
        self.last
    }
}

const FDB_ROWS: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("ldb_rows");
const FDB_META: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("ldb_meta");
const FDB_VERSION_KEY: &[u8] = b"version";

fn fdb_backend_err<E: std::fmt::Display>(e: E) -> KernelError {
    KernelError::Backend(e.to_string())
}

fn fdb_exclusive_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.pop() {
        if last < 0xFF {
            out.push(last + 1);
            return Some(out);
        }
    }
    None
}

pub(crate) fn validate_key(key: &[u8]) -> Result<()> {
    if key.len() > MAX_KEY_BYTES {
        return Err(KernelError::KeyTooLarge {
            len: key.len(),
            limit: MAX_KEY_BYTES,
        });
    }
    Ok(())
}

pub(crate) fn validate_value(value: &[u8]) -> Result<()> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(KernelError::ValueTooLarge {
            len: value.len(),
            limit: MAX_VALUE_BYTES,
        });
    }
    Ok(())
}

fn fdb_read_table(
    rtxn: &redb::ReadTransaction,
    def: redb::TableDefinition<&[u8], &[u8]>,
) -> Result<Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>> {
    match rtxn.open_table(def) {
        Ok(t) => Ok(Some(t)),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
        Err(e) => Err(fdb_backend_err(e)),
    }
}

fn fdb_committed_version(db: &redb::Database) -> Result<u64> {
    let rtxn = db.begin_read().map_err(fdb_backend_err)?;
    let Some(meta) = fdb_read_table(&rtxn, FDB_META)? else {
        return Ok(0);
    };
    let version = match meta.get(FDB_VERSION_KEY).map_err(fdb_backend_err)? {
        None => 0,
        Some(value) => {
            let bytes: [u8; 8] = value
                .value()
                .try_into()
                .map_err(|_| KernelError::Backend("committed version is not 8 bytes".into()))?;
            u64::from_be_bytes(bytes)
        }
    };
    Ok(version)
}

pub struct EmbeddedBackend {
    inner: crate::memkv::MemKv,
    size_limit_bytes: usize,
    committed: u64,
    db: Option<redb::Database>,
}

impl EmbeddedBackend {
    pub fn new_mem_shim() -> Self {
        Self {
            inner: crate::memkv::MemKv::default(),
            size_limit_bytes: MAX_TRANSACTION_BYTES,
            committed: 0,
            db: None,
        }
    }

    pub fn with_limit(size_limit_bytes: usize) -> Self {
        Self {
            inner: crate::memkv::MemKv::default(),
            size_limit_bytes,
            committed: 0,
            db: None,
        }
    }

    /// File-backed embedded store: one redb transaction per batch with the
    /// committed version in the same durability boundary.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = redb::Database::create(path).map_err(fdb_backend_err)?;
        let committed = fdb_committed_version(&db)?;
        Ok(Self {
            inner: crate::memkv::MemKv::default(),
            size_limit_bytes: MAX_TRANSACTION_BYTES,
            committed,
            db: Some(db),
        })
    }

    pub(crate) fn snapshot_map(&self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        if let Some(db) = &self.db {
            let rtxn = db.begin_read().map_err(fdb_backend_err)?;
            let Some(table) = fdb_read_table(&rtxn, FDB_ROWS)? else {
                return Ok(BTreeMap::new());
            };
            let mut out = BTreeMap::new();
            for row in table.iter().map_err(fdb_backend_err)? {
                let (key, value) = row.map_err(fdb_backend_err)?;
                out.insert(key.value().to_vec(), value.value().to_vec());
            }
            return Ok(out);
        }
        let mut out = BTreeMap::new();
        for (k, v) in self.inner.scan_prefix(&[])? {
            out.insert(k, v);
        }
        Ok(out)
    }

    pub(crate) fn get_point(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(key)
    }

    pub(crate) fn scan_range_limited(
        &self,
        start: &[u8],
        start_exclusive: bool,
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if let Some(db) = &self.db {
            let rtxn = db.begin_read().map_err(fdb_backend_err)?;
            let Some(table) = fdb_read_table(&rtxn, FDB_ROWS)? else {
                return Ok(Vec::new());
            };
            let start_bound = if start_exclusive {
                Bound::Excluded(&start)
            } else {
                Bound::Included(&start)
            };
            let end_bound = Bound::Excluded(&end);
            let mut out = Vec::new();
            for row in table
                .range::<&[u8]>((start_bound, end_bound))
                .map_err(fdb_backend_err)?
                .take(limit)
            {
                let (key, value) = row.map_err(fdb_backend_err)?;
                out.push((key.value().to_vec(), value.value().to_vec()));
            }
            return Ok(out);
        }
        let start_bound = if start_exclusive {
            Bound::Excluded(start)
        } else {
            Bound::Included(start)
        };
        Ok(self
            .inner
            .scan_range_limited(start_bound, Bound::Excluded(end), limit))
    }

    /// Applies data mutations and their committed version as one backend
    /// transaction. This is the only primitive used by `LogicalDb` commit;
    /// callers cannot accidentally split puts, deletes, and metadata.
    pub(crate) fn apply_transaction(
        &mut self,
        puts: &[(Vec<u8>, Vec<u8>)],
        deletes: &[Vec<u8>],
        version: u64,
    ) -> Result<()> {
        if version < self.committed {
            return Err(KernelError::TsRegression {
                last: self.committed,
                got: version,
            });
        }
        for (key, value) in puts {
            validate_key(key)?;
            validate_value(value)?;
        }
        for key in deletes {
            validate_key(key)?;
        }
        let put_bytes = puts.iter().try_fold(0usize, |total, (key, value)| {
            total.checked_add(key.len() + value.len()).ok_or({
                KernelError::TransactionTooLarge {
                    size: usize::MAX,
                    limit: self.size_limit_bytes,
                }
            })
        })?;
        let total = deletes.iter().try_fold(put_bytes, |total, key| {
            total
                .checked_add(key.len())
                .ok_or(KernelError::TransactionTooLarge {
                    size: usize::MAX,
                    limit: self.size_limit_bytes,
                })
        })?;
        if total > self.size_limit_bytes {
            return Err(KernelError::TransactionTooLarge {
                size: total,
                limit: self.size_limit_bytes,
            });
        }
        let key_count = puts.len().checked_add(deletes.len()).ok_or({
            KernelError::TransactionTooLarge {
                size: usize::MAX,
                limit: MAX_TRANSACTION_KEYS,
            }
        })?;
        if key_count.saturating_sub(MAX_TRANSACTION_KEYS) != 0 {
            return Err(KernelError::TransactionTooLarge {
                size: key_count,
                limit: MAX_TRANSACTION_KEYS,
            });
        }

        if let Some(db) = &self.db {
            let wtxn = db.begin_write().map_err(fdb_backend_err)?;
            {
                let mut rows = wtxn.open_table(FDB_ROWS).map_err(fdb_backend_err)?;
                for (key, value) in puts {
                    rows.insert(key.as_slice(), value.as_slice())
                        .map_err(fdb_backend_err)?;
                }
                for key in deletes {
                    rows.remove(key.as_slice()).map_err(fdb_backend_err)?;
                }
                let mut meta = wtxn.open_table(FDB_META).map_err(fdb_backend_err)?;
                meta.insert(FDB_VERSION_KEY, version.to_be_bytes().as_slice())
                    .map_err(fdb_backend_err)?;
            }
            wtxn.commit().map_err(fdb_backend_err)?;
        } else {
            // MemKv's point operations are infallible after the checks above.
            for (key, value) in puts {
                self.inner.put(key.clone(), value.clone())?;
            }
            for key in deletes {
                self.inner.delete(key)?;
            }
        }
        self.committed = self.committed.max(version);
        Ok(())
    }
}

impl KvPort for EmbeddedBackend {
    fn is_durable(&self) -> bool {
        true
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(db) = &self.db {
            let rtxn = db.begin_read().map_err(fdb_backend_err)?;
            match fdb_read_table(&rtxn, FDB_ROWS)? {
                None => Ok(None),
                Some(table) => match table.get(key).map_err(fdb_backend_err)? {
                    None => Ok(None),
                    Some(v) => Ok(Some(v.value().to_vec())),
                },
            }
        } else {
            self.inner.get(key)
        }
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let size = key.len() + value.len();
        if size > self.size_limit_bytes {
            return Err(KernelError::TransactionTooLarge {
                size,
                limit: self.size_limit_bytes,
            });
        }
        if let Some(db) = &self.db {
            let wtxn = db.begin_write().map_err(fdb_backend_err)?;
            {
                let mut rows = wtxn.open_table(FDB_ROWS).map_err(fdb_backend_err)?;
                rows.insert(key.as_slice(), value.as_slice())
                    .map_err(fdb_backend_err)?;
            }
            wtxn.commit().map_err(fdb_backend_err)?;
            return Ok(());
        }
        self.inner.put(key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if let Some(db) = &self.db {
            let wtxn = db.begin_write().map_err(fdb_backend_err)?;
            {
                let mut table = wtxn.open_table(FDB_ROWS).map_err(fdb_backend_err)?;
                table.remove(key).map_err(fdb_backend_err)?;
            }
            wtxn.commit().map_err(fdb_backend_err)?;
            return Ok(());
        }
        self.inner.delete(key)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if let Some(db) = &self.db {
            let rtxn = db.begin_read().map_err(fdb_backend_err)?;
            let Some(table) = fdb_read_table(&rtxn, FDB_ROWS)? else {
                return Ok(Vec::new());
            };
            let mut out = Vec::new();
            let upper = fdb_exclusive_upper_bound(prefix);
            let range: (Bound<&&[u8]>, Bound<&&[u8]>) = match &upper {
                Some(end) => (Bound::Included(&prefix), Bound::Excluded(&end.as_slice())),
                None => (Bound::Included(&prefix), Bound::Unbounded),
            };
            for row in table.range::<&[u8]>(range).map_err(fdb_backend_err)? {
                let (k, v) = row.map_err(fdb_backend_err)?;
                out.push((k.value().to_vec(), v.value().to_vec()));
            }
            return Ok(out);
        }
        self.inner.scan_prefix(prefix)
    }

    fn apply_batch(&mut self, entries: &[(Vec<u8>, Vec<u8>)], version: u64) -> Result<()> {
        self.apply_transaction(entries, &[], version)
    }
}

impl DurableKv for EmbeddedBackend {
    fn committed_version(&self) -> Result<u64> {
        if let Some(db) = &self.db {
            fdb_committed_version(db)
        } else {
            Ok(self.committed)
        }
    }
}

/// Compatibility alias for the pre-ADR-003 name. This is a redb-backed
/// embedded provider, not a FoundationDB client.
pub type FdbBackend = EmbeddedBackend;

#[cfg(test)]
mod tests {
    use super::*;

    fn durable_test_path(tag: &str) -> std::path::PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "logical-db-fdb-{tag}-{}-{nonce}.redb",
            std::process::id()
        ))
    }

    #[test]
    fn fdb_config_defaults_are_pinned() {
        let cfg = FdbConfig::default();
        assert_eq!(MAX_TRANSACTION_BYTES, 10_000_000);
        assert_eq!(MAX_TRANSACTION_KEYS, 100_000);
        assert_eq!(cfg.transaction_size_limit_bytes, 10_000_000);
        assert_eq!(cfg.transaction_timeout_ms, 5_000);
        assert!(cfg.cluster_file.is_none());
        assert_eq!(fdb_exclusive_upper_bound(&[1, 0xFF]), Some(vec![2]));
        assert_eq!(fdb_exclusive_upper_bound(&[0xFF]), None);
    }

    #[test]
    fn embedded_open_rejects_outboard_configuration_and_preserves_options() {
        let configured = FdbConfig {
            cluster_file: None,
            transaction_timeout_ms: 17,
            transaction_size_limit_bytes: 23,
        };
        let db = LogicalDb::open(configured).unwrap();
        assert_eq!(db.config().transaction_timeout_ms, 17);
        assert_eq!(db.config().transaction_size_limit_bytes, 23);
        assert!(validate_database_options(&FdbConfig {
            transaction_timeout_ms: i32::MAX as u64,
            ..FdbConfig::default()
        })
        .is_ok());

        let outboard = FdbConfig {
            cluster_file: Some("fdb.cluster".into()),
            ..FdbConfig::default()
        };
        assert!(matches!(
            LogicalDb::open(outboard),
            Err(KernelError::Backend(message)) if message.contains("native FoundationDB")
        ));

        for invalid in [
            FdbConfig {
                transaction_timeout_ms: 0,
                ..FdbConfig::default()
            },
            FdbConfig {
                transaction_timeout_ms: i32::MAX as u64 + 1,
                ..FdbConfig::default()
            },
            FdbConfig {
                transaction_size_limit_bytes: 0,
                ..FdbConfig::default()
            },
        ] {
            assert!(matches!(
                LogicalDb::open(invalid),
                Err(KernelError::ConfigurationInvalid(_))
            ));
        }
        assert!(matches!(
            LogicalDb::open(FdbConfig {
                transaction_size_limit_bytes: MAX_TRANSACTION_BYTES + 1,
                ..FdbConfig::default()
            }),
            Err(KernelError::TransactionTooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn watch_identity_and_cancellation_are_observable() {
        let mut tx = FdbTransaction::new(MAX_TRANSACTION_BYTES, 5_000);
        let watch = tx.watch(b"identity");
        assert_eq!(watch.key(), b"identity");
        drop(tx);
        assert!(matches!(
            watch.wait().await,
            Err(KernelError::WatchCancelled)
        ));
    }

    #[test]
    fn every_atomic_operation_has_exact_fdb_shim_semantics() {
        fn op(kind: AtomicOpKind, param: Vec<u8>) -> AtomicOp {
            AtomicOp {
                key: b"k".to_vec(),
                param,
                kind,
            }
        }

        let add = apply_atomic_op(
            &op(AtomicOpKind::Add, 9_i64.to_le_bytes().to_vec()),
            Some(&(-4_i64).to_le_bytes()),
        )
        .unwrap();
        assert_eq!(i64::from_le_bytes(add.try_into().unwrap()), 5);
        assert!(matches!(
            apply_atomic_op(&op(AtomicOpKind::Add, vec![1]), None),
            Err(KernelError::AtomicOpViolation(_))
        ));
        assert!(matches!(
            apply_atomic_op(
                &op(AtomicOpKind::Add, 1_i64.to_le_bytes().to_vec()),
                Some(&[1])
            ),
            Err(KernelError::AtomicOpViolation(_))
        ));

        assert_eq!(
            apply_atomic_op(
                &op(AtomicOpKind::And, vec![0b1100, 0b0011]),
                Some(&[0b1010])
            )
            .unwrap(),
            vec![0b1000, 0]
        );
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::Or, vec![0b1100, 0b0011]), Some(&[0b1010])).unwrap(),
            vec![0b1110, 0b0011]
        );

        let numeric_max = apply_atomic_op(
            &op(AtomicOpKind::Max, 256_i64.to_le_bytes().to_vec()),
            Some(&255_i64.to_le_bytes()),
        )
        .unwrap();
        assert_eq!(i64::from_le_bytes(numeric_max.try_into().unwrap()), 256);
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::Max, b"b".to_vec()), Some(b"z")).unwrap(),
            b"z"
        );
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::Max, b"z".to_vec()), Some(b"b")).unwrap(),
            b"z"
        );
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::Max, vec![9; 8]), Some(b"short")).unwrap(),
            b"short"
        );
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::Max, b"new".to_vec()), None).unwrap(),
            b"new"
        );

        let numeric_min = apply_atomic_op(
            &op(AtomicOpKind::Min, (-3_i64).to_le_bytes().to_vec()),
            Some(&2_i64.to_le_bytes()),
        )
        .unwrap();
        assert_eq!(i64::from_le_bytes(numeric_min.try_into().unwrap()), -3);
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::Min, b"z".to_vec()), Some(b"b")).unwrap(),
            b"b"
        );
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::Min, b"b".to_vec()), Some(b"z")).unwrap(),
            b"b"
        );
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::Min, vec![9; 8]), Some(b"short")).unwrap(),
            vec![9; 8]
        );
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::Min, b"new".to_vec()), None).unwrap(),
            b"new"
        );

        assert_eq!(
            apply_atomic_op(
                &op(AtomicOpKind::AppendIfFits, b"tail".to_vec()),
                Some(b"head")
            )
            .unwrap(),
            b"headtail"
        );
        let full = vec![0; MAX_TRANSACTION_BYTES];
        assert_eq!(
            apply_atomic_op(&op(AtomicOpKind::AppendIfFits, Vec::new()), Some(&full))
                .unwrap()
                .len(),
            MAX_TRANSACTION_BYTES
        );
        assert!(matches!(
            apply_atomic_op(&op(AtomicOpKind::AppendIfFits, vec![1]), Some(&full)),
            Err(KernelError::TransactionTooLarge {
                size: 10_000_001,
                limit: 10_000_000
            })
        ));
    }

    #[test]
    fn transaction_builders_account_exact_bytes_keys_and_effects() {
        let mut tx = FdbTransaction::new(1_000, 5_000);
        tx.set(b"ab".to_vec(), b"cde".to_vec()).unwrap();
        assert_eq!((tx.total_bytes, tx.total_keys), (5, 1));
        tx.clear(b"four").unwrap();
        assert_eq!((tx.total_bytes, tx.total_keys), (9, 2));
        tx.atomic_add(b"a".to_vec(), 1_i64.to_le_bytes().to_vec())
            .unwrap();
        tx.atomic_and(b"bb".to_vec(), vec![1, 2, 3]).unwrap();
        tx.atomic_or(b"ccc".to_vec(), vec![1, 2]).unwrap();
        tx.atomic_max(b"dddd".to_vec(), vec![1]).unwrap();
        tx.atomic_min(b"eeeee".to_vec(), vec![1, 2]).unwrap();
        tx.atomic_append_if_fits(b"ffffff".to_vec(), vec![1, 2, 3])
            .unwrap();
        assert_eq!((tx.total_bytes, tx.total_keys), (49, 8));
        assert_eq!(tx.atomic_ops.len(), 6);
        assert_eq!(tx.atomic_ops[0].kind, AtomicOpKind::Add);
        assert_eq!(tx.atomic_ops[1].kind, AtomicOpKind::And);
        assert_eq!(tx.atomic_ops[2].kind, AtomicOpKind::Or);
        assert_eq!(tx.atomic_ops[3].kind, AtomicOpKind::Max);
        assert_eq!(tx.atomic_ops[4].kind, AtomicOpKind::Min);
        assert_eq!(tx.atomic_ops[5].kind, AtomicOpKind::AppendIfFits);

        let mut exact = FdbTransaction::new(5, 5_000);
        exact.set(b"ab".to_vec(), b"cde".to_vec()).unwrap();
        assert!(matches!(
            exact.clear(b"x"),
            Err(KernelError::TransactionTooLarge { size: 6, limit: 5 })
        ));
        let mut keys = FdbTransaction::new(MAX_TRANSACTION_BYTES, 5_000);
        keys.total_keys = MAX_TRANSACTION_KEYS - 1;
        keys.check_limits(0).unwrap();
        keys.total_keys = MAX_TRANSACTION_KEYS;
        assert!(matches!(
            keys.check_limits(0),
            Err(KernelError::TransactionTooLarge {
                size: 100_001,
                limit: 100_000
            })
        ));

        let mut timed_out = FdbTransaction::new(100, 1);
        timed_out.start = std::time::Instant::now() - std::time::Duration::from_millis(5);
        assert!(matches!(
            timed_out.set(vec![1], vec![2]),
            Err(KernelError::TransactionTimedOut { limit_ms: 1, .. })
        ));
        assert!(matches!(
            timed_out.check_timeout(),
            Err(KernelError::TransactionTimedOut { limit_ms: 1, .. })
        ));

        let mut provider_limits = FdbTransaction::new(MAX_TRANSACTION_BYTES, 5_000);
        provider_limits
            .set(vec![0; MAX_KEY_BYTES], vec![0; MAX_VALUE_BYTES])
            .unwrap();
        assert!(matches!(
            provider_limits.set(vec![0; MAX_KEY_BYTES + 1], Vec::new()),
            Err(KernelError::KeyTooLarge { .. })
        ));
        assert!(matches!(
            provider_limits.set(Vec::new(), vec![0; MAX_VALUE_BYTES + 1]),
            Err(KernelError::ValueTooLarge { .. })
        ));

        let mut ranges = FdbTransaction::new_with_snapshot(
            100,
            5_000,
            0,
            BTreeMap::from([
                (b"aa".to_vec(), b"1".to_vec()),
                (b"bb".to_vec(), b"2".to_vec()),
            ]),
        );
        ranges.clear_range(b"a", b"c").unwrap();
        assert_eq!((ranges.total_bytes, ranges.total_keys), (4, 2));
        assert!(ranges.clear_range(b"same", b"same").is_ok());
        assert!(matches!(
            ranges.clear_range(b"z", b"a"),
            Err(KernelError::Backend(_))
        ));

        let mut conflicts = FdbTransaction::new(100, 5_000);
        conflicts
            .add_write_conflict_range(b"a".to_vec(), b"b".to_vec())
            .unwrap();
        assert_eq!(conflicts.write_conflict_ranges.len(), 1);
        conflicts
            .add_conflict_range(b"c".to_vec(), b"d".to_vec())
            .unwrap();
        assert_eq!(conflicts.read_conflict_ranges.len(), 1);
        assert_eq!(conflicts.write_conflict_ranges.len(), 2);

        let mut exact_add = FdbTransaction::new(10, 5_000);
        exact_add
            .atomic_add(b"kk".to_vec(), 1_i64.to_le_bytes().to_vec())
            .unwrap();
        let mut short_add = FdbTransaction::new(9, 5_000);
        assert!(matches!(
            short_add.atomic_add(b"kk".to_vec(), 1_i64.to_le_bytes().to_vec()),
            Err(KernelError::TransactionTooLarge { .. })
        ));

        type AtomicBuilder = fn(&mut FdbTransaction, Vec<u8>, Vec<u8>) -> Result<()>;
        let builders: [AtomicBuilder; 5] = [
            FdbTransaction::atomic_and,
            FdbTransaction::atomic_or,
            FdbTransaction::atomic_max,
            FdbTransaction::atomic_min,
            FdbTransaction::atomic_append_if_fits,
        ];
        for build in builders {
            let mut exact_atomic = FdbTransaction::new(5, 5_000);
            build(&mut exact_atomic, b"kk".to_vec(), vec![1, 2, 3]).unwrap();
            let mut short_atomic = FdbTransaction::new(4, 5_000);
            assert!(matches!(
                build(&mut short_atomic, b"kk".to_vec(), vec![1, 2, 3]),
                Err(KernelError::TransactionTooLarge { .. })
            ));
        }
    }

    #[test]
    fn range_views_honor_every_bound_snapshot_direction_and_limit() {
        let snapshot = BTreeMap::from([
            (b"a".to_vec(), b"A".to_vec()),
            (b"b".to_vec(), b"B".to_vec()),
            (b"c".to_vec(), b"C".to_vec()),
            (b"d".to_vec(), b"D".to_vec()),
        ]);
        let mut tx = FdbTransaction::new_with_snapshot(1_000, 5_000, 7, snapshot);
        tx.clear(b"b").unwrap();
        tx.set(b"c".to_vec(), b"changed".to_vec()).unwrap();

        assert_eq!(tx.read_version().unwrap(), 7);
        assert_eq!(tx.committed_version().unwrap(), 7);
        assert_eq!(tx.get_read_version().unwrap(), 7);
        assert_eq!(
            tx.get_range_snapshot(b"b").unwrap(),
            vec![(b"b".to_vec(), b"B".to_vec())]
        );
        assert_eq!(
            tx.get_range_with_bounds(
                Bound::Excluded(b"a"),
                Bound::Included(b"d"),
                Some(2),
                true,
                false,
            )
            .unwrap(),
            vec![
                (b"d".to_vec(), b"D".to_vec()),
                (b"c".to_vec(), b"changed".to_vec())
            ]
        );
        assert_eq!(
            tx.get_range_with_bounds(
                Bound::Included(b"b"),
                Bound::Excluded(b"d"),
                None,
                false,
                true,
            )
            .unwrap(),
            vec![
                (b"b".to_vec(), b"B".to_vec()),
                (b"c".to_vec(), b"C".to_vec())
            ]
        );
        assert_eq!(
            tx.get_range_with_bounds(
                Bound::Included(b"a"),
                Bound::Included(b"a"),
                None,
                false,
                false,
            )
            .unwrap(),
            vec![(b"a".to_vec(), b"A".to_vec())]
        );

        let mut buffer_only = FdbTransaction::new_with_snapshot(100, 5_000, 0, BTreeMap::new());
        buffer_only.set(b"b".to_vec(), b"B".to_vec()).unwrap();
        buffer_only.set(b"c".to_vec(), b"C".to_vec()).unwrap();
        assert_eq!(
            buffer_only
                .get_range_with_bounds(
                    Bound::Included(b"b"),
                    Bound::Included(b"b"),
                    None,
                    false,
                    false,
                )
                .unwrap(),
            vec![(b"b".to_vec(), b"B".to_vec())]
        );
        assert_eq!(
            buffer_only
                .get_range_with_bounds(
                    Bound::Excluded(b"b"),
                    Bound::Included(b"c"),
                    None,
                    false,
                    false,
                )
                .unwrap(),
            vec![(b"c".to_vec(), b"C".to_vec())]
        );
        assert_eq!(
            buffer_only
                .get_range_with_bounds(
                    Bound::Included(b"b"),
                    Bound::Excluded(b"c"),
                    None,
                    false,
                    false,
                )
                .unwrap(),
            vec![(b"b".to_vec(), b"B".to_vec())]
        );
        assert_eq!(
            tx.get_range_with_bounds(
                Bound::Excluded(b"a"),
                Bound::Included(b"b"),
                None,
                false,
                true,
            )
            .unwrap(),
            vec![(b"b".to_vec(), b"B".to_vec())]
        );
        assert_eq!(
            tx.get_range_with_bounds(
                Bound::Included(b"a"),
                Bound::Excluded(b"b"),
                None,
                false,
                true,
            )
            .unwrap(),
            vec![(b"a".to_vec(), b"A".to_vec())]
        );
    }

    #[test]
    fn versionstamp_codec_and_transaction_flags_are_exact() {
        let stamp = versionstamp_bytes(0x0102_0304_0506_0708);
        assert_eq!(stamp, vec![1, 2, 3, 4, 5, 6, 7, 8, 0, 0]);
        let key_write = VersionstampedWrite {
            key: [b"key".as_slice(), &[0xFF; 10], b"tail"].concat(),
            value: b"value".to_vec(),
            is_key: true,
        };
        let (key, value) = resolve_versionstamp(key_write, 9).unwrap();
        assert_eq!(
            key,
            [b"key".as_slice(), &versionstamp_bytes(9), b"tail"].concat()
        );
        assert_eq!(value, b"value");

        let value_write = VersionstampedWrite {
            key: b"key".to_vec(),
            value: [b"value".as_slice(), &[0; 10]].concat(),
            is_key: false,
        };
        let (_, value) = resolve_versionstamp(value_write, 10).unwrap();
        assert_eq!(
            value,
            [b"value".as_slice(), &versionstamp_bytes(10)].concat()
        );
        let (zero_key, _) = resolve_versionstamp(
            VersionstampedWrite {
                key: [b"prefix".as_slice(), &[0; 10]].concat(),
                value: vec![],
                is_key: true,
            },
            11,
        )
        .unwrap();
        assert_eq!(
            zero_key,
            [b"prefix".as_slice(), &versionstamp_bytes(11)].concat()
        );
        let (_, ff_value) = resolve_versionstamp(
            VersionstampedWrite {
                key: vec![],
                value: [b"prefix".as_slice(), &[0xFF; 10], b"tail"].concat(),
                is_key: false,
            },
            12,
        )
        .unwrap();
        assert_eq!(
            ff_value,
            [b"prefix".as_slice(), &versionstamp_bytes(12), b"tail"].concat()
        );
        for is_key in [true, false] {
            assert!(matches!(
                resolve_versionstamp(
                    VersionstampedWrite {
                        key: b"key".to_vec(),
                        value: b"value".to_vec(),
                        is_key,
                    },
                    1,
                ),
                Err(KernelError::Backend(_))
            ));
        }

        let mut tx = FdbTransaction::new(100, 5_000);
        tx.require_exact_read_version();
        assert!(tx.requires_exact_read_version);
        assert!(tx
            .set_versionstamped_key([b"k".as_slice(), &[0xFF; 10]].concat(), vec![1])
            .is_ok());
        assert!(tx
            .set_versionstamped_value(b"v".to_vec(), [b"x".as_slice(), &[0; 10]].concat())
            .is_ok());
        assert_eq!((tx.total_bytes, tx.total_keys), (24, 2));
        assert_eq!(tx.versionstamped_writes.len(), 2);
        assert!(matches!(
            tx.set_versionstamped_key([b"bad".as_slice(), &[7; 10]].concat(), vec![]),
            Err(KernelError::Backend(_))
        ));
        assert!(matches!(
            tx.set_versionstamped_value(b"bad".to_vec(), vec![7; 10]),
            Err(KernelError::Backend(_))
        ));

        let mut exact_value = vec![1; MAX_VALUE_BYTES + 4];
        exact_value[..10].fill(0);
        let mut exact_tx = FdbTransaction::new(MAX_TRANSACTION_BYTES, 5_000);
        exact_tx
            .set_versionstamped_value(b"exact".to_vec(), exact_value.clone())
            .unwrap();
        exact_value.push(1);
        assert!(matches!(
            exact_tx.set_versionstamped_value(b"over".to_vec(), exact_value),
            Err(KernelError::ValueTooLarge {
                len,
                limit: MAX_VALUE_BYTES
            }) if len == MAX_VALUE_BYTES + 1
        ));
    }

    #[test]
    fn embedded_backend_port_and_batch_boundaries_are_exact() {
        let mut backend = EmbeddedBackend::with_limit(5);
        assert!(backend.is_durable());
        backend.put(b"ab".to_vec(), b"cde".to_vec()).unwrap();
        assert_eq!(
            backend.scan_prefix(b"a").unwrap(),
            vec![(b"ab".to_vec(), b"cde".to_vec())]
        );
        assert!(matches!(
            backend.put(b"abc".to_vec(), b"def".to_vec()),
            Err(KernelError::TransactionTooLarge { size: 6, limit: 5 })
        ));

        let mut batch = EmbeddedBackend::with_limit(5);
        batch
            .apply_transaction(&[(b"ab".to_vec(), b"cde".to_vec())], &[], 1)
            .unwrap();
        assert_eq!(batch.committed_version().unwrap(), 1);
        batch.apply_transaction(&[], &[], 1).unwrap();
        assert_eq!(batch.committed_version().unwrap(), 1);
        assert!(matches!(
            batch.apply_transaction(&[(b"abc".to_vec(), b"def".to_vec())], &[], 2),
            Err(KernelError::TransactionTooLarge { size: 6, limit: 5 })
        ));
        assert!(matches!(
            batch.apply_transaction(&[], &[], 0),
            Err(KernelError::TsRegression { last: 1, got: 0 })
        ));
    }

    #[tokio::test]
    async fn exact_read_version_accepts_equal_and_rejects_stale() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let (version, snapshot) = db.versioned_snapshot().unwrap();
        let mut exact = FdbTransaction::new_with_snapshot(
            MAX_TRANSACTION_BYTES,
            DEFAULT_TRANSACTION_TIMEOUT_MS,
            version,
            snapshot.clone(),
        );
        exact.require_exact_read_version();
        exact
            .set(b"exact-version".to_vec(), b"one".to_vec())
            .unwrap();

        let mut stale = FdbTransaction::new_with_snapshot(
            MAX_TRANSACTION_BYTES,
            DEFAULT_TRANSACTION_TIMEOUT_MS,
            version,
            snapshot,
        );
        stale.require_exact_read_version();
        stale
            .set(b"stale-version".to_vec(), b"two".to_vec())
            .unwrap();

        assert_eq!(db.try_commit(exact).await.unwrap(), version + 1);
        assert!(matches!(
            db.try_commit(stale).await,
            Err(KernelError::StaleJustification {
                expected,
                got,
            }) if expected == version + 1 && got == version
        ));
    }

    #[test]
    fn fdb_transaction_enforces_size_limit_before_binding() {
        let mut tx = FdbTransaction::new(100, 5_000);
        let big = vec![0u8; 200];
        let err = tx.set(b"k".to_vec(), big).unwrap_err();
        match err {
            KernelError::TransactionTooLarge { size, limit } => {
                assert!(size > limit);
                assert_eq!(limit, 100);
            }
            _ => panic!("expected TransactionTooLarge, got {err:?}"),
        }
    }

    #[test]
    fn fdb_backend_shim_delegates_to_memkv() {
        let mut be = FdbBackend::new_mem_shim();
        be.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        assert_eq!(be.get(b"a").unwrap(), Some(b"1".to_vec()));
        be.delete(b"a").unwrap();
        assert_eq!(be.get(b"a").unwrap(), None);
    }

    #[test]
    fn fdb_clock_holds_monotonic_invariant() {
        let mut c = FdbClock::new();
        assert_eq!(c.now(), 0);
        c.advance_to(5).unwrap();
        assert_eq!(c.now(), 5);
        c.advance_to(5).unwrap();
        assert_eq!(c.now(), 5);
        let err = c.advance_to(3).unwrap_err();
        match err {
            KernelError::TsRegression { last, got } => {
                assert_eq!(last, 5);
                assert_eq!(got, 3);
            }
            _ => panic!("expected TsRegression"),
        }
    }

    #[tokio::test]
    async fn txn_limit_is_enforced() {
        let db = LogicalDb::open(FdbConfig {
            cluster_file: None,
            transaction_timeout_ms: 5_000,
            transaction_size_limit_bytes: 50,
        })
        .unwrap();
        let result = db
            .transact(|tx| {
                let big = vec![0u8; 100];
                tx.set(b"oversized".to_vec(), big)?;
                Ok(())
            })
            .await;
        match result {
            Err(KernelError::TransactionTooLarge { size, limit }) => {
                assert!(size > limit);
                assert_eq!(limit, 50);
            }
            Ok(_) => panic!("expected TransactionTooLarge"),
            Err(e) => panic!("wrong error: {e:?}"),
        }
    }

    #[tokio::test]
    async fn transact_happy_path_round_trips() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let v = db
            .transact(|tx| {
                tx.set(b"k".to_vec(), b"v".to_vec())?;
                let got = tx.get(b"k")?;
                Ok(got)
            })
            .await
            .unwrap();
        assert_eq!(v, Some(b"v".to_vec()));
        // committed read outside tx
        let v2 = db.transact(|tx| tx.get(b"k")).await.unwrap();
        assert_eq!(v2, Some(b"v".to_vec()));
    }

    #[tokio::test]
    async fn transact_retries_closure_and_commit_conflicts_only_within_budget() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let mut closure_attempts = 0;
        let value = db
            .transact(|_| {
                closure_attempts += 1;
                if closure_attempts < 3 {
                    Err(KernelError::TransactionConflict)
                } else {
                    Ok(41)
                }
            })
            .await
            .unwrap();
        assert_eq!(value, 41);
        assert_eq!(closure_attempts, 3);

        let mut exhausted_attempts = 0;
        let exhausted = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            db.transact(|_| -> Result<()> {
                exhausted_attempts += 1;
                Err(KernelError::TransactionConflict)
            }),
        )
        .await
        .expect("bounded closure-conflict retries must terminate");
        assert!(matches!(exhausted, Err(KernelError::TransactionConflict)));
        assert_eq!(exhausted_attempts, MAX_RETRY_ATTEMPTS + 1);

        let commit_db = LogicalDb::open(FdbConfig::default()).unwrap();
        let mut commit_attempts = 0;
        commit_db
            .transact(|tx| {
                commit_attempts += 1;
                tx.add_read_conflict_range(b"conflict".to_vec(), b"conflicu".to_vec())?;
                if commit_attempts == 1 {
                    commit_db.backend.lock().unwrap().apply_transaction(
                        &[(b"conflict".to_vec(), b"outside".to_vec())],
                        &[],
                        1,
                    )?;
                }
                tx.set(b"result".to_vec(), b"inside".to_vec())
            })
            .await
            .unwrap();
        assert_eq!(commit_attempts, 2);
        assert_eq!(commit_db.committed_version().unwrap(), 2);

        let exhausted_commit_db = LogicalDb::open(FdbConfig::default()).unwrap();
        let mut exhausted_commit_attempts = 0_u64;
        let exhausted_commit = tokio::time::timeout(
            std::time::Duration::from_millis(500),
            exhausted_commit_db.transact(|tx| {
                exhausted_commit_attempts += 1;
                tx.add_read_conflict_range(b"conflict".to_vec(), b"conflicu".to_vec())?;
                let next = exhausted_commit_db
                    .committed_version()?
                    .checked_add(1)
                    .unwrap();
                exhausted_commit_db
                    .backend
                    .lock()
                    .unwrap()
                    .apply_transaction(
                        &[(
                            b"conflict".to_vec(),
                            exhausted_commit_attempts.to_be_bytes().to_vec(),
                        )],
                        &[],
                        next,
                    )?;
                tx.set(b"result".to_vec(), b"never".to_vec())
            }),
        )
        .await
        .expect("bounded commit-conflict retries must terminate");
        assert!(matches!(
            exhausted_commit,
            Err(KernelError::TransactionConflict)
        ));
        assert_eq!(exhausted_commit_attempts as usize, MAX_RETRY_ATTEMPTS + 1);
    }

    #[tokio::test]
    async fn get_range_is_ordered_and_prefix_scoped() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        db.transact(|tx| {
            tx.set(b"a\x01".to_vec(), b"1".to_vec())?;
            tx.set(b"a\x02".to_vec(), b"2".to_vec())?;
            tx.set(b"b\x01".to_vec(), b"3".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();
        let range = db.transact(|tx| tx.get_range(b"a")).await.unwrap();
        assert_eq!(range.len(), 2);
        assert!(range[0].0 < range[1].0);
        assert!(range.iter().all(|(k, _)| k.starts_with(b"a")));
    }

    #[tokio::test]
    async fn snapshot_read_does_not_see_uncommitted_buffer() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        db.transact(|tx| {
            tx.set(b"k".to_vec(), b"committed".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();
        let snap = db
            .transact(|tx| {
                tx.set(b"k".to_vec(), b"staged".to_vec())?;
                let snap_val = tx.get_snapshot(b"k")?;
                let cur_val = tx.get(b"k")?;
                Ok((snap_val, cur_val))
            })
            .await
            .unwrap();
        assert_eq!(snap.0, Some(b"committed".to_vec()));
        assert_eq!(snap.1, Some(b"staged".to_vec()));
    }

    #[tokio::test]
    async fn conflict_range_is_tracked_and_retry_succeeds() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        db.transact(|tx| {
            tx.set(b"conflict-key".to_vec(), b"v0".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();
        let res: Result<()> = db
            .transact(|tx| {
                tx.add_read_conflict_range(b"conflict-key".to_vec(), b"conflict-kez".to_vec())?;
                let _ = tx.get(b"conflict-key")?;
                tx.set(b"conflict-key2".to_vec(), b"v2".to_vec())?;
                Ok(())
            })
            .await;
        assert!(res.is_ok());
        // Verify the write landed
        let v = db.transact(|tx| tx.get(b"conflict-key2")).await.unwrap();
        assert_eq!(v, Some(b"v2".to_vec()));
    }

    #[tokio::test]
    async fn versionstamped_key_embeds_committed_version() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let placeholder = vec![0xFF; 10];
        let mut key = b"vs-key-".to_vec();
        key.extend_from_slice(&placeholder);
        db.transact(|tx| {
            tx.set_versionstamped_key(key.clone(), b"value".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();
        let got = db
            .transact(|tx| {
                let range = tx.get_range(b"vs-key-")?;
                Ok(range)
            })
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        assert_ne!(got[0].0, key);
        assert!(got[0].0.ends_with(&[0, 0]) || got[0].0.len() == key.len());
    }

    #[tokio::test]
    async fn atomic_add_is_retry_safe_and_ordered() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        db.transact(|tx| {
            tx.set(b"counter".to_vec(), 0i64.to_le_bytes().to_vec())?;
            Ok(())
        })
        .await
        .unwrap();
        for _ in 0..3 {
            db.transact(|tx| {
                tx.atomic_add(b"counter".to_vec(), 1i64.to_le_bytes().to_vec())?;
                Ok(())
            })
            .await
            .unwrap();
        }
        let v = db.transact(|tx| tx.get(b"counter")).await.unwrap().unwrap();
        assert_eq!(i64::from_le_bytes(v.try_into().unwrap()), 3);
    }

    #[tokio::test]
    async fn watch_fires_on_commit() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let watch = db
            .transact(|tx| {
                let w = tx.watch(b"watched-key");
                Ok(w)
            })
            .await
            .unwrap();
        // Commit a change to watched key
        db.transact(|tx| {
            tx.set(b"watched-key".to_vec(), b"new".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();
        // Watch should fire
        let fired = tokio::time::timeout(std::time::Duration::from_millis(200), watch.wait()).await;
        assert!(fired.is_ok(), "watch must fire after commit");
    }

    #[tokio::test]
    async fn clear_range_removes_prefix() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        db.transact(|tx| {
            tx.set(b"a:1".to_vec(), b"1".to_vec())?;
            tx.set(b"a:2".to_vec(), b"2".to_vec())?;
            tx.set(b"b:1".to_vec(), b"3".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();
        db.transact(|tx| {
            tx.clear_range(b"a:", b"a;")?;
            Ok(())
        })
        .await
        .unwrap();
        let remaining = db.transact(|tx| tx.get_range(b"")).await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, b"b:1".to_vec());
    }

    #[test]
    fn failed_mixed_put_delete_leaves_file_and_version_unchanged() {
        let path = durable_test_path("mixed-failure");
        let mut backend = FdbBackend::open(&path).unwrap();
        backend
            .apply_transaction(&[(b"old".to_vec(), b"value".to_vec())], &[], 1)
            .unwrap();

        backend.size_limit_bytes = 4;
        let error = backend
            .apply_transaction(
                &[(b"new".to_vec(), b"value".to_vec())],
                &[b"old".to_vec()],
                2,
            )
            .unwrap_err();
        assert!(matches!(error, KernelError::TransactionTooLarge { .. }));
        drop(backend);

        let reopened = FdbBackend::open(&path).unwrap();
        assert_eq!(reopened.get(b"old").unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.get(b"new").unwrap(), None);
        assert_eq!(reopened.committed_version().unwrap(), 1);
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn regressing_batch_version_preserves_file_state_across_restart() {
        let path = durable_test_path("version-regression");
        let mut backend = FdbBackend::open(&path).unwrap();
        backend
            .apply_batch(&[(b"stable".to_vec(), b"v5".to_vec())], 5)
            .unwrap();

        let error = backend
            .apply_batch(&[(b"must-not-land".to_vec(), b"v3".to_vec())], 3)
            .unwrap_err();
        assert!(matches!(
            error,
            KernelError::TsRegression { last: 5, got: 3 }
        ));
        drop(backend);

        let reopened = FdbBackend::open(&path).unwrap();
        assert_eq!(reopened.committed_version().unwrap(), 5);
        assert_eq!(reopened.get(b"stable").unwrap(), Some(b"v5".to_vec()));
        assert_eq!(reopened.get(b"must-not-land").unwrap(), None);
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn mixed_commit_restart_preserves_exact_state_and_version() {
        let path = durable_test_path("mixed-restart");
        {
            let db = LogicalDb::open_at(&path, FdbConfig::default()).unwrap();
            db.transact(|tx| {
                tx.set(b"delete-me".to_vec(), b"old".to_vec())?;
                tx.set(b"keep".to_vec(), b"stable".to_vec())?;
                Ok(())
            })
            .await
            .unwrap();
            db.transact(|tx| {
                tx.clear(b"delete-me")?;
                tx.set(b"new".to_vec(), b"committed".to_vec())?;
                Ok(())
            })
            .await
            .unwrap();
            assert_eq!(db.committed_version().unwrap(), 2);
        }

        let reopened = LogicalDb::open_at(&path, FdbConfig::default()).unwrap();
        assert_eq!(reopened.committed_version().unwrap(), 2);
        let rows = reopened.transact(|tx| tx.get_range(b"")).await.unwrap();
        assert_eq!(
            rows,
            vec![
                (b"keep".to_vec(), b"stable".to_vec()),
                (b"new".to_vec(), b"committed".to_vec()),
            ]
        );
        drop(reopened);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn stale_conflicting_file_writers_cannot_both_commit() {
        let path = durable_test_path("stale-conflict");
        let db = LogicalDb::open_at(&path, FdbConfig::default()).unwrap();
        db.transact(|tx| {
            tx.set(b"key".to_vec(), b"initial".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();

        let (version, snapshot) = db.versioned_snapshot().unwrap();
        let mut first = FdbTransaction::new_with_snapshot(
            MAX_TRANSACTION_BYTES,
            DEFAULT_TRANSACTION_TIMEOUT_MS,
            version,
            snapshot.clone(),
        );
        first
            .add_read_conflict_range(b"key".to_vec(), b"kez".to_vec())
            .unwrap();
        first.set(b"key".to_vec(), b"first".to_vec()).unwrap();
        let mut second = FdbTransaction::new_with_snapshot(
            MAX_TRANSACTION_BYTES,
            DEFAULT_TRANSACTION_TIMEOUT_MS,
            version,
            snapshot,
        );
        second
            .add_read_conflict_range(b"key".to_vec(), b"kez".to_vec())
            .unwrap();
        second.set(b"key".to_vec(), b"second".to_vec()).unwrap();

        let (first_result, second_result) =
            tokio::join!(db.try_commit(first), db.try_commit(second));
        assert!(first_result.is_ok() ^ second_result.is_ok());
        assert!(matches!(
            first_result.as_ref().err().or(second_result.as_ref().err()),
            Some(KernelError::TransactionConflict)
        ));
        assert_eq!(db.committed_version().unwrap(), 2);
        drop(db);
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn concurrent_atomic_adds_resolve_against_serialized_state() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        db.transact(|tx| {
            tx.set(b"counter".to_vec(), 0i64.to_le_bytes().to_vec())?;
            Ok(())
        })
        .await
        .unwrap();

        let (version, snapshot) = db.versioned_snapshot().unwrap();
        let mut first = FdbTransaction::new_with_snapshot(
            MAX_TRANSACTION_BYTES,
            DEFAULT_TRANSACTION_TIMEOUT_MS,
            version,
            snapshot.clone(),
        );
        first
            .atomic_add(b"counter".to_vec(), 1i64.to_le_bytes().to_vec())
            .unwrap();
        let mut second = FdbTransaction::new_with_snapshot(
            MAX_TRANSACTION_BYTES,
            DEFAULT_TRANSACTION_TIMEOUT_MS,
            version,
            snapshot,
        );
        second
            .atomic_add(b"counter".to_vec(), 1i64.to_le_bytes().to_vec())
            .unwrap();

        let (first_result, second_result) =
            tokio::join!(db.try_commit(first), db.try_commit(second));
        assert!(first_result.is_ok());
        assert!(second_result.is_ok());
        let value = db.transact(|tx| tx.get(b"counter")).await.unwrap().unwrap();
        assert_eq!(i64::from_le_bytes(value.try_into().unwrap()), 2);
        assert_eq!(db.committed_version().unwrap(), 3);
    }

    #[tokio::test]
    async fn commit_rechecks_atomic_and_serialization_time_clear_limits() {
        let atomic_db = LogicalDb::open(FdbConfig::default()).unwrap();
        let mut atomic = FdbTransaction::new_with_snapshot(17, 5_000, 0, BTreeMap::new());
        atomic
            .atomic_add(b"k".to_vec(), 1_i64.to_le_bytes().to_vec())
            .unwrap();
        assert!(matches!(
            atomic_db.try_commit(atomic).await,
            Err(KernelError::TransactionTooLarge {
                size: 18,
                limit: 17
            })
        ));
        assert_eq!(atomic_db.committed_version().unwrap(), 0);

        let mut exact_atomic = FdbTransaction::new_with_snapshot(18, 5_000, 0, BTreeMap::new());
        exact_atomic
            .atomic_add(b"k".to_vec(), 1_i64.to_le_bytes().to_vec())
            .unwrap();
        assert_eq!(atomic_db.try_commit(exact_atomic).await.unwrap(), 1);

        let mut expired = FdbTransaction::new_with_snapshot(100, 1, 1, BTreeMap::new());
        expired.start = std::time::Instant::now() - std::time::Duration::from_millis(5);
        assert!(matches!(
            atomic_db.try_commit(expired).await,
            Err(KernelError::TransactionTimedOut { limit_ms: 1, .. })
        ));

        let clear_db = LogicalDb::open(FdbConfig::default()).unwrap();
        let mut clear = FdbTransaction::new_with_snapshot(100, 5_000, 0, BTreeMap::new());
        clear.total_keys = MAX_TRANSACTION_KEYS - 1;
        clear.clear_range(b"a", b"z").unwrap();
        clear_db
            .backend
            .lock()
            .unwrap()
            .apply_transaction(
                &[
                    (b"aa".to_vec(), b"1".to_vec()),
                    (b"bb".to_vec(), b"2".to_vec()),
                ],
                &[],
                1,
            )
            .unwrap();
        assert!(matches!(
            clear_db.try_commit(clear).await,
            Err(KernelError::TransactionTooLarge {
                size: 100_001,
                limit: 100_000
            })
        ));
        assert_eq!(clear_db.committed_version().unwrap(), 1);

        let byte_limit_db = LogicalDb::open(FdbConfig::default()).unwrap();
        let mut byte_limited = FdbTransaction::new_with_snapshot(3, 5_000, 0, BTreeMap::new());
        byte_limited.clear_range(b"a", b"z").unwrap();
        byte_limit_db
            .backend
            .lock()
            .unwrap()
            .apply_transaction(
                &[
                    (b"aa".to_vec(), b"1".to_vec()),
                    (b"bb".to_vec(), b"2".to_vec()),
                ],
                &[],
                1,
            )
            .unwrap();
        assert!(matches!(
            byte_limit_db.try_commit(byte_limited).await,
            Err(KernelError::TransactionTooLarge { size: 4, limit: 3 })
        ));
    }

    #[tokio::test]
    async fn read_only_commit_reports_its_snapshot_without_global_conflict() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let (read_version, snapshot) = db.versioned_snapshot().unwrap();
        let read_only = FdbTransaction::new_with_snapshot(
            MAX_TRANSACTION_BYTES,
            DEFAULT_TRANSACTION_TIMEOUT_MS,
            read_version,
            snapshot,
        );

        db.transact(|tx| {
            tx.set(b"later".to_vec(), b"write".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(db.try_commit(read_only).await.unwrap(), read_version);
        assert_eq!(read_version, 0);
        assert_eq!(db.committed_version().unwrap(), 1);
    }

    #[tokio::test]
    async fn clear_range_removes_keys_inserted_after_its_snapshot() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        db.transact(|tx| {
            tx.set(b"a:old".to_vec(), b"old".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();

        let (read_version, snapshot) = db.versioned_snapshot().unwrap();
        let mut clear = FdbTransaction::new_with_snapshot(
            MAX_TRANSACTION_BYTES,
            DEFAULT_TRANSACTION_TIMEOUT_MS,
            read_version,
            snapshot,
        );
        clear.clear_range(b"a:", b"a;").unwrap();
        clear
            .set(b"a:survivor".to_vec(), b"after-clear".to_vec())
            .unwrap();

        db.transact(|tx| {
            tx.set(b"a:phantom".to_vec(), b"concurrent".to_vec())?;
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(db.try_commit(clear).await.unwrap(), 3);
        let remaining = db.transact(|tx| tx.get_range(b"a:")).await.unwrap();
        assert_eq!(
            remaining,
            vec![(b"a:survivor".to_vec(), b"after-clear".to_vec())]
        );
    }

    #[test]
    fn corrupted_committed_version_fails_open() {
        let path = durable_test_path("corrupt-version");
        {
            let mut backend = FdbBackend::open(&path).unwrap();
            backend
                .apply_transaction(&[(b"key".to_vec(), b"value".to_vec())], &[], 1)
                .unwrap();
        }
        {
            let raw = redb::Database::create(&path).unwrap();
            let write = raw.begin_write().unwrap();
            {
                let mut meta = write.open_table(FDB_META).unwrap();
                meta.insert(FDB_VERSION_KEY, b"bad".as_slice()).unwrap();
            }
            write.commit().unwrap();
        }

        let error = match LogicalDb::open_at(&path, FdbConfig::default()) {
            Ok(_) => panic!("corrupted version metadata must fail open"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            KernelError::Backend(message) if message == "committed version is not 8 bytes"
        ));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn poisoned_backend_lock_returns_typed_error() {
        let db = LogicalDb::open(FdbConfig::default()).unwrap();
        let backend = Arc::clone(&db.backend);
        let _ = std::panic::catch_unwind(move || {
            let _guard = backend.lock().unwrap();
            panic!("plant backend lock poison");
        });

        assert!(matches!(
            db.committed_version(),
            Err(KernelError::StateLockPoisoned { lock: "backend" })
        ));
    }
}
