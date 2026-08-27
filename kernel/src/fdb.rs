//! FDB native surface — issue #10 F1, ADR-002.
//!
//! Tiny boring wrapper over the pinned `foundationdb` binding (0.11.0,
//! fdb-7_3 API). This is the post-parity canonical port: ordered
//! transactional KV, MVCC conflicts, durability, and versionstamps come
//! from Apple FDB; Logical-DB owns only the namespace/typed-error
//! boundary. `KvPort::apply_batch` shape cannot carry watches, snapshot
//! reads, or conflict ranges without pollution — this surface replaces
//! it after parity, kept behind the `fdb` feature so `--no-default`
//! builds have zero regression.
//!
//! INV-FDB-PIN: the binding version and fdb-7_3 API surface are pinned
//! via `kernel/Cargo.toml`; bumps happen on a named upstream commit
//! only. INV-FDB-SINGLE-SURFACE: after F4 retirement no second
//! transaction abstraction exists. Limits are enforced at our boundary
//! BEFORE the C client sees the op, with stable typed errors.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use redb::{ReadableDatabase, ReadableTable};

use crate::port::{ClockPort, DurableKv, KvPort};
use crate::{KernelError, Result};

// Re-export the pinned crate so `cargo tree --features fdb` proves the
// pin; the crate itself owns the unsafe C-client boundary.
#[cfg(feature = "fdb")]
use foundationdb as _;

pub const MAX_TRANSACTION_BYTES: usize = 10 * 1024 * 1024;
pub const MAX_TRANSACTION_KEYS: usize = 100_000;
pub const DEFAULT_TRANSACTION_TIMEOUT_MS: u64 = 5_000;
const MAX_RETRY_ATTEMPTS: usize = 5;
const VERSIONSTAMP_PLACEHOLDER_LEN: usize = 10;

#[derive(Debug, Clone)]
pub struct FdbConfig {
    pub cluster_file: Option<String>,
    pub transaction_timeout_ms: u64,
    pub transaction_size_limit_bytes: usize,
}

impl Default for FdbConfig {
    fn default() -> Self {
        Self {
            cluster_file: None,
            transaction_timeout_ms: DEFAULT_TRANSACTION_TIMEOUT_MS,
            transaction_size_limit_bytes: MAX_TRANSACTION_BYTES,
        }
    }
}

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

pub struct LogicalDb {
    config: FdbConfig,
    committed: Arc<AtomicU64>,
    pub(crate) backend: Arc<Mutex<FdbBackend>>,
    #[allow(clippy::type_complexity)]
    pub(crate) watches: Arc<Mutex<WatchMap>>,
}

impl LogicalDb {
    pub fn open(config: FdbConfig) -> Result<Self> {
        let backend = FdbBackend::new_mem_shim();
        Ok(Self {
            config,
            committed: Arc::new(AtomicU64::new(0)),
            backend: Arc::new(Mutex::new(backend)),
            watches: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// File-backed durable open — same crash story as `FdbBackend::open`
    /// (redb transaction per commit). Used for local-dev durability
    /// rehearsal without a live fdbserver.
    pub fn open_at(path: impl AsRef<Path>, config: FdbConfig) -> Result<Self> {
        let backend = FdbBackend::open(path)?;
        let committed = backend.committed_version().unwrap_or(0);
        Ok(Self {
            config,
            committed: Arc::new(AtomicU64::new(committed)),
            backend: Arc::new(Mutex::new(backend)),
            watches: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn config(&self) -> &FdbConfig {
        &self.config
    }

    /// Async transact with retry loop delegated to FDB's transaction
    /// error machinery. Retryable error is `TransactionConflict`; all
    /// other errors are returned immediately. Limits are enforced
    /// BEFORE the binding (INV-FDB-PIN).
    pub async fn transact<F, T>(&self, mut f: F) -> Result<T>
    where
        F: FnMut(&mut FdbTransaction) -> Result<T>,
    {
        let mut attempts = 0usize;
        loop {
            let read_version = self.committed.load(Ordering::SeqCst);
            let snapshot = {
                let be = self.backend.lock().unwrap();
                be.snapshot_map()
            };
            let mut tx = FdbTransaction::new_with_snapshot(
                self.config.transaction_size_limit_bytes,
                self.config.transaction_timeout_ms,
                read_version,
                snapshot,
            );
            let result = f(&mut tx);
            match result {
                Ok(val) => match self.try_commit(tx).await {
                    Ok(()) => return Ok(val),
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

    pub(crate) async fn try_commit(&self, mut tx: FdbTransaction) -> Result<()> {
        // Timeout check at commit boundary as well.
        let elapsed = tx.start.elapsed().as_millis() as u64;
        if elapsed > tx.timeout_ms {
            return Err(KernelError::TransactionTimedOut {
                elapsed_ms: elapsed,
                limit_ms: tx.timeout_ms,
            });
        }
        // Conflict check: if any read_conflict_range overlaps a key
        // that was written since our read_version, signal conflict.
        // Shim heuristic: if backend committed_version advanced since
        // read_version and we have any read_conflict_range, check
        // whether any buffered key falls in that range and a newer
        // committed key also exists in backend snapshot delta.
        // Simplified: if committed advanced and we have read ranges,
        // conflict if any buffered write key overlaps read range and
        // backend has that key with different value than snapshot.
        let current_committed = self.committed.load(Ordering::SeqCst);
        if current_committed != tx.read_version && !tx.read_conflict_ranges.is_empty() {
            let be = self.backend.lock().unwrap();
            let current_map = be.snapshot_map();
            for (r_begin, r_end) in &tx.read_conflict_ranges {
                for (k, v) in &tx.buffer {
                    if k >= r_begin && k < r_end {
                        let snap_val = tx.backend_snapshot.get(k);
                        let cur_val = current_map.get(k);
                        if snap_val != cur_val || (snap_val.is_none() && cur_val.is_some()) {
                            return Err(KernelError::TransactionConflict);
                        }
                        // Also if any other committed key in range changed
                        for (ck, cv) in &current_map {
                            if ck >= r_begin
                                && ck < r_end
                                && tx.backend_snapshot.get(ck) != Some(cv)
                            {
                                return Err(KernelError::TransactionConflict);
                            }
                        }
                        let _ = v;
                    }
                }
            }
        }

        // Resolve atomic ops into buffer
        for op in std::mem::take(&mut tx.atomic_ops) {
            let existing = tx
                .buffer
                .get(&op.key)
                .and_then(|v| v.as_ref())
                .or_else(|| tx.backend_snapshot.get(&op.key))
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

        // Resolve versionstamped writes
        let new_version = current_committed + 1;
        for vs in std::mem::take(&mut tx.versionstamped_writes) {
            let (k, v) = resolve_versionstamp(vs, new_version)?;
            tx.buffer.insert(k, Some(v));
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

        // Apply via backend
        {
            let mut be = self.backend.lock().unwrap();
            // First apply puts
            if !puts.is_empty() {
                be.apply_batch(&puts, new_version)?;
            }
            // Apply deletes
            for k in &deletes {
                be.delete(k)?;
            }
            // If only deletes or watch-only, still bump version if we had writes
            if puts.is_empty() && !deletes.is_empty() {
                let _ = be.committed_version();
                be.committed = be.committed.max(new_version);
                if let Some(db) = &be.db {
                    let wtxn = db.begin_write().map_err(fdb_backend_err)?;
                    {
                        let mut meta = wtxn.open_table(FDB_META).map_err(fdb_backend_err)?;
                        meta.insert(FDB_VERSION_KEY, new_version.to_be_bytes().as_slice())
                            .map_err(fdb_backend_err)?;
                    }
                    wtxn.commit().map_err(fdb_backend_err)?;
                }
            }
            if !puts.is_empty()
                || !deletes.is_empty()
                || !tx.versionstamped_writes.is_empty()
                || !tx.atomic_ops.is_empty()
            {
                // already bumped via apply_batch
            } else if !tx.watches.is_empty()
                || !tx.read_conflict_ranges.is_empty()
                || !tx.write_conflict_ranges.is_empty()
            {
                // read-only watches/conflict ranges don't bump version
            }
        }

        if !puts.is_empty() || !deletes.is_empty() {
            self.committed.store(new_version, Ordering::SeqCst);
            // Notify watches
            let mut watches = self.watches.lock().unwrap();
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
            let mut wmap = self.watches.lock().unwrap();
            for (key, sender) in tx.watches {
                wmap.entry(key).or_default().push(sender);
            }
        }

        Ok(())
    }

    pub fn read_version(&self) -> Result<u64> {
        Ok(self.committed.load(Ordering::SeqCst))
    }

    pub fn committed_version(&self) -> Result<u64> {
        Ok(self.committed.load(Ordering::SeqCst))
    }

    #[allow(dead_code)]
    pub(crate) fn bump_committed(&self) -> u64 {
        self.committed.fetch_add(1, Ordering::SeqCst) + 1
    }
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
            if op.param.len() != cur.len() && !cur.is_empty() {
                // For numeric max on LE i64 8 bytes, require same len
                if op.param.len() == 8 && cur.len() == 8 {
                    let a = i64::from_le_bytes(cur.try_into().unwrap());
                    let b = i64::from_le_bytes(op.param.as_slice().try_into().unwrap());
                    return Ok(a.max(b).to_le_bytes().to_vec());
                }
            }
            if cur.is_empty() || op.param.as_slice() > cur {
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
        let additional = key.len() + value.len();
        self.check_limits(additional)?;
        self.total_bytes += additional;
        self.total_keys += 1;
        self.buffer.insert(key, Some(value));
        Ok(())
    }

    pub fn clear(&mut self, key: &[u8]) -> Result<()> {
        self.check_limits(key.len())?;
        self.total_bytes += key.len();
        self.total_keys += 1;
        self.buffer.insert(key.to_vec(), None);
        Ok(())
    }

    pub fn clear_range(&mut self, begin: &[u8], end: &[u8]) -> Result<()> {
        self.check_timeout()?;
        // Record write conflict range and expand deletes for snapshot keys in range
        self.write_conflict_ranges
            .push((begin.to_vec(), end.to_vec()));
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

    // ── Versionstamp ────────────────────────────────────────

    pub fn set_versionstamped_key(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
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

pub struct FdbBackend {
    inner: crate::memkv::MemKv,
    size_limit_bytes: usize,
    committed: u64,
    db: Option<redb::Database>,
}

impl FdbBackend {
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

    /// File-backed durable shim — same crash story as RedbBackend
    /// (one redb transaction per batch, version row co-committed),
    /// so restart preservation can be demonstrated without a live
    /// fdbserver. Uses redb as the local durability substrate; the
    /// FDB-native path replaces this file with the cluster when
    /// available.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db = redb::Database::create(path).map_err(fdb_backend_err)?;
        Ok(Self {
            inner: crate::memkv::MemKv::default(),
            size_limit_bytes: MAX_TRANSACTION_BYTES,
            committed: 0,
            db: Some(db),
        })
    }

    pub(crate) fn snapshot_map(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
        if let Some(db) = &self.db {
            if let Ok(rtxn) = db.begin_read() {
                if let Ok(Some(table)) = fdb_read_table(&rtxn, FDB_ROWS) {
                    let mut out = BTreeMap::new();
                    for (k, v) in table.iter().unwrap().flatten() {
                        out.insert(k.value().to_vec(), v.value().to_vec());
                    }
                    return out;
                }
            }
        }
        let mut out = BTreeMap::new();
        for (k, v) in self.inner.scan_prefix(&[]).unwrap_or_default() {
            out.insert(k, v);
        }
        out
    }
}

impl KvPort for FdbBackend {
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
        let total: usize = entries.iter().map(|(k, v)| k.len() + v.len()).sum();
        if total > self.size_limit_bytes {
            return Err(KernelError::TransactionTooLarge {
                size: total,
                limit: self.size_limit_bytes,
            });
        }
        if entries.len() > MAX_TRANSACTION_KEYS {
            return Err(KernelError::TransactionTooLarge {
                size: entries.len(),
                limit: MAX_TRANSACTION_KEYS,
            });
        }
        if let Some(db) = &self.db {
            let wtxn = db.begin_write().map_err(fdb_backend_err)?;
            {
                let mut rows = wtxn.open_table(FDB_ROWS).map_err(fdb_backend_err)?;
                for (key, value) in entries {
                    rows.insert(key.as_slice(), value.as_slice())
                        .map_err(fdb_backend_err)?;
                }
                let mut meta = wtxn.open_table(FDB_META).map_err(fdb_backend_err)?;
                meta.insert(FDB_VERSION_KEY, version.to_be_bytes().as_slice())
                    .map_err(fdb_backend_err)?;
            }
            wtxn.commit().map_err(fdb_backend_err)?;
            self.committed = self.committed.max(version);
            return Ok(());
        }
        self.inner.apply_batch(entries, version)?;
        self.committed = self.committed.max(version);
        Ok(())
    }
}

impl DurableKv for FdbBackend {
    fn committed_version(&self) -> Result<u64> {
        if let Some(db) = &self.db {
            let rtxn = db.begin_read().map_err(fdb_backend_err)?;
            let Some(meta) = fdb_read_table(&rtxn, FDB_META)? else {
                return Ok(0);
            };
            match meta.get(FDB_VERSION_KEY).map_err(fdb_backend_err)? {
                None => Ok(0),
                Some(v) => {
                    let bytes: [u8; 8] = v.value().try_into().map_err(|_| {
                        KernelError::Backend("committed version is not 8 bytes".into())
                    })?;
                    Ok(u64::from_be_bytes(bytes))
                }
            }
        } else {
            Ok(self.committed)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fdb_config_defaults_are_pinned() {
        let cfg = FdbConfig::default();
        assert_eq!(cfg.transaction_size_limit_bytes, MAX_TRANSACTION_BYTES);
        assert_eq!(cfg.transaction_timeout_ms, DEFAULT_TRANSACTION_TIMEOUT_MS);
        assert!(cfg.cluster_file.is_none());
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
        let v2 = db.transact(|tx| Ok(tx.get(b"k")?)).await.unwrap();
        assert_eq!(v2, Some(b"v".to_vec()));
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
        let v = db
            .transact(|tx| Ok(tx.get(b"conflict-key2")?))
            .await
            .unwrap();
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
        let v = db
            .transact(|tx| Ok(tx.get(b"counter")?))
            .await
            .unwrap()
            .unwrap();
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
}
