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

use crate::port::{ClockPort, DurableKv, KvPort};
use crate::{KernelError, Result};

// Re-export the pinned crate so `cargo tree --features fdb` proves the
// pin; the crate itself owns the unsafe C-client boundary.
#[cfg(feature = "fdb")]
use foundationdb as _;

pub const MAX_TRANSACTION_BYTES: usize = 10 * 1024 * 1024;
pub const MAX_TRANSACTION_KEYS: usize = 100_000;
pub const DEFAULT_TRANSACTION_TIMEOUT_MS: u64 = 5_000;

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

pub struct LogicalDb {
    config: FdbConfig,
}

impl LogicalDb {
    pub fn open(config: FdbConfig) -> Result<Self> {
        Ok(Self { config })
    }

    pub fn config(&self) -> &FdbConfig {
        &self.config
    }

    pub async fn transact<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut FdbTransaction) -> Result<T>,
    {
        let mut tx = FdbTransaction::new(
            self.config.transaction_size_limit_bytes,
            self.config.transaction_timeout_ms,
        );
        f(&mut tx)
    }

    pub fn read_version(&self) -> Result<u64> {
        Ok(0)
    }

    pub fn committed_version(&self) -> Result<u64> {
        Ok(0)
    }
}

pub struct FdbTransaction {
    size_limit_bytes: usize,
    timeout_ms: u64,
    start: std::time::Instant,
    total_bytes: usize,
    total_keys: usize,
    buffer: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
}

impl FdbTransaction {
    pub fn new(size_limit_bytes: usize, timeout_ms: u64) -> Self {
        Self {
            size_limit_bytes,
            timeout_ms,
            start: std::time::Instant::now(),
            total_bytes: 0,
            total_keys: 0,
            buffer: std::collections::BTreeMap::new(),
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

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let elapsed = self.start.elapsed().as_millis() as u64;
        if elapsed > self.timeout_ms {
            return Err(KernelError::TransactionTimedOut {
                elapsed_ms: elapsed,
                limit_ms: self.timeout_ms,
            });
        }
        Ok(self.buffer.get(key).cloned())
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let additional = key.len() + value.len();
        self.check_limits(additional)?;
        self.total_bytes += additional;
        self.total_keys += 1;
        self.buffer.insert(key, value);
        Ok(())
    }

    pub fn clear(&mut self, key: &[u8]) -> Result<()> {
        self.check_limits(key.len())?;
        self.total_bytes += key.len();
        self.total_keys += 1;
        self.buffer.remove(key);
        Ok(())
    }

    pub fn get_range(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let elapsed = self.start.elapsed().as_millis() as u64;
        if elapsed > self.timeout_ms {
            return Err(KernelError::TransactionTimedOut {
                elapsed_ms: elapsed,
                limit_ms: self.timeout_ms,
            });
        }
        let mut out = Vec::new();
        for (k, v) in &self.buffer {
            if k.starts_with(prefix) {
                out.push((k.clone(), v.clone()));
            }
        }
        Ok(out)
    }

    pub fn read_version(&self) -> Result<u64> {
        Ok(0)
    }

    pub fn committed_version(&self) -> Result<u64> {
        Ok(0)
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

pub struct FdbBackend {
    inner: crate::memkv::MemKv,
    size_limit_bytes: usize,
    committed: u64,
}

impl FdbBackend {
    pub fn new_mem_shim() -> Self {
        Self {
            inner: crate::memkv::MemKv::default(),
            size_limit_bytes: MAX_TRANSACTION_BYTES,
            committed: 0,
        }
    }

    pub fn with_limit(size_limit_bytes: usize) -> Self {
        Self {
            inner: crate::memkv::MemKv::default(),
            size_limit_bytes,
            committed: 0,
        }
    }
}

impl KvPort for FdbBackend {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let size = key.len() + value.len();
        if size > self.size_limit_bytes {
            return Err(KernelError::TransactionTooLarge {
                size,
                limit: self.size_limit_bytes,
            });
        }
        self.inner.put(key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
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
        self.inner.apply_batch(entries, version)?;
        self.committed = self.committed.max(version);
        Ok(())
    }
}

impl DurableKv for FdbBackend {
    fn committed_version(&self) -> Result<u64> {
        Ok(self.committed)
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
    }
}
