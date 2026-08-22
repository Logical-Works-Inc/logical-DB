//! Owns the Record storage class port: typed relational rows with
//! secondary lookup by id and full-class scan. INV-RECORD-IS-ROW:
//! every record operation maps to exactly one `Class::Record` row
//! in the underlying store — no shadow state, no separate index.

use crate::model::{Class, Row};
use crate::port::{ClockPort, KvPort};
use crate::store::{Ldb, Mutation};
use crate::Result;

// ── RecordIndex port ─────────────────────────────────────────

/// Port trait for the Record storage class. Engines are swappable
/// behind this; MemRecordIndex is the deterministic reference impl.
pub trait RecordIndex {
    fn insert(&mut self, id: &[u8], payload: Vec<u8>) -> Result<u64>;
    fn get_latest(&self, id: &[u8]) -> Result<Option<(u64, Vec<u8>)>>;
    fn scan_all(&self) -> Result<Vec<Row>>;
    /// Tombstone: writes a zero-length payload at the current ts.
    fn delete(&mut self, id: &[u8]) -> Result<u64>;
}

// ── MemRecordIndex ───────────────────────────────────────────

pub struct MemRecordIndex<S: KvPort, C: ClockPort> {
    store: Ldb<S, C>,
}

impl<S: KvPort, C: ClockPort> MemRecordIndex<S, C> {
    pub fn new(store: Ldb<S, C>) -> Self {
        Self { store }
    }

    pub fn into_store(self) -> Ldb<S, C> {
        self.store
    }
}

impl<S: KvPort, C: ClockPort> RecordIndex for MemRecordIndex<S, C> {
    fn insert(&mut self, id: &[u8], payload: Vec<u8>) -> Result<u64> {
        self.store
            .commit(&[Mutation::new(Class::Record, id, payload)])
    }

    fn get_latest(&self, id: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        self.store.latest(Class::Record, id)
    }

    fn scan_all(&self) -> Result<Vec<Row>> {
        self.store.scan_class(Class::Record)
    }

    fn delete(&mut self, id: &[u8]) -> Result<u64> {
        self.store
            .commit(&[Mutation::new(Class::Record, id, vec![])])
    }
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memkv::MemKv;
    use std::cell::Cell;
    use std::path::PathBuf;

    struct Seq(Cell<u64>);
    impl ClockPort for Seq {
        fn now(&self) -> u64 {
            self.0.set(self.0.get() + 1);
            self.0.get()
        }
    }

    fn tmp_path(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("ldb-rec-{}-{tag}.journal", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn insert_then_get_latest_roundtrips() {
        let path = tmp_path("rt");
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        let mut idx = MemRecordIndex::new(store);
        let ts = idx.insert(b"doc/1", b"hello".to_vec()).unwrap();
        assert_eq!(ts, 1);
        let (got_ts, payload) = idx.get_latest(b"doc/1").unwrap().unwrap();
        assert_eq!(got_ts, 1);
        assert_eq!(payload, b"hello");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_writes_a_zero_length_tombstone() {
        let path = tmp_path("tomb");
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        let mut idx = MemRecordIndex::new(store);
        idx.insert(b"doc/1", b"alive".to_vec()).unwrap();
        idx.delete(b"doc/1").unwrap();
        let (_, payload) = idx.get_latest(b"doc/1").unwrap().unwrap();
        assert!(payload.is_empty(), "tombstone has zero-length payload");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn scan_all_returns_every_record_version() {
        let path = tmp_path("scan");
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        let mut idx = MemRecordIndex::new(store);
        idx.insert(b"a", b"v1".to_vec()).unwrap();
        idx.insert(b"b", b"v1".to_vec()).unwrap();
        idx.insert(b"a", b"v2".to_vec()).unwrap();
        let rows = idx.scan_all().unwrap();
        assert_eq!(rows.len(), 3, "two versions of a + one of b");
        let _ = std::fs::remove_file(&path);
    }
}
