//! Owns the Record storage class port: typed relational rows with
//! secondary lookup by id and full-class scan. INV-RECORD-IS-ROW:
//! every record operation maps to exactly one `Class::Record` row
//! in the underlying store — no shadow state, no separate index.

use crate::model::{Class, Op};
use crate::port::{ClockPort, KvPort};
use crate::store::{Ldb, Mutation};
use crate::Result;

// ── LiveRecord ───────────────────────────────────────────────

pub struct LiveRecord {
    pub id: Vec<u8>,
    pub ts: u64,
    pub payload: Vec<u8>,
}

// ── RecordIndex port ─────────────────────────────────────────

/// Port trait for the Record storage class. Engines are swappable
/// behind this; MemRecordIndex is the deterministic reference impl.
pub trait RecordIndex {
    fn put(&mut self, id: &[u8], payload: Vec<u8>) -> Result<u64>;
    fn get_latest(&self, id: &[u8]) -> Result<Option<(u64, Vec<u8>)>>;
    fn get_at(&self, id: &[u8], version: u64) -> Result<Option<Vec<u8>>>;
    fn history(&self, id: &[u8]) -> Result<Vec<(u64, Op)>>;
    fn scan_all_live(&self) -> Result<Vec<LiveRecord>>;
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
    fn put(&mut self, id: &[u8], payload: Vec<u8>) -> Result<u64> {
        self.store
            .commit(&[Mutation::put(Class::Record, id, payload)])
    }

    fn get_latest(&self, id: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        self.store.latest(Class::Record, id)
    }

    fn get_at(&self, id: &[u8], version: u64) -> Result<Option<Vec<u8>>> {
        self.store.get(Class::Record, id, version)
    }

    fn history(&self, id: &[u8]) -> Result<Vec<(u64, Op)>> {
        let prefix = crate::model::history_prefix(Class::Record, id);
        let raw = self.store.kv().scan_prefix(&prefix)?;
        let mut out = Vec::new();
        for (key, stored) in raw {
            let (_, _, ts) = crate::model::split_key(&key)?;
            let op = if !stored.is_empty() && stored[0] == 2 {
                Op::Delete
            } else {
                Op::Put(stored[1..].to_vec())
            };
            out.push((ts, op));
        }
        Ok(out)
    }

    fn scan_all_live(&self) -> Result<Vec<LiveRecord>> {
        let rows = self.store.scan_class(Class::Record)?;
        let mut latest: std::collections::HashMap<Vec<u8>, (u64, Op)> =
            std::collections::HashMap::new();
        for row in rows {
            latest.insert(row.id.clone(), (row.ts, row.op));
        }
        Ok(latest
            .into_iter()
            .filter_map(|(id, (ts, op))| match op {
                Op::Put(payload) => Some(LiveRecord { id, ts, payload }),
                Op::Delete => None,
            })
            .collect())
    }

    fn delete(&mut self, id: &[u8]) -> Result<u64> {
        self.store.commit(&[Mutation::delete(Class::Record, id)])
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
    fn put_then_get_latest_roundtrips() {
        let path = tmp_path("rt");
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        let mut idx = MemRecordIndex::new(store);
        let ts = idx.put(b"doc/1", b"hello".to_vec()).unwrap();
        assert_eq!(ts, 1);
        let (got_ts, payload) = idx.get_latest(b"doc/1").unwrap().unwrap();
        assert_eq!(got_ts, 1);
        assert_eq!(payload, b"hello");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn delete_hides_from_latest() {
        let path = tmp_path("tomb");
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        let mut idx = MemRecordIndex::new(store);
        idx.put(b"doc/1", b"alive".to_vec()).unwrap();
        idx.delete(b"doc/1").unwrap();
        assert!(idx.get_latest(b"doc/1").unwrap().is_none());
    }

    #[test]
    fn empty_payload_put_is_valid_live_data() {
        let path = tmp_path("empty");
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        let mut idx = MemRecordIndex::new(store);
        idx.put(b"doc/1", vec![]).unwrap();
        let (_, payload) = idx.get_latest(b"doc/1").unwrap().unwrap();
        assert!(payload.is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn history_includes_puts_and_deletes() {
        let path = tmp_path("hist");
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        let mut idx = MemRecordIndex::new(store);
        idx.put(b"a", b"v1".to_vec()).unwrap();
        idx.put(b"a", b"v2".to_vec()).unwrap();
        idx.delete(b"a").unwrap();
        let hist = idx.history(b"a").unwrap();
        assert_eq!(hist.len(), 3);
        assert_eq!(hist[0], (1, Op::Put(b"v1".to_vec())));
        assert_eq!(hist[1], (2, Op::Put(b"v2".to_vec())));
        assert_eq!(hist[2], (3, Op::Delete));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn scan_all_live_excludes_deleted() {
        let path = tmp_path("scan");
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        let mut idx = MemRecordIndex::new(store);
        idx.put(b"a", b"v1".to_vec()).unwrap();
        idx.put(b"b", b"v1".to_vec()).unwrap();
        idx.delete(b"a").unwrap();
        let live = idx.scan_all_live().unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].id, b"b");
        let _ = std::fs::remove_file(&path);
    }
}
