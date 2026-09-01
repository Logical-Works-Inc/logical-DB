//! Owns the reference KvPort: an ordered BTreeMap. This is the
//! correctness baseline every outboard adapter is diffed against, not
//! a production engine — no cache, no concurrency, no compaction.

use std::collections::BTreeMap;

use crate::{port::KvPort, Result};

// ── MemKv ────────────────────────────────────────────────────

#[derive(Default)]
pub struct MemKv(BTreeMap<Vec<u8>, Vec<u8>>);

impl MemKv {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn scan_range_limited(
        &self,
        start: std::ops::Bound<&[u8]>,
        end: std::ops::Bound<&[u8]>,
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        use std::ops::Bound;

        let owned_start = match start {
            Bound::Included(key) => Bound::Included(key.to_vec()),
            Bound::Excluded(key) => Bound::Excluded(key.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let owned_end = match end {
            Bound::Included(key) => Bound::Included(key.to_vec()),
            Bound::Excluded(key) => Bound::Excluded(key.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        self.0
            .range((owned_start, owned_end))
            .take(limit)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }
}

impl KvPort for MemKv {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.0.get(key).cloned())
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.0.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.0.remove(key);
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Ascending from the first key >= prefix; stop when the prefix
        // no longer matches. BTreeMap range keeps this O(log n + hits).
        Ok(self
            .0
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_scan_returns_ordered_matches_only() {
        let mut kv = MemKv::new();
        for k in [b"\x01b", b"\x01a", b"\x02z", b"\x01c"] {
            kv.put(k.to_vec(), b"v".to_vec()).unwrap();
        }
        let got = kv.scan_prefix(b"\x01").unwrap();
        let keys: Vec<_> = got.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, [&b"\x01a"[..], b"\x01b", b"\x01c"]);
    }

    #[test]
    fn empty_prefix_scans_everything() {
        let mut kv = MemKv::new();
        kv.put(vec![1], vec![]).unwrap();
        assert_eq!(kv.scan_prefix(&[]).unwrap().len(), 1);
    }

    #[test]
    fn len_delete_and_emptiness_track_the_map() {
        let mut kv = MemKv::new();
        assert!(kv.is_empty());
        assert_eq!(kv.len(), 0);
        kv.put(b"a".to_vec(), b"1".to_vec()).unwrap();
        kv.put(b"b".to_vec(), b"2".to_vec()).unwrap();
        assert_eq!(kv.len(), 2);
        assert!(!kv.is_empty());
        kv.delete(b"a").unwrap();
        assert_eq!(kv.get(b"a").unwrap(), None, "delete must remove");
        assert_eq!(kv.get(b"b").unwrap().as_deref(), Some(b"2".as_slice()));
        assert_eq!(kv.len(), 1);
        kv.delete(b"missing").unwrap();
        assert_eq!(kv.len(), 1, "deleting a missing key is a no-op");
    }
}
