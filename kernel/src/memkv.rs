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
}
