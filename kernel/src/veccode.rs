//! Owns the VecCode storage class port: quantized vector codes with
//! centroid-indexed similarity search. INV-VECCODE-DUAL-KEY: every
//! `insert_vec` writes both the main entry `[0x01][vector_id]` and
//! the centroid index entry `[0x00][centroid_id BE][0xFF][vector_id]`
//! in one atomic batch — they are never out of sync.
//!
//! Hamming distance over quantized codes is the primary scan
//! primitive (popcount of XOR). Full-precision rescore is the
//! caller's responsibility.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::Class;
use crate::port::{ClockPort, KvPort};
use crate::store::{Ldb, Mutation};
use crate::{KernelError, Result};

// ── VecEntry ────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VecEntry {
    pub vector_id: Vec<u8>,
    pub code: Vec<u8>,
    pub norm: f32,
    pub centroid_id: u32,
}

// ── VectorCodes port ────────────────────────────────────────

/// Port trait for the VecCode storage class.
pub trait VectorCodes {
    fn insert_vec(
        &mut self,
        vector_id: &[u8],
        code: Vec<u8>,
        norm: f32,
        centroid_id: u32,
    ) -> Result<u64>;

    /// Tombstone: writes zero-length payload for both keys.
    fn remove_vec(&mut self, vector_id: &[u8]) -> Result<u64>;

    fn get_vec(&self, vector_id: &[u8]) -> Result<Option<VecEntry>>;

    fn scan_centroid(&self, centroid_id: u32) -> Result<Vec<VecEntry>>;

    /// Hamming distance brute scan over all live vectors, returning
    /// top-k `(vector_id, distance)` ascending by distance.
    fn brute_scan(&self, query_code: &[u8], k: usize) -> Result<Vec<(Vec<u8>, u32)>>;
}

// ── Key helpers ─────────────────────────────────────────────

const MAIN_PREFIX: u8 = 0x01;
const CENTROID_PREFIX: u8 = 0x00;
const SEP: u8 = 0xFF;

fn validate_id(id: &[u8]) -> Result<()> {
    if id.contains(&SEP) {
        return Err(KernelError::SeparatorInId);
    }
    Ok(())
}

fn main_id(vector_id: &[u8]) -> Vec<u8> {
    let mut id = Vec::with_capacity(1 + vector_id.len());
    id.push(MAIN_PREFIX);
    id.extend_from_slice(vector_id);
    id
}

fn centroid_id(centroid: u32, vector_id: &[u8]) -> Vec<u8> {
    let mut id = Vec::with_capacity(1 + 4 + 1 + vector_id.len());
    id.push(CENTROID_PREFIX);
    id.extend_from_slice(&centroid.to_be_bytes());
    id.push(SEP);
    id.extend_from_slice(vector_id);
    id
}

fn centroid_scan_prefix(centroid: u32) -> Vec<u8> {
    let mut p = Vec::with_capacity(1 + 4 + 1);
    p.push(CENTROID_PREFIX);
    p.extend_from_slice(&centroid.to_be_bytes());
    p.push(SEP);
    p
}

fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| (x ^ y).count_ones())
        .sum()
}

// ── MemVectorCodes ──────────────────────────────────────────

pub struct MemVectorCodes<S: KvPort, C: ClockPort> {
    store: Ldb<S, C>,
}

impl<S: KvPort, C: ClockPort> MemVectorCodes<S, C> {
    pub fn new(store: Ldb<S, C>) -> Self {
        Self { store }
    }

    pub fn into_store(self) -> Ldb<S, C> {
        self.store
    }
}

impl<S: KvPort, C: ClockPort> VectorCodes for MemVectorCodes<S, C> {
    fn insert_vec(
        &mut self,
        vector_id: &[u8],
        code: Vec<u8>,
        norm: f32,
        centroid_id_val: u32,
    ) -> Result<u64> {
        validate_id(vector_id)?;
        let entry = VecEntry {
            vector_id: vector_id.to_vec(),
            code,
            norm,
            centroid_id: centroid_id_val,
        };
        let payload = serde_json::to_vec(&entry).map_err(|e| {
            KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        // INV-VECCODE-DUAL-KEY: main + centroid index in one batch.
        self.store.commit(&[
            Mutation::new(Class::VecCode, main_id(vector_id), payload.clone()),
            Mutation::new(
                Class::VecCode,
                centroid_id(centroid_id_val, vector_id),
                payload,
            ),
        ])
    }

    fn remove_vec(&mut self, vector_id: &[u8]) -> Result<u64> {
        validate_id(vector_id)?;
        // Need the centroid_id to tombstone the index entry. Read the
        // current main entry to find it.
        let main_key_id = main_id(vector_id);
        let current = self.store.latest(Class::VecCode, &main_key_id)?;
        let mut batch = vec![Mutation::new(Class::VecCode, main_key_id, vec![])];
        if let Some((_ts, payload)) = current {
            if !payload.is_empty() {
                if let Ok(entry) = serde_json::from_slice::<VecEntry>(&payload) {
                    batch.push(Mutation::new(
                        Class::VecCode,
                        centroid_id(entry.centroid_id, vector_id),
                        vec![],
                    ));
                }
            }
        }
        self.store.commit(&batch)
    }

    fn get_vec(&self, vector_id: &[u8]) -> Result<Option<VecEntry>> {
        let key_id = main_id(vector_id);
        match self.store.latest(Class::VecCode, &key_id)? {
            None => Ok(None),
            Some((_ts, payload)) => {
                if payload.is_empty() {
                    return Ok(None);
                }
                let entry = serde_json::from_slice::<VecEntry>(&payload).map_err(|e| {
                    KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                })?;
                Ok(Some(entry))
            }
        }
    }

    fn scan_centroid(&self, centroid_id_val: u32) -> Result<Vec<VecEntry>> {
        let mut key_prefix = crate::model::class_prefix(Class::VecCode);
        key_prefix.extend_from_slice(&centroid_scan_prefix(centroid_id_val));
        let raw = self.store.kv().scan_prefix(&key_prefix)?;
        let mut best: HashMap<Vec<u8>, Option<VecEntry>> = HashMap::new();
        for (_key, payload) in raw {
            if payload.is_empty() {
                if let Ok((_class, id, _ts)) = crate::model::split_key(&_key) {
                    best.insert(id, None);
                }
            } else if let Ok(entry) = serde_json::from_slice::<VecEntry>(&payload) {
                best.insert(entry.vector_id.clone(), Some(entry));
            }
        }
        Ok(best.into_values().flatten().collect())
    }

    fn brute_scan(&self, query_code: &[u8], k: usize) -> Result<Vec<(Vec<u8>, u32)>> {
        // Scan all main entries (prefix 0x01 under VecCode class).
        let mut key_prefix = crate::model::class_prefix(Class::VecCode);
        key_prefix.push(MAIN_PREFIX);
        let raw = self.store.kv().scan_prefix(&key_prefix)?;

        // Latest version per vector_id.
        let mut latest: HashMap<Vec<u8>, VecEntry> = HashMap::new();
        for (_key, payload) in raw {
            if payload.is_empty() {
                if let Ok((_class, id, _ts)) = crate::model::split_key(&_key) {
                    latest.remove(&id);
                }
                continue;
            }
            if let Ok(entry) = serde_json::from_slice::<VecEntry>(&payload) {
                latest.insert(entry.vector_id.clone(), entry);
            }
        }

        let mut distances: Vec<(Vec<u8>, u32)> = latest
            .into_values()
            .map(|e| {
                let dist = hamming_distance(query_code, &e.code);
                (e.vector_id, dist)
            })
            .collect();
        distances.sort_by_key(|&(_, d)| d);
        distances.truncate(k);
        Ok(distances)
    }
}

// ── Tests ───────────────────────────────────────────────────

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
        let p = std::env::temp_dir().join(format!("ldb-vec-{}-{tag}.journal", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn make_vc(tag: &str) -> (MemVectorCodes<MemKv, Seq>, PathBuf) {
        let path = tmp_path(tag);
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        (MemVectorCodes::new(store), path)
    }

    #[test]
    fn insert_then_get_roundtrips() {
        let (mut vc, path) = make_vc("rt");
        vc.insert_vec(b"vec1", vec![0xAA, 0xBB], 1.5, 7).unwrap();
        let entry = vc.get_vec(b"vec1").unwrap().unwrap();
        assert_eq!(entry.vector_id, b"vec1");
        assert_eq!(entry.code, vec![0xAA, 0xBB]);
        assert!((entry.norm - 1.5).abs() < f32::EPSILON);
        assert_eq!(entry.centroid_id, 7);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn remove_writes_tombstone() {
        let (mut vc, path) = make_vc("tomb");
        vc.insert_vec(b"v", vec![0x01], 1.0, 0).unwrap();
        vc.remove_vec(b"v").unwrap();
        assert!(vc.get_vec(b"v").unwrap().is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn scan_centroid_groups_correctly() {
        let (mut vc, path) = make_vc("centroid");
        vc.insert_vec(b"a", vec![0x00], 1.0, 5).unwrap();
        vc.insert_vec(b"b", vec![0x01], 2.0, 5).unwrap();
        vc.insert_vec(b"c", vec![0x02], 3.0, 9).unwrap();
        let group5 = vc.scan_centroid(5).unwrap();
        assert_eq!(group5.len(), 2);
        let ids: Vec<_> = group5.iter().map(|e| e.vector_id.clone()).collect();
        assert!(ids.contains(&b"a".to_vec()));
        assert!(ids.contains(&b"b".to_vec()));
        let group9 = vc.scan_centroid(9).unwrap();
        assert_eq!(group9.len(), 1);
        assert_eq!(group9[0].vector_id, b"c");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn brute_scan_orders_by_distance() {
        let (mut vc, path) = make_vc("brute");
        // query = 0xFF (all ones)
        // vec "close" = 0xFE (1 bit diff)
        // vec "far"   = 0x00 (8 bits diff)
        // vec "mid"   = 0xF0 (4 bits diff)
        vc.insert_vec(b"far", vec![0x00], 1.0, 0).unwrap();
        vc.insert_vec(b"mid", vec![0xF0], 1.0, 0).unwrap();
        vc.insert_vec(b"close", vec![0xFE], 1.0, 0).unwrap();
        let results = vc.brute_scan(&[0xFF], 3).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, b"close");
        assert_eq!(results[0].1, 1);
        assert_eq!(results[1].0, b"mid");
        assert_eq!(results[1].1, 4);
        assert_eq!(results[2].0, b"far");
        assert_eq!(results[2].1, 8);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn separator_in_vector_id_is_rejected() {
        let (mut vc, path) = make_vc("sep");
        let err = vc
            .insert_vec(&[0x01, 0xFF, 0x02], vec![0x00], 1.0, 0)
            .unwrap_err();
        assert!(matches!(err, KernelError::SeparatorInId));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn hamming_distance_is_correct() {
        assert_eq!(hamming_distance(&[0xFF], &[0x00]), 8);
        assert_eq!(hamming_distance(&[0xFF], &[0xFF]), 0);
        assert_eq!(hamming_distance(&[0xFF], &[0xFE]), 1);
        assert_eq!(hamming_distance(&[0xFF, 0x00], &[0x00, 0xFF]), 16);
    }
}
