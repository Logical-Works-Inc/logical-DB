//! Owns the VecCode storage class port: quantized vector codes with
//! centroid-indexed similarity search. INV-VECCODE-DUAL-KEY: every
//! `insert_vec` writes both the main entry and the centroid index
//! entry in one atomic batch — they are never out of sync.
//!
//! Key layout (length-prefixed composite, no reserved bytes):
//! - Main:     namespace 0x01: `[0x01][len(vector_id) u16 BE][vector_id]`
//! - Centroid: namespace 0x02: `[0x02][len(centroid_be4) u16 BE][centroid_be4][len(vector_id) u16 BE][vector_id]`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::{self, Class, Op};
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

    fn remove_vec(&mut self, vector_id: &[u8]) -> Result<u64>;
    fn get_vec(&self, vector_id: &[u8]) -> Result<Option<VecEntry>>;
    fn scan_centroid(&self, centroid_id: u32) -> Result<Vec<VecEntry>>;
    fn brute_scan(&self, query_code: &[u8], k: usize) -> Result<Vec<(Vec<u8>, u32)>>;
}

// ── Key helpers ─────────────────────────────────────────────

const MAIN_NS: u8 = 0x01;
const CENTROID_NS: u8 = 0x02;

fn main_id(vector_id: &[u8]) -> Result<Vec<u8>> {
    model::encode_composite(MAIN_NS, &[vector_id])
}

fn centroid_index_id(centroid: u32, vector_id: &[u8]) -> Result<Vec<u8>> {
    let centroid_bytes = centroid.to_be_bytes();
    model::encode_composite(CENTROID_NS, &[&centroid_bytes, vector_id])
}

fn centroid_scan_prefix(centroid: u32) -> Result<Vec<u8>> {
    let centroid_bytes = centroid.to_be_bytes();
    model::composite_prefix(CENTROID_NS, &centroid_bytes)
}

fn hamming_distance(a: &[u8], b: &[u8]) -> Result<u32> {
    // INV-CODE-LENGTH-EQUAL: the distance between different-length
    // codes is undefined, so a mismatch is an error — never a
    // silently truncated zip (FORMAT.md §1: corrupt input is a hard
    // error, never skipped).
    if a.len() != b.len() {
        return Err(KernelError::CodeLengthMismatch {
            expected: a.len(),
            got: b.len(),
        });
    }
    Ok(a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| (x ^ y).count_ones())
        .sum())
}

fn deserialize_entry(stored: &[u8]) -> Result<Option<VecEntry>> {
    if stored.is_empty() || stored[0] == 2 {
        return Ok(None);
    }
    let payload = &stored[1..];
    lgwks_std::json::from_slice::<VecEntry>(payload)
        .map(Some)
        .map_err(|e| KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
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
        // Same rule as Claim.confidence (KernelError::InvalidConfidence):
        // a non-finite norm would serialize to JSON `null` and the row
        // could never be read back.
        if !norm.is_finite() {
            return Err(KernelError::InvalidNorm);
        }
        let entry = VecEntry {
            vector_id: vector_id.to_vec(),
            code,
            norm,
            centroid_id: centroid_id_val,
        };
        let payload = lgwks_std::json::to_vec(&entry).map_err(|e| {
            KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        self.store.commit(&[
            Mutation::put(Class::VecCode, main_id(vector_id)?, payload.clone()),
            Mutation::put(
                Class::VecCode,
                centroid_index_id(centroid_id_val, vector_id)?,
                payload,
            ),
        ])
    }

    fn remove_vec(&mut self, vector_id: &[u8]) -> Result<u64> {
        let mid = main_id(vector_id)?;
        let current = self.store.latest_with_op(Class::VecCode, &mid)?;
        let mut batch = vec![Mutation::delete(Class::VecCode, mid)];
        // INV-VECCODE-DUAL-KEY covers the corrupt-payload case too: a
        // main row whose entry cannot be decoded must fail the removal
        // loudly, because silently deleting only the main row would
        // strand its centroid index row forever (FORMAT.md §1: a
        // decodable-format violation is a hard error, never skipped).
        if let Some((_, Op::Put(payload))) = current {
            let entry: VecEntry = lgwks_std::json::from_slice(&payload).map_err(|e| {
                KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            batch.push(Mutation::delete(
                Class::VecCode,
                centroid_index_id(entry.centroid_id, vector_id)?,
            ));
        }
        self.store.commit(&batch)
    }

    fn get_vec(&self, vector_id: &[u8]) -> Result<Option<VecEntry>> {
        let key_id = main_id(vector_id)?;
        match self.store.latest_with_op(Class::VecCode, &key_id)? {
            None => Ok(None),
            Some((_, Op::Delete)) => Ok(None),
            Some((_, Op::Put(payload))) => lgwks_std::json::from_slice::<VecEntry>(&payload)
                .map(Some)
                .map_err(|e| {
                    KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                }),
        }
    }

    fn scan_centroid(&self, centroid_id_val: u32) -> Result<Vec<VecEntry>> {
        let key_prefix =
            model::class_id_prefix(Class::VecCode, &centroid_scan_prefix(centroid_id_val)?);
        let raw = self.store.kv().scan_prefix(&key_prefix)?;
        let mut best: HashMap<Vec<u8>, Option<VecEntry>> = HashMap::new();
        for (key, stored) in raw {
            let (_, id, _) = model::split_key(&key)?;
            let entry = deserialize_entry(&stored)?;
            best.insert(id, entry);
        }
        Ok(best.into_values().flatten().collect())
    }

    fn brute_scan(&self, query_code: &[u8], k: usize) -> Result<Vec<(Vec<u8>, u32)>> {
        let key_prefix =
            model::class_id_prefix(Class::VecCode, &model::composite_ns_prefix(MAIN_NS));
        let raw = self.store.kv().scan_prefix(&key_prefix)?;

        let mut latest: HashMap<Vec<u8>, VecEntry> = HashMap::new();
        for (key, stored) in raw {
            let (_, id, _) = model::split_key(&key)?;
            match deserialize_entry(&stored)? {
                Some(entry) => {
                    latest.insert(id, entry);
                }
                None => {
                    latest.remove(&id);
                }
            }
        }

        let mut distances: Vec<(Vec<u8>, u32)> = latest
            .into_values()
            .map(|e| hamming_distance(query_code, &e.code).map(|dist| (e.vector_id, dist)))
            .collect::<Result<Vec<_>>>()?;
        distances.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
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
    fn remove_hides_from_get() {
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
    fn arbitrary_byte_vector_ids_are_accepted() {
        let (mut vc, path) = make_vc("arb");
        let id = &[0xFF, 0x00, 0xFF];
        vc.insert_vec(id, vec![0x42], 1.0, 0).unwrap();
        let entry = vc.get_vec(id).unwrap().unwrap();
        assert_eq!(entry.vector_id, id);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn removed_vec_excluded_from_brute_scan() {
        let (mut vc, path) = make_vc("rm-scan");
        vc.insert_vec(b"a", vec![0xFF], 1.0, 0).unwrap();
        vc.insert_vec(b"b", vec![0x00], 1.0, 0).unwrap();
        vc.remove_vec(b"a").unwrap();
        let results = vc.brute_scan(&[0xFF], 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, b"b");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn hamming_distance_is_correct() {
        assert_eq!(hamming_distance(&[0xFF], &[0x00]).unwrap(), 8);
        assert_eq!(hamming_distance(&[0xFF], &[0xFF]).unwrap(), 0);
        assert_eq!(hamming_distance(&[0xFF], &[0xFE]).unwrap(), 1);
        assert_eq!(hamming_distance(&[0xFF, 0x00], &[0x00, 0xFF]).unwrap(), 16);
    }

    #[test]
    fn a_non_finite_norm_is_rejected_at_insert() {
        let (mut vc, path) = make_vc("nan-norm");
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let err = vc.insert_vec(b"v", vec![0x01], bad, 0).unwrap_err();
            assert!(matches!(err, KernelError::InvalidNorm));
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn brute_scan_refuses_length_mismatched_codes_instead_of_truncating() {
        let (mut vc, path) = make_vc("len-mix");
        vc.insert_vec(b"one", vec![0xFF], 1.0, 0).unwrap();
        vc.insert_vec(b"two", vec![0xFF, 0x00], 1.0, 0).unwrap();
        let err = vc.brute_scan(&[0xFF], 10).unwrap_err();
        assert!(
            matches!(
                err,
                KernelError::CodeLengthMismatch {
                    expected: 1,
                    got: 2
                }
            ),
            "got {err:?}"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn removing_a_vec_with_an_undecodable_main_row_fails_loudly() {
        // INV-VECCODE-DUAL-KEY under corruption: a main row whose
        // payload no longer decodes must abort the removal — silently
        // deleting only the main row would strand the centroid row.
        let (mut vc, path) = make_vc("corrupt-rm");
        vc.insert_vec(b"v", vec![0x01], 1.0, 7).unwrap();
        let mut store = vc.into_store();
        let mid = model::encode_composite(0x01, &[&b"v"[..]]).unwrap();
        // Plant a fresh main version whose payload no longer decodes,
        // through the store's own commit path.
        store
            .commit(&[Mutation::put(Class::VecCode, mid, b"{not json".to_vec())])
            .unwrap();
        let mut vc = MemVectorCodes::new(store);
        let err = vc.remove_vec(b"v").unwrap_err();
        assert!(
            matches!(err, KernelError::Io(_)),
            "corrupt payload must fail the removal, got {err:?}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
