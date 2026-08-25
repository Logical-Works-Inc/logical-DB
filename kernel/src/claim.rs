//! Owns the Claim storage class port: content-addressed provenance
//! assertions with secondary indexes on subject and predicate.
//! INV-CLAIM-TRIPLE-KEY: every `assert_claim` writes main + subject
//! index + predicate index in one atomic batch — they are never out
//! of sync.
//!
//! Key layout (length-prefixed composite, no reserved bytes):
//! - Main:      namespace 0x01: `[0x01][len(claim_id) u16 BE][claim_id]`
//! - Subject:   namespace 0x02: `[0x02][len(subject) u16 BE][subject][len(claim_id) u16 BE][claim_id]`
//! - Predicate: namespace 0x03: `[0x03][len(predicate) u16 BE][predicate][len(claim_id) u16 BE][claim_id]`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::{self, Class, Op};
use crate::port::{ClockPort, KvPort};
use crate::store::{Ldb, Mutation};
use crate::{KernelError, Result};

// ── Claim ───────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    pub claim_id: Vec<u8>,
    pub subject: Vec<u8>,
    pub predicate: Vec<u8>,
    pub object: Vec<u8>,
    pub evidence: Vec<u8>,
    pub confidence: f32,
}

// ── ClaimIndex port ─────────────────────────────────────────

/// Port trait for the Claim storage class.
pub trait ClaimIndex {
    fn assert_claim(&mut self, claim: &Claim) -> Result<u64>;
    fn retract_claim(&mut self, claim_id: &[u8]) -> Result<u64>;
    fn get_claim(&self, claim_id: &[u8]) -> Result<Option<Claim>>;
    fn claims_by_subject(&self, subject: &[u8]) -> Result<Vec<Claim>>;
    fn claims_by_predicate(&self, predicate: &[u8]) -> Result<Vec<Claim>>;
}

// ── Key helpers ─────────────────────────────────────────────

const MAIN_NS: u8 = 0x01;
const SUBJECT_NS: u8 = 0x02;
const PREDICATE_NS: u8 = 0x03;

fn main_id(claim_id: &[u8]) -> Result<Vec<u8>> {
    model::encode_composite(MAIN_NS, &[claim_id])
}

fn subject_id(subject: &[u8], claim_id: &[u8]) -> Result<Vec<u8>> {
    model::encode_composite(SUBJECT_NS, &[subject, claim_id])
}

fn predicate_id(predicate: &[u8], claim_id: &[u8]) -> Result<Vec<u8>> {
    model::encode_composite(PREDICATE_NS, &[predicate, claim_id])
}

fn subject_scan_prefix(subject: &[u8]) -> Result<Vec<u8>> {
    model::composite_prefix(SUBJECT_NS, subject)
}

fn predicate_scan_prefix(predicate: &[u8]) -> Result<Vec<u8>> {
    model::composite_prefix(PREDICATE_NS, predicate)
}

fn deserialize_claim(payload: &[u8]) -> Result<Claim> {
    lgwks_std::json::from_slice(payload)
        .map_err(|e| KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
}

fn serialize_claim(claim: &Claim) -> Result<Vec<u8>> {
    lgwks_std::json::to_vec(claim)
        .map_err(|e| KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
}

fn validate_confidence(c: f32) -> Result<()> {
    if c.is_nan() || c.is_infinite() {
        return Err(KernelError::InvalidConfidence);
    }
    Ok(())
}

// ── MemClaimIndex ───────────────────────────────────────────

pub struct MemClaimIndex<S: KvPort, C: ClockPort> {
    store: Ldb<S, C>,
}

impl<S: KvPort, C: ClockPort> MemClaimIndex<S, C> {
    pub fn new(store: Ldb<S, C>) -> Self {
        Self { store }
    }

    pub fn into_store(self) -> Ldb<S, C> {
        self.store
    }

    fn scan_index(&self, prefix: Vec<u8>) -> Result<Vec<Claim>> {
        let full_prefix = model::class_id_prefix(Class::Claim, &prefix);
        let raw = self.store.kv().scan_prefix(&full_prefix)?;
        let mut best: HashMap<Vec<u8>, Option<Claim>> = HashMap::new();
        for (key, stored) in raw {
            let (_, id, _) = model::split_key(&key)?;
            if stored.is_empty() || stored[0] == 2 {
                best.insert(id, None);
            } else {
                let payload = &stored[1..];
                if let Ok(claim) = deserialize_claim(payload) {
                    best.insert(id, Some(claim));
                }
            }
        }
        Ok(best.into_values().flatten().collect())
    }
}

impl<S: KvPort, C: ClockPort> ClaimIndex for MemClaimIndex<S, C> {
    fn assert_claim(&mut self, claim: &Claim) -> Result<u64> {
        validate_confidence(claim.confidence)?;
        let payload = serialize_claim(claim)?;
        self.store.commit(&[
            Mutation::put(Class::Claim, main_id(&claim.claim_id)?, payload.clone()),
            Mutation::put(
                Class::Claim,
                subject_id(&claim.subject, &claim.claim_id)?,
                payload.clone(),
            ),
            Mutation::put(
                Class::Claim,
                predicate_id(&claim.predicate, &claim.claim_id)?,
                payload,
            ),
        ])
    }

    fn retract_claim(&mut self, claim_id: &[u8]) -> Result<u64> {
        let mid = main_id(claim_id)?;
        let current = self.store.latest_with_op(Class::Claim, &mid)?;
        let mut batch = vec![Mutation::delete(Class::Claim, mid)];
        if let Some((_, Op::Put(payload))) = current {
            if let Ok(claim) = deserialize_claim(&payload) {
                batch.push(Mutation::delete(
                    Class::Claim,
                    subject_id(&claim.subject, claim_id)?,
                ));
                batch.push(Mutation::delete(
                    Class::Claim,
                    predicate_id(&claim.predicate, claim_id)?,
                ));
            }
        }
        self.store.commit(&batch)
    }

    fn get_claim(&self, claim_id: &[u8]) -> Result<Option<Claim>> {
        let key_id = main_id(claim_id)?;
        match self.store.latest_with_op(Class::Claim, &key_id)? {
            None => Ok(None),
            Some((_, Op::Delete)) => Ok(None),
            Some((_, Op::Put(payload))) => Ok(Some(deserialize_claim(&payload)?)),
        }
    }

    fn claims_by_subject(&self, subject: &[u8]) -> Result<Vec<Claim>> {
        self.scan_index(subject_scan_prefix(subject)?)
    }

    fn claims_by_predicate(&self, predicate: &[u8]) -> Result<Vec<Claim>> {
        self.scan_index(predicate_scan_prefix(predicate)?)
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
        let p =
            std::env::temp_dir().join(format!("ldb-claim-{}-{tag}.journal", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn make_ci(tag: &str) -> (MemClaimIndex<MemKv, Seq>, PathBuf) {
        let path = tmp_path(tag);
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        (MemClaimIndex::new(store), path)
    }

    fn test_claim(id: &[u8], subj: &[u8], pred: &[u8]) -> Claim {
        Claim {
            claim_id: id.to_vec(),
            subject: subj.to_vec(),
            predicate: pred.to_vec(),
            object: b"obj".to_vec(),
            evidence: b"ev".to_vec(),
            confidence: 0.9,
        }
    }

    #[test]
    fn assert_then_get_roundtrips() {
        let (mut ci, path) = make_ci("rt");
        let c = test_claim(b"c1", b"alice", b"knows");
        ci.assert_claim(&c).unwrap();
        let got = ci.get_claim(b"c1").unwrap().unwrap();
        assert_eq!(got, c);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn retract_hides_from_all_indexes() {
        let (mut ci, path) = make_ci("tomb");
        let c = test_claim(b"c1", b"alice", b"knows");
        ci.assert_claim(&c).unwrap();
        ci.retract_claim(b"c1").unwrap();
        assert!(ci.get_claim(b"c1").unwrap().is_none());
        assert!(ci.claims_by_subject(b"alice").unwrap().is_empty());
        assert!(ci.claims_by_predicate(b"knows").unwrap().is_empty());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn claims_by_subject_filters_correctly() {
        let (mut ci, path) = make_ci("subj");
        ci.assert_claim(&test_claim(b"c1", b"alice", b"knows"))
            .unwrap();
        ci.assert_claim(&test_claim(b"c2", b"alice", b"likes"))
            .unwrap();
        ci.assert_claim(&test_claim(b"c3", b"bob", b"knows"))
            .unwrap();
        let alice = ci.claims_by_subject(b"alice").unwrap();
        assert_eq!(alice.len(), 2);
        let ids: Vec<_> = alice.iter().map(|c| c.claim_id.clone()).collect();
        assert!(ids.contains(&b"c1".to_vec()));
        assert!(ids.contains(&b"c2".to_vec()));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn claims_by_predicate_filters_correctly() {
        let (mut ci, path) = make_ci("pred");
        ci.assert_claim(&test_claim(b"c1", b"alice", b"knows"))
            .unwrap();
        ci.assert_claim(&test_claim(b"c2", b"bob", b"knows"))
            .unwrap();
        ci.assert_claim(&test_claim(b"c3", b"alice", b"likes"))
            .unwrap();
        let knows = ci.claims_by_predicate(b"knows").unwrap();
        assert_eq!(knows.len(), 2);
        let ids: Vec<_> = knows.iter().map(|c| c.claim_id.clone()).collect();
        assert!(ids.contains(&b"c1".to_vec()));
        assert!(ids.contains(&b"c2".to_vec()));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn arbitrary_byte_ids_are_accepted() {
        let (mut ci, path) = make_ci("arb");
        let mut c = test_claim(b"ok", b"ok", b"ok");
        c.claim_id = vec![0xFF, 0x00, 0xFF];
        c.subject = vec![0x00, 0xFF];
        c.predicate = vec![0xFF];
        ci.assert_claim(&c).unwrap();
        let got = ci.get_claim(&[0xFF, 0x00, 0xFF]).unwrap().unwrap();
        assert_eq!(got.claim_id, vec![0xFF, 0x00, 0xFF]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn nan_confidence_is_rejected() {
        let (mut ci, path) = make_ci("nan");
        let mut c = test_claim(b"c1", b"s", b"p");
        c.confidence = f32::NAN;
        let err = ci.assert_claim(&c).unwrap_err();
        assert!(matches!(err, KernelError::InvalidConfidence));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn infinite_confidence_is_rejected() {
        let (mut ci, path) = make_ci("inf");
        let mut c = test_claim(b"c1", b"s", b"p");
        c.confidence = f32::INFINITY;
        let err = ci.assert_claim(&c).unwrap_err();
        assert!(matches!(err, KernelError::InvalidConfidence));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn all_three_rows_land_under_their_expected_composite_keys() {
        // The index keys are built independently here so a broken key
        // helper cannot agree with itself across write and read.
        let (mut ci, path) = make_ci("keys");
        let c = test_claim(b"c1", b"alice", b"knows");
        let ts = ci.assert_claim(&c).unwrap();

        let payload = lgwks_std::json::to_vec(&c).unwrap();
        let main = model::encode_composite(0x01, &[&b"c1"[..]]).unwrap();
        let subject = model::encode_composite(0x02, &[&b"alice"[..], &b"c1"[..]]).unwrap();
        let predicate = model::encode_composite(0x03, &[&b"knows"[..], &b"c1"[..]]).unwrap();
        let store = ci.into_store();
        let stored = [vec![1], payload].concat();
        for id in [main, subject, predicate] {
            assert_eq!(
                store.get_raw(Class::Claim, &id, ts).unwrap(),
                Some(stored.clone()),
                "row under {id:?} must carry the op-tagged claim"
            );
        }
        let _ = std::fs::remove_file(&path);
    }
}
