//! Owns the Claim storage class port: content-addressed provenance
//! assertions with secondary indexes on subject and predicate.
//! INV-CLAIM-TRIPLE-KEY: every `assert_claim` writes main + subject
//! index + predicate index in one atomic batch — they are never out
//! of sync.
//!
//! Key layout (id field passed to `Mutation::new`):
//! - Main:      `[0x01][claim_id]`
//! - Subject:   `[0x02][subject][0xFF][claim_id]`
//! - Predicate: `[0x03][predicate][0xFF][claim_id]`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::Class;
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
    /// Tombstone: writes zero-length payload for all three keys.
    fn retract_claim(&mut self, claim_id: &[u8]) -> Result<u64>;
    fn get_claim(&self, claim_id: &[u8]) -> Result<Option<Claim>>;
    fn claims_by_subject(&self, subject: &[u8]) -> Result<Vec<Claim>>;
    fn claims_by_predicate(&self, predicate: &[u8]) -> Result<Vec<Claim>>;
}

// ── Key helpers ─────────────────────────────────────────────

const MAIN_TAG: u8 = 0x01;
const SUBJECT_TAG: u8 = 0x02;
const PREDICATE_TAG: u8 = 0x03;
const SEP: u8 = 0xFF;

fn validate_id(id: &[u8]) -> Result<()> {
    if id.contains(&SEP) {
        return Err(KernelError::SeparatorInId);
    }
    Ok(())
}

fn main_id(claim_id: &[u8]) -> Vec<u8> {
    let mut id = Vec::with_capacity(1 + claim_id.len());
    id.push(MAIN_TAG);
    id.extend_from_slice(claim_id);
    id
}

fn subject_id(subject: &[u8], claim_id: &[u8]) -> Vec<u8> {
    let mut id = Vec::with_capacity(1 + subject.len() + 1 + claim_id.len());
    id.push(SUBJECT_TAG);
    id.extend_from_slice(subject);
    id.push(SEP);
    id.extend_from_slice(claim_id);
    id
}

fn predicate_id(predicate: &[u8], claim_id: &[u8]) -> Vec<u8> {
    let mut id = Vec::with_capacity(1 + predicate.len() + 1 + claim_id.len());
    id.push(PREDICATE_TAG);
    id.extend_from_slice(predicate);
    id.push(SEP);
    id.extend_from_slice(claim_id);
    id
}

fn subject_scan_prefix(subject: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(1 + subject.len() + 1);
    p.push(SUBJECT_TAG);
    p.extend_from_slice(subject);
    p.push(SEP);
    p
}

fn predicate_scan_prefix(predicate: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(1 + predicate.len() + 1);
    p.push(PREDICATE_TAG);
    p.extend_from_slice(predicate);
    p.push(SEP);
    p
}

fn deserialize_claim(payload: &[u8]) -> Result<Claim> {
    serde_json::from_slice(payload)
        .map_err(|e| KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
}

fn serialize_claim(claim: &Claim) -> Result<Vec<u8>> {
    serde_json::to_vec(claim)
        .map_err(|e| KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))
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
        let mut full_prefix = crate::model::class_prefix(Class::Claim);
        full_prefix.extend_from_slice(&prefix);
        let raw = self.store.kv().scan_prefix(&full_prefix)?;
        let mut best: HashMap<Vec<u8>, Option<Claim>> = HashMap::new();
        for (key, payload) in raw {
            let (_class, id, _ts) = crate::model::split_key(&key)?;
            if payload.is_empty() {
                best.insert(id, None);
            } else if let Ok(claim) = deserialize_claim(&payload) {
                best.insert(id, Some(claim));
            }
        }
        Ok(best.into_values().flatten().collect())
    }
}

impl<S: KvPort, C: ClockPort> ClaimIndex for MemClaimIndex<S, C> {
    fn assert_claim(&mut self, claim: &Claim) -> Result<u64> {
        validate_id(&claim.claim_id)?;
        validate_id(&claim.subject)?;
        validate_id(&claim.predicate)?;
        let payload = serialize_claim(claim)?;
        // INV-CLAIM-TRIPLE-KEY: main + subject + predicate in one batch.
        self.store.commit(&[
            Mutation::new(Class::Claim, main_id(&claim.claim_id), payload.clone()),
            Mutation::new(
                Class::Claim,
                subject_id(&claim.subject, &claim.claim_id),
                payload.clone(),
            ),
            Mutation::new(
                Class::Claim,
                predicate_id(&claim.predicate, &claim.claim_id),
                payload,
            ),
        ])
    }

    fn retract_claim(&mut self, claim_id: &[u8]) -> Result<u64> {
        validate_id(claim_id)?;
        let mid = main_id(claim_id);
        let current = self.store.latest(Class::Claim, &mid)?;
        let mut batch = vec![Mutation::new(Class::Claim, mid, vec![])];
        if let Some((_ts, payload)) = current {
            if !payload.is_empty() {
                if let Ok(claim) = deserialize_claim(&payload) {
                    batch.push(Mutation::new(
                        Class::Claim,
                        subject_id(&claim.subject, claim_id),
                        vec![],
                    ));
                    batch.push(Mutation::new(
                        Class::Claim,
                        predicate_id(&claim.predicate, claim_id),
                        vec![],
                    ));
                }
            }
        }
        self.store.commit(&batch)
    }

    fn get_claim(&self, claim_id: &[u8]) -> Result<Option<Claim>> {
        let key_id = main_id(claim_id);
        match self.store.latest(Class::Claim, &key_id)? {
            None => Ok(None),
            Some((_ts, payload)) => {
                if payload.is_empty() {
                    return Ok(None);
                }
                Ok(Some(deserialize_claim(&payload)?))
            }
        }
    }

    fn claims_by_subject(&self, subject: &[u8]) -> Result<Vec<Claim>> {
        self.scan_index(subject_scan_prefix(subject))
    }

    fn claims_by_predicate(&self, predicate: &[u8]) -> Result<Vec<Claim>> {
        self.scan_index(predicate_scan_prefix(predicate))
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
    fn retract_writes_tombstone() {
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
    fn separator_in_claim_id_is_rejected() {
        let (mut ci, path) = make_ci("sep");
        let mut c = test_claim(b"ok", b"ok", b"ok");
        c.claim_id = vec![0x01, 0xFF, 0x02];
        let err = ci.assert_claim(&c).unwrap_err();
        assert!(matches!(err, KernelError::SeparatorInId));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn separator_in_subject_is_rejected() {
        let (mut ci, path) = make_ci("sep-subj");
        let mut c = test_claim(b"ok", b"ok", b"ok");
        c.subject = vec![0xFF];
        let err = ci.assert_claim(&c).unwrap_err();
        assert!(matches!(err, KernelError::SeparatorInId));
        let _ = std::fs::remove_file(&path);
    }
}
