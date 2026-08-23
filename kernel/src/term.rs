//! Owns the Term storage class port: BM25 full-text search postings
//! over the canonical row model. INV-TERM-IS-ROW: every posting maps
//! to exactly one `Class::Term` row — no shadow index, no separate
//! inverted file.
//!
//! Key layout (id field passed to `Mutation::new`):
//! - `[term][0xFF][doc_id]`
//!
//! The class tag `0x02` is prepended by `model::point_key`; the id
//! field here is everything after the tag. Neither term nor doc_id
//! may contain `0xFF`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::Class;
use crate::port::{ClockPort, KvPort};
use crate::store::{Ldb, Mutation};
use crate::{KernelError, Result};

// ── Posting ─────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Posting {
    pub doc_id: Vec<u8>,
    pub term_freq: u32,
    pub field_len: u32,
}

// ── TermIndex port ──────────────────────────────────────────

/// Port trait for the Term storage class. Engines are swappable
/// behind this; MemTermIndex is the deterministic reference impl.
pub trait TermIndex {
    /// Indexes all terms for one document in one atomic batch.
    /// `terms` is `(term_bytes, term_frequency)` per term.
    fn index_doc(&mut self, doc_id: &[u8], terms: &[(Vec<u8>, u32)], field_len: u32)
        -> Result<u64>;

    /// Tombstone removal: writes zero-length payload for each
    /// (term, doc_id) pair in one atomic batch.
    fn remove_doc(&mut self, doc_id: &[u8], terms: &[Vec<u8>]) -> Result<u64>;

    /// BM25 ranked search returning top-k (doc_id, score) pairs,
    /// descending by score.
    fn search(&self, query_terms: &[Vec<u8>], k: usize) -> Result<Vec<(Vec<u8>, f64)>>;

    /// Raw posting list for one term (all live versions).
    fn postings_for_term(&self, term: &[u8]) -> Result<Vec<Posting>>;
}

// ── Key helpers ─────────────────────────────────────────────

const SEP: u8 = 0xFF;

fn validate_id(id: &[u8]) -> Result<()> {
    if id.contains(&SEP) {
        return Err(KernelError::SeparatorInId);
    }
    Ok(())
}

fn term_doc_id(term: &[u8], doc_id: &[u8]) -> Vec<u8> {
    let mut id = Vec::with_capacity(term.len() + 1 + doc_id.len());
    id.extend_from_slice(term);
    id.push(SEP);
    id.extend_from_slice(doc_id);
    id
}

/// Prefix for all postings of one term: `[term][0xFF]`.
fn term_prefix(term: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(term.len() + 1);
    p.extend_from_slice(term);
    p.push(SEP);
    p
}

/// Splits a Term-class key's id portion `[term][0xFF][doc_id]`
/// back into `(term, doc_id)`.
fn split_term_id(id: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let sep_pos = id
        .iter()
        .position(|&b| b == SEP)
        .ok_or(KernelError::TruncatedRow)?;
    Ok((id[..sep_pos].to_vec(), id[sep_pos + 1..].to_vec()))
}

// ── MemTermIndex ────────────────────────────────────────────

pub struct MemTermIndex<S: KvPort, C: ClockPort> {
    store: Ldb<S, C>,
}

impl<S: KvPort, C: ClockPort> MemTermIndex<S, C> {
    pub fn new(store: Ldb<S, C>) -> Self {
        Self { store }
    }

    pub fn into_store(self) -> Ldb<S, C> {
        self.store
    }

    /// Collects corpus stats from all live Term rows: (doc_count,
    /// avg_field_len). A doc is counted once per unique doc_id seen
    /// in the latest posting version.
    fn corpus_stats(&self) -> Result<(usize, f64)> {
        let prefix = crate::model::class_prefix(Class::Term);
        let rows = self.store.kv().scan_prefix(&prefix)?;
        let mut docs: HashMap<Vec<u8>, u32> = HashMap::new();
        for (key, payload) in &rows {
            if payload.is_empty() {
                continue;
            }
            let (_class, id, _ts) = crate::model::split_key(key)?;
            let (_term, doc_id) = split_term_id(&id)?;
            if let Ok(posting) = serde_json::from_slice::<Posting>(payload) {
                docs.entry(doc_id)
                    .and_modify(|fl| *fl = posting.field_len)
                    .or_insert(posting.field_len);
            }
        }
        let n = docs.len();
        if n == 0 {
            return Ok((0, 0.0));
        }
        let total_fl: u64 = docs.values().map(|&fl| fl as u64).sum();
        Ok((n, total_fl as f64 / n as f64))
    }
}

impl<S: KvPort, C: ClockPort> TermIndex for MemTermIndex<S, C> {
    fn index_doc(
        &mut self,
        doc_id: &[u8],
        terms: &[(Vec<u8>, u32)],
        field_len: u32,
    ) -> Result<u64> {
        validate_id(doc_id)?;
        let mut batch = Vec::with_capacity(terms.len());
        for (term, freq) in terms {
            validate_id(term)?;
            let posting = Posting {
                doc_id: doc_id.to_vec(),
                term_freq: *freq,
                field_len,
            };
            let payload = serde_json::to_vec(&posting).map_err(|e| {
                KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            batch.push(Mutation::new(
                Class::Term,
                term_doc_id(term, doc_id),
                payload,
            ));
        }
        self.store.commit(&batch)
    }

    fn remove_doc(&mut self, doc_id: &[u8], terms: &[Vec<u8>]) -> Result<u64> {
        validate_id(doc_id)?;
        let mut batch = Vec::with_capacity(terms.len());
        for term in terms {
            validate_id(term)?;
            batch.push(Mutation::new(
                Class::Term,
                term_doc_id(term, doc_id),
                vec![],
            ));
        }
        self.store.commit(&batch)
    }

    fn postings_for_term(&self, term: &[u8]) -> Result<Vec<Posting>> {
        let mut key_prefix = crate::model::class_prefix(Class::Term);
        key_prefix.extend_from_slice(&term_prefix(term));
        let raw = self.store.kv().scan_prefix(&key_prefix)?;
        // Keys sort ascending by (doc_id, ts); last entry per doc_id
        // is the latest version. Overwrite unconditionally, then drop
        // tombstones (empty payload).
        let mut best: HashMap<Vec<u8>, Option<Posting>> = HashMap::new();
        for (_key, payload) in raw {
            if payload.is_empty() {
                if let Ok((_class, id, _ts)) = crate::model::split_key(&_key) {
                    let (_term, doc_id) = split_term_id(&id)?;
                    best.insert(doc_id, None);
                }
            } else if let Ok(posting) = serde_json::from_slice::<Posting>(&payload) {
                best.insert(posting.doc_id.clone(), Some(posting));
            }
        }
        Ok(best.into_values().flatten().collect())
    }

    fn search(&self, query_terms: &[Vec<u8>], k: usize) -> Result<Vec<(Vec<u8>, f64)>> {
        let (n, avgdl) = self.corpus_stats()?;
        if n == 0 {
            return Ok(Vec::new());
        }

        const K1: f64 = 1.2;
        const B: f64 = 0.75;

        let mut scores: HashMap<Vec<u8>, f64> = HashMap::new();

        for term in query_terms {
            let postings = self.postings_for_term(term)?;
            let df = postings.len() as f64;
            // IDF: log((N - df + 0.5) / (df + 0.5) + 1.0)
            let idf = ((n as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();

            for p in &postings {
                let tf = p.term_freq as f64;
                let dl = p.field_len as f64;
                let tf_norm = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avgdl));
                *scores.entry(p.doc_id.clone()).or_insert(0.0) += idf * tf_norm;
            }
        }

        let mut ranked: Vec<_> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(k);
        Ok(ranked)
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
        let p = std::env::temp_dir().join(format!("ldb-term-{}-{tag}.journal", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn make_idx(tag: &str) -> (MemTermIndex<MemKv, Seq>, PathBuf) {
        let path = tmp_path(tag);
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        (MemTermIndex::new(store), path)
    }

    #[test]
    fn index_then_search_roundtrips() {
        let (mut idx, path) = make_idx("search");
        idx.index_doc(b"doc1", &[(b"rust".to_vec(), 3), (b"code".to_vec(), 1)], 10)
            .unwrap();
        idx.index_doc(b"doc2", &[(b"rust".to_vec(), 1)], 5).unwrap();
        let results = idx.search(&[b"rust".to_vec()], 10).unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].1 >= results[1].1, "results sorted by score");
        let doc_ids: Vec<_> = results.iter().map(|(d, _)| d.clone()).collect();
        assert!(doc_ids.contains(&b"doc1".to_vec()));
        assert!(doc_ids.contains(&b"doc2".to_vec()));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn remove_doc_writes_tombstone() {
        let (mut idx, path) = make_idx("tomb");
        idx.index_doc(b"d1", &[(b"hello".to_vec(), 2)], 8).unwrap();
        idx.remove_doc(b"d1", &[b"hello".to_vec()]).unwrap();
        let postings = idx.postings_for_term(b"hello").unwrap();
        assert!(postings.is_empty(), "tombstoned postings are filtered out");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn postings_for_term_returns_all_docs() {
        let (mut idx, path) = make_idx("postings");
        idx.index_doc(b"a", &[(b"x".to_vec(), 1)], 5).unwrap();
        idx.index_doc(b"b", &[(b"x".to_vec(), 2)], 7).unwrap();
        idx.index_doc(b"c", &[(b"y".to_vec(), 1)], 3).unwrap();
        let postings = idx.postings_for_term(b"x").unwrap();
        assert_eq!(postings.len(), 2);
        let doc_ids: Vec<_> = postings.iter().map(|p| p.doc_id.clone()).collect();
        assert!(doc_ids.contains(&b"a".to_vec()));
        assert!(doc_ids.contains(&b"b".to_vec()));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn separator_in_term_is_rejected() {
        let (mut idx, path) = make_idx("sep");
        let err = idx
            .index_doc(b"d", &[(vec![0x01, 0xFF, 0x02], 1)], 5)
            .unwrap_err();
        assert!(matches!(err, KernelError::SeparatorInId));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn search_on_empty_index_returns_nothing() {
        let (idx, path) = make_idx("empty");
        let results = idx.search(&[b"anything".to_vec()], 10).unwrap();
        assert!(results.is_empty());
        let _ = std::fs::remove_file(&path);
    }
}
