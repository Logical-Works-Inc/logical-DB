//! Owns the Term storage class port: BM25 full-text search postings
//! over the canonical row model. INV-TERM-IS-ROW: every posting maps
//! to exactly one `Class::Term` row — no shadow index, no separate
//! inverted file.
//!
//! Key layout (length-prefixed composite, no reserved bytes):
//! - Posting: namespace 0x01: `[0x01][len(term) u16 BE][term][len(doc_id) u16 BE][doc_id]`
//! - Doc manifest: namespace 0x02: `[0x02][len(doc_id) u16 BE][doc_id]`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::model::{self, Class};
use crate::port::{ClockPort, KvPort};
use crate::store::{Ldb, Mutation};
use crate::Result;

// ── Posting ─────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Posting {
    pub doc_id: Vec<u8>,
    pub term_freq: u32,
    pub field_len: u32,
}

/// Per-document manifest stored alongside postings for O(1) stats.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DocManifest {
    field_len: u32,
    term_count: u32,
}

// ── TermIndex port ──────────────────────────────────────────

/// Port trait for the Term storage class. Engines are swappable
/// behind this; MemTermIndex is the deterministic reference impl.
pub trait TermIndex {
    fn index_doc(&mut self, doc_id: &[u8], terms: &[(Vec<u8>, u32)], field_len: u32)
        -> Result<u64>;

    fn remove_doc(&mut self, doc_id: &[u8], terms: &[Vec<u8>]) -> Result<u64>;

    fn search(&self, query_terms: &[Vec<u8>], k: usize) -> Result<Vec<(Vec<u8>, f64)>>;

    fn postings_for_term(&self, term: &[u8]) -> Result<Vec<Posting>>;
}

// ── Key helpers ─────────────────────────────────────────────

const POSTING_NS: u8 = 0x01;
const MANIFEST_NS: u8 = 0x02;

fn posting_id(term: &[u8], doc_id: &[u8]) -> Result<Vec<u8>> {
    model::encode_composite(POSTING_NS, &[term, doc_id])
}

fn posting_term_prefix(term: &[u8]) -> Result<Vec<u8>> {
    model::composite_prefix(POSTING_NS, term)
}

fn manifest_id(doc_id: &[u8]) -> Result<Vec<u8>> {
    model::encode_composite(MANIFEST_NS, &[doc_id])
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

    fn corpus_stats(&self) -> Result<(usize, f64)> {
        let mut prefix = model::class_prefix(Class::Term);
        prefix.extend_from_slice(&model::composite_ns_prefix(MANIFEST_NS));
        let raw = self.store.kv().scan_prefix(&prefix)?;
        let mut docs: HashMap<Vec<u8>, u32> = HashMap::new();
        for (key, stored) in &raw {
            if stored.is_empty() || stored[0] == 2 {
                let (_, id, _) = model::split_key(key)?;
                let (_, parts) = model::decode_composite(&id)?;
                if !parts.is_empty() {
                    docs.remove(&parts[0]);
                }
                continue;
            }
            let payload = &stored[1..];
            if let Ok(manifest) = serde_json::from_slice::<DocManifest>(payload) {
                let (_, id, _) = model::split_key(key)?;
                let (_, parts) = model::decode_composite(&id)?;
                if !parts.is_empty() {
                    docs.insert(parts[0].clone(), manifest.field_len);
                }
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
        let mut batch = Vec::with_capacity(terms.len() + 1);
        for (term, freq) in terms {
            let posting = Posting {
                doc_id: doc_id.to_vec(),
                term_freq: *freq,
                field_len,
            };
            let payload = serde_json::to_vec(&posting).map_err(|e| {
                crate::KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })?;
            batch.push(Mutation::put(
                Class::Term,
                posting_id(term, doc_id)?,
                payload,
            ));
        }
        let manifest = DocManifest {
            field_len,
            term_count: terms.len() as u32,
        };
        let manifest_payload = serde_json::to_vec(&manifest).map_err(|e| {
            crate::KernelError::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?;
        batch.push(Mutation::put(
            Class::Term,
            manifest_id(doc_id)?,
            manifest_payload,
        ));
        self.store.commit(&batch)
    }

    fn remove_doc(&mut self, doc_id: &[u8], terms: &[Vec<u8>]) -> Result<u64> {
        let mut batch = Vec::with_capacity(terms.len() + 1);
        for term in terms {
            batch.push(Mutation::delete(Class::Term, posting_id(term, doc_id)?));
        }
        batch.push(Mutation::delete(Class::Term, manifest_id(doc_id)?));
        self.store.commit(&batch)
    }

    fn postings_for_term(&self, term: &[u8]) -> Result<Vec<Posting>> {
        let mut key_prefix = model::class_prefix(Class::Term);
        key_prefix.extend_from_slice(&posting_term_prefix(term)?);
        let raw = self.store.kv().scan_prefix(&key_prefix)?;
        let mut best: HashMap<Vec<u8>, Option<Posting>> = HashMap::new();
        for (key, stored) in raw {
            let (_, id, _) = model::split_key(&key)?;
            if stored.is_empty() || stored[0] == 2 {
                best.insert(id, None);
            } else {
                let payload = &stored[1..];
                if let Ok(posting) = serde_json::from_slice::<Posting>(payload) {
                    best.insert(id, Some(posting));
                }
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
            let idf = ((n as f64 - df + 0.5) / (df + 0.5) + 1.0).ln();

            for p in &postings {
                let tf = p.term_freq as f64;
                let dl = p.field_len as f64;
                let tf_norm = (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avgdl));
                *scores.entry(p.doc_id.clone()).or_insert(0.0) += idf * tf_norm;
            }
        }

        let mut ranked: Vec<_> = scores.into_iter().collect();
        ranked.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
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
    fn remove_doc_hides_postings() {
        let (mut idx, path) = make_idx("tomb");
        idx.index_doc(b"d1", &[(b"hello".to_vec(), 2)], 8).unwrap();
        idx.remove_doc(b"d1", &[b"hello".to_vec()]).unwrap();
        let postings = idx.postings_for_term(b"hello").unwrap();
        assert!(postings.is_empty(), "deleted postings are hidden");
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
    fn arbitrary_byte_terms_are_accepted() {
        let (mut idx, path) = make_idx("arb");
        let term = vec![0xFF, 0x00, 0xFF];
        idx.index_doc(b"d", &[(term.clone(), 1)], 5).unwrap();
        let postings = idx.postings_for_term(&term).unwrap();
        assert_eq!(postings.len(), 1);
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
