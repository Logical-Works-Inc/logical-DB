//! Owns the Edge storage class port: graph adjacency as prefix-scanned
//! rows with bidirectional indexing. INV-EDGE-BIDIRECTIONAL: every
//! `link` writes both the forward `(src→dst)` and reverse `(dst←src)`
//! rows in one atomic batch — they are never out of sync.
//!
//! Key layout (arroy-inspired u8-packed keys, 0xFF separator):
//! - Forward: `[0x03][src][0xFF][dst][ts BE]`
//! - Reverse: `[0x03][0x00][dst][0xFF][src][ts BE]`
//!
//! The `0x00` byte after the class tag separates the reverse namespace.
//! Constraint: neither `src` nor `dst` may contain `0xFF`.

use std::collections::{HashSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::model::Class;
use crate::port::{ClockPort, KvPort};
use crate::store::{Ldb, Mutation};
use crate::{KernelError, Result};

// ── EdgeRow ──────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeRow {
    pub src: Vec<u8>,
    pub dst: Vec<u8>,
    pub ts: u64,
    pub payload: Vec<u8>,
}

// ── Adjacency port ───────────────────────────────────────────

/// Port trait for the Edge storage class. Engines are swappable behind
/// this; MemAdjacency is the deterministic reference impl.
pub trait Adjacency {
    fn link(&mut self, src: &[u8], dst: &[u8], payload: Vec<u8>) -> Result<u64>;
    /// Tombstone: writes zero-length payload for both directions.
    fn unlink(&mut self, src: &[u8], dst: &[u8]) -> Result<u64>;
    fn outgoing(&self, src: &[u8]) -> Result<Vec<EdgeRow>>;
    fn incoming(&self, dst: &[u8]) -> Result<Vec<EdgeRow>>;
    /// BFS k-hop neighborhood gather (GNN message-passing primitive).
    /// Iteration order is non-deterministic; callers needing stable
    /// output must sort the returned ids.
    fn neighbors_k_hop(&self, src: &[u8], k: usize) -> Result<Vec<Vec<u8>>>;
}

// ── Key construction ─────────────────────────────────────────

const EDGE_TAG: u8 = Class::Edge as u8;
const SEP: u8 = 0xFF;
const REV_MARKER: u8 = 0x00;

fn validate_id(id: &[u8]) -> Result<()> {
    if id.contains(&SEP) {
        return Err(KernelError::SeparatorInId);
    }
    Ok(())
}

/// `[0x03][src][0xFF]` — all forward edges from `src`.
pub fn forward_prefix(src: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(2 + src.len());
    p.push(EDGE_TAG);
    p.extend_from_slice(src);
    p.push(SEP);
    p
}

/// `[0x03][src][0xFF][dst][ts BE]` — one forward edge at a version.
#[cfg(test)]
fn forward_key(src: &[u8], dst: &[u8], ts: u64) -> Vec<u8> {
    let mut k = forward_prefix(src);
    k.extend_from_slice(dst);
    k.extend_from_slice(&ts.to_be_bytes());
    k
}

/// `[0x03][0x00][dst][0xFF]` — all reverse edges into `dst`.
pub fn reverse_prefix(dst: &[u8]) -> Vec<u8> {
    let mut p = Vec::with_capacity(3 + dst.len());
    p.push(EDGE_TAG);
    p.push(REV_MARKER);
    p.extend_from_slice(dst);
    p.push(SEP);
    p
}

/// `[0x03][0x00][dst][0xFF][src][ts BE]` — one reverse edge at a version.
#[cfg(test)]
fn reverse_key(dst: &[u8], src: &[u8], ts: u64) -> Vec<u8> {
    let mut k = reverse_prefix(dst);
    k.extend_from_slice(src);
    k.extend_from_slice(&ts.to_be_bytes());
    k
}

// ── Key parsing ──────────────────────────────────────────────

fn find_sep(key: &[u8], start: usize) -> Option<usize> {
    key[start..]
        .iter()
        .position(|&b| b == SEP)
        .map(|i| i + start)
}

/// Parses `[0x03][src][0xFF][dst][ts BE]` → `(src, dst, ts)`.
pub fn parse_forward_key(key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u64)> {
    if key.len() < 1 + 1 + 1 + 8 || key[0] != EDGE_TAG {
        return Err(KernelError::TruncatedRow);
    }
    let sep = find_sep(key, 1).ok_or(KernelError::TruncatedRow)?;
    let src = key[1..sep].to_vec();
    let after_sep = sep + 1;
    if key.len() < after_sep + 8 {
        return Err(KernelError::TruncatedRow);
    }
    let ts_start = key.len() - 8;
    let dst = key[after_sep..ts_start].to_vec();
    let ts = u64::from_be_bytes(key[ts_start..].try_into().unwrap());
    Ok((src, dst, ts))
}

/// Parses `[0x03][0x00][dst][0xFF][src][ts BE]` → `(dst, src, ts)`.
pub fn parse_reverse_key(key: &[u8]) -> Result<(Vec<u8>, Vec<u8>, u64)> {
    if key.len() < 2 + 1 + 1 + 8 || key[0] != EDGE_TAG || key[1] != REV_MARKER {
        return Err(KernelError::TruncatedRow);
    }
    let sep = find_sep(key, 2).ok_or(KernelError::TruncatedRow)?;
    let dst = key[2..sep].to_vec();
    let after_sep = sep + 1;
    if key.len() < after_sep + 8 {
        return Err(KernelError::TruncatedRow);
    }
    let ts_start = key.len() - 8;
    let src = key[after_sep..ts_start].to_vec();
    let ts = u64::from_be_bytes(key[ts_start..].try_into().unwrap());
    Ok((dst, src, ts))
}

// ── MemAdjacency ─────────────────────────────────────────────

pub struct MemAdjacency<S: KvPort, C: ClockPort> {
    store: Ldb<S, C>,
}

impl<S: KvPort, C: ClockPort> MemAdjacency<S, C> {
    pub fn new(store: Ldb<S, C>) -> Self {
        Self { store }
    }

    pub fn into_store(self) -> Ldb<S, C> {
        self.store
    }
}

impl<S: KvPort, C: ClockPort> Adjacency for MemAdjacency<S, C> {
    fn link(&mut self, src: &[u8], dst: &[u8], payload: Vec<u8>) -> Result<u64> {
        validate_id(src)?;
        validate_id(dst)?;
        // INV-EDGE-BIDIRECTIONAL: forward + reverse in one atomic batch.
        let fwd_id = {
            let mut id = src.to_vec();
            id.push(SEP);
            id.extend_from_slice(dst);
            id
        };
        let rev_id = {
            let mut id = vec![REV_MARKER];
            id.extend_from_slice(dst);
            id.push(SEP);
            id.extend_from_slice(src);
            id
        };
        self.store.commit(&[
            Mutation::new(Class::Edge, fwd_id, payload.clone()),
            Mutation::new(Class::Edge, rev_id, payload),
        ])
    }

    fn unlink(&mut self, src: &[u8], dst: &[u8]) -> Result<u64> {
        validate_id(src)?;
        validate_id(dst)?;
        let fwd_id = {
            let mut id = src.to_vec();
            id.push(SEP);
            id.extend_from_slice(dst);
            id
        };
        let rev_id = {
            let mut id = vec![REV_MARKER];
            id.extend_from_slice(dst);
            id.push(SEP);
            id.extend_from_slice(src);
            id
        };
        self.store.commit(&[
            Mutation::new(Class::Edge, fwd_id, vec![]),
            Mutation::new(Class::Edge, rev_id, vec![]),
        ])
    }

    fn outgoing(&self, src: &[u8]) -> Result<Vec<EdgeRow>> {
        let prefix = forward_prefix(src);
        let raw = self.store.kv().scan_prefix(&prefix)?;
        let mut edges = Vec::new();
        for (key, payload) in raw {
            let (_, dst, ts) = parse_forward_key(&key)?;
            edges.push(EdgeRow {
                src: src.to_vec(),
                dst,
                ts,
                payload,
            });
        }
        Ok(edges)
    }

    fn incoming(&self, dst: &[u8]) -> Result<Vec<EdgeRow>> {
        let prefix = reverse_prefix(dst);
        let raw = self.store.kv().scan_prefix(&prefix)?;
        let mut edges = Vec::new();
        for (key, payload) in raw {
            let (_, src, ts) = parse_reverse_key(&key)?;
            edges.push(EdgeRow {
                src,
                dst: dst.to_vec(),
                ts,
                payload,
            });
        }
        Ok(edges)
    }

    fn neighbors_k_hop(&self, src: &[u8], k: usize) -> Result<Vec<Vec<u8>>> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        visited.insert(src.to_vec());
        queue.push_back(src.to_vec());
        for _ in 0..k {
            let level_size = queue.len();
            for _ in 0..level_size {
                let node = queue.pop_front().unwrap();
                for edge in self.outgoing(&node)? {
                    if visited.insert(edge.dst.clone()) {
                        queue.push_back(edge.dst);
                    }
                }
            }
        }
        visited.remove(src);
        Ok(visited.into_iter().collect())
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
        let p = std::env::temp_dir().join(format!("ldb-adj-{}-{tag}.journal", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn make_adj(tag: &str) -> (MemAdjacency<MemKv, Seq>, PathBuf) {
        let path = tmp_path(tag);
        let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
        (MemAdjacency::new(store), path)
    }

    #[test]
    fn link_then_outgoing_roundtrips() {
        let (mut adj, path) = make_adj("out");
        adj.link(b"alice", b"bob", b"friend".to_vec()).unwrap();
        let edges = adj.outgoing(b"alice").unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].src, b"alice");
        assert_eq!(edges[0].dst, b"bob");
        assert_eq!(edges[0].payload, b"friend");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bidirectional_invariant_holds() {
        let (mut adj, path) = make_adj("bidi");
        adj.link(b"a", b"b", b"edge".to_vec()).unwrap();
        let out = adj.outgoing(b"a").unwrap();
        let inc = adj.incoming(b"b").unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(inc.len(), 1);
        assert_eq!(out[0].src, inc[0].src);
        assert_eq!(out[0].dst, inc[0].dst);
        assert_eq!(out[0].payload, inc[0].payload);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unlink_writes_tombstones() {
        let (mut adj, path) = make_adj("unlink");
        adj.link(b"x", b"y", b"live".to_vec()).unwrap();
        adj.unlink(b"x", b"y").unwrap();
        let edges = adj.outgoing(b"x").unwrap();
        let latest = edges.last().unwrap();
        assert!(latest.payload.is_empty(), "tombstone has empty payload");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn k_hop_traverses_a_chain() {
        let (mut adj, path) = make_adj("khop");
        // a -> b -> c -> d
        adj.link(b"a", b"b", vec![]).unwrap();
        adj.link(b"b", b"c", vec![]).unwrap();
        adj.link(b"c", b"d", vec![]).unwrap();
        let hop1 = adj.neighbors_k_hop(b"a", 1).unwrap();
        assert_eq!(hop1.len(), 1);
        assert!(hop1.contains(&b"b".to_vec()));
        let hop2 = adj.neighbors_k_hop(b"a", 2).unwrap();
        assert_eq!(hop2.len(), 2);
        assert!(hop2.contains(&b"b".to_vec()));
        assert!(hop2.contains(&b"c".to_vec()));
        let hop3 = adj.neighbors_k_hop(b"a", 3).unwrap();
        assert_eq!(hop3.len(), 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn separator_byte_in_id_is_rejected() {
        let (mut adj, path) = make_adj("sep");
        let bad_id = &[0x01, 0xFF, 0x02];
        let err = adj.link(bad_id, b"ok", vec![]).unwrap_err();
        assert!(matches!(err, KernelError::SeparatorInId));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn forward_key_roundtrips_through_parse() {
        let key = forward_key(b"src", b"dst", 42);
        let (src, dst, ts) = parse_forward_key(&key).unwrap();
        assert_eq!(src, b"src");
        assert_eq!(dst, b"dst");
        assert_eq!(ts, 42);
    }

    #[test]
    fn reverse_key_roundtrips_through_parse() {
        let key = reverse_key(b"dst", b"src", 99);
        let (dst, src, ts) = parse_reverse_key(&key).unwrap();
        assert_eq!(dst, b"dst");
        assert_eq!(src, b"src");
        assert_eq!(ts, 99);
    }
}
