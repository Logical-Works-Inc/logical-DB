//! Owns the Edge storage class port: graph adjacency as prefix-scanned
//! rows with bidirectional indexing. INV-EDGE-BIDIRECTIONAL: every
//! `link` writes both the forward `(src->dst)` and reverse `(dst<-src)`
//! rows in one atomic batch — they are never out of sync.
//!
//! Key layout (length-prefixed composite, no reserved bytes):
//! - Forward: namespace 0x01: `[0x01][len(src) u16 BE][src][len(dst) u16 BE][dst]`
//! - Reverse: namespace 0x02: `[0x02][len(dst) u16 BE][dst][len(src) u16 BE][src]`

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use serde::{Deserialize, Serialize};

use crate::model::{self, Class, Op};
use crate::port::{ClockPort, KvPort};
use crate::store::{Ldb, Mutation};
use crate::Result;

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
    fn unlink(&mut self, src: &[u8], dst: &[u8]) -> Result<u64>;
    fn outgoing(&self, src: &[u8]) -> Result<Vec<EdgeRow>>;
    fn incoming(&self, dst: &[u8]) -> Result<Vec<EdgeRow>>;
    fn neighbors_k_hop(&self, src: &[u8], k: usize, budget: usize) -> Result<Vec<Vec<u8>>>;
}

// ── Key construction ─────────────────────────────────────────

const FWD_NS: u8 = 0x01;
const REV_NS: u8 = 0x02;

fn forward_id(src: &[u8], dst: &[u8]) -> Result<Vec<u8>> {
    model::encode_composite(FWD_NS, &[src, dst])
}

fn reverse_id(dst: &[u8], src: &[u8]) -> Result<Vec<u8>> {
    model::encode_composite(REV_NS, &[dst, src])
}

fn forward_scan_prefix(src: &[u8]) -> Result<Vec<u8>> {
    model::composite_prefix(FWD_NS, src)
}

fn reverse_scan_prefix(dst: &[u8]) -> Result<Vec<u8>> {
    model::composite_prefix(REV_NS, dst)
}

type EdgeState = (Vec<u8>, Vec<u8>, u64, Op);

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

    fn scan_live_edges(
        &self,
        kv_prefix: Vec<u8>,
        parse: impl Fn(&[u8]) -> Result<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<EdgeRow>> {
        let full_prefix = model::class_id_prefix(Class::Edge, &kv_prefix);
        let raw = self.store.kv().scan_prefix(&full_prefix)?;
        // BTreeMap keeps the newest version per composite id and hands
        // the live edges back in ascending key order — deterministic
        // reference order (DB_PATHWAY §4).
        let mut latest: BTreeMap<Vec<u8>, EdgeState> = BTreeMap::new();
        for (key, stored) in raw {
            let (_, id, ts) = model::split_key(&key)?;
            let op = if !stored.is_empty() && stored[0] == 2 {
                Op::Delete
            } else {
                Op::Put(stored[1..].to_vec())
            };
            let (src, dst) = parse(&id)?;
            let composite_id = id.clone();
            latest
                .entry(composite_id)
                .and_modify(|e| {
                    if ts >= e.2 {
                        *e = (src.clone(), dst.clone(), ts, op.clone());
                    }
                })
                .or_insert((src, dst, ts, op));
        }
        Ok(latest
            .into_values()
            .filter_map(|(src, dst, ts, op)| match op {
                Op::Put(payload) => Some(EdgeRow {
                    src,
                    dst,
                    ts,
                    payload,
                }),
                Op::Delete => None,
            })
            .collect())
    }
}

fn parse_forward(id: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let (_, parts) = model::decode_composite(id)?;
    if parts.len() != 2 {
        return Err(crate::KernelError::MalformedCompositeKey);
    }
    Ok((parts[0].clone(), parts[1].clone()))
}

fn parse_reverse(id: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let (_, parts) = model::decode_composite(id)?;
    if parts.len() != 2 {
        return Err(crate::KernelError::MalformedCompositeKey);
    }
    // Stored as (dst, src) in reverse namespace
    Ok((parts[1].clone(), parts[0].clone()))
}

impl<S: KvPort, C: ClockPort> Adjacency for MemAdjacency<S, C> {
    fn link(&mut self, src: &[u8], dst: &[u8], payload: Vec<u8>) -> Result<u64> {
        let fwd = forward_id(src, dst)?;
        let rev = reverse_id(dst, src)?;
        self.store.commit(&[
            Mutation::put(Class::Edge, fwd, payload.clone()),
            Mutation::put(Class::Edge, rev, payload),
        ])
    }

    fn unlink(&mut self, src: &[u8], dst: &[u8]) -> Result<u64> {
        let fwd = forward_id(src, dst)?;
        let rev = reverse_id(dst, src)?;
        self.store.commit(&[
            Mutation::delete(Class::Edge, fwd),
            Mutation::delete(Class::Edge, rev),
        ])
    }

    fn outgoing(&self, src: &[u8]) -> Result<Vec<EdgeRow>> {
        let prefix = forward_scan_prefix(src)?;
        self.scan_live_edges(prefix, parse_forward)
    }

    fn incoming(&self, dst: &[u8]) -> Result<Vec<EdgeRow>> {
        let prefix = reverse_scan_prefix(dst)?;
        self.scan_live_edges(prefix, parse_reverse)
    }

    fn neighbors_k_hop(&self, src: &[u8], k: usize, budget: usize) -> Result<Vec<Vec<u8>>> {
        // BTreeSet makes the returned neighbor order deterministic —
        // ascending by id on both exit paths (budget cut and full
        // traversal alike).
        let mut visited = BTreeSet::new();
        let mut queue = VecDeque::new();
        let mut result_count = 0usize;
        visited.insert(src.to_vec());
        queue.push_back(src.to_vec());
        for _ in 0..k {
            let level_size = queue.len();
            for _ in 0..level_size {
                let node = queue.pop_front().unwrap();
                for edge in self.outgoing(&node)? {
                    if visited.insert(edge.dst.clone()) {
                        result_count += 1;
                        if result_count >= budget {
                            visited.remove(src);
                            return Ok(visited.into_iter().collect());
                        }
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
    fn unlink_hides_from_live_queries() {
        let (mut adj, path) = make_adj("unlink");
        adj.link(b"x", b"y", b"live".to_vec()).unwrap();
        adj.unlink(b"x", b"y").unwrap();
        let edges = adj.outgoing(b"x").unwrap();
        assert!(
            edges.is_empty(),
            "unlinked edge must not appear in live query"
        );
        let inc = adj.incoming(b"y").unwrap();
        assert!(inc.is_empty(), "reverse side also hidden");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn k_hop_traverses_a_chain() {
        let (mut adj, path) = make_adj("khop");
        adj.link(b"a", b"b", vec![]).unwrap();
        adj.link(b"b", b"c", vec![]).unwrap();
        adj.link(b"c", b"d", vec![]).unwrap();
        let hop1 = adj.neighbors_k_hop(b"a", 1, 100).unwrap();
        assert_eq!(hop1.len(), 1);
        assert!(hop1.contains(&b"b".to_vec()));
        let hop2 = adj.neighbors_k_hop(b"a", 2, 100).unwrap();
        assert_eq!(hop2.len(), 2);
        let hop3 = adj.neighbors_k_hop(b"a", 3, 100).unwrap();
        assert_eq!(hop3.len(), 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn k_hop_respects_budget() {
        let (mut adj, path) = make_adj("budget");
        adj.link(b"a", b"b", vec![]).unwrap();
        adj.link(b"a", b"c", vec![]).unwrap();
        adj.link(b"a", b"d", vec![]).unwrap();
        let results = adj.neighbors_k_hop(b"a", 1, 2).unwrap();
        assert_eq!(results.len(), 2, "budget caps the result set");
        // Exact order on the budget-cut path too: ascending by id.
        assert_eq!(results, [b"b".to_vec(), b"c".to_vec()]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn outgoing_edges_come_back_in_ascending_dst_order() {
        let (mut adj, path) = make_adj("order");
        for other in ["zeta", "alpha", "mid"] {
            adj.link(b"n", other.as_bytes(), vec![]).unwrap();
            adj.link(other.as_bytes(), b"n", vec![]).unwrap();
        }
        let edges = adj.outgoing(b"n").unwrap();
        // Order is ascending composite-key order — length-prefix first,
        // so shorter dsts sort before longer ones. Deterministic either
        // way; this pins it.
        let dsts: Vec<_> = edges.iter().map(|e| e.dst.as_slice()).collect();
        assert_eq!(dsts, [&b"mid"[..], b"zeta", b"alpha"]);
        let inc = adj.incoming(b"n").unwrap();
        let srcs: Vec<_> = inc.iter().map(|e| e.src.as_slice()).collect();
        assert_eq!(srcs, [&b"mid"[..], b"zeta", b"alpha"]);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn arbitrary_byte_ids_are_accepted() {
        let (mut adj, path) = make_adj("arb");
        let src = &[0xFF, 0x00, 0xFF];
        let dst = &[0x00, 0xFF, 0x00];
        adj.link(src, dst, b"ok".to_vec()).unwrap();
        let edges = adj.outgoing(src).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].dst, dst);
        let inc = adj.incoming(dst).unwrap();
        assert_eq!(inc.len(), 1);
        assert_eq!(inc[0].src, src);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unlinked_edge_is_not_traversed() {
        let (mut adj, path) = make_adj("no-traverse");
        adj.link(b"a", b"b", vec![]).unwrap();
        adj.link(b"b", b"c", vec![]).unwrap();
        adj.unlink(b"a", b"b").unwrap();
        let hop = adj.neighbors_k_hop(b"a", 2, 100).unwrap();
        assert!(hop.is_empty(), "deleted edge must not be traversed");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn forward_reverse_exact_inverse_after_mixed_ops() {
        let (mut adj, path) = make_adj("inverse");
        adj.link(b"a", b"b", vec![]).unwrap();
        adj.link(b"a", b"c", vec![]).unwrap();
        adj.link(b"b", b"c", vec![]).unwrap();
        adj.unlink(b"a", b"c").unwrap();
        let out_a: BTreeSet<Vec<u8>> = adj
            .outgoing(b"a")
            .unwrap()
            .into_iter()
            .map(|e| e.dst)
            .collect();
        let inc_b: BTreeSet<Vec<u8>> = adj
            .incoming(b"b")
            .unwrap()
            .into_iter()
            .map(|e| e.src)
            .collect();
        assert!(out_a.contains(b"b".as_slice()));
        assert!(
            !out_a.contains(b"c".as_slice()),
            "unlinked edge gone from forward"
        );
        assert!(inc_b.contains(b"a".as_slice()));
        let inc_c: BTreeSet<Vec<u8>> = adj
            .incoming(b"c")
            .unwrap()
            .into_iter()
            .map(|e| e.src)
            .collect();
        assert!(inc_c.contains(b"b".as_slice()));
        assert!(
            !inc_c.contains(b"a".as_slice()),
            "unlinked edge gone from reverse"
        );
        let _ = std::fs::remove_file(&path);
    }
}
