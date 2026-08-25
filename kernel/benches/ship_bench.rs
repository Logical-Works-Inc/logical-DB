//! Owns the v0.1 benchmark suite (issue #6 §9): eight workloads over
//! the shipped default backend, one JSONL result line each — throughput,
//! p50/p95 latency, and journal size. No new dependencies: timing is
//! stdlib Instant; percentiles are sorted slices. The algorithmic gates
//! these results must respect live in docs/FORMAT.md.

use logical_db::adjacency::{Adjacency, MemAdjacency};
use logical_db::memkv::MemKv;
use logical_db::model::{self, Class};
use logical_db::port::ClockPort;
use logical_db::store::{Ldb, Mutation};
use logical_db::term::{MemTermIndex, TermIndex};
use logical_db::veccode::{MemVectorCodes, VectorCodes};
use std::cell::Cell;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Instant;

// ── Harness ──────────────────────────────────────────────────

struct Xorshift(u64);

impl Xorshift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct Seq(Cell<u64>);

impl ClockPort for Seq {
    fn now(&self) -> u64 {
        self.0.set(self.0.get() + 1);
        self.0.get()
    }
}

/// Fresh journal path; any prior run's bytes are removed first.
fn fresh_journal(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("ldb-bench-{tag}.journal"));
    let _ = std::fs::remove_file(&p);
    p
}

fn db_bytes(path: &Path) -> u128 {
    std::fs::metadata(path)
        .map(|m| m.len() as u128)
        .unwrap_or(0)
}

/// One JSONL line per workload: schema documented in docs/FORMAT.md.
fn bench(workload: &str, samples: Vec<u128>, ops_per_sample: u128, path: &Path) {
    assert!(!samples.is_empty(), "a workload with no samples is void");
    let mut sorted = samples;
    sorted.sort_unstable();
    let pick = |frac: f64| sorted[((sorted.len() as f64 - 1.0) * frac) as usize];
    let total_ns: u128 = sorted.iter().sum();
    let total_ops = ops_per_sample * sorted.len() as u128;
    println!(
        "{{\"workload\":\"{workload}\",\"samples\":{},\"ops\":{},\
         \"throughput_ops_per_s\":{:.0},\"p50_ns\":{},\"p95_ns\":{},\
         \"db_bytes\":{}}}",
        sorted.len(),
        total_ops,
        total_ops as f64 / (total_ns as f64 / 1e9),
        pick(0.50),
        pick(0.95),
        db_bytes(path),
    );
}

fn time_ns<T>(f: impl FnOnce() -> T) -> (u128, T) {
    let t = Instant::now();
    let out = f();
    (t.elapsed().as_nanos(), out)
}

// ── W1/W2 — record bulk insert + random latest reads ────────

const RECORDS: usize = 1_000_000;
const RECORD_BATCH: usize = 1_000;

fn build_record_store(tag: &str) -> (Ldb<MemKv, Seq>, PathBuf) {
    let path = fresh_journal(tag);
    let mut db = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
    for batch in 0..RECORDS / RECORD_BATCH {
        let mutations: Vec<Mutation> = (0..RECORD_BATCH)
            .map(|i| {
                let n = batch * RECORD_BATCH + i;
                Mutation::put(
                    Class::Record,
                    format!("r/{n:07}").into_bytes(),
                    vec![n as u8; 64],
                )
            })
            .collect();
        db.commit(&mutations).unwrap();
    }
    (db, path)
}

fn record_workloads() {
    // W1: the insert number is measured on a fresh identical build.
    let (ns, timed_path) = time_ns(|| {
        let (db, path) = build_record_store("record-timed");
        drop(db);
        path
    });
    bench("record_insert_1m", vec![ns], RECORDS as u128, &timed_path);

    // W2: random latest reads over an independently built store.
    let (db, path) = build_record_store("record-main");
    let mut rng = Xorshift(0xB1_2026_0825);
    let mut samples = Vec::new();
    for _ in 0..20_000u32 {
        let id = format!("r/{:07}", rng.below(RECORDS as u64)).into_bytes();
        let (ns, _) = time_ns(|| db.latest(Class::Record, &id).unwrap());
        samples.push(ns);
    }
    bench("record_random_latest_reads", samples, 1, &path);
    let _ = std::fs::remove_file(&path);
}

// ── W3 — as-of reads over highly versioned keys ──────────────

fn history_as_of_reads() {
    const KEYS: usize = 1_000;
    const VERSIONS: usize = 100;
    let path = fresh_journal("history");
    let mut db = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
    for v in 0..VERSIONS {
        let mutations: Vec<Mutation> = (0..KEYS)
            .map(|k| {
                Mutation::put(
                    Class::Record,
                    format!("h/{k:05}").into_bytes(),
                    vec![v as u8; 16],
                )
            })
            .collect();
        db.commit(&mutations).unwrap();
    }
    let mut rng = Xorshift(0xC7_2026_0825);
    let mut samples = Vec::new();
    for _ in 0..10_000u32 {
        let k = rng.below(KEYS as u64);
        let ts = 1 + rng.below(VERSIONS as u64);
        let id = format!("h/{k:05}").into_bytes();
        let (ns, _) = time_ns(|| db.get(Class::Record, &id, ts).unwrap());
        samples.push(ns);
    }
    bench("history_as_of_reads_100_versions", samples, 1, &path);
    let _ = std::fs::remove_file(&path);
}

// ── W4 — adjacency over low- and high-degree nodes ───────────

// Mirrors adjacency.rs namespace bytes; the scan-count asserts below
// fail loudly if that layout ever drifts from this seeding.
const FWD_NS: u8 = 0x01;
const REV_NS: u8 = 0x02;

fn edge_rows(src: &[u8], dst: &[u8]) -> Vec<Mutation> {
    vec![
        Mutation::put(
            Class::Edge,
            model::encode_composite(FWD_NS, &[src, dst]).unwrap(),
            b"e".to_vec(),
        ),
        Mutation::put(
            Class::Edge,
            model::encode_composite(REV_NS, &[dst, src]).unwrap(),
            b"e".to_vec(),
        ),
    ]
}

fn adjacency_workloads(path: &Path) {
    const HUB_DEGREE: usize = 200_000;
    const SPREAD_NODES: u64 = 800;
    const SPREAD_PER: usize = 999;

    let mut seed_db = Ldb::open(path, MemKv::new(), Seq(Cell::new(10_000_000))).unwrap();
    let hub = b"node/hub".to_vec();
    let mut batch: Vec<Mutation> = Vec::new();
    for i in 0..HUB_DEGREE {
        batch.extend(edge_rows(&hub, format!("n/{i:06}").as_bytes()));
        if batch.len() >= 1_000 {
            seed_db.commit(&batch).unwrap();
            batch.clear();
        }
    }
    for s in 0..SPREAD_NODES {
        for d in 0..SPREAD_PER as u64 {
            let dst = format!("d/{d:06}");
            batch.extend(edge_rows(format!("s/{s:03}").as_bytes(), dst.as_bytes()));
            if batch.len() >= 1_000 {
                seed_db.commit(&batch).unwrap();
                batch.clear();
            }
        }
    }
    if !batch.is_empty() {
        seed_db.commit(&batch).unwrap();
    }

    let adj = MemAdjacency::new(seed_db);
    let low = b"s/000";
    let (ns_low, low_out) = time_ns(|| adj.outgoing(low).unwrap());
    assert_eq!(low_out.len(), SPREAD_PER, "layout drift: low-degree scan");
    bench("adjacency_scan_low_degree_999", vec![ns_low], 1, path);

    let (ns_hub, hub_out) = time_ns(|| adj.outgoing(&hub).unwrap());
    assert_eq!(hub_out.len(), HUB_DEGREE, "layout drift: hub scan");
    bench("adjacency_scan_hub_200k", vec![ns_hub], 1, path);

    let mid = b"d/000500";
    let (ns_inc, inc) = time_ns(|| adj.incoming(mid).unwrap());
    // Every one of the SPREAD_NODES sources links this dst once.
    assert_eq!(
        inc.len(),
        SPREAD_NODES as usize,
        "layout drift: incoming scan"
    );
    bench("adjacency_incoming_degree_800", vec![ns_inc], 1, path);

    // Degree-proportional gate (§9): cost per row must stay flat as
    // degree grows; FORMAT.md pins the ratio bound.
    let per_row_hub = ns_hub / HUB_DEGREE.max(1) as u128;
    println!(
        "{{\"workload\":\"adjacency_cost_per_row_ns\",\"node\":\"hub\",\
         \"degree\":{HUB_DEGREE},\"value\":{per_row_hub}}}"
    );
}

// ── W5 — BM25 corpus ingest + 1/3/8-term queries ─────────────

fn term_corpus_and_queries() {
    let path = fresh_journal("term");
    let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
    let mut idx = MemTermIndex::new(store);
    let mut rng = Xorshift(0xD3_2026_0825);

    const DOCS: usize = 5_000;
    const VOCAB: u64 = 2_000;
    let mut ingest_samples: Vec<u128> = Vec::new();
    for d in 0..DOCS {
        let mut terms: BTreeSet<Vec<u8>> = BTreeSet::new();
        for _ in 0..30 {
            terms.insert(format!("t{}", rng.below(VOCAB)).into_bytes());
        }
        let postings: Vec<(Vec<u8>, u32)> = terms.into_iter().map(|t| (t, 1)).collect();
        let doc_id = format!("doc/{d:05}").into_bytes();
        let field_len = postings.len() as u32;
        let owned_id = doc_id.clone();
        let (ns, _) = time_ns(|| {
            idx.index_doc(&owned_id, &postings, field_len).unwrap();
        });
        ingest_samples.push(ns);
    }
    bench("term_ingest_docs", ingest_samples, 1, &path);

    let query = |terms: &[&[u8]], label: &str| {
        let owned: Vec<Vec<u8>> = terms.iter().map(|t| t.to_vec()).collect();
        let mut samples = Vec::new();
        for _ in 0..200u32 {
            let q = owned.clone();
            let (ns, hits) = time_ns(|| idx.search(&q, 10).unwrap());
            assert!(!hits.is_empty(), "{label} returned nothing");
            samples.push(ns);
        }
        bench(label, samples, 1, &path);
    };
    query(&[b"t42"], "term_query_1_term");
    query(&[b"t42", b"t777", b"t1999"], "term_query_3_terms");
    query(
        &[
            b"t42", b"t777", b"t1999", b"t5", b"t123", b"t900", b"t1600", b"t33",
        ],
        "term_query_8_terms",
    );
    let _ = std::fs::remove_file(&path);
}

// ── W6 — binary-code brute scan ──────────────────────────────

fn vector_code_scan() {
    let path = fresh_journal("vec");
    let store = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
    let mut vc = MemVectorCodes::new(store);
    let mut rng = Xorshift(0xE5_2026_0825);

    const VECTORS: usize = 5_000;
    const CODE_BYTES: usize = 32;
    for i in 0..VECTORS {
        let code: Vec<u8> = (0..CODE_BYTES).map(|_| rng.next() as u8).collect();
        vc.insert_vec(format!("v/{i:05}").as_bytes(), code, 1.0, (i % 64) as u32)
            .unwrap();
    }
    let q: Vec<u8> = (0..CODE_BYTES).map(|_| rng.next() as u8).collect();
    let mut samples = Vec::new();
    for _ in 0..200u32 {
        let qc = q.clone();
        let (ns, top) = time_ns(|| vc.brute_scan(&qc, 10).unwrap());
        assert_eq!(top.len(), 10, "top-K must be full at k=10");
        samples.push(ns);
    }
    bench("veccode_brute_scan_5k_x32b_top10", samples, 1, &path);
    let _ = std::fs::remove_file(&path);
}

// ── W8 — mixed transaction across four classes ───────────────

fn mixed_txn() {
    let path = fresh_journal("mixed");
    let mut db = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
    const BATCHES: usize = 2_000;
    const OPS_PER: usize = 6;
    let mut samples = Vec::new();
    for b in 0..BATCHES {
        let batch = vec![
            Mutation::put(
                Class::Record,
                format!("mr/{b:06}").into_bytes(),
                vec![b as u8; 48],
            ),
            Mutation::put(
                Class::Edge,
                format!("me/f{b:06}").into_bytes(),
                b"e".to_vec(),
            ),
            Mutation::put(
                Class::Edge,
                format!("me/r{b:06}").into_bytes(),
                b"e".to_vec(),
            ),
            Mutation::put(
                Class::Term,
                format!("mt/{b:06}").into_bytes(),
                b"p".to_vec(),
            ),
            Mutation::put(
                Class::VecCode,
                format!("mv/{b:06}").into_bytes(),
                vec![0u8; 32],
            ),
            Mutation::delete(Class::Record, format!("mr/del/{b:06}")),
        ];
        let (ns, _) = time_ns(|| db.commit(&batch).unwrap());
        samples.push(ns);
    }
    bench("mixed_txn_4_classes", samples, OPS_PER as u128, &path);
    let _ = std::fs::remove_file(&path);
}

// ── Entry ────────────────────────────────────────────────────

fn main() {
    record_workloads();
    history_as_of_reads();

    let edge_path = fresh_journal("edges");
    adjacency_workloads(&edge_path);
    // W7: reopen replays whichever journal is largest — the edges.
    let before = db_bytes(&edge_path);
    let (ns, reopened) =
        time_ns(|| Ldb::<MemKv, Seq>::open(&edge_path, MemKv::new(), Seq(Cell::new(0))).unwrap());
    let rows_restored = reopened.scan_class(Class::Edge).unwrap().len();
    drop(reopened);
    println!(
        "{{\"workload\":\"reopen_replay\",\"rows_restored\":{rows_restored},\
         \"p50_ns\":{ns},\"db_bytes\":{before}}}"
    );
    let _ = std::fs::remove_file(&edge_path);

    vector_code_scan();
    term_corpus_and_queries();
    mixed_txn();
}
