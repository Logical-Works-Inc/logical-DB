//! Property gauntlet for the spine (DONE conjunct 1 at the store
//! boundary): for a generated workload W and EVERY byte cut c of its
//! journal, reopening at c must yield exactly the fold of the batches
//! fully contained in c — never a partial batch, never a lost prefix.

use ldb_kernel::memkv::MemKv;
use ldb_kernel::model::{Class, Op};
use ldb_kernel::port::ClockPort;
use ldb_kernel::store::{Ldb, Mutation};
use std::cell::Cell;
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;

// ── Seeded generator (no dep: stdlib has no PRNG) ────────────

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

// ── Harness ──────────────────────────────────────────────────

type Latest = HashMap<(u8, Vec<u8>), (u64, Op)>;

fn tmp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ldb-prop-{}-{tag}.journal", std::process::id()))
}

fn fold_in(s: &mut Latest, batch: &[Mutation], ts: u64) {
    for m in batch {
        s.insert((m.class.tag(), m.id.clone()), (ts, m.op.clone()));
    }
}

fn recovered_latest(path: &PathBuf) -> Latest {
    let db = Ldb::open(path, MemKv::new(), CountingClock::default()).unwrap();
    let mut actual = Latest::new();
    for class in [
        Class::Record,
        Class::Term,
        Class::Edge,
        Class::VecCode,
        Class::Claim,
    ] {
        for row in db.scan_class(class).unwrap() {
            actual.insert((row.class.tag(), row.id.clone()), (row.ts, row.op.clone()));
        }
    }
    actual
}

#[test]
fn replay_of_every_byte_cut_equals_the_committed_prefix_fold() {
    let mut rng = Xorshift(0x5EED_2026_0821);
    let ids: Vec<Vec<u8>> = (0..6).map(|i| format!("e{i}").into_bytes()).collect();
    let classes = [
        Class::Record,
        Class::Term,
        Class::Edge,
        Class::VecCode,
        Class::Claim,
    ];

    let path = tmp_path("main");
    let _ = std::fs::remove_file(&path);

    let mut expected: Vec<Latest> = vec![Latest::new()];
    let mut batch_ends: Vec<u64> = Vec::new();
    {
        let mut db = Ldb::open(&path, MemKv::new(), CountingClock::default()).unwrap();
        let mut fold = Latest::new();
        for _ in 0..12 {
            let n = 1 + rng.below(4) as usize;
            let batch: Vec<Mutation> = (0..n)
                .map(|_| {
                    let id = ids[rng.below(ids.len() as u64) as usize].clone();
                    let class = classes[rng.below(classes.len() as u64) as usize];
                    if rng.below(5) == 0 {
                        Mutation::delete(class, id)
                    } else {
                        let len = rng.below(17) as usize;
                        Mutation::put(class, id, (0..len).map(|_| rng.next() as u8).collect())
                    }
                })
                .collect();
            let ts = db.commit(&batch).unwrap();
            fold_in(&mut fold, &batch, ts);
            expected.push(fold.clone());
            batch_ends.push(File::open(&path).unwrap().metadata().unwrap().len());
        }
    }

    let bytes = std::fs::read(&path).unwrap();
    let cut_path = tmp_path("cut");
    for cut in 0..=bytes.len() {
        std::fs::write(&cut_path, &bytes[..cut]).unwrap();
        let k = batch_ends.iter().filter(|e| **e <= cut as u64).count();
        let actual = recovered_latest(&cut_path);
        assert_eq!(
            actual,
            expected[k],
            "cut {cut}/{len} recovered batch prefix {k}",
            len = bytes.len()
        );
    }

    let mid = batch_ends[5] as usize - 1;
    std::fs::write(&cut_path, &bytes[..mid]).unwrap();
    let _ = recovered_latest(&cut_path);
    assert_eq!(
        File::open(&cut_path).unwrap().metadata().unwrap().len(),
        batch_ends[4],
        "the torn sixth frame is cut back to the fifth boundary"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&cut_path);
}

// ── Clock ────────────────────────────────────────────────────

#[derive(Default)]
struct CountingClock(Cell<u64>);

impl ClockPort for CountingClock {
    fn now(&self) -> u64 {
        self.0.set(self.0.get() + 1);
        self.0.get()
    }
}
