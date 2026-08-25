//! Owns the state-machine gauntlet at the store boundary: a generated
//! mixed workload across all five classes — empty payloads, arbitrary
//! byte ids, repeated writes inside one batch, reopen cycles — must
//! keep the production store exactly equal to an independently written
//! in-memory reference through every read path (INV-SM-REFERENCE).
//! The identical workload runs on both backends: the journaled
//! reference path and the durable redb backend (issue #6 §5 parity).

use logical_db::memkv::MemKv;
use logical_db::model::{Class, Op, Row};
use logical_db::port::{ClockPort, KvPort};
use logical_db::redb_backend::RedbBackend;
use logical_db::store::{Ldb, Mutation};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::rc::Rc;

// ── Seeded generator (stdlib has no PRNG) ────────────────────

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

// ── Clock that survives reopens ──────────────────────────────

/// Clones share one counter, so a reopened store continues the same
/// monotonic timeline instead of committing behind committed time.
#[derive(Clone, Default)]
struct SharedClock(Rc<Cell<u64>>);

impl ClockPort for SharedClock {
    fn now(&self) -> u64 {
        self.0.set(self.0.get() + 1);
        self.0.get()
    }
}

// ── Independent reference model ──────────────────────────────

/// One entity's full version history: `(ts, op)` in commit order.
type History = Vec<(u64, Op)>;

/// Full version history per `(class, id)`. Deliberately shares no code
/// with the kernel: agreement between two independently written models
/// is the evidence, not a tautology.
#[derive(Default)]
struct Reference {
    histories: BTreeMap<(u8, Vec<u8>), History>,
}

impl Reference {
    /// Last write wins per timestamp, mirroring one-batch-one-version:
    /// two writes to one key inside a batch leave a single row.
    fn apply(&mut self, batch: &[Mutation], ts: u64) {
        for m in batch {
            let history = self
                .histories
                .entry((m.class.tag(), m.id.clone()))
                .or_default();
            match history.last_mut() {
                Some((last_ts, op)) if *last_ts == ts => *op = m.op.clone(),
                _ => history.push((ts, m.op.clone())),
            }
        }
    }

    /// Every version of every entity in one class, in KV key order:
    /// id bytes ascending, then ts ascending within an id.
    fn rows(&self, class: Class) -> Vec<Row> {
        self.histories
            .iter()
            .filter(|((tag, _), h)| *tag == class.tag() && !h.is_empty())
            .flat_map(|((_, id), h)| {
                h.iter().map(move |&(ts, ref op)| Row {
                    class,
                    id: id.clone(),
                    ts,
                    op: op.clone(),
                })
            })
            .collect()
    }
}

// ── Full-surface comparison ──────────────────────────────────

/// Backend-generic: the same read-path assertions bind whichever
/// backend the store is opened over.
fn assert_reference_holds<S: KvPort, C: ClockPort>(db: &Ldb<S, C>, r: &Reference) {
    for class in [
        Class::Record,
        Class::Term,
        Class::Edge,
        Class::VecCode,
        Class::Claim,
    ] {
        assert_eq!(db.scan_class(class).unwrap(), r.rows(class));
    }
    for ((tag, id), history) in &r.histories {
        let class = Class::from_tag(*tag).unwrap();
        let &(latest_ts, ref latest_op) = history.last().unwrap();

        for &(ts, ref op) in history {
            let got = db.get(class, id, ts).unwrap();
            match op {
                Op::Put(p) => assert_eq!(got.as_deref(), Some(p.as_slice())),
                Op::Delete => assert_eq!(got, None, "a tombstone reads as none"),
            }
        }
        assert_eq!(
            db.get(class, id, latest_ts + 1).unwrap(),
            None,
            "a version that never existed reads as none"
        );
        match latest_op {
            Op::Put(p) => assert_eq!(db.latest(class, id).unwrap(), Some((latest_ts, p.clone()))),
            Op::Delete => assert_eq!(db.latest(class, id).unwrap(), None),
        }
        assert_eq!(
            db.latest_with_op(class, id).unwrap(),
            Some((latest_ts, latest_op.clone()))
        );
    }
}

// ── Workload ─────────────────────────────────────────────────

fn tmp_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ldb-sm-{}-{tag}.journal", std::process::id()))
}

/// The one batch generator both backend suites drive — identical
/// shapes so a backend difference can only be in the store, never in
/// the workload.
fn gen_batch(rng: &mut Xorshift) -> Vec<Mutation> {
    const CLASSES: [Class; 5] = [
        Class::Record,
        Class::Term,
        Class::Edge,
        Class::VecCode,
        Class::Claim,
    ];
    // Arbitrary byte ids including 0xFF, which the old separator codec
    // reserved, and lone 0x00.
    let mut ids: Vec<Vec<u8>> = (0..5).map(|i| format!("e{i}").into_bytes()).collect();
    ids.push(vec![0x00]);
    ids.push(vec![0xFF]);
    ids.push(vec![0xFF, 0x00, 0xFF]);

    let n = 1 + rng.below(4) as usize;
    (0..n)
        .map(|_| {
            let id = ids[rng.below(ids.len() as u64) as usize].clone();
            let class = CLASSES[rng.below(CLASSES.len() as u64) as usize];
            if rng.below(5) == 0 {
                Mutation::delete(class, id)
            } else {
                let len = rng.below(9) as usize;
                Mutation::put(class, id, (0..len).map(|_| rng.next() as u8).collect())
            }
        })
        .collect()
}

#[test]
fn generated_mixed_workload_stays_equal_to_the_reference_across_reopens() {
    let mut rng = Xorshift(0x5EED_2026_0825);
    let path = tmp_path("main");
    let _ = std::fs::remove_file(&path);

    let clock = SharedClock::default();
    let mut reference = Reference::default();
    let mut db = Ldb::open(&path, MemKv::new(), clock.clone()).unwrap();

    for step in 0..40usize {
        let batch = gen_batch(&mut rng);
        let ts = db.commit(&batch).unwrap();
        assert_eq!(ts, step as u64 + 1, "one tick per non-empty batch");
        reference.apply(&batch, ts);
        assert_reference_holds(&db, &reference);

        if step % 7 == 6 {
            // Reopen against a fresh empty KV so every read after this
            // point is served from journal replay, not carried memory.
            let (_kv, _clock) = db.into_parts();
            db = Ldb::open(&path, MemKv::new(), clock.clone()).unwrap();
            assert_reference_holds(&db, &reference);
        }
    }

    let _ = std::fs::remove_file(&path);
}

#[test]
fn the_durable_backend_serves_the_identical_workload_equal_to_the_reference() {
    let mut rng = Xorshift(0x5EED_2026_0825); // same seed, same batches
    let path = tmp_path("redb");
    let _ = std::fs::remove_file(&path);

    let clock = SharedClock::default();
    let mut reference = Reference::default();
    let reopen = |clock: SharedClock| -> Ldb<RedbBackend, SharedClock> {
        Ldb::open_durable(RedbBackend::open(&path).unwrap(), clock).unwrap()
    };
    let mut db = reopen(clock.clone());
    assert_eq!(
        db.scan_class(Class::Record).unwrap(),
        Vec::new(),
        "a fresh durable store reads as empty"
    );

    for step in 0..40usize {
        let batch = gen_batch(&mut rng);
        let ts = db.commit(&batch).unwrap();
        assert_eq!(ts, step as u64 + 1, "one tick per non-empty batch");
        reference.apply(&batch, ts);
        assert_reference_holds(&db, &reference);

        if step % 7 == 6 {
            // Reopen against the persisted file: every read after this
            // point comes out of redb's own crash story, no journal.
            drop(db);
            db = reopen(clock.clone());
            assert_eq!(
                db.last_ts(),
                ts,
                "the committed version seeds committed time across reopen"
            );
            assert_reference_holds(&db, &reference);
        }
    }

    let _ = std::fs::remove_file(&path);
}

#[test]
fn repeated_writes_to_one_key_inside_a_batch_collapse_to_the_last_write() {
    let path = tmp_path("lw");
    let _ = std::fs::remove_file(&path);
    let mut db = Ldb::open(&path, MemKv::new(), SharedClock::default()).unwrap();

    let ts = db
        .commit(&[
            Mutation::put(Class::Record, "k", b"first".to_vec()),
            Mutation::put(Class::Record, "k", b"second".to_vec()),
            Mutation::delete(Class::Record, "other"),
            Mutation::put(Class::Record, "other", b"revived".to_vec()),
        ])
        .unwrap();

    let rows = db.scan_class(Class::Record).unwrap();
    assert_eq!(rows.len(), 2, "two keys, one version each");
    assert_eq!(rows[0].id, b"k".to_vec());
    assert_eq!(rows[0].ts, ts);
    assert_eq!(rows[0].op, Op::Put(b"second".to_vec()));
    assert_eq!(rows[1].id, b"other".to_vec());
    assert_eq!(rows[1].ts, ts);
    assert_eq!(rows[1].op, Op::Put(b"revived".to_vec()));
    assert_eq!(
        db.get(Class::Record, b"k", ts).unwrap().as_deref(),
        Some(b"second".as_slice())
    );

    let _ = std::fs::remove_file(&path);
}
