//! The v0.1 library journey in one runnable file: open → mixed write
//! txn → query → close/reopen → verify. A fresh clone runs this with
//! zero external services (GOVERNANCE §2 out-of-box rule).

use logical_db::memkv::MemKv;
use logical_db::model::{Class, Op};
use logical_db::port::ClockPort;
use logical_db::store::{Ldb, Mutation};
use std::cell::Cell;

// ── Clock ────────────────────────────────────────────────────

/// One tick per commit; strictly monotonic by construction.
struct Seq(Cell<u64>);

impl ClockPort for Seq {
    fn now(&self) -> u64 {
        self.0.set(self.0.get() + 1);
        self.0.get()
    }
}

// ── Journey ──────────────────────────────────────────────────

fn main() {
    let path =
        std::env::temp_dir().join(format!("logical-db-example-{}.journal", std::process::id()));
    let _ = std::fs::remove_file(&path);

    // Open: one journal file, zero daemons.
    let mut db = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();

    // Seed then delete inside one batch: explicit deletion is part of
    // the model, not an afterthought.
    db.commit(&[Mutation::put(Class::Record, "doc/tmp", b"scratch".to_vec())])
        .unwrap();

    // Mixed write txn: four classes in one atomic commit.
    let ts = db
        .commit(&[
            Mutation::put(Class::Record, "doc/1", br#"{"title":"hello"}"#.to_vec()),
            Mutation::put(Class::Edge, "edge/1->2", b"{}".to_vec()),
            Mutation::put(Class::VecCode, "vec/1", vec![0b1011_0001]),
            Mutation::put(
                Class::Claim,
                "claim/1",
                br#"{"subject":"doc/1","predicate":"mentions"}"#.to_vec(),
            ),
            Mutation::delete(Class::Record, "doc/tmp"),
        ])
        .unwrap();
    println!("committed mixed batch at ts {ts}");

    // Query through the read surface: scans expose full version
    // history; latest() collapses it to live state.
    let history = db.scan_class(Class::Record).unwrap();
    assert_eq!(history.len(), 3, "two versions of doc/tmp + one of doc/1");
    assert_eq!(
        db.latest(Class::Record, b"doc/tmp").unwrap(),
        None,
        "the tombstoned id must not read"
    );
    let (_, payload) = db.latest(Class::Record, b"doc/1").unwrap().unwrap();
    assert_eq!(payload, br#"{"title":"hello"}"#.to_vec());
    let (row_ts, vec_payload) = db.latest(Class::VecCode, b"vec/1").unwrap().unwrap();
    assert_eq!(row_ts, ts);
    assert_eq!(vec_payload, vec![0b1011_0001]);
    println!("queries read {} record versions", history.len());

    // Close and reopen: state comes back from journal replay alone.
    let classes = [Class::Record, Class::Edge, Class::VecCode, Class::Claim];
    let before: Vec<_> = classes
        .iter()
        .map(|&c| (c, db.scan_class(c).unwrap()))
        .collect();
    let (_kv, _clock) = db.into_parts();
    let reopened = Ldb::open(&path, MemKv::new(), Seq(Cell::new(0))).unwrap();
    for (class, rows) in &before {
        assert_eq!(
            &reopened.scan_class(*class).unwrap(),
            rows,
            "replay must restore {class:?} exactly"
        );
    }
    assert_eq!(
        reopened.latest_with_op(Class::Claim, b"claim/1").unwrap(),
        Some((
            ts,
            Op::Put(br#"{"subject":"doc/1","predicate":"mentions"}"#.to_vec())
        ))
    );

    println!("reopen verified: journal replay restored every class");
    let _ = std::fs::remove_file(&path);
}
