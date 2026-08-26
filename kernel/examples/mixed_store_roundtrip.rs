//! The v0.1 library journey in one runnable file: open → mixed write
//! txn → query → close/reopen → verify. A fresh clone runs this with
//! zero external services (GOVERNANCE §2 out-of-box rule).

use logical_db::model::{Class, Op};
use logical_db::store::{Ldb, Mutation};

// ── Journey ──────────────────────────────────────────────────

fn main() {
    let path =
        std::env::temp_dir().join(format!("logical-db-example-{}.journal", std::process::id()));
    let _ = std::fs::remove_file(&path);

    // Open: one journal file, zero daemons.
    // 1-click K,V — `open_at` collapses the 3-layer (path+Kv+Clock) to
    // just a path; `put_record` collapses `Class::Record + id + payload`
    // (K,V,x) to `K,V`. Advanced callers still use `Ldb::open`+`Mutation::put`.
    let mut db = Ldb::open_at(&path).unwrap();

    // K,V in one token — no Class, no x to supply (ts is the store's clock)
    db.put_record("doc/tmp", b"scratch".to_vec()).unwrap();

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

    // Query: `get_record` is the 1-click read (no Class, no ts)
    assert_eq!(db.get_record(b"doc/tmp").unwrap(), None);
    let payload = db.get_record(b"doc/1").unwrap().unwrap();
    assert_eq!(payload, br#"{"title":"hello"}"#.to_vec());
    // Full history still available when you need it
    let history = db.scan_class(Class::Record).unwrap();
    assert_eq!(history.len(), 3, "two versions of doc/tmp + one of doc/1");
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
    let reopened = Ldb::open_at(&path).unwrap();
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
