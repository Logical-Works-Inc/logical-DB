//! Owns the process-kill fault harness: a child process commits
//! deterministic batches until SIGKILL lands mid-flight; every reopen
//! must observe either the full committed prefix or the prior state —
//! never a partial batch, never a gap in the durable marker sequence
//! (INV-KILL-ALL-OR-NOTHING).

use logical_db::memkv::MemKv;
use logical_db::model::{Class, Op};
use logical_db::port::ClockPort;
use logical_db::store::{Ldb, Mutation};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ── Parent/child contract ────────────────────────────────────

const CHILD_ENV: &str = "LDB_KILL_ROLE";
const PATH_ENV: &str = "LDB_KILL_JOURNAL";
const ROLE_WRITER: &str = "writer";
const ITERATIONS: usize = 12;
/// Batch `k` plants `batch/{k:05}` plus its `/pair` sibling in one
/// commit; both present or both absent proves batch atomicity.
const MARKER_PREFIX: &[u8] = b"batch/";
const PAIR_SUFFIX: &[u8] = b"/pair";
const CHILD_BATCH_CAP: u64 = 5000;

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

#[derive(Default)]
struct CountingClock(Cell<u64>);

impl ClockPort for CountingClock {
    fn now(&self) -> u64 {
        self.0.set(self.0.get() + 1);
        self.0.get()
    }
}

// ── Deterministic batch sequence (no IPC about contents) ─────

fn batch_for(k: u64) -> Vec<Mutation> {
    let mut rng = Xorshift(0xD06_0000_0000 ^ k);
    let marker = format!("batch/{k:05}");
    let pair = format!("batch/{k:05}/pair");
    let mut batch = vec![
        Mutation::put(
            Class::Record,
            marker.into_bytes(),
            format!("payload-{k:05}").into_bytes(),
        ),
        Mutation::put(
            Class::Record,
            pair.into_bytes(),
            vec![0xAB; 1 + rng.below(31) as usize],
        ),
    ];
    // Filler churn on a small working set so the recovered fold is
    // non-trivial and deletes are exercised on the kill path too.
    for _ in 0..rng.below(4) {
        let id = format!("churn/{}", rng.below(6)).into_bytes();
        if rng.below(4) == 0 {
            batch.push(Mutation::delete(Class::Record, id));
        } else {
            let len = rng.below(16) as usize;
            batch.push(Mutation::put(
                Class::Record,
                id,
                (0..len).map(|_| rng.next() as u8).collect(),
            ));
        }
    }
    batch
}

/// The live fold of batches `1..=k`: latest op per key, tombstones kept
/// so the comparison can also catch resurrected ids.
fn expected_fold(up_to: u64) -> BTreeMap<Vec<u8>, Op> {
    let mut ordered: BTreeMap<Vec<u8>, Op> = BTreeMap::new();
    for k in 1..=up_to {
        for m in batch_for(k) {
            ordered.insert(m.id.clone(), m.op.clone());
        }
    }
    ordered
}

fn marker_index(id: &[u8]) -> Option<u64> {
    if !id.starts_with(MARKER_PREFIX) || id.ends_with(PAIR_SUFFIX) {
        return None;
    }
    std::str::from_utf8(&id[MARKER_PREFIX.len()..])
        .ok()?
        .parse()
        .ok()
}

// ── Recovery observation ─────────────────────────────────────

/// Reopens the journal from scratch and returns `(batches, live)`:
/// the highest contiguous surviving batch number and the live payload
/// per key. A gap in the marker sequence is itself a violation —
/// replay is sequential, so no middle batch can be missing.
fn observe(path: &PathBuf) -> (u64, BTreeMap<Vec<u8>, Vec<u8>>) {
    let db = Ldb::open(path, MemKv::new(), CountingClock::default()).unwrap();
    // scan_class yields versions ascending by ts within an id, so the
    // last insert per id is its newest version.
    let mut latest_by_id: BTreeMap<Vec<u8>, Op> = BTreeMap::new();
    for row in db.scan_class(Class::Record).unwrap() {
        latest_by_id.insert(row.id, row.op);
    }

    let mut markers: Vec<u64> = Vec::new();
    let mut live = BTreeMap::new();
    for (id, op) in latest_by_id {
        if let Op::Put(payload) = op {
            if let Some(k) = marker_index(&id) {
                markers.push(k);
            }
            live.insert(id, payload);
        }
    }

    markers.sort_unstable();
    markers.dedup();
    for (position, k) in markers.iter().enumerate() {
        assert_eq!(
            *k,
            position as u64 + 1,
            "durable marker sequence has gaps or is unsorted: {markers:?}"
        );
    }
    ((markers.len() as u64), live)
}

// ── Harness ──────────────────────────────────────────────────

#[test]
fn process_kill_recovery_never_leaves_a_partial_batch() {
    if std::env::var(CHILD_ENV).as_deref() == Ok(ROLE_WRITER) {
        let path = std::env::var(PATH_ENV).expect("child needs a journal path");
        let mut db = Ldb::open(&path, MemKv::new(), CountingClock::default()).unwrap();
        for k in 1..=CHILD_BATCH_CAP {
            db.commit(&batch_for(k)).unwrap();
            std::thread::sleep(Duration::from_micros(300));
        }
        return; // reached only if the parent's kill never landed
    }

    let mut rng = Xorshift({
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos() as u64;
        nanos ^ (std::process::id() as u64) << 32
    });
    let exe = std::env::current_exe().unwrap();

    let mut total_survived = 0u64;
    for iteration in 0..ITERATIONS {
        let path = std::env::temp_dir().join(format!(
            "ldb-kill-{}-{iteration}.journal",
            std::process::id()
        ));
        let _ = std::fs::remove_file(&path);

        let mut child = Command::new(&exe)
            .args([
                "process_kill_recovery_never_leaves_a_partial_batch",
                "--exact",
                "--nocapture",
            ])
            .env(CHILD_ENV, ROLE_WRITER)
            .env(PATH_ENV, &path)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the writer child");

        // Wait for the first frame bytes to land, then kill at a random
        // moment inside the commit cadence.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            match std::fs::metadata(&path) {
                Ok(m) if m.len() > 0 => break,
                _ if Instant::now() > deadline => {
                    panic!("child never produced a durable byte at iteration {iteration}")
                }
                _ => std::thread::sleep(Duration::from_micros(150)),
            }
        }
        std::thread::sleep(Duration::from_micros(1 + rng.below(9000)));

        let _ = Command::new("/bin/kill")
            .arg("-9")
            .arg(child.id().to_string())
            .status();
        child.wait().expect("reap the killed child");

        let (batches, live) = observe(&path);
        total_survived += batches;
        if batches == 0 {
            assert!(live.is_empty(), "no durable batch but data appeared");
        } else {
            let expected_live: BTreeMap<Vec<u8>, Vec<u8>> = expected_fold(batches)
                .into_iter()
                .filter_map(|(id, op)| match op {
                    Op::Put(p) => Some((id, p)),
                    Op::Delete => None,
                })
                .collect();
            assert_eq!(
                live, expected_live,
                "iteration {iteration}: recovered state diverged at batch {batches}"
            );
            let pair = format!("batch/{batches:05}/pair").into_bytes();
            assert!(
                live.contains_key(&pair),
                "the newest surviving batch must be whole, not half-applied"
            );
        }
        let _ = std::fs::remove_file(&path);
    }

    assert!(
        total_survived >= ITERATIONS as u64,
        "harness went vacuous: {total_survived} batches survived across {ITERATIONS} kills"
    );
}
