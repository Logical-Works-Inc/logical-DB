//! Owns the spine: `Ldb` binds a KvPort, a ClockPort and the framed
//! journal into one store. Invariants enforced here: INV-TS-MONOTONIC
//! (a batch commits at one clock tick and never behind committed time —
//! the FDB version-ordered-commit graft), INV-BATCH-IS-ONE-FRAME (one
//! journal frame per batch, so replay is all-or-nothing), and
//! INV-DURABLE-BEFORE-ACK (`sync_data` lands before the KV sees the
//! write; replay is the recovery path for a crash in between).

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::journal;
use crate::model::{self, Class, Row};
use crate::port::{ClockPort, KvPort};
use crate::{KernelError, Result};

// ── Mutations ────────────────────────────────────────────────

/// One pending write. The store assigns the ts: a batch commits at a
/// single version, exactly as FDB commits a transaction at one version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Mutation {
    pub class: Class,
    pub id: Vec<u8>,
    pub op: model::Op,
}

impl Mutation {
    pub fn put(class: Class, id: impl Into<Vec<u8>>, payload: Vec<u8>) -> Self {
        Mutation {
            class,
            id: id.into(),
            op: model::Op::Put(payload),
        }
    }

    pub fn delete(class: Class, id: impl Into<Vec<u8>>) -> Self {
        Mutation {
            class,
            id: id.into(),
            op: model::Op::Delete,
        }
    }
}

// ── Ldb ──────────────────────────────────────────────────────

pub struct Ldb<S: KvPort, C: ClockPort> {
    kv: S,
    clock: C,
    journal: BufWriter<File>,
    last_ts: u64,
}

impl<S: KvPort, C: ClockPort> Ldb<S, C> {
    /// Opens (or creates) the journal at `path`, replays every intact
    /// frame into `kv`, and truncates any torn tail. A decode failure
    /// inside an intact frame aborts the open loudly — corruption is
    /// never silently cut off.
    pub fn open(path: impl AsRef<Path>, mut kv: S, clock: C) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut last_ts = 0u64;
        let stats = journal::replay(&mut file, |frame| {
            let mut cursor: &[u8] = frame;
            while !cursor.is_empty() {
                let row = model::decode_row(&mut cursor)?;
                last_ts = last_ts.max(row.ts);
                let key = row.key();
                let value = row.op.payload().to_vec();
                // Store the op tag as the first byte of the KV value so
                // latest/scan can distinguish Put from Delete.
                let mut stored = Vec::with_capacity(1 + value.len());
                stored.push(row.op.wire_tag());
                stored.extend_from_slice(&value);
                kv.put(key, stored)?;
            }
            Ok(())
        })?;
        let end = file.metadata()?.len();
        if stats.valid_bytes < end {
            file.set_len(stats.valid_bytes)?;
        }
        file.seek(SeekFrom::Start(stats.valid_bytes))?;
        Ok(Ldb {
            kv,
            clock,
            journal: BufWriter::new(file),
            last_ts,
        })
    }

    /// Commits `batch` at one clock tick: one frame, fsynced, then
    /// applied to the KV. Returns the committed ts. An empty batch is a
    /// no-op that consumes no tick.
    pub fn commit(&mut self, batch: &[Mutation]) -> Result<u64> {
        if batch.is_empty() {
            return Ok(self.last_ts);
        }
        let ts = self.clock.now();
        if ts < self.last_ts {
            return Err(KernelError::TsRegression {
                last: self.last_ts,
                got: ts,
            });
        }
        let mut frame = Vec::new();
        for m in batch {
            model::encode_row(
                &Row::new(m.class, m.id.clone(), ts, m.op.clone()),
                &mut frame,
            )?;
        }
        journal::append_frame(&mut self.journal, &frame)?;
        self.journal.flush()?;
        // INV-DURABLE-BEFORE-ACK: bytes are on disk before the KV
        // changes. A crash before the apply loop replays from here.
        self.journal.get_ref().sync_data()?;
        for m in batch {
            let key = model::point_key(m.class, &m.id, ts);
            let payload = m.op.payload();
            let mut stored = Vec::with_capacity(1 + payload.len());
            stored.push(m.op.wire_tag());
            stored.extend_from_slice(payload);
            self.kv.put(key, stored)?;
        }
        self.last_ts = ts;
        Ok(ts)
    }

    /// Single-mutation convenience; still one frame, one tick.
    pub fn insert(&mut self, m: &Mutation) -> Result<u64> {
        self.commit(std::slice::from_ref(m))
    }

    /// Returns `(op_tag, payload)` from the stored value format.
    fn decode_stored(stored: &[u8]) -> (u8, &[u8]) {
        if stored.is_empty() {
            (0, &[])
        } else {
            (stored[0], &stored[1..])
        }
    }

    pub fn get(&self, class: Class, id: &[u8], ts: u64) -> Result<Option<Vec<u8>>> {
        match self.kv.get(&model::point_key(class, id, ts))? {
            None => Ok(None),
            Some(stored) => {
                let (tag, payload) = Self::decode_stored(&stored);
                if tag == 2 {
                    Ok(None)
                } else {
                    Ok(Some(payload.to_vec()))
                }
            }
        }
    }

    /// Returns raw stored value including op tag for internal callers.
    pub fn get_raw(&self, class: Class, id: &[u8], ts: u64) -> Result<Option<Vec<u8>>> {
        self.kv.get(&model::point_key(class, id, ts))
    }

    /// The newest version of one entity. Returns `None` if the entity
    /// never existed or its latest operation is `Delete`.
    pub fn latest(&self, class: Class, id: &[u8]) -> Result<Option<(u64, Vec<u8>)>> {
        let history = self.kv.scan_prefix(&model::history_prefix(class, id))?;
        match history.last() {
            None => Ok(None),
            Some((k, stored)) => {
                let (_, _, ts) = model::split_key(k)?;
                let (tag, payload) = Self::decode_stored(stored);
                if tag == 2 {
                    Ok(None)
                } else {
                    Ok(Some((ts, payload.to_vec())))
                }
            }
        }
    }

    /// The newest version of one entity, including deletes. Returns
    /// `(ts, Op)` or `None` if no history exists at all.
    pub fn latest_with_op(&self, class: Class, id: &[u8]) -> Result<Option<(u64, model::Op)>> {
        let history = self.kv.scan_prefix(&model::history_prefix(class, id))?;
        match history.last() {
            None => Ok(None),
            Some((k, stored)) => {
                let (_, _, ts) = model::split_key(k)?;
                let (tag, payload) = Self::decode_stored(stored);
                let op = if tag == 2 {
                    model::Op::Delete
                } else {
                    model::Op::Put(payload.to_vec())
                };
                Ok(Some((ts, op)))
            }
        }
    }

    /// Every version of every entity in one class, ascending by key.
    pub fn scan_class(&self, class: Class) -> Result<Vec<Row>> {
        self.kv
            .scan_prefix(&model::class_prefix(class))?
            .into_iter()
            .map(|(k, stored)| {
                let (class, id, ts) = model::split_key(&k)?;
                let (tag, payload) = Self::decode_stored(&stored);
                let op = if tag == 2 {
                    model::Op::Delete
                } else {
                    model::Op::Put(payload.to_vec())
                };
                Ok(Row { class, id, ts, op })
            })
            .collect()
    }

    pub fn last_ts(&self) -> u64 {
        self.last_ts
    }

    pub fn kv(&self) -> &S {
        &self.kv
    }

    pub fn into_parts(self) -> (S, C) {
        (self.kv, self.clock)
    }
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memkv::MemKv;
    use std::cell::Cell;
    use std::path::PathBuf;

    fn tmp_path(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("ldb-test-{}-{tag}.journal", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    /// Deterministic clock: each tick advances by one.
    struct Seq(Cell<u64>);

    impl ClockPort for Seq {
        fn now(&self) -> u64 {
            self.0.set(self.0.get() + 1);
            self.0.get()
        }
    }

    /// A clock stuck in the past, for regression tests.
    struct Frozen(u64);

    impl ClockPort for Frozen {
        fn now(&self) -> u64 {
            self.0
        }
    }

    fn seq() -> Seq {
        Seq(Cell::new(0))
    }

    #[test]
    fn rows_survive_reopen() {
        let path = tmp_path("roundtrip");
        let mut db = Ldb::open(&path, MemKv::new(), seq()).unwrap();
        db.commit(&[
            Mutation::put(Class::Record, "doc/1", b"one".to_vec()),
            Mutation::put(Class::Term, "hello", b"idx".to_vec()),
        ])
        .unwrap();
        let (kv, _) = db.into_parts();
        let db = Ldb::open(&path, kv, seq()).unwrap();
        assert_eq!(
            db.get(Class::Record, b"doc/1", 1).unwrap().as_deref(),
            Some(b"one".as_slice())
        );
        assert_eq!(
            db.latest(Class::Term, b"hello").unwrap().unwrap().1,
            b"idx".to_vec()
        );
        assert_eq!(db.last_ts(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_torn_tail_is_truncated_on_open() {
        let path = tmp_path("torn");
        let clean_len;
        {
            let mut db = Ldb::open(&path, MemKv::new(), seq()).unwrap();
            db.insert(&Mutation::put(Class::Record, "a", b"1".to_vec()))
                .unwrap();
            clean_len = File::open(&path).unwrap().metadata().unwrap().len();
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xDE, 0xAD, 0xBE]).unwrap();
        }
        let db = Ldb::open(&path, MemKv::new(), seq()).unwrap();
        assert_eq!(db.scan_class(Class::Record).unwrap().len(), 1);
        assert_eq!(
            File::open(&path).unwrap().metadata().unwrap().len(),
            clean_len,
            "the torn tail must be cut back to the last intact frame"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_batch_is_atomic_when_the_journal_tears_mid_batch() {
        let path = tmp_path("atomic");
        let mut db = Ldb::open(&path, MemKv::new(), seq()).unwrap();
        db.commit(&[Mutation::put(Class::Record, "keep/1", b"k".to_vec())])
            .unwrap();
        db.commit(&[
            Mutation::put(Class::Record, "drop/1", b"d".to_vec()),
            Mutation::put(Class::Record, "drop/2", b"d".to_vec()),
        ])
        .unwrap();
        drop(db);

        let mut bytes = std::fs::read(&path).unwrap();
        let torn = bytes.len() - 3;
        bytes.truncate(torn);
        std::fs::write(&path, &bytes).unwrap();

        let db = Ldb::open(&path, MemKv::new(), seq()).unwrap();
        let live: Vec<_> = db.scan_class(Class::Record).unwrap();
        assert_eq!(live.len(), 1, "the torn batch must vanish whole");
        assert_eq!(live[0].id, b"keep/1");
        assert_eq!(db.get(Class::Record, b"drop/1", 2).unwrap(), None);
        assert_eq!(db.get(Class::Record, b"drop/2", 2).unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_clock_regression_is_rejected() {
        let path = tmp_path("regress");
        let mut db = Ldb::open(&path, MemKv::new(), Seq(Cell::new(10))).unwrap();
        db.insert(&Mutation::put(Class::Claim, "c", b"x".to_vec()))
            .unwrap();
        let mut backwards = Ldb::open(&path, MemKv::new(), Frozen(3)).unwrap();
        let err = backwards
            .insert(&Mutation::put(Class::Claim, "c", b"y".to_vec()))
            .unwrap_err();
        assert!(matches!(
            err,
            KernelError::TsRegression { last: 11, got: 3 }
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn an_empty_batch_commits_nothing() {
        let path = tmp_path("empty");
        let mut db = Ldb::open(&path, MemKv::new(), seq()).unwrap();
        let ts = db.commit(&[]).unwrap();
        assert_eq!(ts, 0, "no tick consumed");
        assert_eq!(File::open(&path).unwrap().metadata().unwrap().len(), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn latest_returns_the_newest_version() {
        let path = tmp_path("latest");
        let mut db = Ldb::open(&path, MemKv::new(), seq()).unwrap();
        db.insert(&Mutation::put(Class::Record, "d", b"v1".to_vec()))
            .unwrap();
        db.insert(&Mutation::put(Class::Record, "d", b"v2".to_vec()))
            .unwrap();
        let (ts, payload) = db.latest(Class::Record, b"d").unwrap().unwrap();
        assert_eq!((ts, payload.as_slice()), (2, b"v2".as_slice()));
        assert_eq!(
            db.scan_class(Class::Record).unwrap().len(),
            2,
            "history is kept"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn explicit_delete_hides_from_latest() {
        let path = tmp_path("del");
        let mut db = Ldb::open(&path, MemKv::new(), seq()).unwrap();
        db.insert(&Mutation::put(Class::Record, "d", b"alive".to_vec()))
            .unwrap();
        db.insert(&Mutation::delete(Class::Record, "d")).unwrap();
        assert!(db.latest(Class::Record, b"d").unwrap().is_none());
        assert_eq!(
            db.scan_class(Class::Record).unwrap().len(),
            2,
            "history includes the delete"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_payload_put_is_valid_live_data() {
        let path = tmp_path("empty-put");
        let mut db = Ldb::open(&path, MemKv::new(), seq()).unwrap();
        db.insert(&Mutation::put(Class::Record, "e", vec![]))
            .unwrap();
        let (_, payload) = db.latest(Class::Record, b"e").unwrap().unwrap();
        assert!(
            payload.is_empty(),
            "empty put is live data, not a tombstone"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn unknown_class_tags_never_enter_through_the_store() {
        assert!(Class::from_tag(0).is_err());
        assert!(Class::from_tag(6).is_err());
        assert_eq!(Class::from_tag(5).unwrap(), Class::Claim);
    }
}
