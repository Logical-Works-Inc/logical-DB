//! Owns the production durable backend: redb owns page layout, MVCC
//! and crash safety; this module adapts it to [`KvPort`]. INV-ONE-
//! CRASH-STORY: batches commit through one redb transaction — no
//! external journal ever sees production bytes (issue #6 §5). The
//! committed version is persisted in the same transaction as its
//! batch, so a reopened store never commits behind committed time.

use std::ops::Bound;

use redb::{Database, ReadableDatabase, TableDefinition};

use crate::port::{DurableKv, KvPort};
use crate::{KernelError, Result};

// ── Tables ───────────────────────────────────────────────────

/// All logical rows: key = framed point key, value = `[op_tag][payload]`.
const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ldb_rows");
/// Single-row bookkeeping: the highest committed batch version.
const META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ldb_meta");

const VERSION_KEY: &[u8] = b"version";

// ── Backend ──────────────────────────────────────────────────

pub struct RedbBackend {
    db: Database,
}

impl RedbBackend {
    /// Opens (or creates) the database file. redb takes an OS-level
    /// exclusive lock: a second writer on the same file fails here,
    /// which is the backend contract's open-locking requirement.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let db = Database::create(path)
            .map_err(|e| KernelError::Backend(format!("open failed: {e}")))?;
        Ok(RedbBackend { db })
    }
}

// ── Prefix bounds ────────────────────────────────────────────

/// The smallest key strictly greater than every key starting with
/// `prefix`, or `None` when every byte is 0xFF (no upper bound).
fn exclusive_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.pop() {
        if last < 0xFF {
            out.push(last + 1);
            return Some(out);
        }
    }
    None
}

/// Opens a read view of `table`, mapping "table never created" to
/// `None` so a fresh store reads as empty instead of erroring. The
/// returned handle keeps its transaction alive through redb's own
/// shared guard, hence the `'static` key/value regions.
fn read_table(
    rtxn: &redb::ReadTransaction,
    def: TableDefinition<&[u8], &[u8]>,
) -> Result<Option<redb::ReadOnlyTable<&'static [u8], &'static [u8]>>> {
    match rtxn.open_table(def) {
        Ok(t) => Ok(Some(t)),
        // `open_table` only ever names the table it was asked for, so
        // the payload carries nothing to double-check.
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
        Err(e) => Err(backend_err(e)),
    }
}

fn backend_err<E: std::fmt::Display>(e: E) -> KernelError {
    KernelError::Backend(e.to_string())
}

// ── KvPort ───────────────────────────────────────────────────

impl KvPort for RedbBackend {
    fn is_durable(&self) -> bool {
        true
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let rtxn = self.db.begin_read().map_err(backend_err)?;
        match read_table(&rtxn, ROWS)? {
            None => Ok(None),
            Some(table) => match table.get(key).map_err(backend_err)? {
                None => Ok(None),
                Some(v) => Ok(Some(v.value().to_vec())),
            },
        }
    }

    fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        // A bare point write never touches the version row: versions
        // move only with batches, so the two paths cannot clobber
        // each other's bookkeeping.
        let wtxn = self.db.begin_write().map_err(backend_err)?;
        {
            let mut rows = wtxn.open_table(ROWS).map_err(backend_err)?;
            rows.insert(key.as_slice(), value.as_slice())
                .map_err(backend_err)?;
        }
        wtxn.commit().map_err(backend_err)?;
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        let wtxn = self.db.begin_write().map_err(backend_err)?;
        {
            let mut table = wtxn.open_table(ROWS).map_err(backend_err)?;
            table.remove(key).map_err(backend_err)?;
        }
        wtxn.commit().map_err(backend_err)?;
        Ok(())
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rtxn = self.db.begin_read().map_err(backend_err)?;
        let Some(table) = read_table(&rtxn, ROWS)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        // The upper bound is exclusive, so a key equal to it — which
        // does not start with `prefix` — can never enter the scan.
        let upper = exclusive_upper_bound(prefix);
        // `(Bound<&T>, Bound<&T>)` is the tuple shape std's
        // `RangeBounds<&[u8]>` impl covers; redb then pins `KR = &[u8]`.
        let range: (Bound<&&[u8]>, Bound<&&[u8]>) = match &upper {
            Some(end) => (Bound::Included(&prefix), Bound::Excluded(&end.as_slice())),
            None => (Bound::Included(&prefix), Bound::Unbounded),
        };
        for row in table.range::<&[u8]>(range).map_err(backend_err)? {
            let (k, v) = row.map_err(backend_err)?;
            out.push((k.value().to_vec(), v.value().to_vec()));
        }
        Ok(out)
    }

    /// One write transaction per batch; redb syncs before `commit`
    /// returns, so an ack implies durability. The version row rides in
    /// the same transaction as its batch.
    fn apply_batch(&mut self, entries: &[(Vec<u8>, Vec<u8>)], version: u64) -> Result<()> {
        let wtxn = self.db.begin_write().map_err(backend_err)?;
        {
            let mut rows = wtxn.open_table(ROWS).map_err(backend_err)?;
            for (key, value) in entries {
                rows.insert(key.as_slice(), value.as_slice())
                    .map_err(backend_err)?;
            }
            let mut meta = wtxn.open_table(META).map_err(backend_err)?;
            meta.insert(VERSION_KEY, version.to_be_bytes().as_slice())
                .map_err(backend_err)?;
        }
        wtxn.commit().map_err(backend_err)?;
        Ok(())
    }
}

impl DurableKv for RedbBackend {
    fn committed_version(&self) -> Result<u64> {
        let rtxn = self.db.begin_read().map_err(backend_err)?;
        let Some(meta) = read_table(&rtxn, META)? else {
            return Ok(0);
        };
        match meta.get(VERSION_KEY).map_err(backend_err)? {
            None => Ok(0),
            Some(v) => {
                let bytes: [u8; 8] = v
                    .value()
                    .try_into()
                    .map_err(|_| KernelError::Backend("committed version is not 8 bytes".into()))?;
                Ok(u64::from_be_bytes(bytes))
            }
        }
    }
}

// ── Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_db(tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("ldb-redb-{}-{tag}.redb", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn exclusive_upper_bound_increments_the_last_open_byte() {
        assert_eq!(exclusive_upper_bound(b"ab").unwrap(), b"ac");
        // Carry past trailing 0xFF bytes.
        assert_eq!(exclusive_upper_bound(&[0x01, 0xFF]).unwrap(), vec![0x02]);
        assert_eq!(
            exclusive_upper_bound(&[0xFF, 0x00]).unwrap(),
            vec![0xFF, 0x01]
        );
        // An all-0xFF or empty prefix has no bound above it.
        assert_eq!(exclusive_upper_bound(&[0xFF, 0xFF]), None);
        assert_eq!(exclusive_upper_bound(&[]), None);
    }

    #[test]
    fn point_ops_roundtrip_and_delete_hides() {
        let path = tmp_db("point");
        let mut kv = RedbBackend::open(&path).unwrap();
        kv.put(b"k/1".to_vec(), b"one".to_vec()).unwrap();
        assert_eq!(kv.get(b"k/1").unwrap(), Some(b"one".to_vec()));
        kv.put(b"k/1".to_vec(), b"two".to_vec()).unwrap();
        assert_eq!(kv.get(b"k/1").unwrap(), Some(b"two".to_vec()));
        kv.delete(b"k/1").unwrap();
        assert_eq!(kv.get(b"k/1").unwrap(), None);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn scan_prefix_is_ordered_and_never_leaks_past_the_prefix() {
        let path = tmp_db("scan");
        let mut kv = RedbBackend::open(&path).unwrap();
        kv.apply_batch(
            &[
                (b"p/a".to_vec(), vec![1]),
                (b"p/b".to_vec(), vec![2]),
                (b"q/a".to_vec(), vec![3]),
                (b"p\xFF".to_vec(), vec![4]),
                // Exactly at the exclusive bound of "p": not a member.
                (b"q".to_vec(), vec![5]),
            ],
            1,
        )
        .unwrap();
        let got = kv.scan_prefix(b"p").unwrap();
        let keys: Vec<_> = got.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            keys,
            vec![b"p/a".to_vec(), b"p/b".to_vec(), b"p\xFF".to_vec()]
        );
        // A prefix of all 0xFF scans everything from there upward.
        let all = kv.scan_prefix(&[]).unwrap();
        assert_eq!(all.len(), 5, "empty prefix scans the whole store");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn committed_version_persists_with_its_batch_across_reopen() {
        let path = tmp_db("version");
        {
            let mut kv = RedbBackend::open(&path).unwrap();
            assert_eq!(kv.committed_version().unwrap(), 0);
            kv.apply_batch(&[(b"a".to_vec(), vec![1])], 7).unwrap();
            kv.apply_batch(&[(b"b".to_vec(), vec![2])], 9).unwrap();
        }
        let kv = RedbBackend::open(&path).unwrap();
        assert_eq!(kv.committed_version().unwrap(), 9);
        assert_eq!(kv.get(b"a").unwrap(), Some(vec![1]));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_multi_entry_batch_lands_whole_across_reopen() {
        // Deterministic half of the atomicity contract; the torn
        // half belongs to tests/process_kill_recovery.rs, which kills
        // a writer mid-flight against this same backend.
        let path = tmp_db("atomic");
        {
            let mut kv = RedbBackend::open(&path).unwrap();
            kv.apply_batch(
                &[
                    (b"a/1".to_vec(), vec![1]),
                    (b"a/2".to_vec(), vec![2]),
                    (b"a/3".to_vec(), vec![3]),
                ],
                1,
            )
            .unwrap();
        }
        let kv = RedbBackend::open(&path).unwrap();
        for (k, n) in [
            (b"a/1".as_slice(), 1u8),
            (b"a/2".as_slice(), 2),
            (b"a/3".as_slice(), 3),
        ] {
            assert_eq!(kv.get(k).unwrap(), Some(vec![n]), "{k:?} must land whole");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_second_writer_on_the_same_file_is_refused() {
        let path = tmp_db("lock");
        let _kv1 = RedbBackend::open(&path).unwrap();
        let second = RedbBackend::open(&path);
        assert!(
            second.is_err(),
            "the backend must hold an exclusive open lock"
        );
        let _ = std::fs::remove_file(&path);
    }
}
