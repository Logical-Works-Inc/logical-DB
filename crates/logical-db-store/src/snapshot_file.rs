//! Durable snapshot files served through bounded positional reads (#119 row 4).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! [`FileProjectionIndex`] serves the `LWMX` bytes [`super::projection_snapshot`]
//! defines through seek-and-read windows instead of `mmap(2)`: every crate on
//! this path keeps `#![forbid(unsafe_code)]`, and no mapping edge exists that
//! a `forbid` crate could legally call. The bounded-memory property is proved
//! by counting bytes read rather than resident pages, which also makes the
//! proof portable instead of Linux-only.
//!
//! Builds are atomic: bytes go to a sibling temp file, sync, and rename over
//! the target with a parent-directory sync, so a crash leaves either the old
//! snapshot or the new one, never a partial file.

use std::cell::Cell;

use super::projection_indexed::{IndexedMatch, IndexedProjection};
use super::projection_snapshot::{
    SnapshotError, SnapshotWindow, parse_header, query_with, to_snapshot_bytes,
    verify_snapshot_integrity,
};

/// A durable projection snapshot index over a file, queried through
/// positional reads so a term-scoped query touches only its working set.
#[derive(Debug)]
pub struct FileProjectionIndex {
    /// Open handle to the durable snapshot file.
    file: std::fs::File,
    /// Rows indexed by the snapshot.
    row_count: usize,
    /// On-disk byte length of the snapshot.
    byte_len: u64,
}

impl FileProjectionIndex {
    /// Builds a durable snapshot at `path` from an in-memory index.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] when the temp file cannot be written,
    /// synced, or renamed, and [`SnapshotError::Malformed`] when a section
    /// length overflows.
    pub fn build(
        path: impl AsRef<std::path::Path>,
        projection: &IndexedProjection,
    ) -> Result<(), SnapshotError> {
        let path = path.as_ref();
        let bytes = to_snapshot_bytes(projection)?;
        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        let tmp = std::path::PathBuf::from(tmp);
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|error| io_error("open snapshot tmp", error))?;
            file.write_all(&bytes)
                .map_err(|error| io_error("write snapshot", error))?;
            file.sync_all()
                .map_err(|error| io_error("fsync snapshot", error))?;
        }
        std::fs::rename(&tmp, path).map_err(|error| io_error("rename snapshot", error))?;
        if let Some(dir) = path.parent()
            && let Ok(handle) = std::fs::File::open(dir)
        {
            let _synced: bool = handle.sync_all().is_ok();
        }
        Ok(())
    }

    /// Opens an existing snapshot, validating only the header.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::Io`] when the file cannot be opened and
    /// [`SnapshotError::Malformed`] for a bad header. The full body hash is
    /// NOT verified; call [`Self::verify_integrity`] to pay that cost.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, SnapshotError> {
        let path = path.as_ref();
        let file = std::fs::File::open(path).map_err(|error| io_error("open snapshot", error))?;
        let byte_len = file
            .metadata()
            .map_err(|error| io_error("stat snapshot", error))?
            .len();
        let header = read_exact_at(&file, 0, super::projection_snapshot::HEADER_LEN)?;
        let row_count = parse_header(&header, file_len(byte_len)?)?.fact_count;
        Ok(Self {
            file,
            row_count,
            byte_len,
        })
    }

    /// Runs a bounded hybrid query, returning the matches and the bytes read
    /// to serve them. The byte count is the portability-proof of the bounded
    /// property: it stays a fraction of the file for term-scoped queries.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] for I/O failures, malformed bytes, or an
    /// over-long seed.
    pub(crate) fn query_counted(
        &self,
        query_text: &str,
        vector_seed: &[i32],
        top_k: usize,
    ) -> Result<(Vec<IndexedMatch>, u64), SnapshotError> {
        let reader = WindowReader::new(&self.file);
        let header_bytes = reader.read_at(0, super::projection_snapshot::HEADER_LEN)?;
        let header = parse_header(&header_bytes, file_len(self.byte_len)?)?;
        let matches = query_with(&reader, &header, query_text, vector_seed, top_k)?;
        Ok((matches, reader.bytes_read()))
    }

    /// Runs a bounded hybrid query, identical to the in-memory
    /// [`IndexedProjection::query`].
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] for I/O failures, malformed bytes, or an
    /// over-long seed.
    pub fn query(
        &self,
        query_text: &str,
        vector_seed: &[i32],
        top_k: usize,
    ) -> Result<Vec<IndexedMatch>, SnapshotError> {
        Ok(self.query_counted(query_text, vector_seed, top_k)?.0)
    }

    /// Verifies the snapshot body hash by streaming the file.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] for I/O failures or a hash mismatch.
    pub fn verify_integrity(&self) -> Result<(), SnapshotError> {
        use std::io::{Read, Seek, SeekFrom};
        let mut file = self.reopen()?;
        file.seek(SeekFrom::Start(0))
            .map_err(|error| io_error("seek snapshot", error))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| io_error("read snapshot", error))?;
        verify_snapshot_integrity(&bytes)
    }

    /// Reopens the snapshot file for a full streaming read.
    fn reopen(&self) -> Result<std::fs::File, SnapshotError> {
        self.file
            .try_clone()
            .map_err(|error| io_error("clone snapshot handle", error))
    }

    /// Returns the number of indexed rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.row_count
    }

    /// Returns true when the snapshot indexes no rows.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    /// Returns the on-disk byte length of the snapshot.
    #[must_use]
    pub fn byte_len(&self) -> u64 {
        self.byte_len
    }
}

/// Reads one exact window through seek plus read.
fn read_exact_at(file: &std::fs::File, offset: u64, len: usize) -> Result<Vec<u8>, SnapshotError> {
    use std::io::{Read, Seek, SeekFrom};
    let mut handle = file
        .try_clone()
        .map_err(|error| io_error("clone snapshot handle", error))?;
    handle
        .seek(SeekFrom::Start(offset))
        .map_err(|error| io_error("seek snapshot", error))?;
    let mut window = vec![0_u8; len];
    handle
        .read_exact(&mut window)
        .map_err(|error| io_error("read snapshot window", error))?;
    Ok(window)
}

/// One refusal constructor for file I/O failures.
fn io_error(at: &'static str, error: std::io::Error) -> SnapshotError {
    SnapshotError::Io {
        at,
        reason: error.to_string(),
    }
}

/// Converts a file length into its checked `usize` form.
fn file_len(byte_len: u64) -> Result<usize, SnapshotError> {
    usize::try_from(byte_len)
        .map_err(|error| SnapshotError::Malformed(format!("snapshot size overflow: {error}")))
}

/// A positional reader that counts the bytes it serves.
struct WindowReader<'a> {
    /// The file read through.
    file: &'a std::fs::File,
    /// Bytes served so far.
    bytes_read: Cell<u64>,
}

impl<'a> WindowReader<'a> {
    /// Creates a counting reader over an open file.
    fn new(file: &'a std::fs::File) -> Self {
        Self {
            file,
            bytes_read: Cell::new(0),
        }
    }

    /// Reads exactly `len` bytes at `offset`, counting them.
    fn read_at(&self, offset: usize, len: usize) -> Result<Vec<u8>, SnapshotError> {
        let offset = u64::try_from(offset)
            .map_err(|error| SnapshotError::Malformed(format!("read offset overflow: {error}")))?;
        let window = read_exact_at(self.file, offset, len)?;
        let increment = u64::try_from(len)
            .map_err(|error| SnapshotError::Malformed(format!("read length overflow: {error}")))?;
        self.bytes_read
            .set(self.bytes_read.get().saturating_add(increment));
        Ok(window)
    }

    /// Returns the bytes served so far.
    fn bytes_read(&self) -> u64 {
        self.bytes_read.get()
    }
}

impl SnapshotWindow for WindowReader<'_> {
    /// Serves one counted window through seek plus read.
    fn read_window(&self, offset: usize, len: usize) -> Result<Vec<u8>, SnapshotError> {
        self.read_at(offset, len)
    }
}

#[cfg(test)]
mod tests {
    use super::super::projection_indexed::IndexedProjection;
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Builds a synthetic index without I/O.
    fn synthetic_index(count: u8) -> IndexedProjection {
        let mut projection = IndexedProjection::new();
        for index in 0..count {
            let mut hash = [0_u8; 32];
            hash[0] = index;
            hash[1] = index.wrapping_mul(7);
            let terms = vec![String::from("shared"), format!("term{index}")];
            projection.insert(hash, terms);
        }
        projection
    }

    /// One unique temporary snapshot path for a named fixture.
    fn snapshot_path(name: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let sequence = COUNTER.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "ldb-snap-{name}-{}-{sequence}.lwmx",
            std::process::id()
        ))
    }

    /// Sample queries shared by the parity tests.
    fn queries() -> Vec<(String, Vec<i32>, usize)> {
        vec![
            (String::from("shared"), vec![], 10),
            (String::from("term3"), vec![], 5),
            (String::from("shared term7"), vec![1, 2, 3], 8),
            (String::from("absent"), vec![], 8),
            (String::new(), vec![5, 0, 0, 9], 8),
        ]
    }

    #[test]
    fn build_open_query_matches_in_memory() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(40);
        let path = snapshot_path("parity");
        FileProjectionIndex::build(&path, &projection)?;

        let index = FileProjectionIndex::open(&path)?;
        assert_eq!(index.len(), projection.len(), "row counts must agree");
        index.verify_integrity()?;
        for (text, seed, top_k) in queries() {
            assert_eq!(
                index.query(&text, &seed, top_k)?,
                projection.query(&text, &seed, top_k)?,
                "file query diverged from in-memory for {text:?}"
            );
        }
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn empty_index_builds_and_queries() -> Result<(), Box<dyn std::error::Error>> {
        let projection = IndexedProjection::new();
        let path = snapshot_path("empty");
        FileProjectionIndex::build(&path, &projection)?;
        let index = FileProjectionIndex::open(&path)?;
        assert!(index.is_empty(), "an empty snapshot reports empty");
        assert!(
            index.query("anything", &[], 8)?.is_empty(),
            "an empty snapshot answers nothing"
        );
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn corrupted_snapshot_fails_integrity() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(10);
        let path = snapshot_path("corrupt");
        FileProjectionIndex::build(&path, &projection)?;

        let mut bytes = std::fs::read(&path)?;
        let last = bytes.len().saturating_sub(1);
        bytes[last] ^= 0xFF;
        std::fs::write(&path, &bytes)?;

        let index = FileProjectionIndex::open(&path)?;
        let error = index.verify_integrity();
        assert!(
            error.is_err(),
            "a corrupted file must fail integrity, got {error:?}"
        );
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn stale_tmp_never_shadows_a_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(10);
        let path = snapshot_path("stale-tmp");
        FileProjectionIndex::build(&path, &projection)?;
        let before = FileProjectionIndex::open(&path)?;
        before.verify_integrity()?;

        let mut tmp = path.as_os_str().to_owned();
        tmp.push(".tmp");
        std::fs::write(&tmp, b"partial garbage from a killed build")?;
        let reopened = FileProjectionIndex::open(&path)?;
        assert_eq!(
            reopened.len(),
            before.len(),
            "a stale tmp file must not shadow the durable snapshot"
        );
        reopened.verify_integrity()?;

        FileProjectionIndex::build(&path, &projection)?;
        assert!(
            !std::path::Path::new(&tmp).exists(),
            "a rebuild must replace the stale tmp file"
        );
        let rebuilt = FileProjectionIndex::open(&path)?;
        rebuilt.verify_integrity()?;
        std::fs::remove_file(&path)?;
        Ok(())
    }

    #[test]
    fn term_query_reads_a_fraction_of_the_file() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(200);
        let path = snapshot_path("budget");
        FileProjectionIndex::build(&path, &projection)?;
        let index = FileProjectionIndex::open(&path)?;

        let (hits, bytes_read) = index.query_counted("term42", &[], 8)?;
        assert!(!hits.is_empty(), "the probe term must match");
        let half = index
            .byte_len()
            .checked_div(2)
            .ok_or("test snapshot too small")?;
        assert!(
            bytes_read < half,
            "a term query read {bytes_read} of {} file bytes: the whole index was loaded, not a bounded projection",
            index.byte_len()
        );
        std::fs::remove_file(&path)?;
        Ok(())
    }
}
