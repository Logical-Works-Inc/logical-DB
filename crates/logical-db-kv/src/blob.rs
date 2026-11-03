//! Tiered blob store: chunked transfer, verified install, reads, and sweep.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use lgwks_std::hash::Hasher;

use crate::envelope::{Decoded, Envelope, SlotValue};
use crate::{
    BLOB_HASH_BUFFER_BYTES, KvCache, KvError, MAX_BLOB_CHUNK_BYTES, MAX_TIERED_VALUE_BYTES,
    staging_suffix,
};

impl KvCache {
    /// Reads up to `len` blob bytes at `offset` for content address
    /// `digest`, validating the bounded blob stream before returning one
    /// page-bounded chunk.
    ///
    /// Pair with [`KvCache::write_blob_chunk`] to move tiered bytes between
    /// replicas after the sidecar reference syncs through the existing
    /// change pages.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::InvalidChunkLength`] when `len` exceeds
    /// [`MAX_BLOB_CHUNK_BYTES`], [`KvError::InvalidChunkOffset`] when the
    /// range falls outside the stored file, [`KvError::MissingBlob`] when
    /// no file exists for `digest`, [`KvError::CorruptBlob`] when the file
    /// shrinks mid-read, or [`KvError::BlobIo`] for other I/O failures.
    pub fn read_blob_chunk(
        &self,
        digest: &[u8; 32],
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, KvError> {
        if len > MAX_BLOB_CHUNK_BYTES {
            return Err(KvError::InvalidChunkLength(len));
        }
        let _blob_guard = self.lock_blobs();
        let path = self.blob_path(digest);
        let stored = fs::metadata(&path).map_err(|error| missing_or_io(error, digest))?;
        let stored_len = stored.len();
        if !within_tiered_max(stored_len) || !Self::blob_valid_path(&path, digest, stored_len)? {
            return Err(KvError::CorruptBlob {
                digest: hex_digest(digest),
            });
        }
        let want = u64::try_from(len)?;
        let end = offset
            .checked_add(want)
            .ok_or(KvError::InvalidChunkOffset(offset))?;
        if end > stored_len {
            return Err(KvError::InvalidChunkOffset(offset));
        }
        let mut file = File::open(&path).map_err(|error| missing_or_io(error, digest))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(KvError::BlobIo)?;
        let mut chunk = vec![0_u8; len];
        file.read_exact(&mut chunk).map_err(|error| {
            if error.kind() == std::io::ErrorKind::UnexpectedEof {
                KvError::CorruptBlob {
                    digest: hex_digest(digest),
                }
            } else {
                KvError::BlobIo(error)
            }
        })?;
        Ok(chunk)
    }

    /// Writes `chunk` at `offset` into the staging blob file for content
    /// address `digest`, whose complete length is `total`.
    ///
    /// Every bound is validated before allocation or I/O: the total against
    /// [`MAX_TIERED_VALUE_BYTES`], the chunk against
    /// [`MAX_BLOB_CHUNK_BYTES`], and the range against the total. Reports
    /// whether the file reached its declared total; a completed file is
    /// hash-verified before reporting success, so a diverged transfer fails
    /// here instead of poisoning a later read. Partial files are never
    /// trusted by reads until length and hash both verify.
    ///
    /// # Errors
    ///
    /// Returns [`KvError::BlobTooLarge`] when `total` exceeds
    /// [`MAX_TIERED_VALUE_BYTES`], [`KvError::InvalidChunkLength`] for an
    /// oversized chunk, [`KvError::InvalidChunkOffset`] when the range
    /// falls outside `total` (or `total` exceeds the address space),
    /// [`KvError::CorruptBlob`] when a completed file diverges from `digest`,
    /// or [`KvError::BlobIo`] for I/O failures.
    pub fn write_blob_chunk(
        &self,
        digest: &[u8; 32],
        total: u64,
        offset: u64,
        chunk: &[u8],
    ) -> Result<bool, KvError> {
        match usize::try_from(total) {
            Ok(actual) if actual > MAX_TIERED_VALUE_BYTES => {
                return Err(KvError::BlobTooLarge {
                    actual,
                    maximum: MAX_TIERED_VALUE_BYTES,
                });
            }
            Ok(_) => {}
            Err(_) => return Err(KvError::InvalidChunkOffset(offset)),
        }
        if chunk.len() > MAX_BLOB_CHUNK_BYTES {
            return Err(KvError::InvalidChunkLength(chunk.len()));
        }
        let width = u64::try_from(chunk.len())?;
        let end = offset
            .checked_add(width)
            .ok_or(KvError::InvalidChunkOffset(offset))?;
        if end > total {
            return Err(KvError::InvalidChunkOffset(offset));
        }
        let _blob_guard = self.lock_blobs();
        fs::create_dir_all(&self.blob_dir)?;
        let path = self.blob_staging_path(digest, total);
        let mut file = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(chunk)?;
        file.sync_all()?;
        drop(file);
        let stored = fs::metadata(&path)?.len();
        if stored != total {
            return Ok(false);
        }
        if !Self::blob_valid_path(&path, digest, total)? {
            return Err(KvError::CorruptBlob {
                digest: hex_digest(digest),
            });
        }
        self.install_blob(&path, digest, total)?;
        Ok(true)
    }

    /// Removes every unreferenced blob file, reporting how many were
    /// reclaimed: orphan bytes from a crash between reference removal and
    /// blob collection, superseded import staging, and leftover temporary
    /// files. Files still referenced by any live slot in any namespace are
    /// kept, including expired-but-not-yet-evicted tiered slots.
    ///
    /// Call after crash recovery or startup during a quiescent maintenance
    /// window with no concurrent cache writer, reader, or sync importer on any
    /// handle or process; staging files only become referenced once their
    /// sidecar envelope commits.
    ///
    /// # Errors
    ///
    /// Returns envelope decoding failures, or [`KvError::BlobIo`] for
    /// directory and file failures.
    pub fn sweep_blobs(&self) -> Result<usize, KvError> {
        let _blob_guard = self.lock_blobs();
        let live = self.blob_references_locked()?;
        let entries = match fs::read_dir(&self.blob_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(KvError::BlobIo(error)),
        };
        let mut removed = 0_usize;
        for entry in entries {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let orphan = if let Some((stem, suffix)) = name.split_once(".tmp-") {
                let Some(digest) = hex_to_digest(stem) else {
                    continue;
                };
                if suffix.starts_with("gc-") {
                    self.gc_staging_is_orphan(&entry.path(), &digest, &live)?
                } else if !live.contains(&digest) {
                    true
                } else if let Ok(total) = suffix.parse::<u64>() {
                    let complete = fs::metadata(entry.path())?.len() == total;
                    complete && !Self::blob_valid_path(&entry.path(), &digest, total)?
                } else {
                    true
                }
            } else if is_blob_name(&name) {
                match hex_to_digest(&name) {
                    Some(digest) => !live.contains(&digest),
                    None => false,
                }
            } else {
                continue;
            };
            if orphan {
                match fs::remove_file(entry.path()) {
                    Ok(()) => removed = removed.saturating_add(1),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(KvError::BlobIo(error)),
                }
            }
        }
        Ok(removed)
    }

    /// Returns whether a staging blob should be discarded as orphaned.
    fn gc_staging_is_orphan(
        &self,
        path: &Path,
        digest: &[u8; 32],
        live: &BTreeSet<[u8; 32]>,
    ) -> Result<bool, KvError> {
        if !live.contains(digest) {
            return Ok(true);
        }
        let size = fs::metadata(path)?.len();
        if !Self::blob_valid_path(path, digest, size)? {
            return Ok(true);
        }
        let canonical = self.blob_path(digest);
        match fs::metadata(&canonical) {
            Ok(metadata) if Self::blob_valid_path(&canonical, digest, metadata.len())? => Ok(true),
            Ok(_) => {
                self.install_blob(path, digest, size)?;
                Ok(false)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.install_blob(path, digest, size)?;
                Ok(false)
            }
            Err(error) => Err(KvError::BlobIo(error)),
        }
    }

    /// Collects digests of every live tiered blob reference under the lock.
    fn blob_references_locked(&self) -> Result<BTreeSet<[u8; 32]>, KvError> {
        let mut live = BTreeSet::new();
        for document in self.snapshot_documents()? {
            match self.decode_slot(document)? {
                Decoded::Found(Envelope {
                    value: SlotValue::Tiered { digest, .. },
                    ..
                }) => {
                    live.insert(digest);
                }
                Decoded::Found(_) | Decoded::Absent => {}
                Decoded::Corrupt => return Err(KvError::CorruptEnvelope),
                Decoded::Unsupported(actual) => {
                    return Err(KvError::UnsupportedEnvelopeVersion { actual });
                }
            }
        }
        Ok(live)
    }

    /// Returns the canonical path for a blob digest.
    pub(crate) fn blob_path(&self, digest: &[u8; 32]) -> PathBuf {
        self.blob_dir.join(hex_digest(digest))
    }

    /// Returns the staging path for a blob of the given total size.
    fn blob_staging_path(&self, digest: &[u8; 32], total: u64) -> PathBuf {
        self.blob_dir
            .join(format!("{}.tmp-{total}", hex_digest(digest)))
    }

    /// Writes blob bytes to a staged file and installs them atomically.
    pub(crate) fn write_blob_file(&self, digest: &[u8; 32], bytes: &[u8]) -> Result<(), KvError> {
        let size = blob_len(bytes.len())?;
        if self.blob_valid(digest, size)? {
            return Ok(());
        }
        fs::create_dir_all(&self.blob_dir)?;
        let staged =
            self.blob_dir
                .join(format!("{}.tmp-{}", hex_digest(digest), staging_suffix()?));
        let mut file = File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&staged)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        self.install_blob(&staged, digest, size)?;
        Ok(())
    }

    /// Returns whether the canonical blob file matches digest and size.
    pub(crate) fn blob_valid(&self, digest: &[u8; 32], size: u64) -> Result<bool, KvError> {
        Self::blob_valid_path(&self.blob_path(digest), digest, size)
    }

    /// Hashes a path and checks it against the expected digest and size.
    fn blob_valid_path(path: &Path, digest: &[u8; 32], size: u64) -> Result<bool, KvError> {
        let stored = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(KvError::BlobIo(error)),
        };
        if stored.len() != size || !within_tiered_max(size) {
            return Ok(false);
        }
        let mut file = File::open(path)?;
        let mut hasher = Hasher::new();
        let mut buffer = vec![0_u8; BLOB_HASH_BUFFER_BYTES].into_boxed_slice();
        let mut remaining = size;
        while remaining > 0 {
            // A remainder wider than `usize` is still at least one buffer.
            let width = match usize::try_from(remaining) {
                Ok(left) => left.min(buffer.len()),
                Err(_) => buffer.len(),
            };
            match file.read_exact(&mut buffer[..width]) {
                Ok(()) => {
                    hasher.update(&buffer[..width]);
                }
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(false);
                }
                Err(error) => return Err(KvError::BlobIo(error)),
            }
            remaining = remaining.saturating_sub(blob_len(width)?);
        }
        Ok(*hasher.finalize().as_bytes() == *digest)
    }

    /// Renames a staged blob into place, resolving existing targets.
    fn install_blob(&self, source: &Path, digest: &[u8; 32], size: u64) -> Result<(), KvError> {
        let target = self.blob_path(digest);
        match fs::rename(source, &target) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if Self::blob_valid_path(&target, digest, size)? {
                    match fs::remove_file(source) {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(KvError::BlobIo(error)),
                    }
                    return Ok(());
                }
                match fs::remove_file(&target) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(KvError::BlobIo(error)),
                }
                fs::rename(source, target)?;
                Ok(())
            }
            Err(error) => Err(KvError::BlobIo(error)),
        }
    }

    /// Acquires the blob lock, recovering from a poisoned mutex.
    pub(crate) fn lock_blobs(&self) -> std::sync::MutexGuard<'_, ()> {
        match self.blob_lock.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Reads, verifies, and returns a tiered blob under the blob lock.
    pub(crate) fn resolve_tiered_locked(
        &self,
        digest: &[u8; 32],
        size: u64,
    ) -> Result<Vec<u8>, KvError> {
        if !within_tiered_max(size) {
            return Err(KvError::CorruptBlob {
                digest: hex_digest(digest),
            });
        }
        let path = self.blob_path(digest);
        let stored = fs::metadata(&path).map_err(|error| missing_or_io(error, digest))?;
        let bytes = if stored.len() == size {
            fs::read(&path).map_err(|error| missing_or_io(error, digest))?
        } else {
            Vec::new()
        };
        if stored.len() != size
            || blob_len(bytes.len())? != size
            || content_digest(&bytes) != *digest
        {
            return Err(KvError::CorruptBlob {
                digest: hex_digest(digest),
            });
        }
        Ok(bytes)
    }
}

/// Reports whether a byte count fits the tiered-value bound; a count no
/// `usize` can hold is beyond it on every target.
fn within_tiered_max(size: u64) -> bool {
    matches!(usize::try_from(size), Ok(bytes) if bytes <= MAX_TIERED_VALUE_BYTES)
}

/// Widens an in-memory byte count to the `u64` the blob store records,
/// refusing a count no `u64` can carry instead of recording a stand-in.
pub(crate) fn blob_len(len: usize) -> Result<u64, KvError> {
    Ok(u64::try_from(len)?)
}

/// Maps a blob open or stat failure: absence is a missing blob, anything
/// else is store I/O.
fn missing_or_io(error: std::io::Error, digest: &[u8; 32]) -> KvError {
    if error.kind() == std::io::ErrorKind::NotFound {
        KvError::MissingBlob {
            digest: hex_digest(digest),
        }
    } else {
        KvError::BlobIo(error)
    }
}

/// Computes the content digest used as a blob's canonical address.
pub(crate) fn content_digest(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}

/// Encodes a digest as lowercase hex for blob file names.
pub(crate) fn hex_digest(digest: &[u8; 32]) -> String {
    lgwks_std::hex::encode(digest)
}

/// Parses a lowercase hex blob name into a 32-byte digest.
pub(crate) fn hex_to_digest(name: &str) -> Option<[u8; 32]> {
    if name.len() != 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    lgwks_std::hex::decode(name).ok()?.try_into().ok()
}

/// Returns whether a name is a well-formed hex blob digest.
pub(crate) fn is_blob_name(name: &str) -> bool {
    hex_to_digest(name).is_some()
}

/// Returns the sibling blob directory for a cache database path.
pub(crate) fn blob_dir_for(path: &Path) -> PathBuf {
    let mut dir = path.as_os_str().to_owned();
    dir.push(".blobs");
    PathBuf::from(dir)
}
