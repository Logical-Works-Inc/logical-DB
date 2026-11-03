//! Relational projection parity pair (kernel SPEC 365-P absorption, #119 row 4).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! The kernel's tape projections prove one invariant this module absorbs: the
//! canonical state is engine-free. [`RelationalProjection`] (backed by this
//! replica's memory and, through the restore path, its redb rows) and
//! [`SqliteRelationalProjection`] (backed by SQLite) must emit byte-identical
//! `serialize_state` and `checkpoint` for the same applied row sequence, so a
//! projection file can be deleted and replayed without changing one byte of
//! observable state (the disposable-projection posture).
//!
//! Rows are opaque 32-byte identities supplied by the caller (a document
//! content hash on the [`crate`] side). Deduplication is by hash: applying the
//! same row twice, or replaying a duplicate outbox entry, never appends a
//! second row. Tombstones are recorded as rows, exactly like the kernel
//! oracle, so both engines agree on them byte for byte.

use std::collections::HashSet;

use lgwks_std::hash::Hasher;
use thiserror::Error;

/// Domain separator binding a checkpoint to this projection. Identical to the
/// kernel oracle (`lw.relproj.checkpoint.v2`), so checkpoints agree across the
/// absorption boundary for the same row sequence.
const CHECKPOINT_DOMAIN: &[u8] = b"lw.relproj.checkpoint.v2";
/// Maximum rows one projection holds. An apply past the ceiling is refused
/// with [`ProjectionError::RowBudgetExceeded`] instead of growing a heap
/// nobody bounded.
const MAX_RELATIONAL_ROWS: usize = 1_048_576;

/// One row application, mirroring the kernel `FactAction` fold surface:
/// live rows append by hash, tombstones record the tombstoned hash as a row.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[non_exhaustive]
pub enum ProjectionAction {
    /// A live row observed in causal order.
    Apply([u8; 32]),
    /// A checkpoint marker row.
    Checkpoint([u8; 32]),
    /// The tombstone of the named row, recorded as a row.
    Tombstone([u8; 32]),
}

/// Typed refusal from either relational engine.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ProjectionError {
    /// The engine refused the operation (SQLite failure, row budget, I/O).
    #[error("relational projection backend refused: {0}")]
    Backend(String),
    /// A state blob or file row is not well-formed.
    #[error("relational projection state malformed: {0}")]
    Malformed(String),
    /// An apply would grow the projection past [`MAX_RELATIONAL_ROWS`].
    #[error("relational projection row budget of {limit} exceeded")]
    RowBudgetExceeded {
        /// The ceiling the apply would have crossed.
        limit: usize,
    },
}

/// Ordered row list with its deduplication set, shared by both engines.
type RowCache = (Vec<[u8; 32]>, HashSet<[u8; 32]>);

/// The row identity an action carries.
#[must_use]
pub(crate) const fn row_of_action(action: &ProjectionAction) -> [u8; 32] {
    match *action {
        ProjectionAction::Apply(row)
        | ProjectionAction::Checkpoint(row)
        | ProjectionAction::Tombstone(row) => row,
    }
}

/// Admits one row into a shared row cache: inserts into the dedup set, refuses
/// past the ceiling while rolling the set back, and reports whether the row
/// is newly admitted. Both engines run this before their own write, so the
/// budget and the dedup rule exist exactly once.
fn admit_row(
    seen: &mut HashSet<[u8; 32]>,
    len: usize,
    row: [u8; 32],
) -> Result<bool, ProjectionError> {
    if !seen.insert(row) {
        return Ok(false);
    }
    if len >= MAX_RELATIONAL_ROWS {
        seen.remove(&row);
        return Err(ProjectionError::RowBudgetExceeded {
            limit: MAX_RELATIONAL_ROWS,
        });
    }
    Ok(true)
}

/// Computes the order-committing checkpoint over causal rows. Both engines
/// read the same bytes through this one function, so their checkpoints
/// agree by construction rather than by duplicated hashing.
fn checkpoint_rows(rows: &[[u8; 32]]) -> [u8; 32] {
    let mut hasher = Hasher::new();
    hasher.update(CHECKPOINT_DOMAIN);
    for row in rows {
        hasher.update(row);
    }
    *hasher.finalize().as_bytes()
}

/// Serializes causal rows as concatenated 32-byte hashes. Both engines emit
/// these bytes, so their states agree byte for byte by construction.
fn serialize_rows(rows: &[[u8; 32]]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.len().saturating_mul(32));
    for row in rows {
        out.extend_from_slice(row);
    }
    out
}

/// Refuses a state blob whose length is not a whole number of rows.
fn check_state_blob(bytes: &[u8]) -> Result<(), ProjectionError> {
    if !bytes.len().is_multiple_of(32) {
        return Err(ProjectionError::Malformed(format!(
            "state blob length {} is not a multiple of 32",
            bytes.len()
        )));
    }
    Ok(())
}

/// Decodes a state blob into its ordered rows plus dedup set, refusing
/// misalignment and duplicates. The SQLite engine writes each decoded row
/// into its table after this; the memory engine is done here.
fn decode_state_rows(bytes: &[u8]) -> Result<RowCache, ProjectionError> {
    check_state_blob(bytes)?;
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    let (chunks, _) = bytes.as_chunks::<32>();
    for chunk in chunks {
        let mut row = [0_u8; 32];
        row.copy_from_slice(chunk);
        if !seen.insert(row) {
            return Err(ProjectionError::Malformed(String::from(
                "duplicate row hash in state blob",
            )));
        }
        rows.push(row);
    }
    Ok((rows, seen))
}

/// In-memory relational projection: ordered unique row hashes.
///
/// Cloning is structural: snapshot restore compares a restored projection
/// against a full replay of the same value.
#[derive(Clone, Debug, Default)]
pub struct RelationalProjection {
    /// Rows in causal application order.
    rows: Vec<[u8; 32]>,
    /// Deduplication set over the same hashes.
    seen: HashSet<[u8; 32]>,
}

impl RelationalProjection {
    /// Creates an empty projection.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rows: Vec::new(),
            seen: HashSet::new(),
        }
    }

    /// Applies one action, appending its row unless already present.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionError::RowBudgetExceeded`] past the row ceiling.
    pub fn apply(&mut self, action: ProjectionAction) -> Result<(), ProjectionError> {
        let row = row_of_action(&action);
        if admit_row(&mut self.seen, self.rows.len(), row)? {
            self.rows.push(row);
        }
        Ok(())
    }

    /// Computes the order-committing checkpoint over all rows.
    #[must_use]
    pub fn checkpoint(&self) -> [u8; 32] {
        checkpoint_rows(&self.rows)
    }

    /// Returns the materialized rows in causal order.
    #[must_use]
    pub fn state(&self) -> &[[u8; 32]] {
        &self.rows
    }

    /// Serializes the rows as concatenated 32-byte hashes.
    #[must_use]
    pub fn serialize_state(&self) -> Vec<u8> {
        serialize_rows(&self.rows)
    }

    /// Restores a projection from [`Self::serialize_state`] bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionError::Malformed`] for a misaligned blob or a
    /// duplicated row hash.
    pub fn restore_from_state(bytes: &[u8]) -> Result<Self, ProjectionError> {
        check_state_blob(bytes)?;
        let (rows, seen) = decode_state_rows(bytes)?;
        Ok(Self { rows, seen })
    }
}

/// SQLite-backed relational projection: the disposable second engine.
///
/// Holds a `rusqlite::Connection` and an in-memory mirror of the row order so
/// infallible read paths (`checkpoint`, `state`, `serialize_state`) never
/// touch the database. The database is the write and dedup path; the mirror
/// is the read path.
#[derive(Debug)]
pub struct SqliteRelationalProjection {
    /// The canonical SQLite state.
    connection: rusqlite::Connection,
    /// In-memory mirror of the row order.
    rows: Vec<[u8; 32]>,
    /// Deduplication set over the same hashes.
    seen: HashSet<[u8; 32]>,
}

impl SqliteRelationalProjection {
    /// Opens an in-memory projection.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionError::Backend`] when the database cannot be made.
    pub fn new() -> Result<Self, ProjectionError> {
        let connection = rusqlite::Connection::open_in_memory()
            .map_err(|error| ProjectionError::Backend(error.to_string()))?;
        Self::prepare(&connection)?;
        Ok(Self {
            connection,
            rows: Vec::new(),
            seen: HashSet::new(),
        })
    }

    /// Opens or creates a file-backed projection at `path`, loading existing
    /// rows into the mirror so the read path is valid immediately.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionError::Backend`] when the file cannot be opened
    /// and [`ProjectionError::Malformed`] for a tampered row.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, ProjectionError> {
        let connection = rusqlite::Connection::open(path)
            .map_err(|error| ProjectionError::Backend(error.to_string()))?;
        connection
            .execute(
                "CREATE TABLE IF NOT EXISTS rows(idx INTEGER PRIMARY KEY, hash BLOB NOT NULL UNIQUE)",
                [],
            )
            .map_err(|error| ProjectionError::Backend(error.to_string()))?;
        let (rows, seen) = Self::load_existing_rows(&connection)?;
        Ok(Self {
            connection,
            rows,
            seen,
        })
    }

    /// Creates the rows table on a fresh connection.
    fn prepare(connection: &rusqlite::Connection) -> Result<(), ProjectionError> {
        connection
            .execute(
                "CREATE TABLE rows(idx INTEGER PRIMARY KEY, hash BLOB NOT NULL UNIQUE)",
                [],
            )
            .map_err(|error| ProjectionError::Backend(error.to_string()))?;
        Ok(())
    }

    /// Loads every persisted row in causal order into the read mirror.
    fn load_existing_rows(connection: &rusqlite::Connection) -> Result<RowCache, ProjectionError> {
        let mut rows = Vec::new();
        let mut seen = HashSet::new();
        let mut statement = connection
            .prepare("SELECT hash FROM rows ORDER BY idx")
            .map_err(|error| ProjectionError::Backend(error.to_string()))?;
        let row_iter = statement
            .query_map([], |row| {
                let hash_bytes: Vec<u8> = row.get(0)?;
                Ok(hash_bytes)
            })
            .map_err(|error| ProjectionError::Backend(error.to_string()))?;
        for queried in row_iter {
            let hash_bytes =
                queried.map_err(|error| ProjectionError::Backend(error.to_string()))?;
            let row = Self::decode_row_hash(&hash_bytes)?;
            if !seen.insert(row) {
                return Err(ProjectionError::Malformed(String::from(
                    "duplicate row hash in projection file",
                )));
            }
            rows.push(row);
        }
        Ok((rows, seen))
    }

    /// Validates one stored row hash instead of trusting the file.
    fn decode_row_hash(hash_bytes: &[u8]) -> Result<[u8; 32], ProjectionError> {
        if hash_bytes.len() != 32 {
            return Err(ProjectionError::Malformed(format!(
                "row hash length {} is not 32 in projection file",
                hash_bytes.len()
            )));
        }
        let mut row = [0_u8; 32];
        row.copy_from_slice(hash_bytes);
        Ok(row)
    }

    /// Applies one action through the SQLite write path, appending its row
    /// unless already present. The row lands in the table first; the
    /// in-memory mirror follows, so readers never touch the database.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionError::Backend`] when the insert fails and
    /// [`ProjectionError::RowBudgetExceeded`] past the row ceiling.
    pub fn apply(&mut self, action: ProjectionAction) -> Result<(), ProjectionError> {
        let row = row_of_action(&action);
        if admit_row(&mut self.seen, self.rows.len(), row)? {
            self.connection
                .execute(
                    "INSERT INTO rows (hash) VALUES (?1)",
                    rusqlite::params![&row[..]],
                )
                .map_err(|error| ProjectionError::Backend(error.to_string()))?;
            self.rows.push(row);
        }
        Ok(())
    }

    /// Returns the mirrored rows in causal order.
    #[must_use]
    pub fn state(&self) -> &[[u8; 32]] {
        &self.rows
    }

    /// Serializes the mirrored rows as concatenated 32-byte hashes.
    #[must_use]
    pub fn serialize_state(&self) -> Vec<u8> {
        serialize_rows(&self.rows)
    }

    /// Computes the order-committing checkpoint over the mirrored rows.
    #[must_use]
    pub fn checkpoint(&self) -> [u8; 32] {
        checkpoint_rows(&self.rows)
    }

    /// Restores a projection from [`Self::serialize_state`] bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ProjectionError::Malformed`] for a misaligned blob or a
    /// duplicated row hash, [`ProjectionError::Backend`] when the fresh
    /// in-memory database cannot be made.
    pub fn restore_from_state(bytes: &[u8]) -> Result<Self, ProjectionError> {
        let connection = rusqlite::Connection::open_in_memory()
            .map_err(|error| ProjectionError::Backend(error.to_string()))?;
        Self::prepare(&connection)?;
        let (rows, seen) = decode_state_rows(bytes)?;
        for row in &rows {
            connection
                .execute(
                    "INSERT INTO rows (hash) VALUES (?1)",
                    rusqlite::params![&row[..]],
                )
                .map_err(|error| ProjectionError::Backend(error.to_string()))?;
        }
        Ok(Self {
            connection,
            rows,
            seen,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic row identity for fact `n`: the estate hash of its index.
    fn row(n: u64) -> [u8; 32] {
        *lgwks_std::hash::blake3(&n.to_le_bytes()).as_bytes()
    }

    /// Applies `n` distinct rows in order.
    fn apply_many(
        apply: &mut dyn FnMut(ProjectionAction) -> Result<(), ProjectionError>,
        n: u64,
    ) -> Result<(), ProjectionError> {
        for i in 0..n {
            apply(ProjectionAction::Apply(row(i)))?;
        }
        Ok(())
    }

    #[test]
    fn projection_parity_byte_identical() -> Result<(), Box<dyn std::error::Error>> {
        let mut mem = RelationalProjection::new();
        apply_many(&mut |action| mem.apply(action), 100)?;
        mem.apply(ProjectionAction::Apply(row(50)))?;

        let mut sqlite = SqliteRelationalProjection::new()?;
        apply_many(&mut |action| sqlite.apply(action), 100)?;
        sqlite.apply(ProjectionAction::Apply(row(50)))?;

        assert_eq!(
            mem.serialize_state(),
            sqlite.serialize_state(),
            "serialize_state must be byte-identical across engines"
        );
        assert_eq!(
            mem.checkpoint(),
            sqlite.checkpoint(),
            "checkpoint must be identical across engines"
        );
        Ok(())
    }

    #[test]
    fn cross_restore_mem_to_sqlite() -> Result<(), Box<dyn std::error::Error>> {
        let mut mem = RelationalProjection::new();
        apply_many(&mut |action| mem.apply(action), 50)?;
        let blob = mem.serialize_state();
        let mem_checkpoint = mem.checkpoint();

        let restored = SqliteRelationalProjection::restore_from_state(&blob)?;
        assert_eq!(
            restored.checkpoint(),
            mem_checkpoint,
            "restored sqlite checkpoint must match original mem checkpoint"
        );
        Ok(())
    }

    #[test]
    fn cross_restore_sqlite_to_mem() -> Result<(), Box<dyn std::error::Error>> {
        let mut sqlite = SqliteRelationalProjection::new()?;
        apply_many(&mut |action| sqlite.apply(action), 50)?;
        let blob = sqlite.serialize_state();
        let sqlite_checkpoint = sqlite.checkpoint();

        let restored = RelationalProjection::restore_from_state(&blob)?;
        assert_eq!(
            restored.checkpoint(),
            sqlite_checkpoint,
            "restored mem checkpoint must match original sqlite checkpoint"
        );
        Ok(())
    }

    #[test]
    fn file_backed_durability_and_rebuild() -> Result<(), Box<dyn std::error::Error>> {
        let dir = std::env::temp_dir().join(format!(
            "ldb-relproj-{}-{}",
            std::process::id(),
            "durability"
        ));
        std::fs::create_dir_all(&dir)?;
        let path = dir.join("projection.db");

        let checkpoint_before = {
            let mut file = SqliteRelationalProjection::open(&path)?;
            apply_many(&mut |action| file.apply(action), 50)?;
            let checkpoint = file.checkpoint();
            drop(file);
            let reopened = SqliteRelationalProjection::open(&path)?;
            assert_eq!(
                reopened.checkpoint(),
                checkpoint,
                "reopened projection must preserve checkpoint"
            );
            drop(reopened);
            std::fs::remove_file(&path)?;
            checkpoint
        };

        let mut replayed = SqliteRelationalProjection::new()?;
        apply_many(&mut |action| replayed.apply(action), 50)?;
        assert_eq!(
            replayed.checkpoint(),
            checkpoint_before,
            "replay after delete must reproduce identical checkpoint"
        );
        std::fs::remove_dir_all(&dir)?;
        Ok(())
    }

    #[test]
    fn adversarial_duplicates() -> Result<(), Box<dyn std::error::Error>> {
        let mut mem = RelationalProjection::new();
        for _ in 0..5 {
            mem.apply(ProjectionAction::Apply(row(0)))?;
        }
        let mut sqlite = SqliteRelationalProjection::new()?;
        for _ in 0..5 {
            sqlite.apply(ProjectionAction::Apply(row(0)))?;
        }

        assert_eq!(mem.state().len(), 1, "mem: one unique fact");
        assert_eq!(sqlite.state().len(), 1, "sqlite: one unique fact");
        assert_eq!(mem.serialize_state(), sqlite.serialize_state());
        assert_eq!(mem.checkpoint(), sqlite.checkpoint());
        Ok(())
    }

    #[test]
    fn adversarial_empty() -> Result<(), Box<dyn std::error::Error>> {
        let mem = RelationalProjection::new();
        let sqlite = SqliteRelationalProjection::new()?;
        assert_eq!(mem.serialize_state(), sqlite.serialize_state());
        assert_eq!(mem.checkpoint(), sqlite.checkpoint());
        Ok(())
    }

    #[test]
    fn adversarial_10k() -> Result<(), Box<dyn std::error::Error>> {
        let mut mem = RelationalProjection::new();
        apply_many(&mut |action| mem.apply(action), 10_000)?;
        let mut sqlite = SqliteRelationalProjection::new()?;
        apply_many(&mut |action| sqlite.apply(action), 10_000)?;
        assert_eq!(
            mem.serialize_state(),
            sqlite.serialize_state(),
            "10k facts: serialize_state must match byte-for-byte"
        );
        assert_eq!(
            mem.checkpoint(),
            sqlite.checkpoint(),
            "10k facts: checkpoint must match"
        );
        Ok(())
    }

    #[test]
    fn row_budget_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let mut mem = RelationalProjection {
            rows: Vec::new(),
            seen: HashSet::new(),
        };
        for i in 0..MAX_RELATIONAL_ROWS {
            let id =
                u64::try_from(i).map_err(|error| format!("test row index overflow: {error}"))?;
            mem.apply(ProjectionAction::Apply(row(id)))?;
        }
        let refused = mem.apply(ProjectionAction::Apply(row(u64::MAX)));
        assert!(
            matches!(refused, Err(ProjectionError::RowBudgetExceeded { .. })),
            "one row past the ceiling must be refused, got {refused:?}"
        );
        Ok(())
    }

    #[test]
    fn misaligned_blob_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let error = RelationalProjection::restore_from_state(&[1_u8; 31]);
        assert!(
            matches!(error, Err(ProjectionError::Malformed(_))),
            "31 bytes must be refused, got {error:?}"
        );
        let error = SqliteRelationalProjection::restore_from_state(&[1_u8; 33]);
        assert!(
            matches!(error, Err(ProjectionError::Malformed(_))),
            "33 bytes must be refused, got {error:?}"
        );
        Ok(())
    }

    #[test]
    fn duplicate_blob_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let mut blob = Vec::new();
        blob.extend_from_slice(&row(7));
        blob.extend_from_slice(&row(7));
        let error = RelationalProjection::restore_from_state(&blob);
        assert!(
            matches!(error, Err(ProjectionError::Malformed(_))),
            "duplicated row must be refused, got {error:?}"
        );
        Ok(())
    }
}
