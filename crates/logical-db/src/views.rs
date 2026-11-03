//! Hybrid text-plus-vector projection views over one index.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::path::Path;

use logical_db_model::{DocumentId, IndexId};
use logical_db_store::projection_indexed::{IndexedMatch, IndexedProjection, tokenize};
use logical_db_store::snapshot_file::FileProjectionIndex;
use logical_db_store::{EmbeddedStore, TextIndexPage};

use super::db::Database;
use super::error::DatabaseError;

/// One ranked document from a hybrid text-plus-vector projection query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IndexedHit {
    /// The matching document.
    pub document: DocumentId,
    /// Deterministic fixed-point score; higher ranks first.
    pub score: u64,
}

impl IndexedHit {
    /// Builds one hit from its document and score.
    #[must_use]
    pub(crate) const fn new(document: DocumentId, score: u64) -> Self {
        Self { document, score }
    }
}

/// Report from a durable indexed-snapshot build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SnapshotBuildReport {
    /// Rows folded into the snapshot.
    pub rows: usize,
    /// On-disk byte length of the snapshot file.
    pub bytes: u64,
}

/// Derives the 32-byte projection row identity for a document.
///
/// The layout keeps the document UUID in the first sixteen bytes so a
/// snapshot hit maps back to its document; the remaining bytes are zero.
fn row_identity(document: DocumentId) -> [u8; 32] {
    let mut row = [0_u8; 32];
    row[..16].copy_from_slice(document.as_uuid().as_bytes());
    row
}

/// Recovers the document from a [`row_identity`] row identity.
///
/// # Errors
///
/// Returns [`DatabaseError::ProjectionView`] when the row bytes do not
/// decode, which cannot happen for rows this module wrote.
fn document_of_row(hash: &[u8; 32]) -> Result<DocumentId, DatabaseError> {
    let mut raw = [0_u8; 16];
    raw.copy_from_slice(&hash[..16]);
    Ok(DocumentId::from_uuid(super::identity::decode_identity_id(
        &raw,
    )?))
}

/// Maps scored index matches back to ranked document hits.
///
/// Both the live hybrid query and the durable snapshot query share this
/// fold instead of repeating it.
fn ranked_hits(matches: Vec<IndexedMatch>) -> Result<Vec<IndexedHit>, DatabaseError> {
    matches
        .into_iter()
        .map(|scored| {
            document_of_row(&scored.hash).map(|document| IndexedHit::new(document, scored.score))
        })
        .collect()
}

impl Database {
    /// Folds one index's live text rows into a hybrid projection, paging
    /// through the materialized text index. Both the live hybrid query and
    /// the durable snapshot build share this fold instead of repeating it.
    fn materialize_projection(&self, index: IndexId) -> Result<IndexedProjection, DatabaseError> {
        let mut projection = IndexedProjection::new();
        let mut cursor: Option<Vec<u8>> = None;
        let store: &EmbeddedStore = &self.store;
        loop {
            let page: TextIndexPage =
                store.scan_index_text(self.scope, index, cursor.as_deref(), 256)?;
            for &(document, ref text) in page.rows() {
                projection.insert(row_identity(document), tokenize(text));
            }
            if page.next.is_none() {
                break;
            }
            cursor = page.next;
        }
        Ok(projection)
    }

    /// Runs a bounded hybrid text-plus-vector query over one index.
    ///
    /// Text rows materialized under `index` are folded into a deterministic
    /// hybrid index and ranked by text overlap plus caller-seed vector
    /// similarity. Results carry content identities and fixed-point scores
    /// only, never raw values. Sealed bytes are never decrypted for
    /// indexing: only plaintext `Text` rows participate.
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::ProjectionView`] for an over-long seed and
    /// [`DatabaseError::Store`] for storage failures.
    pub fn query_indexed_hybrid(
        &self,
        index: IndexId,
        query_text: &str,
        vector_seed: &[i32],
        top_k: usize,
    ) -> Result<Vec<IndexedHit>, DatabaseError> {
        let projection = self.materialize_projection(index)?;
        if projection.is_empty() {
            return Ok(Vec::new());
        }
        let matches = projection.query(query_text, vector_seed, top_k)?;
        ranked_hits(matches)
    }

    /// Builds a durable indexed snapshot for one index at `path`.
    ///
    /// The write is atomic: bytes go to a sibling temp file, sync, and rename
    /// over the target, so a crash leaves the previous snapshot or the new
    /// one, never a partial file. Query it with
    /// [`Database::query_indexed_snapshot`].
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::Store`] for storage failures and
    /// [`DatabaseError::Io`] when the built file cannot be measured.
    pub fn build_indexed_snapshot(
        &self,
        index: IndexId,
        path: impl AsRef<Path>,
    ) -> Result<SnapshotBuildReport, DatabaseError> {
        let projection = self.materialize_projection(index)?;
        let rows = projection.len();
        FileProjectionIndex::build(&path, &projection)?;
        let bytes = std::fs::metadata(&path).map(|metadata| metadata.len())?;
        Ok(SnapshotBuildReport { rows, bytes })
    }

    /// Queries a durable indexed snapshot built by
    /// [`Database::build_indexed_snapshot`].
    ///
    /// Touches only the query's working set through positional reads. The
    /// answer is identical to [`Database::query_indexed_hybrid`] over the
    /// same rows.
    ///
    /// # Errors
    ///
    /// Returns [`DatabaseError::ProjectionView`] for malformed snapshots,
    /// over-long seeds, and file I/O failures.
    pub fn query_indexed_snapshot(
        path: impl AsRef<Path>,
        query_text: &str,
        vector_seed: &[i32],
        top_k: usize,
    ) -> Result<Vec<IndexedHit>, DatabaseError> {
        let index = FileProjectionIndex::open(path)?;
        let matches = index.query(query_text, vector_seed, top_k)?;
        ranked_hits(matches)
    }
}
