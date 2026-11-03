//! Deterministic text-plus-vector index over application rows (#119 row 4).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! This absorbs the kernel `IndexedProjection` (`state-fabric`, #609): an
//! inverted term index plus per-row deterministic vectors, scored by an
//! integer fixed-point hybrid and ranked with a hash tie-break so results are
//! identical across runs and across the in-memory and durable readers.
//!
//! Two deliberate absorption deltas, both fail-closed:
//!
//! * Rows are opaque 32-byte identities with caller-supplied terms. The kernel
//!   decrypted sealed fact metadata through a keyring; logical-DB stores
//!   sealed bytes opaquely and never decrypts them for indexing, so terms come
//!   only from plaintext the caller already holds (for example `Text` index
//!   values). A row the caller cannot read contributes no terms.
//! * Tombstones remove the row instead of recording it: the kernel recorded a
//!   tombstone hash as a relational row, but a text index answers "what
//!   matches", and a removed row matching nothing is the honest answer here.

use std::collections::BTreeMap;

use lgwks_std::hash::Hasher;
use thiserror::Error;

/// Dimension of the deterministic row and query vectors.
pub(crate) const VECTOR_DIM: usize = 16;
/// Maximum results one query returns.
pub(crate) const MAX_TOP_K: usize = 64;
/// Maximum tokens indexed for one row. Bounds index growth per row.
pub(crate) const MAX_TOKENS_PER_ROW: usize = 64;
/// Fixed-point scale for the cosine component (2^16).
pub(crate) const ACTIVATION_SCALE: u64 = 1 << 16;
/// Weight of the text-overlap component in the final score.
pub(crate) const TEXT_WEIGHT: u64 = ACTIVATION_SCALE;
/// Weight of the vector-similarity component in the final score.
pub(crate) const VECTOR_WEIGHT: u64 = ACTIVATION_SCALE;
/// Domain separator for hashing one term into a vector bucket and weight.
const VECTOR_DERIVE_DOMAIN: &[u8] = b"ldb.indexedproj.vector.v1";

/// One matching row from an indexed hybrid query.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct IndexedMatch {
    /// The indexed row identity.
    pub hash: [u8; 32],
    /// Deterministic fixed-point score; higher ranks first.
    pub score: u64,
}

impl IndexedMatch {
    /// Builds one match from its row identity and score.
    #[must_use]
    pub const fn new(hash: [u8; 32], score: u64) -> Self {
        Self { hash, score }
    }
}

/// Typed refusal from the indexed projection.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IndexedError {
    /// The query seed is longer than [`VECTOR_DIM`].
    #[error("query seed of {len} exceeds vector dimension {VECTOR_DIM}")]
    SeedTooLong {
        /// The refused seed length.
        len: usize,
    },
    /// A state blob is not well-formed.
    #[error("indexed projection state malformed: {0}")]
    Malformed(String),
}

/// Candidate set: row hash to term-overlap count and that row's vector.
type Candidates = BTreeMap<[u8; 32], (usize, [i32; VECTOR_DIM])>;
/// A borrowed view of the families the `LWMX` writer lays out.
pub(crate) type LwmxFamilies<'a> = (
    &'a BTreeMap<String, Vec<[u8; 32]>>,
    &'a BTreeMap<[u8; 32], [i32; VECTOR_DIM]>,
);

/// Deterministic text-plus-vector index over a bounded row set.
#[derive(Clone, Debug, Default)]
pub struct IndexedProjection {
    /// Term to posting row hashes.
    term_index: BTreeMap<String, Vec<[u8; 32]>>,
    /// Row hash to derived vector.
    row_vectors: BTreeMap<[u8; 32], [i32; VECTOR_DIM]>,
    /// Row hash to indexed terms (the reverse map tombstones prune).
    row_terms: BTreeMap<[u8; 32], Vec<String>>,
}

impl IndexedProjection {
    /// Creates an empty index.
    #[must_use]
    pub fn new() -> Self {
        Self {
            term_index: BTreeMap::new(),
            row_vectors: BTreeMap::new(),
            row_terms: BTreeMap::new(),
        }
    }

    /// Indexes one row under its terms, replacing any prior entry for the
    /// same hash. Idempotent across replays.
    pub fn insert(&mut self, hash: [u8; 32], terms: Vec<String>) {
        self.remove(&hash);
        let mut normalized = terms;
        normalized.sort();
        normalized.dedup();
        normalized.truncate(MAX_TOKENS_PER_ROW);
        let vector = derive_vector(&normalized);
        for term in &normalized {
            self.term_index.entry(term.clone()).or_default().push(hash);
        }
        self.row_vectors.insert(hash, vector);
        self.row_terms.insert(hash, normalized);
    }

    /// Removes one row from every index family. Missing rows are a no-op.
    pub(crate) fn remove(&mut self, hash: &[u8; 32]) {
        if let Some(terms) = self.row_terms.remove(hash) {
            for term in terms {
                if let Some(list) = self.term_index.get_mut(&term) {
                    list.retain(|candidate| candidate != hash);
                    if list.is_empty() {
                        self.term_index.remove(&term);
                    }
                }
            }
        }
        self.row_vectors.remove(hash);
    }

    /// Executes a bounded text-plus-vector hybrid query.
    ///
    /// # Errors
    ///
    /// Returns [`IndexedError::SeedTooLong`] when the seed is longer than
    /// [`VECTOR_DIM`].
    pub fn query(
        &self,
        query_text: &str,
        vector_seed: &[i32],
        top_k: usize,
    ) -> Result<Vec<IndexedMatch>, IndexedError> {
        let top_k = top_k.min(MAX_TOP_K);
        if top_k == 0 {
            return Ok(Vec::new());
        }
        require_seed_fits(vector_seed)?;
        let query_terms = tokenize(query_text);
        let seed = seed_vector(vector_seed);
        if nothing_to_rank(&query_terms, &seed) {
            return Ok(Vec::new());
        }
        let candidates = if query_terms.is_empty() {
            let mut all = Candidates::new();
            for (hash, vector) in &self.row_vectors {
                all.insert(*hash, (0, *vector));
            }
            all
        } else {
            let mut scoped = Candidates::new();
            for term in &query_terms {
                let Some(hashes) = self.term_index.get(term) else {
                    continue;
                };
                for hash in hashes {
                    let Some(vector) = self.row_vectors.get(hash).copied() else {
                        return Err(IndexedError::Malformed(format!(
                            "indexed row has postings but no vector: {hash:?}"
                        )));
                    };
                    scoped
                        .entry(*hash)
                        .and_modify(|entry| {
                            entry.0 = entry.0.saturating_add(1);
                        })
                        .or_insert((1, vector));
                }
            }
            scoped
        };
        rank_candidates(candidates, &seed, top_k)
    }

    /// Returns the number of indexed rows.
    #[must_use]
    pub fn len(&self) -> usize {
        self.row_vectors.len()
    }

    /// Returns true when no rows are indexed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.row_vectors.is_empty()
    }

    /// Returns the index families the durable `LWMX` writer lays out.
    ///
    /// Borrowed, never copied: the writer streams them straight into bytes.
    pub(crate) fn lwmx_families(&self) -> LwmxFamilies<'_> {
        (&self.term_index, &self.row_vectors)
    }
}

/// Lower-cases, splits on non-alphanumeric boundaries, drops empties, dedups.
#[must_use]
pub fn tokenize(input: &str) -> Vec<String> {
    let mut current = String::new();
    let mut out = Vec::new();
    for ch in input.chars() {
        if ch.is_alphanumeric() {
            current.extend(ch.to_lowercase());
        } else if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out.sort();
    out.dedup();
    out
}

/// Derives a deterministic fixed vector from a term list.
#[must_use]
pub(crate) fn derive_vector(terms: &[String]) -> [i32; VECTOR_DIM] {
    let mut out = [0_i32; VECTOR_DIM];
    for term in terms {
        let mut hasher = Hasher::new();
        hasher.update(VECTOR_DERIVE_DOMAIN);
        hasher.update(term.as_bytes());
        let digest = hasher.finalize();
        let bytes = digest.as_bytes();
        let dim = usize::from(bytes[0]) % VECTOR_DIM;
        let weight = i16::from_le_bytes([bytes[1], bytes[2]]);
        out[dim] = out[dim].saturating_add(i32::from(weight));
    }
    out
}

/// Fixed-point cosine similarity in `[0, ACTIVATION_SCALE]`, integer math so
/// results are deterministic across platforms.
///
/// # Errors
///
/// Returns [`IndexedError::Malformed`] when the fixed-point rescale overflows
/// its target width, which cannot happen for the declared constants.
pub(crate) fn cosine_similarity_fixed(
    left: &[i32; VECTOR_DIM],
    right: &[i32; VECTOR_DIM],
) -> Result<u64, IndexedError> {
    let mut dot: i64 = 0;
    let mut left_norm: i64 = 0;
    let mut right_norm: i64 = 0;
    for i in 0..VECTOR_DIM {
        let (Some(left_component), Some(right_component)) = (left.get(i), right.get(i)) else {
            continue;
        };
        let (left_value, right_value) = (i64::from(*left_component), i64::from(*right_component));
        dot = dot.saturating_add(left_value.saturating_mul(right_value));
        left_norm = left_norm.saturating_add(left_value.saturating_mul(left_value));
        right_norm = right_norm.saturating_add(right_value.saturating_mul(right_value));
    }
    if left_norm == 0 || right_norm == 0 {
        return Ok(0);
    }
    let denominator = left_norm
        .saturating_mul(right_norm)
        .isqrt()
        .saturating_add(1);
    if denominator <= 0 {
        return Ok(0);
    }
    let scaled = dot
        .abs()
        .saturating_mul(FIXED_POINT_SCALE_I64)
        .checked_div(denominator)
        .ok_or_else(|| IndexedError::Malformed(String::from("cosine rescale overflowed")))?;
    u64::try_from(scaled.clamp(0, FIXED_POINT_SCALE_I64))
        .map_err(|error| IndexedError::Malformed(format!("cosine score out of range: {error}")))
}

/// The fixed-point scale as a signed integer for score math.
const FIXED_POINT_SCALE_I64: i64 = 1 << 16;

/// Refuses an over-long query seed instead of truncating it.
pub(crate) fn require_seed_fits(vector_seed: &[i32]) -> Result<(), IndexedError> {
    if vector_seed.len() > VECTOR_DIM {
        return Err(IndexedError::SeedTooLong {
            len: vector_seed.len(),
        });
    }
    Ok(())
}

/// Widens a caller seed to the fixed dimension; missing parts read as zero.
pub(crate) fn seed_vector(vector_seed: &[i32]) -> [i32; VECTOR_DIM] {
    let mut seed = [0_i32; VECTOR_DIM];
    for (slot, component) in seed.iter_mut().zip(vector_seed) {
        *slot = *component;
    }
    seed
}

/// True when there is nothing to rank against: no terms and an all-zero seed
/// would score every row identically.
pub(crate) fn nothing_to_rank(query_terms: &[String], seed: &[i32; VECTOR_DIM]) -> bool {
    query_terms.is_empty() && seed.iter().all(|component| *component == 0)
}

/// Scores every candidate, orders by score then row hash, truncates to top-k.
pub(crate) fn rank_candidates(
    candidates: Candidates,
    seed: &[i32; VECTOR_DIM],
    top_k: usize,
) -> Result<Vec<IndexedMatch>, IndexedError> {
    let mut scored: Vec<IndexedMatch> = Vec::new();
    for (hash, (overlap, vector)) in candidates {
        let overlap_u64 = u64::try_from(overlap).map_err(|error| {
            IndexedError::Malformed(format!("candidate overlap overflow: {error}"))
        })?;
        let text_score = overlap_u64.saturating_mul(TEXT_WEIGHT);
        let vector_score = cosine_similarity_fixed(seed, &vector)?
            .saturating_mul(VECTOR_WEIGHT)
            .checked_div(ACTIVATION_SCALE)
            .ok_or_else(|| {
                IndexedError::Malformed(String::from("vector score rescale overflowed"))
            })?;
        let score = text_score.saturating_add(vector_score);
        scored.push(IndexedMatch::new(hash, score));
    }
    scored.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.hash.cmp(&right.hash))
    });
    scored.truncate(top_k);
    Ok(scored)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic row identity for fixture `n`.
    fn row(number: u8) -> [u8; 32] {
        let mut hash = [0_u8; 32];
        hash[0] = number;
        hash[1] = number.wrapping_mul(7);
        hash
    }

    #[test]
    fn tokenize_splits_and_lowers() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(
            tokenize("Hello, world!  hello"),
            vec![String::from("hello"), String::from("world")],
            "tokenizer must split and lowercase"
        );
        assert!(tokenize("").is_empty(), "empty input tokenizes to nothing");
        Ok(())
    }

    #[test]
    fn empty_query_returns_no_results() -> Result<(), Box<dyn std::error::Error>> {
        let projection = IndexedProjection::new();
        assert!(
            projection.query("", &[0_i32; VECTOR_DIM], 8)?.is_empty(),
            "empty index must answer nothing"
        );
        Ok(())
    }

    #[test]
    fn vector_seed_too_long_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let projection = IndexedProjection::new();
        let oversized = VECTOR_DIM.saturating_add(1);
        let seed = vec![1_i32; oversized];
        let error = projection.query("", &seed, 8);
        assert!(
            matches!(error, Err(IndexedError::SeedTooLong { .. })),
            "over-long seed must be refused, got {error:?}"
        );
        Ok(())
    }

    #[test]
    fn lwmx_families_cover_inserted_rows() -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = IndexedProjection::new();
        let hash = row(1);
        let terms = vec![String::from("alpha"), String::from("beta")];
        projection.insert(hash, terms);
        let (terms_view, vectors_view) = projection.lwmx_families();
        assert!(
            terms_view.contains_key("alpha") && terms_view.contains_key("beta"),
            "both terms must reach the durable writer families"
        );
        assert!(
            vectors_view.contains_key(&hash),
            "the row vector must reach the durable writer families"
        );
        assert_eq!(projection.len(), 1, "one row indexed");
        Ok(())
    }

    #[test]
    fn top_k_is_bounded() -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = IndexedProjection::new();
        for index in 0..100_u8 {
            projection.insert(row(index), vec![String::from("shared")]);
        }
        let results = projection.query("shared", &[0_i32; VECTOR_DIM], 10)?;
        assert_eq!(results.len(), 10, "top-k must bound the answer");
        Ok(())
    }

    #[test]
    fn text_query_returns_matching_rows() -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = IndexedProjection::new();
        projection.insert(row(1), vec![String::from("alpha")]);
        projection.insert(row(2), vec![String::from("beta")]);
        projection.insert(row(3), vec![String::from("gamma")]);

        let results = projection.query("alpha", &[], 8)?;
        assert_eq!(results.len(), 1, "one row carries the term");
        assert_eq!(results[0].hash, row(1), "the matching row ranks first");

        let results = projection.query("absent", &[], 8)?;
        assert!(results.is_empty(), "unknown terms match nothing");
        Ok(())
    }

    #[test]
    fn top_k_bound_carries_only_hashes_and_scores() -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = IndexedProjection::new();
        for index in 0..20_u8 {
            projection.insert(row(index), vec![String::from("shared")]);
        }
        let results = projection.query("shared", &[], 5)?;
        assert_eq!(results.len(), 5, "top-k bounds the answer to five");
        for scored in &results {
            let _ = scored.score;
            let _ = scored.hash;
        }
        Ok(())
    }

    #[test]
    fn vector_seed_ranking_is_deterministic() -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = IndexedProjection::new();
        projection.insert(row(1), vec![String::from("term-a")]);
        projection.insert(row(2), vec![String::from("term-b")]);

        let seed = vec![1000_i32; VECTOR_DIM];
        let first = projection.query("", &seed, 8)?;
        let second = projection.query("", &seed, 8)?;
        assert_eq!(first, second, "same seed must rank identically");
        assert_eq!(first.len(), 2, "both rows rank under a live seed");
        Ok(())
    }

    #[test]
    fn tombstone_removes_the_row() -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = IndexedProjection::new();
        projection.insert(row(1), vec![String::from("alpha")]);
        projection.remove(&row(1));
        assert!(
            projection.query("alpha", &[], 8)?.is_empty(),
            "a removed row must match nothing"
        );
        assert!(projection.is_empty(), "removal empties a one-row index");
        Ok(())
    }

    #[test]
    fn reinsert_replaces_prior_terms() -> Result<(), Box<dyn std::error::Error>> {
        let mut projection = IndexedProjection::new();
        projection.insert(row(1), vec![String::from("alpha")]);
        projection.insert(row(1), vec![String::from("beta")]);
        assert!(
            projection.query("alpha", &[], 8)?.is_empty(),
            "reindexing must drop the old term"
        );
        assert_eq!(
            projection.query("beta", &[], 8)?.len(),
            1,
            "reindexing must install the new term"
        );
        Ok(())
    }
}
