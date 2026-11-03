//! Durable LWMX projection snapshot format and in-memory reader (#119 row 4).
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! This absorbs the kernel LWMX snapshot (`state-fabric::projection_snapshot`,
//! #620): the index is laid out so a query is served in place over the durable
//! bytes, touching only the binary-search paths plus the matched postings and
//! vectors, never the whole file. The writer, the format, the reader math, and
//! the hash domains are unchanged from the kernel oracle. The file mapping
//! itself lives in [`super::snapshot_file`], which serves the identical bytes
//! through positional reads.
//!
//! ## Layout (little-endian)
//!
//! ```text
//! Header (96 bytes):
//!   [ 0.. 4)  magic   = "LWMX"
//!   [ 4.. 8)  version = 1 (u32)
//!   [ 8..16)  fact_count       (u64)
//!   [16..24)  term_count       (u64)
//!   [24..32)  term_table_off   (u64, == 96)
//!   [32..40)  postings_off     (u64)
//!   [40..48)  vectors_off      (u64)
//!   [48..56)  terms_blob_off   (u64)
//!   [56..64)  total_len        (u64, == bytes.len())
//!   [64..96)  body_hash        (blake3 over header[0..64] ++ bytes[96..])
//! term_table : term_count × 24 bytes, sorted by term (BTreeMap order):
//!   term_blob_off (u64) | term_len (u32) | postings_rel_off (u64) | postings_count (u32)
//! postings   : concatenated row hashes (32 bytes each), grouped per term
//! vectors    : fact_count × (32-byte hash ++ VECTOR_DIM×i32), sorted by hash
//! terms_blob : concatenated UTF-8 term bytes
//! ```

use std::cmp::Ordering;
use std::collections::BTreeMap;

use lgwks_std::hash::Hasher;
use thiserror::Error;

use super::projection_indexed::{
    IndexedMatch, IndexedProjection, MAX_TOP_K, VECTOR_DIM, nothing_to_rank, rank_candidates,
    require_seed_fits, seed_vector, tokenize,
};

/// Magic identifying an `LWMX` snapshot.
pub(crate) const SNAPSHOT_MAGIC: &[u8; 4] = b"LWMX";
/// Format version.
pub(crate) const SNAPSHOT_FORMAT_VERSION: u32 = 1;
/// Header length in bytes.
pub(crate) const HEADER_LEN: usize = 96;
/// Bytes per term-table entry.
pub(crate) const TERM_ENTRY_LEN: usize = 24;
/// Bytes per vector record: 32-byte hash plus `VECTOR_DIM` little-endian i32s.
const VECTOR_RECORD_LEN: usize = 32 + VECTOR_DIM * 4;
/// Domain separator for the snapshot body hash.
const SNAPSHOT_HASH_DOMAIN: &[u8] = b"ldb.projsnapshot.body.v1";

/// Typed refusal from the snapshot format or reader.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SnapshotError {
    /// The bytes are not a well-formed snapshot.
    #[error("projection snapshot malformed: {0}")]
    Malformed(String),
    /// File I/O failed.
    #[error("projection snapshot I/O refused at {at}: {reason}")]
    Io {
        /// The operation that failed.
        at: &'static str,
        /// What the OS reported.
        reason: String,
    },
}

impl From<super::projection_indexed::IndexedError> for SnapshotError {
    /// Adapts an in-memory query refusal for the durable reader.
    fn from(error: super::projection_indexed::IndexedError) -> Self {
        match error {
            super::projection_indexed::IndexedError::SeedTooLong { len } => {
                Self::Malformed(format!("query seed of {len} exceeds vector dimension"))
            }
            super::projection_indexed::IndexedError::Malformed(message) => Self::Malformed(message),
        }
    }
}

/// Serializes an [`IndexedProjection`] into the `LWMX` mmap-queryable format.
///
/// Deterministic: the same index always produces the same bytes, including a
/// self-describing `body_hash`. The reverse row-terms map is not written; the
/// query path never needs it.
///
/// # Errors
///
/// Returns [`SnapshotError::Malformed`] when a section length overflows `u64`.
pub(crate) fn to_snapshot_bytes(projection: &IndexedProjection) -> Result<Vec<u8>, SnapshotError> {
    let (term_index, row_vectors) = projection.lwmx_families();
    let terms = build_term_sections(term_index)?;
    let vectors = build_vector_section(row_vectors)?;

    let term_table_off = u64_len(HEADER_LEN)?;
    let postings_off = term_table_off
        .checked_add(u64_len(terms.table.len())?)
        .ok_or_else(|| section_overflow("postings offset"))?;
    let vectors_off = postings_off
        .checked_add(u64_len(terms.postings.len())?)
        .ok_or_else(|| section_overflow("vectors offset"))?;
    let terms_blob_off = vectors_off
        .checked_add(u64_len(vectors.len())?)
        .ok_or_else(|| section_overflow("terms blob offset"))?;
    let total_len = terms_blob_off
        .checked_add(u64_len(terms.blob.len())?)
        .ok_or_else(|| section_overflow("total length"))?;

    let mut out = Vec::new();
    out.extend_from_slice(SNAPSHOT_MAGIC);
    out.extend_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    for field in [
        u64_len(row_vectors.len())?,
        u64_len(term_index.len())?,
        term_table_off,
        postings_off,
        vectors_off,
        terms_blob_off,
        total_len,
    ] {
        out.extend_from_slice(&field.to_le_bytes());
    }
    out.extend_from_slice(&[0_u8; 32]);
    out.extend_from_slice(&terms.table);
    out.extend_from_slice(&terms.postings);
    out.extend_from_slice(&vectors);
    out.extend_from_slice(&terms.blob);

    let digest = body_hash(&out)?;
    out[64..96].copy_from_slice(&digest);
    Ok(out)
}

/// Refuses an overflowing section offset.
fn section_overflow(section: &str) -> SnapshotError {
    SnapshotError::Malformed(format!("{section} overflow"))
}

/// Converts a section length into its `u64` field.
///
/// # Errors
///
/// Returns [`SnapshotError::Malformed`] on overflow, which cannot happen on
/// any supported target but is refused rather than saturated.
pub(crate) fn u64_len(len: usize) -> Result<u64, SnapshotError> {
    u64::try_from(len)
        .map_err(|error| SnapshotError::Malformed(format!("section length overflow: {error}")))
}

/// The three variable-length term sections.
struct TermSections {
    /// `term_count` × 24-byte rows.
    table: Vec<u8>,
    /// Concatenated postings.
    postings: Vec<u8>,
    /// Concatenated term bytes.
    blob: Vec<u8>,
}

/// Builds all three term sections in one pass so relative offsets cannot
/// drift from a second traversal.
fn build_term_sections(
    term_index: &BTreeMap<String, Vec<[u8; 32]>>,
) -> Result<TermSections, SnapshotError> {
    let mut table = Vec::new();
    let mut postings = Vec::new();
    let mut blob = Vec::new();
    for (term, hashes) in term_index {
        let term_blob_off = u64_len(blob.len())?;
        blob.extend_from_slice(term.as_bytes());
        let postings_rel_off = u64_len(postings.len())?;
        // Postings sort by row hash so the bytes are canonical: insertion
        // order never leaks into the durable snapshot.
        let mut ordered: Vec<[u8; 32]> = hashes.clone();
        ordered.sort();
        for hash in &ordered {
            postings.extend_from_slice(hash);
        }
        table.extend_from_slice(&term_blob_off.to_le_bytes());
        let term_len = u32::try_from(term.len())
            .map_err(|error| SnapshotError::Malformed(format!("term length overflow: {error}")))?;
        table.extend_from_slice(&term_len.to_le_bytes());
        table.extend_from_slice(&postings_rel_off.to_le_bytes());
        let hash_count = u32::try_from(hashes.len()).map_err(|error| {
            SnapshotError::Malformed(format!("postings count overflow: {error}"))
        })?;
        table.extend_from_slice(&hash_count.to_le_bytes());
    }
    Ok(TermSections {
        table,
        postings,
        blob,
    })
}

/// Builds the vector section in hash order so the reader can binary-search it.
fn build_vector_section(
    row_vectors: &BTreeMap<[u8; 32], [i32; VECTOR_DIM]>,
) -> Result<Vec<u8>, SnapshotError> {
    let mut vectors = Vec::new();
    for (hash, vector) in row_vectors {
        vectors.extend_from_slice(hash);
        for component in vector {
            vectors.extend_from_slice(&component.to_le_bytes());
        }
    }
    Ok(vectors)
}

/// Computes the body hash over header-sans-hash plus body.
///
/// # Errors
///
/// Returns [`SnapshotError::Malformed`] when the input is shorter than the
/// hashed regions, which cannot happen for writer output or parsed input.
fn body_hash(bytes: &[u8]) -> Result<[u8; 32], SnapshotError> {
    let malformed = || SnapshotError::Malformed(String::from("snapshot too short to hash"));
    let Some(head) = bytes.get(..64) else {
        return Err(malformed());
    };
    let Some(body) = bytes.get(HEADER_LEN..) else {
        return Err(malformed());
    };
    let mut hasher = Hasher::new();
    hasher.update(SNAPSHOT_HASH_DOMAIN);
    hasher.update(head);
    hasher.update(body);
    Ok(*hasher.finalize().as_bytes())
}

/// Parsed header with every structural invariant checked.
#[derive(Debug)]
pub(crate) struct Header {
    /// Number of indexed rows.
    pub(crate) fact_count: usize,
    /// Number of indexed terms.
    pub(crate) term_count: usize,
    /// Absolute offset of the term table.
    pub(crate) term_table_off: usize,
    /// Absolute offset of the postings region.
    pub(crate) postings_off: usize,
    /// Absolute offset of the vector section.
    pub(crate) vectors_off: usize,
    /// Absolute offset of the terms blob.
    pub(crate) terms_blob_off: usize,
}

/// Refuses what is not an `LWMX` snapshot before any offset is believed.
///
/// `expected_total` is the byte length the snapshot claims against: the full
/// slice length for in-memory reads, the file metadata length for the
/// header-only open path.
pub(crate) fn parse_header(bytes: &[u8], expected_total: usize) -> Result<Header, SnapshotError> {
    if bytes.len() < HEADER_LEN {
        return Err(malformed("snapshot shorter than header"));
    }
    if bytes.get(..4) != Some(SNAPSHOT_MAGIC.as_slice()) {
        return Err(malformed("snapshot magic mismatch"));
    }
    if bytes.get(4..8) != Some(SNAPSHOT_FORMAT_VERSION.to_le_bytes().as_slice()) {
        return Err(malformed("snapshot format version unsupported"));
    }
    let mut fields = [0_usize; 7];
    for (slot, offset) in fields.iter_mut().zip((8_usize..).step_by(8)) {
        let end = offset.saturating_add(8);
        let Some(slice) = bytes.get(offset..end) else {
            return Err(malformed("header field out of bounds"));
        };
        let mut buffer = [0_u8; 8];
        buffer.copy_from_slice(slice);
        let Some(value) = usize::try_from(u64::from_le_bytes(buffer)).ok() else {
            return Err(malformed("header field overflow"));
        };
        *slot = value;
    }
    let (
        fact_count,
        term_count,
        term_table_off,
        postings_off,
        vectors_off,
        terms_blob_off,
        total_len,
    ) = (
        fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6],
    );
    if total_len != expected_total {
        return Err(malformed("snapshot total_len does not match byte length"));
    }
    if term_table_off != HEADER_LEN {
        return Err(malformed("term_table must follow header"));
    }
    let Some(term_table_len) = term_count.checked_mul(TERM_ENTRY_LEN) else {
        return Err(malformed("term_table size overflow"));
    };
    if postings_off != term_table_off.saturating_add(term_table_len) {
        return Err(malformed("postings offset inconsistent with term_count"));
    }
    if vectors_off < postings_off || vectors_off.saturating_sub(postings_off) % 32 != 0 {
        return Err(malformed("postings region not a whole number of hashes"));
    }
    let Some(vectors_len) = fact_count.checked_mul(VECTOR_RECORD_LEN) else {
        return Err(malformed("vectors size overflow"));
    };
    if terms_blob_off != vectors_off.saturating_add(vectors_len) {
        return Err(malformed("terms_blob offset inconsistent with fact_count"));
    }
    if terms_blob_off > total_len {
        return Err(malformed("terms_blob offset past end"));
    }
    Ok(Header {
        fact_count,
        term_count,
        term_table_off,
        postings_off,
        vectors_off,
        terms_blob_off,
    })
}

/// One refusal constructor shared by every structural check below.
fn malformed(message: &str) -> SnapshotError {
    SnapshotError::Malformed(String::from(message))
}

/// Converts a decoded offset to `usize`, refusing overflow with its site, so
/// the four offset conversions below share one refusal path.
fn usize_field(value: u64, what: &str) -> Result<usize, SnapshotError> {
    usize::try_from(value).map_err(|error| malformed(&format!("{what} overflow: {error}")))
}

/// A bounded byte source the snapshot readers serve from.
///
/// Slices and the counted file reader both implement it, so the offset
/// arithmetic below exists exactly once and serves the in-memory and the
/// positional query paths alike.
pub(crate) trait SnapshotWindow {
    /// Reads exactly `len` bytes at `offset`, refusing out-of-bounds access.
    fn read_window(&self, offset: usize, len: usize) -> Result<Vec<u8>, SnapshotError>;
}

/// A [`SnapshotWindow`] over one validated in-memory slice.
pub(crate) struct SliceWindow<'a> {
    /// The bytes read through.
    bytes: &'a [u8],
}

impl<'a> SliceWindow<'a> {
    /// Borrows one validated snapshot.
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl SnapshotWindow for SliceWindow<'_> {
    fn read_window(&self, offset: usize, len: usize) -> Result<Vec<u8>, SnapshotError> {
        let Some(end) = offset.checked_add(len) else {
            return Err(malformed("read window overflow"));
        };
        self.bytes
            .get(offset..end)
            .map(|window| window.to_vec())
            .ok_or_else(|| malformed("read window out of bounds"))
    }
}

/// Reads exactly `N` bytes at an absolute offset through any window, so the
/// three width-specific readers below share one refusal path.
fn read_array_win<const N: usize>(
    window: &impl SnapshotWindow,
    offset: usize,
) -> Result<[u8; N], SnapshotError> {
    let bytes = window.read_window(offset, N)?;
    bytes.try_into().map_err(|error: Vec<u8>| {
        malformed(&format!("window has the wrong length: {}", error.len()))
    })
}

/// Reads eight bytes at an absolute offset through any window.
fn read_u64_win(window: &impl SnapshotWindow, offset: usize) -> Result<u64, SnapshotError> {
    Ok(u64::from_le_bytes(read_array_win(window, offset)?))
}

/// Reads four bytes at an absolute offset through any window.
fn read_u32_win(window: &impl SnapshotWindow, offset: usize) -> Result<u32, SnapshotError> {
    Ok(u32::from_le_bytes(read_array_win(window, offset)?))
}

/// Reads a 32-bit integer at an absolute offset through any window.
fn read_i32_win(window: &impl SnapshotWindow, offset: usize) -> Result<i32, SnapshotError> {
    Ok(i32::from_le_bytes(read_array_win(window, offset)?))
}

/// Reads the term-table row `i` through any window: term blob offset and
/// length plus the postings relative offset and count.
fn term_entry_win(
    window: &impl SnapshotWindow,
    header: &Header,
    i: usize,
) -> Result<(usize, usize, usize, usize), SnapshotError> {
    let Some(stride) = i.checked_mul(TERM_ENTRY_LEN) else {
        return Err(malformed("term row overflow"));
    };
    let Some(base) = header.term_table_off.checked_add(stride) else {
        return Err(malformed("term row overflow"));
    };
    let term_blob_off = usize_field(read_u64_win(window, base)?, "term blob offset")?;
    let term_len = usize_field(
        u64::from(read_u32_win(window, base.saturating_add(8))?),
        "term length",
    )?;
    let postings_rel_off = usize_field(
        read_u64_win(window, base.saturating_add(12))?,
        "postings offset",
    )?;
    let postings_count = usize_field(
        u64::from(read_u32_win(window, base.saturating_add(20))?),
        "postings count",
    )?;
    Ok((term_blob_off, term_len, postings_rel_off, postings_count))
}

/// Slices the term bytes of term-table row `i` through any window.
fn term_bytes_win(
    window: &impl SnapshotWindow,
    header: &Header,
    i: usize,
) -> Result<Vec<u8>, SnapshotError> {
    let (term_blob_off, term_len, _, _) = term_entry_win(window, header, i)?;
    let Some(start) = header.terms_blob_off.checked_add(term_blob_off) else {
        return Err(malformed("term blob offset overflow"));
    };
    window.read_window(start, term_len)
}

/// Reads posting `posting` of the region starting at `postings_rel_off`
/// through any window.
fn posting_hash_win(
    window: &impl SnapshotWindow,
    header: &Header,
    postings_rel_off: usize,
    posting: usize,
) -> Result<[u8; 32], SnapshotError> {
    let Some(stride) = posting.checked_mul(32) else {
        return Err(malformed("posting overflow"));
    };
    let Some(offset) = postings_rel_off.checked_add(stride) else {
        return Err(malformed("posting overflow"));
    };
    let Some(start) = header.postings_off.checked_add(offset) else {
        return Err(malformed("posting overflow"));
    };
    let bytes = window.read_window(start, 32)?;
    let hash: [u8; 32] = bytes.try_into().map_err(|error: Vec<u8>| {
        malformed(&format!(
            "posting window has the wrong length: {}",
            error.len()
        ))
    })?;
    Ok(hash)
}

/// Reads vector record `i` through any window: row hash plus its vector.
fn vector_record_win(
    window: &impl SnapshotWindow,
    header: &Header,
    i: usize,
) -> Result<([u8; 32], [i32; VECTOR_DIM]), SnapshotError> {
    let Some(record_len) = 32_usize.checked_add(VECTOR_DIM.saturating_mul(4)) else {
        return Err(malformed("vector record size overflow"));
    };
    let Some(stride) = i.checked_mul(record_len) else {
        return Err(malformed("vector overflow"));
    };
    let Some(base) = header.vectors_off.checked_add(stride) else {
        return Err(malformed("vector overflow"));
    };
    let bytes = window.read_window(base, record_len)?;
    let Some((hash_bytes, tail)) = bytes.split_at_checked(32) else {
        return Err(malformed("vector window too short"));
    };
    let mut hash = [0_u8; 32];
    hash.copy_from_slice(hash_bytes);
    let mut vector = [0_i32; VECTOR_DIM];
    let tail_window = SliceWindow::new(tail);
    for (slot_index, slot) in vector.iter_mut().enumerate() {
        let Some(offset) = slot_index.checked_mul(4) else {
            return Err(malformed("vector slot overflow"));
        };
        *slot = read_i32_win(&tail_window, offset)?;
    }
    Ok((hash, vector))
}

/// Binary-searches the term table for `needle` through any window.
fn find_term_win(
    window: &impl SnapshotWindow,
    header: &Header,
    needle: &str,
) -> Result<Option<usize>, SnapshotError> {
    let needle = needle.as_bytes();
    let (mut low, mut high) = (0_usize, header.term_count);
    while low < high {
        let mid = low.saturating_add(high.saturating_sub(low) >> 1);
        match term_bytes_win(window, header, mid)?.as_slice().cmp(needle) {
            Ordering::Less => low = mid.saturating_add(1),
            Ordering::Greater => high = mid,
            Ordering::Equal => return Ok(Some(mid)),
        }
    }
    Ok(None)
}

/// Binary-searches the vector section for `needle` through any window,
/// returning the zero vector when the row is absent.
fn find_vector_win(
    window: &impl SnapshotWindow,
    header: &Header,
    needle: &[u8; 32],
) -> Result<[i32; VECTOR_DIM], SnapshotError> {
    let (mut low, mut high) = (0_usize, header.fact_count);
    while low < high {
        let mid = low.saturating_add(high.saturating_sub(low) >> 1);
        let (hash, vector) = vector_record_win(window, header, mid)?;
        match hash.as_slice().cmp(needle.as_slice()) {
            Ordering::Less => low = mid.saturating_add(1),
            Ordering::Greater => high = mid,
            Ordering::Equal => return Ok(vector),
        }
    }
    Ok([0_i32; VECTOR_DIM])
}

/// Executes a bounded hybrid query over one parsed header through any window.
///
/// The single query path behind both [`query_snapshot`] and the positional
/// file reader, so the two answers agree by construction rather than by
/// duplicated logic.
pub(crate) fn query_with<W: SnapshotWindow>(
    window: &W,
    header: &Header,
    query_text: &str,
    vector_seed: &[i32],
    top_k: usize,
) -> Result<Vec<IndexedMatch>, SnapshotError> {
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
    let mut candidates = BTreeMap::new();
    if query_terms.is_empty() {
        for i in 0..header.fact_count {
            let (hash, vector) = vector_record_win(window, header, i)?;
            candidates.insert(hash, (0_usize, vector));
        }
    } else {
        for term in &query_terms {
            let Some(index) = find_term_win(window, header, term)? else {
                continue;
            };
            let (_, _, postings_rel_off, postings_count) = term_entry_win(window, header, index)?;
            for posting in 0..postings_count {
                let hash = posting_hash_win(window, header, postings_rel_off, posting)?;
                let vector = find_vector_win(window, header, &hash)?;
                candidates
                    .entry(hash)
                    .and_modify(|entry| {
                        entry.0 = entry.0.saturating_add(1);
                    })
                    .or_insert((1_usize, vector));
            }
        }
    }
    Ok(rank_candidates(candidates, &seed, top_k)?)
}

/// Returns the number of rows indexed by a snapshot.
pub fn snapshot_row_count(bytes: &[u8]) -> Result<usize, SnapshotError> {
    Ok(parse_header(bytes, bytes.len())?.fact_count)
}

/// Recomputes and verifies the snapshot body hash. Touches every page, so it
/// is an explicit opt-in rather than part of the bounded query path.
///
/// # Errors
///
/// Returns [`SnapshotError::Malformed`] for a bad header or hash mismatch.
pub fn verify_snapshot_integrity(bytes: &[u8]) -> Result<(), SnapshotError> {
    parse_header(bytes, bytes.len())?;
    let Some(stored) = bytes.get(64..96) else {
        return Err(SnapshotError::Malformed(String::from(
            "snapshot shorter than stored hash",
        )));
    };
    if body_hash(bytes)? != stored {
        return Err(SnapshotError::Malformed(String::from(
            "snapshot body hash mismatch",
        )));
    }
    Ok(())
}

/// Executes a bounded hybrid query directly over the `LWMX` bytes,
/// returning the same top-k matches the in-memory [`IndexedProjection::query`]
/// would.
///
/// # Errors
///
/// Returns [`SnapshotError`] for a malformed snapshot or an over-long seed.
pub fn query_snapshot(
    bytes: &[u8],
    query_text: &str,
    vector_seed: &[i32],
    top_k: usize,
) -> Result<Vec<IndexedMatch>, SnapshotError> {
    let header = parse_header(bytes, bytes.len())?;
    query_with(
        &SliceWindow::new(bytes),
        &header,
        query_text,
        vector_seed,
        top_k,
    )
}

#[cfg(test)]
mod tests {
    use super::super::projection_indexed::IndexedProjection;
    use super::*;

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
    fn header_roundtrips_and_validates() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(20);
        let bytes = to_snapshot_bytes(&projection)?;
        let header = parse_header(&bytes, bytes.len())?;
        assert_eq!(header.fact_count, 20, "header must carry the row count");
        assert_eq!(
            snapshot_row_count(&bytes)?,
            20,
            "row count must read from the header"
        );
        verify_snapshot_integrity(&bytes)?;
        Ok(())
    }

    #[test]
    fn snapshot_query_matches_in_memory() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(50);
        let bytes = to_snapshot_bytes(&projection)?;
        for (text, seed, top_k) in queries() {
            let in_memory = projection.query(&text, &seed, top_k)?;
            let on_disk = query_snapshot(&bytes, &text, &seed, top_k)?;
            assert_eq!(
                in_memory, on_disk,
                "parity mismatch for query {text:?}/{seed:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn empty_index_serializes_and_queries() -> Result<(), Box<dyn std::error::Error>> {
        let projection = IndexedProjection::new();
        let bytes = to_snapshot_bytes(&projection)?;
        verify_snapshot_integrity(&bytes)?;
        assert!(
            query_snapshot(&bytes, "anything", &[], 8)?.is_empty(),
            "an empty snapshot answers nothing"
        );
        Ok(())
    }

    #[test]
    fn top_k_is_bounded_on_disk() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(100);
        let bytes = to_snapshot_bytes(&projection)?;
        let results = query_snapshot(&bytes, "shared", &[0_i32; VECTOR_DIM], 10)?;
        assert_eq!(results.len(), 10, "top-k bounds the on-disk answer");
        Ok(())
    }

    #[test]
    fn vector_seed_too_long_rejected_on_disk() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(4);
        let bytes = to_snapshot_bytes(&projection)?;
        let oversized = VECTOR_DIM.saturating_add(1);
        let seed = vec![1_i32; oversized];
        let error = query_snapshot(&bytes, "", &seed, 8);
        assert!(
            matches!(error, Err(SnapshotError::Malformed(_))),
            "over-long seed must be refused, got {error:?}"
        );
        Ok(())
    }

    #[test]
    fn corruption_is_detected() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(10);
        let mut bytes = to_snapshot_bytes(&projection)?;
        let last = bytes.len().saturating_sub(1);
        bytes[last] ^= 0xFF;
        let error = verify_snapshot_integrity(&bytes);
        assert!(
            error.is_err(),
            "a flipped body byte must fail integrity, got {error:?}"
        );
        Ok(())
    }

    #[test]
    fn truncated_file_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(10);
        let bytes = to_snapshot_bytes(&projection)?;
        let half = bytes
            .len()
            .checked_div(2)
            .ok_or("test snapshot too short")?;
        let truncated = bytes.get(..half).ok_or("test truncation empty")?;
        let header = parse_header(truncated, truncated.len());
        let queried = query_snapshot(truncated, "shared", &[], 4);
        assert!(
            header.is_err() || queried.is_err(),
            "a halved file must be refused"
        );
        Ok(())
    }

    #[test]
    fn magic_mismatch_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let projection = synthetic_index(4);
        let mut bytes = to_snapshot_bytes(&projection)?;
        bytes[0] = b'X';
        let error = parse_header(&bytes, bytes.len());
        assert!(
            error.is_err(),
            "a wrong magic must be refused, got {error:?}"
        );
        Ok(())
    }

    #[test]
    fn sim_projection_restore_is_deterministic() -> Result<(), Box<dyn std::error::Error>> {
        let first = sim_trace(0x5EED_1234)?;
        let second = sim_trace(0x5EED_1234)?;
        assert_eq!(first, second, "same seed must produce the same trace hash");
        Ok(())
    }

    /// One seeded simulation: parity checked at every step against the
    /// durable bytes, the trace hashed. Deterministic in the seed alone.
    /// Fault injection lives in the integration sweeps
    /// (`tests/sim_snapshot_format.rs`, `tests/sim_snapshot_file.rs`), which
    /// share no text with this loop by construction.
    fn sim_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
        let mut rng = XorShift::new(seed);
        let mut projection = IndexedProjection::new();
        let mut hasher = lgwks_std::hash::Hasher::new();
        hasher.update(b"ldb.sim.projection.v1");

        for step in 0..300_u32 {
            let op = rng
                .next_u64()
                .checked_rem(100)
                .ok_or("sim step dispatch overflow")?;
            let mut hash = [0_u8; 32];
            let id = u64::from(step).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let digest = lgwks_std::hash::blake3(&id.to_le_bytes());
            hash.copy_from_slice(&digest.as_bytes()[..]);
            if op < 55 {
                let term_count = rng
                    .next_u64()
                    .checked_rem(5)
                    .ok_or("sim term count overflow")?;
                let term_count = usize::try_from(term_count)
                    .map_err(|error| format!("sim term count overflow: {error}"))?;
                let mut terms = Vec::new();
                for _ in 0..term_count {
                    let word = rng.next_u64().checked_rem(12).ok_or("sim word overflow")?;
                    terms.push(format!("w{word}"));
                }
                projection.insert(hash, terms);
            } else if op < 70 {
                projection.remove(&hash);
            } else if op < 80 {
                projection.insert(hash, vec![String::from("shared")]);
                projection.insert(hash, vec![String::from("shared")]);
            }
            let durable = to_snapshot_bytes(&projection)?;
            hasher.update(&durable);
        }

        let terms = ["w3", "shared", "absent", ""];
        for text in terms {
            let seed_vec = [1_i32, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
            let in_memory = projection.query(text, &seed_vec, 8)?;
            let bytes = to_snapshot_bytes(&projection)?;
            let on_disk = query_snapshot(&bytes, text, &seed_vec, 8)?;
            assert_eq!(
                in_memory, on_disk,
                "sim diverged for query {text:?} at seed {seed}"
            );
            verify_snapshot_integrity(&bytes)?;
            for scored in &on_disk {
                hasher.update(&scored.hash);
                hasher.update(&scored.score.to_le_bytes());
            }
            hasher.update(&[u8::from(!on_disk.is_empty())]);
        }
        Ok(*hasher.finalize().as_bytes())
    }

    /// Deterministic xorshift64: the only randomness the sim uses.
    struct XorShift {
        /// Current state, never zero.
        state: u64,
    }

    impl XorShift {
        /// Creates the generator; a zero seed maps to a fixed nonzero start.
        fn new(seed: u64) -> Self {
            Self {
                state: if seed == 0 {
                    0x243F_6A88_85A3_08D3
                } else {
                    seed
                },
            }
        }

        /// Returns the next pseudorandom word.
        fn next_u64(&mut self) -> u64 {
            let mut mixer = self.state;
            mixer ^= mixer << 13;
            mixer ^= mixer >> 7;
            mixer ^= mixer << 17;
            self.state = mixer;
            mixer
        }
    }
}
