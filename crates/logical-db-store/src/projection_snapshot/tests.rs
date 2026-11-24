//! Tests for the `LWMX` projection snapshot writer and reader.

use super::super::projection_indexed::IndexedProjection;
use super::*;

/// Builds a synthetic index without I/O.
pub(crate) fn synthetic_index(count: u8) -> IndexedProjection {
    let mut projection = IndexedProjection::new();
    for index in 0..count {
        let hash = std::array::from_fn(|position| match position {
            0 => index,
            1 => index.wrapping_mul(7),
            _ => 0,
        });
        let terms = vec![String::from("shared"), format!("term{index}")];
        projection.insert(hash, terms);
    }
    projection
}

/// Sample queries shared by the parity tests.
pub(crate) fn queries() -> Vec<(String, Vec<i32>, usize)> {
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
    *bytes.last_mut().ok_or("snapshot bytes are empty")? ^= 0xFF;
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
    *bytes.first_mut().ok_or("snapshot bytes are empty")? = b'X';
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
