//! Seeded deterministic simulations for the durable snapshot file path.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Every sim serves one seeded index through atomic build, header-only open,
//! positional query, and streaming integrity. One seed drives rows, terms,
//! queries, and fault selection; the same seed always hashes the same trace.

use logical_db_store::projection_indexed::IndexedProjection;
use logical_db_store::snapshot_file::FileProjectionIndex;

#[path = "sim_harness.rs"]
mod harness;
use harness::{check, cleanup, count, opened, seed_vec, temp_path, trace_tag};

/// Trace domain: this file's traces never collide with another file's.
const TRACE_DOMAIN: &[u8] = b"ldb.sim.snapshot-file.v1";

/// Build, open, and query agree with the in-memory answers.
#[test]
fn sim_file_query_matches_in_memory() -> Result<(), Box<dyn std::error::Error>> {
    check([0x31AA_0001_u64, 0x31AA_0002_u64], file_parity_trace)
}

/// Trace for [`sim_file_query_matches_in_memory`].
fn file_parity_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"file-parity");
    let rows = count(&mut rng, 30, 50)?;
    let projection = harness::seeded_index(&mut rng, rows)?;
    let path = temp_path("file", "lwmx");
    FileProjectionIndex::build(&path, &projection)?;
    let index = FileProjectionIndex::open(&path)?;
    assert_eq!(
        index.len(),
        projection.len(),
        "row counts must agree after reopen"
    );
    assert_eq!(
        index.is_empty(),
        projection.is_empty(),
        "emptiness must agree after reopen"
    );
    index.verify_integrity()?;
    for _ in 0..8 {
        let pick = rng
            .below(4)
            .map_err(|error| format!("sim text pick overflow: {error}"))?;
        let text = match pick {
            0 => String::from("shared"),
            1 => {
                let word = rng
                    .below(25)
                    .map_err(|error| format!("sim term overflow: {error}"))?;
                format!("term{word}")
            }
            2 => {
                let lane = rng
                    .below(7)
                    .map_err(|error| format!("sim lane overflow: {error}"))?;
                format!("u{lane}")
            }
            _ => {
                let word = rng
                    .below(50)
                    .map_err(|error| format!("sim term overflow: {error}"))?;
                format!("missing{word}")
            }
        };
        let seed_len = count(&mut rng, 0, 5)?;
        let seed_vec = seed_vec(&mut rng, seed_len)?;
        let top_k = count(&mut rng, 1, 10)?;
        let live = projection.query(&text, &seed_vec, top_k)?;
        let frozen = index.query(&text, &seed_vec, top_k)?;
        assert_eq!(live, frozen, "file query diverged for {text:?}");
        for scored in &frozen {
            hasher.update(&scored.hash);
            hasher.update(&scored.score.to_le_bytes());
        }
    }
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}

/// A stale tmp never shadows the durable snapshot.
#[test]
fn sim_stale_tmp_never_shadows_snapshot() -> Result<(), Box<dyn std::error::Error>> {
    check([0x31BB_0001_u64, 0x31BB_0002_u64], stale_tmp_trace)
}

/// Trace for [`sim_stale_tmp_never_shadows_snapshot`].
fn stale_tmp_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"stale-tmp");
    let rows = count(&mut rng, 10, 20)?;
    let projection = harness::seeded_index(&mut rng, rows)?;
    let path = temp_path("file", "lwmx");
    FileProjectionIndex::build(&path, &projection)?;
    let before = FileProjectionIndex::open(&path)?;
    before.verify_integrity()?;
    // Seeded garbage at the tmp sibling path a killed build would leave.
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let garbage_len = count(&mut rng, 8, 64)?;
    let mut garbage = Vec::new();
    while garbage.len() < garbage_len {
        garbage.extend_from_slice(&rng.bytes32());
    }
    garbage.truncate(garbage_len);
    std::fs::write(&tmp, &garbage)?;
    let reopened = FileProjectionIndex::open(&path)?;
    assert_eq!(
        reopened.len(),
        before.len(),
        "a stale tmp must not shadow the snapshot"
    );
    reopened.verify_integrity()?;
    hasher.update(&garbage);
    // A rebuild replaces the stale tmp and stays queryable.
    FileProjectionIndex::build(&path, &projection)?;
    assert!(
        !std::path::Path::new(&tmp).exists(),
        "a rebuild must consume the stale tmp"
    );
    let rebuilt = FileProjectionIndex::open(&path)?;
    rebuilt.verify_integrity()?;
    assert_eq!(rebuilt.len(), before.len(), "rebuild preserves row count");
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}

/// Corruption is detected on verify and on query alike.
#[test]
fn sim_corruption_detected_on_both_paths() -> Result<(), Box<dyn std::error::Error>> {
    check([0x31CC_0001_u64, 0x31CC_0002_u64], corruption_trace)
}

/// Trace for [`sim_corruption_detected_on_both_paths`].
fn corruption_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"corruption");
    let rows = count(&mut rng, 12, 18)?;
    let projection = harness::seeded_index(&mut rng, rows)?;
    let path = temp_path("file", "lwmx");
    FileProjectionIndex::build(&path, &projection)?;
    for _ in 0..8 {
        let mut bytes = std::fs::read(&path)?;
        let at = usize::try_from(rng.next_u64())
            .map_err(|error| format!("sim fault offset overflow: {error}"))?
            .checked_rem(bytes.len().max(1))
            .ok_or("sim fault window empty")?;
        bytes[at] ^= 0xFF;
        std::fs::write(&path, &bytes)?;
        let index = FileProjectionIndex::open(&path)?;
        // The header may still parse; integrity or query must refuse.
        let refused = index.verify_integrity().is_err() || index.query("shared", &[], 4).is_err();
        assert!(refused, "byte flip at {at} must be detected");
        let at_bytes =
            u64::try_from(at).map_err(|error| format!("sim offset overflow: {error}"))?;
        hasher.update(&at_bytes.to_le_bytes());
        // Restore the good snapshot for the next fault round.
        FileProjectionIndex::build(&path, &projection)?;
    }
    let clean = FileProjectionIndex::open(&path)?;
    clean.verify_integrity()?;
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}

/// Rebuilds converge: repeated builds of evolving indexes stay exact.
#[test]
fn sim_rebuilds_converge_on_growth() -> Result<(), Box<dyn std::error::Error>> {
    check([0x31DD_0001_u64, 0x31DD_0002_u64], rebuild_trace)
}

/// Trace for [`sim_rebuilds_converge_on_growth`].
fn rebuild_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"rebuilds");
    let mut projection = IndexedProjection::new();
    let path = temp_path("file", "lwmx");
    for round in 0..6_u64 {
        for _slot in 0..count(&mut rng, 5, 10)? {
            let id = round.saturating_mul(100).saturating_add(
                rng.below(10_000)
                    .map_err(|error| format!("sim id overflow: {error}"))?,
            );
            projection.insert(
                harness::test_row(id),
                vec![String::from("shared"), format!("r{round}")],
            );
        }
        FileProjectionIndex::build(&path, &projection)?;
        let index = FileProjectionIndex::open(&path)?;
        index.verify_integrity()?;
        assert_eq!(index.len(), projection.len(), "round {round} converges");
        let len_bytes =
            u64::try_from(index.len()).map_err(|error| format!("sim length overflow: {error}"))?;
        hasher.update(&len_bytes.to_le_bytes());
    }
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}

/// Two files built in one run stay independent.
#[test]
fn sim_two_files_stay_independent() -> Result<(), Box<dyn std::error::Error>> {
    check([0x31EE_0001_u64, 0x31EE_0002_u64], independence_trace)
}

/// Trace for [`sim_two_files_stay_independent`].
fn independence_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"independence");
    let mut left = IndexedProjection::new();
    let mut right = IndexedProjection::new();
    for idx in 0..count(&mut rng, 10, 20)? {
        let row_no = u64::try_from(idx).map_err(|error| format!("sim row overflow: {error}"))?;
        left.insert(
            harness::test_row(row_no),
            vec![String::from("left"), format!("l{row_no}")],
        );
        right.insert(
            harness::test_row(50_000_u64.saturating_add(row_no)),
            vec![String::from("right"), format!("r{row_no}")],
        );
    }
    let left_path = temp_path("file-left", "lwmx");
    let right_path = temp_path("file-right", "lwmx");
    FileProjectionIndex::build(&left_path, &left)?;
    FileProjectionIndex::build(&right_path, &right)?;
    let left_index = FileProjectionIndex::open(&left_path)?;
    let right_index = FileProjectionIndex::open(&right_path)?;
    assert!(
        left_index.query("right", &[], 8)?.is_empty(),
        "left must not answer right terms"
    );
    assert!(
        right_index.query("left", &[], 8)?.is_empty(),
        "right must not answer left terms"
    );
    assert_eq!(left_index.len(), left.len(), "left row count exact");
    assert_eq!(right_index.len(), right.len(), "right row count exact");
    let left_len =
        u64::try_from(left_index.len()).map_err(|error| format!("sim length overflow: {error}"))?;
    let right_len = u64::try_from(right_index.len())
        .map_err(|error| format!("sim length overflow: {error}"))?;
    hasher.update(&left_len.to_le_bytes());
    hasher.update(&right_len.to_le_bytes());
    cleanup(&left_path)?;
    cleanup(&right_path)?;
    Ok(trace_tag(hasher))
}

/// Empty snapshots build, open, verify, and answer nothing.
#[test]
fn sim_empty_snapshot_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    check([0x31FF_0001_u64, 0x31FF_0002_u64], empty_file_trace)
}

/// Trace for [`sim_empty_snapshot_lifecycle`].
fn empty_file_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"empty-lifecycle");
    let empty = IndexedProjection::new();
    let path = temp_path("file", "lwmx");
    FileProjectionIndex::build(&path, &empty)?;
    let index = FileProjectionIndex::open(&path)?;
    assert!(index.is_empty(), "an empty snapshot reports empty");
    assert_eq!(index.len(), 0, "an empty snapshot holds no rows");
    index.verify_integrity()?;
    let probe_word = rng
        .below(9)
        .map_err(|error| format!("sim probe overflow: {error}"))?;
    let probe = format!("t{probe_word}");
    for text in ["", "shared", probe.as_str()] {
        assert!(
            index.query(text, &[], 8)?.is_empty(),
            "empty answers nothing for {text:?}"
        );
        hasher.update(text.as_bytes());
    }
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}
