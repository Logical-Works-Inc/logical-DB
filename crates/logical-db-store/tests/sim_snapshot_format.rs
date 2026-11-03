//! Seeded deterministic simulations for the LWMX snapshot format.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Every sim builds the seeded index through the public file path, reads the
//! raw bytes back, and asserts byte-reader parity plus fail-closed refusals.
//! One seed drives row identities, term choices, query shapes, and fault
//! selection; the same seed always hashes the same trace.

use logical_db_store::projection_indexed::IndexedProjection;
use logical_db_store::projection_snapshot::{
    query_snapshot, snapshot_row_count, verify_snapshot_integrity,
};
use logical_db_store::snapshot_file::FileProjectionIndex;

#[path = "sim_harness.rs"]
mod harness;
use harness::{build_snapshot, check, cleanup, count, opened, seed_vec, trace_tag};

/// Trace domain: this file's traces never collide with another file's.
const TRACE_DOMAIN: &[u8] = b"ldb.sim.snapshot-format.v1";

/// The writer is deterministic: one index always yields one byte string.
#[test]
fn sim_writer_emits_identical_bytes() -> Result<(), Box<dyn std::error::Error>> {
    check([0x21AA_0001_u64, 0x21AA_0002_u64], writer_trace)
}

/// Trace for [`sim_writer_emits_identical_bytes`].
fn writer_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"writer-determinism");
    let rows = count(&mut rng, 20, 60)?;
    let projection = harness::seeded_index(&mut rng, rows)?;
    let (path, first) = build_snapshot(&projection)?;
    let rebuild_path = harness::temp_path("snap", "lwmx");
    FileProjectionIndex::build(&rebuild_path, &projection)?;
    let second = std::fs::read(&rebuild_path)?;
    assert_eq!(first, second, "two builds of one index match byte for byte");
    verify_snapshot_integrity(&first)?;
    hasher.update(&first);
    cleanup(&path)?;
    cleanup(&rebuild_path)?;
    Ok(trace_tag(hasher))
}

/// Insertion order never leaks into the durable bytes.
#[test]
fn sim_insertion_order_leaves_bytes_unchanged() -> Result<(), Box<dyn std::error::Error>> {
    check([0x21BB_0001_u64, 0x21BB_0002_u64], order_trace)
}

/// Trace for [`sim_insertion_order_leaves_bytes_unchanged`].
fn order_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"insertion-order");
    // One fixed row set, inserted in two different seeded orders.
    let total = count(&mut rng, 30, 40)?;
    let total_rows = u64::try_from(total).map_err(|error| format!("sim row overflow: {error}"))?;
    let mut ids: Vec<u64> = (0..total_rows).collect();
    for idx in (1..ids.len()).rev() {
        let swap = usize::try_from(rng.next_u64())
            .map_err(|error| format!("sim shuffle overflow: {error}"))?
            .checked_rem(idx.saturating_add(1))
            .ok_or("sim shuffle dispatch overflow")?;
        ids.swap(idx, swap);
    }
    let mut forward = IndexedProjection::new();
    for idx in 0..total {
        let row_no = u64::try_from(idx).map_err(|error| format!("sim row overflow: {error}"))?;
        let lane = row_no.checked_rem(11).ok_or("sim lane dispatch overflow")?;
        forward.insert(
            harness::test_row(row_no),
            vec![String::from("shared"), format!("t{lane}")],
        );
    }
    let mut shuffled = IndexedProjection::new();
    for row_no in &ids {
        let lane = row_no.checked_rem(11).ok_or("sim lane dispatch overflow")?;
        shuffled.insert(
            harness::test_row(*row_no),
            vec![String::from("shared"), format!("t{lane}")],
        );
    }
    let (forward_path, forward_bytes) = build_snapshot(&forward)?;
    let (shuffled_path, shuffled_bytes) = build_snapshot(&shuffled)?;
    assert_eq!(
        forward_bytes, shuffled_bytes,
        "insertion order must not leak into bytes"
    );
    hasher.update(&forward_bytes);
    cleanup(&forward_path)?;
    cleanup(&shuffled_path)?;
    Ok(trace_tag(hasher))
}

/// In-memory and byte-reader answers agree on every seeded query shape.
#[test]
fn sim_reader_matches_in_memory_queries() -> Result<(), Box<dyn std::error::Error>> {
    check([0x21CC_0001_u64, 0x21CC_0002_u64], parity_trace)
}

/// Trace for [`sim_reader_matches_in_memory_queries`].
fn parity_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"reader-parity");
    let rows = count(&mut rng, 40, 80)?;
    let projection = harness::seeded_index(&mut rng, rows)?;
    let (path, bytes) = build_snapshot(&projection)?;
    assert_eq!(
        snapshot_row_count(&bytes)?,
        projection.len(),
        "row count must agree"
    );
    for _ in 0..10 {
        let dispatch = rng
            .below(4)
            .map_err(|error| format!("sim query dispatch overflow: {error}"))?;
        let text = if dispatch == 0 {
            String::from("shared")
        } else {
            let slot = rng
                .below(25)
                .map_err(|error| format!("sim query slot overflow: {error}"))?;
            let lane = slot.checked_rem(7).ok_or("sim query lane overflow")?;
            if dispatch == 1 {
                format!("term{slot}")
            } else if dispatch == 2 {
                format!("u{lane}")
            } else {
                format!("missing{slot}")
            }
        };
        let seed_count = count(&mut rng, 0, 6)?;
        let seed_vec = seed_vec(&mut rng, seed_count)?;
        let top_k = count(&mut rng, 1, 12)?;
        let in_memory = projection.query(&text, &seed_vec, top_k)?;
        let on_disk = query_snapshot(&bytes, &text, &seed_vec, top_k)?;
        assert_eq!(in_memory, on_disk, "parity mismatch for {text:?}");
        for scored in &on_disk {
            hasher.update(&scored.hash);
            hasher.update(&scored.score.to_le_bytes());
        }
    }
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}

/// A fault sweep across every seeded offset is refused, never misread.
#[test]
fn sim_fault_sweep_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    check([0x21DD_0001_u64, 0x21DD_0002_u64], fault_trace)
}

/// Trace for [`sim_fault_sweep_is_refused`].
fn fault_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"fault-sweep");
    let rows = count(&mut rng, 25, 25)?;
    let projection = harness::seeded_index(&mut rng, rows)?;
    let (path, bytes) = build_snapshot(&projection)?;
    for _ in 0..24 {
        let mut faulty = bytes.clone();
        let fault = rng
            .below(5)
            .map_err(|error| format!("sim fault overflow: {error}"))?;
        match fault {
            0 => {
                let at = usize::try_from(rng.next_u64())
                    .map_err(|error| format!("sim fault offset overflow: {error}"))?
                    .checked_rem(faulty.len().max(1))
                    .ok_or("sim fault window empty")?;
                faulty[at] ^= 0xFF;
            }
            1 => {
                let half = faulty
                    .len()
                    .checked_div(2)
                    .ok_or("sim truncation overflow")?;
                faulty.truncate(half);
            }
            2 => {
                if let Some(first) = faulty.first_mut() {
                    *first = b'X';
                }
            }
            3 => {
                faulty.push(rng.bytes32()[0]);
            }
            _ => {
                let at = usize::try_from(rng.next_u64())
                    .map_err(|error| format!("sim fault offset overflow: {error}"))?
                    .checked_rem(faulty.len().max(1))
                    .ok_or("sim fault window empty")?;
                faulty.swap_remove(at.min(faulty.len().saturating_sub(1)));
            }
        }
        let refused = verify_snapshot_integrity(&faulty).is_err()
            || query_snapshot(&faulty, "shared", &[], 4).is_err();
        assert!(refused, "faulted bytes must be refused, never misread");
        hasher.update(&[u8::from(refused)]);
    }
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}

/// Truncation at seeded lengths always fails closed.
#[test]
fn sim_truncation_lengths_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
    check([0x21EE_0001_u64, 0x21EE_0002_u64], truncation_trace)
}

/// Trace for [`sim_truncation_lengths_fail_closed`].
fn truncation_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"truncation");
    let rows = count(&mut rng, 15, 20)?;
    let projection = harness::seeded_index(&mut rng, rows)?;
    let (path, bytes) = build_snapshot(&projection)?;
    for _ in 0..12 {
        let cut = usize::try_from(rng.next_u64())
            .map_err(|error| format!("sim cut overflow: {error}"))?
            .checked_rem(bytes.len().max(1))
            .ok_or("sim cut window empty")?;
        let Some(prefix) = bytes.get(..cut) else {
            continue;
        };
        let refused = snapshot_row_count(prefix).is_err()
            || query_snapshot(prefix, "shared", &[], 4).is_err();
        // A cut can only pass when it is the full length; every shorter cut
        // must refuse on at least one boundary.
        if cut != bytes.len() {
            assert!(refused, "truncation at {cut} must fail closed");
        }
        let cut_bytes = u64::try_from(cut).map_err(|error| format!("sim cut overflow: {error}"))?;
        hasher.update(&cut_bytes.to_le_bytes());
        hasher.update(&[u8::from(refused)]);
    }
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}

/// Empty, single-row, and single-term indexes round-trip exactly.
#[test]
fn sim_edge_shaped_indexes_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    check([0x21FF_0001_u64, 0x21FF_0002_u64], edge_trace)
}

/// Trace for [`sim_edge_shaped_indexes_round_trip`].
fn edge_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"edge-shapes");
    // Empty.
    let empty = IndexedProjection::new();
    let (empty_path, empty_bytes) = build_snapshot(&empty)?;
    verify_snapshot_integrity(&empty_bytes)?;
    assert_eq!(snapshot_row_count(&empty_bytes)?, 0, "empty holds no rows");
    assert!(
        query_snapshot(&empty_bytes, "anything", &[], 8)?.is_empty(),
        "empty answers nothing"
    );
    cleanup(&empty_path)?;
    // Single row, single term.
    let mut single = IndexedProjection::new();
    single.insert(
        harness::test_row(rng.next_u64()),
        vec![String::from("only")],
    );
    let (single_path, single_bytes) = build_snapshot(&single)?;
    verify_snapshot_integrity(&single_bytes)?;
    assert_eq!(snapshot_row_count(&single_bytes)?, 1, "one row indexed");
    let hits = query_snapshot(&single_bytes, "only", &[], 8)?;
    assert_eq!(hits.len(), 1, "the single term resolves");
    hasher.update(&single_bytes);
    cleanup(&single_path)?;
    Ok(trace_tag(hasher))
}

/// A large index keeps parity on sampled queries.
#[test]
fn sim_large_index_keeps_parity() -> Result<(), Box<dyn std::error::Error>> {
    check([0x22AA_0001_u64, 0x22AA_0002_u64], large_trace)
}

/// Trace for [`sim_large_index_keeps_parity`].
fn large_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"large-parity");
    let projection = harness::seeded_index(&mut rng, 1_500)?;
    let (path, bytes) = build_snapshot(&projection)?;
    verify_snapshot_integrity(&bytes)?;
    assert_eq!(
        snapshot_row_count(&bytes)?,
        1_500,
        "all rows survive the round trip"
    );
    for _ in 0..8 {
        let word = rng
            .below(25)
            .map_err(|error| format!("sim term overflow: {error}"))?;
        let text = format!("term{word}");
        let in_memory = projection.query(&text, &[], 10)?;
        let on_disk = query_snapshot(&bytes, &text, &[], 10)?;
        assert_eq!(in_memory, on_disk, "large parity mismatch for {text:?}");
        let found = u64::try_from(on_disk.len())
            .map_err(|error| format!("sim answer length overflow: {error}"))?;
        hasher.update(&found.to_le_bytes());
    }
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}

/// Over-long vector seeds are refused on every index shape.
#[test]
fn sim_oversized_seeds_refused_everywhere() -> Result<(), Box<dyn std::error::Error>> {
    check([0x22BB_0001_u64, 0x22BB_0002_u64], oversized_trace)
}

/// Trace for [`sim_oversized_seeds_refused_everywhere`].
fn oversized_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"oversized-seed");
    let rows = count(&mut rng, 1, 10)?;
    let projection = harness::seeded_index(&mut rng, rows)?;
    let (path, bytes) = build_snapshot(&projection)?;
    for extra in [1_usize, 5, 64] {
        let seed_vec = vec![1_i32; 16_usize.saturating_add(extra)];
        let error = query_snapshot(&bytes, "shared", &seed_vec, 8);
        assert!(
            error.is_err(),
            "a seed of {} must be refused",
            16_usize.saturating_add(extra)
        );
        let extra_bytes =
            u64::try_from(extra).map_err(|error| format!("sim extra overflow: {error}"))?;
        hasher.update(&extra_bytes.to_le_bytes());
    }
    cleanup(&path)?;
    Ok(trace_tag(hasher))
}
