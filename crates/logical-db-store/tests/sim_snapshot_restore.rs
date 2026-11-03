//! Seeded deterministic simulations for snapshot restore-or-replay.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Every sim seals a relational snapshot, replays a seeded tail, and asserts
//! convergence or the documented genesis fallback. One seed drives rows,
//! frontiers, timestamps, and fault selection; the same seed always hashes
//! the same trace.

use logical_db_store::projection_relational::{RelationalProjection, SqliteRelationalProjection};
use logical_db_store::snapshot_envelope::SnapshotStore;
use logical_db_store::snapshot_restore::{RestorableProjection, RestoreStats, restore_or_replay};

#[path = "sim_harness.rs"]
mod harness;
use harness::{Fixture, apply_rows, check};

/// Trace domain: this file's traces never collide with another file's.
const TRACE_DOMAIN: &[u8] = b"ldb.sim.snapshot-restore.v1";

/// Draws a seeded split/total pair: `split` rows sealed, then a seeded tail.
fn split_total(
    rng: &mut harness::XorShift,
    split_base: u64,
    split_spread: u64,
    tail_base: u64,
    tail_spread: u64,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let split = split_base.saturating_add(
        rng.below(split_spread)
            .map_err(|error| format!("sim split overflow: {error}"))?,
    );
    let total = split.saturating_add(tail_base).saturating_add(
        rng.below(tail_spread)
            .map_err(|error| format!("sim tail overflow: {error}"))?,
    );
    Ok((split, total))
}

/// Asserts snapshot-path convergence against the full replay and finalizes
/// the trace hash. The checkpoint comparison, hash update, and finish exist
/// once instead of once per converging trace.
fn finish_converged<P: RestorableProjection>(
    mut case: harness::Case,
    restored: &P,
    full: &RelationalProjection,
) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    assert_eq!(
        restored.checkpoint(),
        full.checkpoint(),
        "restore converges on the full replay checkpoint"
    );
    case.hasher.update(&restored.checkpoint());
    case.finish()
}

/// Runs one restore-or-replay against the harness store, returning the
/// projection and the stats for the trace's own convergence check. The call
/// exists once so the three restore helpers below share it instead of
/// repeating its shape.
fn restore_with<P, F>(
    own: &Fixture,
    seed: P,
    replay: F,
) -> Result<(P, RestoreStats), Box<dyn std::error::Error>>
where
    P: RestorableProjection,
    F: FnMut(&mut P, u64) -> Result<u64, String>,
{
    Ok(restore_or_replay(
        &own.store,
        own.scope,
        own.projection,
        seed,
        replay,
    )?)
}

/// Restores through one genesis replay, returning the projection and the
/// stats for the reason check.
fn restore_with_genesis(
    own: &Fixture,
    rows: u64,
) -> Result<(RelationalProjection, RestoreStats), Box<dyn std::error::Error>> {
    restore_with(
        own,
        RelationalProjection::new(),
        harness::replay_genesis(rows),
    )
}

/// Restores with an empty tail replay, staying on the snapshot.
fn restore_with_empty_tail(
    own: &Fixture,
) -> Result<(RelationalProjection, RestoreStats), Box<dyn std::error::Error>> {
    restore_with(
        own,
        RelationalProjection::new(),
        |_restored: &mut RelationalProjection, _after: u64| -> Result<u64, String> { Ok(0) },
    )
}

/// Restores a SQLite projection against a full in-memory replay of `total`
/// rows, asserting the snapshot path. Returns the restored projection, the
/// stats, and the full replay for the trace's own convergence check.
fn restore_sqlite_against_full(
    own: &Fixture,
    total: u64,
) -> Result<
    (
        SqliteRelationalProjection,
        RestoreStats,
        RelationalProjection,
    ),
    Box<dyn std::error::Error>,
> {
    let mut full = RelationalProjection::new();
    apply_rows(&mut full, total)?;
    let sqlite_seed = SqliteRelationalProjection::new()?;
    let (restored, stats) = restore_with(own, sqlite_seed, harness::replay_all_sqlite(total))?;
    assert!(stats.from_snapshot, "must take the snapshot path");
    Ok((restored, stats, full))
}

/// Snapshot plus tail converges across both engine directions.
#[test]
fn sim_snapshot_tail_converges_both_directions() -> Result<(), Box<dyn std::error::Error>> {
    check([0x61AA_0001_u64, 0x61AA_0002_u64], converge_trace)
}

/// Trace for [`sim_snapshot_tail_converges_both_directions`].
fn converge_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"converge", "restore")?;
    let (split, total) = split_total(&mut case.rng, 20, 60, 10, 60)?;
    let created = harness::TIME_BASE.wrapping_add(
        case.rng
            .below(5_000)
            .map_err(|error| format!("sim time overflow: {error}"))?,
    );
    harness::seal_rows(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        split,
        [9_u8; 32],
        created,
    )?;
    let (restored, stats, full) = restore_sqlite_against_full(&case.own, total)?;
    assert_eq!(
        stats.tail_replayed,
        total.saturating_sub(split),
        "tail replays exactly the post-frontier rows"
    );
    finish_converged(case, &restored, &full)
}

/// No snapshot means genesis replay with no fallback reason.
#[test]
fn sim_missing_snapshot_runs_genesis() -> Result<(), Box<dyn std::error::Error>> {
    check([0x61BB_0001_u64, 0x61BB_0002_u64], genesis_trace)
}

/// Trace for [`sim_missing_snapshot_runs_genesis`].
fn genesis_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"genesis", "restore")?;
    let rows = 5_u64.saturating_add(
        case.rng
            .below(40)
            .map_err(|error| format!("sim rows overflow: {error}"))?,
    );
    let rows_len = usize::try_from(rows).map_err(|error| format!("sim rows overflow: {error}"))?;
    let (restored, stats) = restore_with_genesis(&case.own, rows)?;
    assert!(!stats.from_snapshot, "no snapshot means genesis");
    assert!(
        stats.fallback_reason.is_none(),
        "a missing snapshot is not a refusal"
    );
    assert_eq!(restored.state().len(), rows_len, "genesis replays all");
    case.hasher.update(&restored.checkpoint());
    case.finish()
}

/// A lying checkpoint falls back to genesis with its reason recorded.
#[test]
fn sim_lying_checkpoint_falls_back() -> Result<(), Box<dyn std::error::Error>> {
    check([0x61CC_0001_u64, 0x61CC_0002_u64], lying_trace)
}

/// Trace for [`sim_lying_checkpoint_falls_back`].
fn lying_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"lying", "restore")?;
    let rows = 10_u64.saturating_add(
        case.rng
            .below(30)
            .map_err(|error| format!("sim rows overflow: {error}"))?,
    );
    let rows_len = usize::try_from(rows).map_err(|error| format!("sim rows overflow: {error}"))?;
    let (_mem, mut envelope) = harness::seal_rows(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        rows,
        [9_u8; 32],
        harness::TIME_BASE,
    )?;
    envelope.checkpoint = [0xFF_u8; 32];
    envelope.hash = envelope.compute_hash()?;
    case.own.store.save_snapshot(&envelope)?;
    let (restored, stats) = restore_with_genesis(&case.own, rows)?;
    assert!(!stats.from_snapshot, "a lying envelope must fall back");
    assert!(
        stats
            .fallback_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("checkpoint mismatch")),
        "the reason must name the checkpoint mismatch"
    );
    assert_eq!(restored.state().len(), rows_len, "genesis replays all");
    case.hasher.update(&restored.checkpoint());
    case.finish()
}

/// A failing tail falls back to genesis with its reason recorded.
#[test]
fn sim_failing_tail_falls_back() -> Result<(), Box<dyn std::error::Error>> {
    check([0x61DD_0001_u64, 0x61DD_0002_u64], tail_failure_trace)
}

/// Trace for [`sim_failing_tail_falls_back`].
fn tail_failure_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"tail-failure", "restore")?;
    let (split, total) = split_total(&mut case.rng, 10, 20, 5, 20)?;
    harness::seal_rows(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        split,
        [9_u8; 32],
        harness::TIME_BASE,
    )?;
    let attempt = case
        .rng
        .below(2)
        .map_err(|error| format!("sim attempt overflow: {error}"))?;
    let (restored, stats) = restore_or_replay(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        RelationalProjection::new(),
        |restored: &mut RelationalProjection, after: u64| -> Result<u64, String> {
            if after == split.saturating_sub(1) && attempt == 0 {
                return Err(String::from("simulated tail outage"));
            }
            apply_rows(restored, total).map_err(|error| error.to_string())?;
            Ok(total)
        },
    )?;
    if attempt == 0 {
        assert!(!stats.from_snapshot, "a failing tail must fall back");
        assert!(
            stats
                .fallback_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("tail replay")),
            "the reason must name the tail replay"
        );
    } else {
        assert!(stats.from_snapshot, "a working tail stays on snapshot");
    }
    case.hasher.update(&restored.checkpoint());
    case.hasher.update(&[u8::from(stats.from_snapshot)]);
    case.finish()
}

/// An empty tail at the frontier applies nothing and stays on snapshot.
#[test]
fn sim_empty_tail_applies_nothing() -> Result<(), Box<dyn std::error::Error>> {
    check([0x61EE_0001_u64, 0x61EE_0002_u64], empty_tail_trace)
}

/// Trace for [`sim_empty_tail_applies_nothing`].
fn empty_tail_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"empty-tail", "restore")?;
    let rows = 5_u64.saturating_add(
        case.rng
            .below(25)
            .map_err(|error| format!("sim rows overflow: {error}"))?,
    );
    let (mem, _envelope) = harness::seal_rows(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        rows,
        [9_u8; 32],
        harness::TIME_BASE,
    )?;
    let (restored, stats) = restore_with_empty_tail(&case.own)?;
    assert!(stats.from_snapshot, "an empty tail stays on the snapshot");
    assert_eq!(stats.tail_replayed, 0, "nothing replays after the frontier");
    assert_eq!(
        restored.checkpoint(),
        mem.checkpoint(),
        "the restored state matches the sealed frontier"
    );
    case.hasher.update(&restored.checkpoint());
    case.finish()
}

/// Long tails converge exactly on the full replay checkpoint.
#[test]
fn sim_long_tail_converges_exactly() -> Result<(), Box<dyn std::error::Error>> {
    check([0x61FF_0001_u64, 0x61FF_0002_u64], long_tail_trace)
}

/// Trace for [`sim_long_tail_converges_exactly`].
fn long_tail_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"long-tail", "restore")?;
    // Fixed 400-row tail: the split varies, the tail length does not.
    let (split, total) = split_total(&mut case.rng, 50, 50, 400, 1)?;
    harness::seal_rows(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        split,
        [9_u8; 32],
        harness::TIME_BASE,
    )?;
    let mut full = RelationalProjection::new();
    apply_rows(&mut full, total)?;
    let (restored, stats) = restore_or_replay(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        RelationalProjection::new(),
        harness::replay_all(total),
    )?;
    assert!(stats.from_snapshot, "must take the snapshot path");
    assert_eq!(
        stats.tail_replayed,
        total.saturating_sub(split),
        "a long tail replays completely"
    );
    finish_converged(case, &restored, &full)
}

/// Seeded frontier metadata rides the snapshot without affecting restore.
#[test]
fn sim_frontier_metadata_varies_freely() -> Result<(), Box<dyn std::error::Error>> {
    check([0x62AA_0001_u64, 0x62AA_0002_u64], metadata_trace)
}

/// Trace for [`sim_frontier_metadata_varies_freely`].
fn metadata_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"metadata", "restore")?;
    let rows = 8_u64.saturating_add(
        case.rng
            .below(12)
            .map_err(|error| format!("sim rows overflow: {error}"))?,
    );
    let semantics = case.rng.bytes32();
    let created_at = harness::TIME_BASE.wrapping_add(
        case.rng
            .below(1_000_000)
            .map_err(|error| format!("sim time overflow: {error}"))?,
    );
    let (mem, envelope) = harness::seal_rows(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        rows,
        semantics,
        created_at,
    )?;
    assert_eq!(envelope.semantics, semantics, "semantics ride the seal");
    assert_eq!(
        envelope.created_at_secs, created_at,
        "timestamps ride the seal"
    );
    let (restored, stats) = restore_with_empty_tail(&case.own)?;
    assert!(stats.from_snapshot, "metadata never blocks restore");
    assert_eq!(
        restored.checkpoint(),
        mem.checkpoint(),
        "state restores under any metadata"
    );
    case.hasher.update(&envelope.hash);
    case.finish()
}

/// Sealed relational snapshots are engine-agnostic across directions.
#[test]
fn sim_snapshot_tail_converges_sqlite_direction() -> Result<(), Box<dyn std::error::Error>> {
    check([0x62BB_0001_u64, 0x62BB_0002_u64], sqlite_direction_trace)
}

/// Trace for [`sim_snapshot_tail_converges_sqlite_direction`].
fn sqlite_direction_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"sqlite-direction", "restore")?;
    let (split, total) = split_total(&mut case.rng, 15, 25, 5, 25)?;
    harness::seal_rows(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        split,
        [4_u8; 32],
        harness::TIME_BASE.wrapping_add(7),
    )?;
    let (restored, _stats, full) = restore_sqlite_against_full(&case.own, total)?;
    finish_converged(case, &restored, &full)
}
