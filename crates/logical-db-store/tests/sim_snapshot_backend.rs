//! Seeded deterministic simulations for the snapshot backend.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Every sim opens a dedicated store file, saves seeded envelopes, and
//! asserts latest-pointer, listing, isolation, and delete semantics. One
//! seed drives tenants, state bytes, timestamps, and save order; the same
//! seed always hashes the same trace.

use logical_db_store::snapshot_envelope::SnapshotStore;

#[path = "sim_harness.rs"]
mod harness;
use harness::check;

/// Trace domain: this file's traces never collide with another file's.
const TRACE_DOMAIN: &[u8] = b"ldb.sim.snapshot-backend.v1";

/// Save then load returns the sealed envelope.
#[test]
fn sim_save_load_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    check([0x51AA_0001_u64, 0x51AA_0002_u64], round_trip_trace)
}

/// Trace for [`sim_save_load_round_trip`].
fn round_trip_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"round-trip", "backend")?;
    for seq in 0..harness::count(&mut case.rng, 3, 5)? {
        let created = harness::TIME_BASE.wrapping_add(
            u64::try_from(seq).map_err(|error| format!("sim seq overflow: {error}"))?,
        );
        let envelope = harness::seal_rows(
            &case.own.store,
            case.own.scope,
            case.own.projection,
            u64::try_from(seq).map_err(|error| format!("sim seq overflow: {error}"))?,
            [7_u8; 32],
            created,
        )?
        .1;
        let loaded = case
            .own
            .store
            .load_latest_snapshot(case.own.scope, case.own.projection)?
            .ok_or("saved snapshot should load")?;
        assert_eq!(loaded, envelope, "load returns the saved envelope");
        loaded.verify()?;
        case.hasher.update(&loaded.hash);
    }
    case.finish()
}

/// Cold start answers none on every read path.
#[test]
fn sim_cold_start_answers_none() -> Result<(), Box<dyn std::error::Error>> {
    check([0x51BB_0001_u64, 0x51BB_0002_u64], cold_trace)
}

/// Trace for [`sim_cold_start_answers_none`].
fn cold_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"cold-start", "backend")?;
    for _ in 0..4 {
        let scope = harness::fresh_scope(&mut case.rng)?;
        let projection = harness::fresh_projection(&mut case.rng)?;
        assert!(
            case.own
                .store
                .load_latest_snapshot(scope, projection)?
                .is_none(),
            "cold start loads none"
        );
        assert!(
            case.own.store.list_snapshots(scope, projection)?.is_empty(),
            "cold start lists nothing"
        );
        case.hasher.update(&case.rng.next_u64().to_le_bytes());
    }
    case.finish()
}

/// Tenants never observe each other's snapshots.
#[test]
fn sim_tenants_stay_isolated() -> Result<(), Box<dyn std::error::Error>> {
    check([0x51CC_0001_u64, 0x51CC_0002_u64], isolation_trace)
}

/// Trace for [`sim_tenants_stay_isolated`].
fn isolation_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"isolation", "backend")?;
    let tenants = harness::count(&mut case.rng, 2, 4)?;
    let mut scopes = Vec::new();
    for tenant_no in 0..tenants {
        let scope = harness::fresh_scope(&mut case.rng)?;
        let created = harness::TIME_BASE.wrapping_add(
            u64::try_from(tenant_no).map_err(|error| format!("sim tenant overflow: {error}"))?,
        );
        let envelope = harness::seal_rows(
            &case.own.store,
            scope,
            case.own.projection,
            u64::try_from(tenant_no).map_err(|error| format!("sim tenant overflow: {error}"))?,
            [7_u8; 32],
            created,
        )?
        .1;
        scopes.push((scope, envelope));
    }
    for pair in &scopes {
        let loaded = case
            .own
            .store
            .load_latest_snapshot(pair.0, case.own.projection)?
            .ok_or("each tenant keeps a latest pointer")?;
        assert_eq!(loaded, pair.1, "each scope sees only its own snapshot");
        let listed = case.own.store.list_snapshots(pair.0, case.own.projection)?;
        assert_eq!(listed.len(), 1, "listing stays tenant-scoped");
        case.hasher.update(&loaded.hash);
    }
    case.finish()
}

/// The latest pointer always names the most recent save.
#[test]
fn sim_latest_pointer_tracks_saves() -> Result<(), Box<dyn std::error::Error>> {
    check([0x51DD_0001_u64, 0x51DD_0002_u64], latest_trace)
}

/// Trace for [`sim_latest_pointer_tracks_saves`].
fn latest_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"latest", "backend")?;
    let saves = harness::count(&mut case.rng, 2, 5)?;
    for seq in 0..saves {
        let seq_no = u64::try_from(seq).map_err(|error| format!("sim seq overflow: {error}"))?;
        let envelope = harness::seal_rows(
            &case.own.store,
            case.own.scope,
            case.own.projection,
            seq_no,
            [7_u8; 32],
            harness::TIME_BASE.wrapping_add(seq_no),
        )?
        .1;
        let loaded = case
            .own
            .store
            .load_latest_snapshot(case.own.scope, case.own.projection)?
            .ok_or("latest must exist after save")?;
        assert_eq!(loaded.hash, envelope.hash, "latest tracks the newest save");
        case.hasher.update(&loaded.hash);
    }
    let listed = case
        .own
        .store
        .list_snapshots(case.own.scope, case.own.projection)?;
    assert_eq!(listed.len(), saves, "every save is listed");
    // Newest-first by creation time.
    for window in listed.windows(2) {
        assert!(
            window[0].created_at_secs >= window[1].created_at_secs,
            "listing runs newest-first"
        );
    }
    case.finish()
}

/// Deletes remove records and clear a latest pointer that names them.
#[test]
fn sim_delete_clears_resolve() -> Result<(), Box<dyn std::error::Error>> {
    check([0x51EE_0001_u64, 0x51EE_0002_u64], delete_trace)
}

/// Trace for [`sim_delete_clears_resolve`].
fn delete_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"delete", "backend")?;
    let before = case
        .rng
        .below(1_000)
        .map_err(|error| format!("sim time overflow: {error}"))?;
    let first = harness::seal_rows(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        1,
        [7_u8; 32],
        harness::TIME_BASE,
    )?
    .1;
    let second = harness::seal_rows(
        &case.own.store,
        case.own.scope,
        case.own.projection,
        2,
        [7_u8; 32],
        harness::TIME_BASE.wrapping_add(1_u64.saturating_add(before)),
    )?
    .1;
    // Deleting a non-latest record keeps the latest pointer intact.
    assert!(
        case.own
            .store
            .delete_snapshot(case.own.scope, case.own.projection, &first.hash)?,
        "delete reports the removal"
    );
    let still = case
        .own
        .store
        .load_latest_snapshot(case.own.scope, case.own.projection)?
        .ok_or("latest must survive a non-latest delete")?;
    assert_eq!(still.hash, second.hash, "latest still names the newest");
    // Deleting the latest clears the pointer.
    assert!(
        case.own
            .store
            .delete_snapshot(case.own.scope, case.own.projection, &second.hash)?,
        "delete reports the latest removal"
    );
    assert!(
        case.own
            .store
            .load_latest_snapshot(case.own.scope, case.own.projection)?
            .is_none(),
        "no latest remains after deleting it"
    );
    // Deleting an absent hash reports false.
    assert!(
        !case
            .own
            .store
            .delete_snapshot(case.own.scope, case.own.projection, &second.hash)?,
        "absent delete reports false"
    );
    case.hasher.update(&first.hash);
    case.hasher.update(&second.hash);
    case.finish()
}

/// Projections never observe each other's snapshots.
#[test]
fn sim_projections_stay_isolated() -> Result<(), Box<dyn std::error::Error>> {
    check([0x51FF_0001_u64, 0x51FF_0002_u64], projection_trace)
}

/// Trace for [`sim_projections_stay_isolated`].
fn projection_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let mut case = harness::Case::open(seed, TRACE_DOMAIN, b"projections", "backend")?;
    let total = harness::count(&mut case.rng, 2, 3)?;
    let mut saved = Vec::new();
    for _ in 0..total {
        let projection = harness::fresh_projection(&mut case.rng)?;
        let created = harness::TIME_BASE.wrapping_add(
            case.rng
                .below(500)
                .map_err(|error| format!("sim time overflow: {error}"))?,
        );
        let tail = case
            .rng
            .next_u64()
            .checked_rem(8)
            .ok_or("sim rows dispatch overflow")?;
        let envelope = harness::seal_rows(
            &case.own.store,
            case.own.scope,
            projection,
            tail,
            [7_u8; 32],
            created,
        )?
        .1;
        saved.push((projection, envelope));
    }
    for pair in &saved {
        let loaded = case
            .own
            .store
            .load_latest_snapshot(case.own.scope, pair.0)?
            .ok_or("each projection keeps a latest pointer")?;
        assert_eq!(loaded.hash, pair.1.hash, "projections stay separated");
        case.hasher.update(&loaded.hash);
    }
    case.finish()
}
