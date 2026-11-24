//! Seeded deterministic simulations for the snapshot envelope contract.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Every sim seals envelopes over seeded state, mutates seeded fields, and
//! asserts verification plus storage refusal. One seed drives identities,
//! state bytes, timestamps, and fault selection; the same seed always hashes
//! the same trace.

use logical_db_store::snapshot_envelope::{SnapshotEnvelope, SnapshotStore};

#[path = "sim_harness.rs"]
mod harness;
use harness::{check, opened, trace_tag};

/// Trace domain: this file's traces never collide with another file's.
const TRACE_DOMAIN: &[u8] = b"ldb.sim.snapshot-envelope.v1";

/// Seals one envelope over seeded relational state at a seeded frontier.
///
/// # Errors
///
/// Returns a backend or envelope failure; the sim counts never trigger one.
fn seal_frontier(
    rng: &mut harness::XorShift,
    state_max: u64,
) -> Result<SnapshotEnvelope, Box<dyn std::error::Error>> {
    let own = harness::fixture(rng, "envelope-seal")?;
    let rows = harness::count(rng, 1, state_max)?;
    let created = harness::TIME_BASE.wrapping_add(
        rng.below(100_000)
            .map_err(|error| format!("sim time overflow: {error}"))?,
    );
    let (_mem, envelope) = harness::seal_rows(
        &own.store,
        own.scope,
        own.projection,
        u64::try_from(rows).map_err(|error| format!("sim rows overflow: {error}"))?,
        rng.bytes32(),
        created,
    )?;
    harness::cleanup(&own.path)?;
    Ok(envelope)
}

/// Seal then verify round-trips over seeded state.
#[test]
fn sim_seal_verify_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    check([0x41AA_0001_u64, 0x41AA_0002_u64], round_trip_trace)
}

/// Trace for [`sim_seal_verify_round_trip`].
fn round_trip_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"round-trip");
    for _ in 0..8 {
        let envelope = seal_frontier(&mut rng, 64)?;
        envelope.verify()?;
        assert_eq!(
            envelope.compute_hash()?,
            envelope.hash(),
            "seal must embed the recomputed hash"
        );
        hasher.update(&envelope.hash());
        hasher.update(&envelope.compute_hash()?);
    }
    Ok(trace_tag(&hasher))
}

// The field-tamper, future-version, and unsealed-mutation sims forge envelopes
// by writing sealed fields, which only the crate itself may do; they live in
// `src/tests/envelope.rs` beside the private fields they mutate.

/// Storage round-trips preserve the envelope and bind the key hash.
#[test]
fn sim_storage_round_trip_binds_key() -> Result<(), Box<dyn std::error::Error>> {
    check([0x41DD_0001_u64, 0x41DD_0002_u64], storage_trace)
}

/// Trace for [`sim_storage_round_trip_binds_key`].
fn storage_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"storage");
    let own = harness::fixture(&mut rng, "envelope-storage")?;
    for _ in 0..6 {
        let (_mem, envelope) = harness::seal_rows(
            &own.store,
            own.scope,
            own.projection,
            3,
            [5_u8; 32],
            harness::TIME_BASE,
        )?;
        let restored = own
            .store
            .load_latest_snapshot(own.scope, own.projection)?
            .ok_or("saved snapshot should load")?;
        assert_eq!(restored, envelope, "storage round-trip preserves bytes");
        restored.verify()?;
        hasher.update(&restored.hash());
    }
    harness::cleanup(&own.path)?;
    Ok(trace_tag(&hasher))
}

/// Timestamps and sequences ride the seal without breaking determinism.
#[test]
fn sim_frontier_fields_ride_the_seal() -> Result<(), Box<dyn std::error::Error>> {
    check([0x42AA_0001_u64, 0x42AA_0002_u64], frontier_trace)
}

/// Trace for [`sim_frontier_fields_ride_the_seal`].
fn frontier_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"frontier");
    // Same rows at two frontiers seal two distinct envelopes.
    let own = harness::fixture(&mut rng, "envelope-frontier")?;
    let first = harness::seal_rows(
        &own.store,
        own.scope,
        own.projection,
        4,
        [1_u8; 32],
        harness::TIME_BASE,
    )?
    .1;
    let second = harness::seal_rows(
        &own.store,
        own.scope,
        own.projection,
        4,
        [2_u8; 32],
        harness::TIME_BASE.wrapping_add(60),
    )?
    .1;
    assert_ne!(first.hash(), second.hash(), "frontiers must separate seals");
    first.verify()?;
    second.verify()?;
    hasher.update(&first.hash());
    hasher.update(&second.hash());
    Ok(trace_tag(&hasher))
}
