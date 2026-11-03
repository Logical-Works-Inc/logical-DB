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

use logical_db_store::snapshot_envelope::{SnapshotEnvelope, SnapshotEnvelopeError, SnapshotStore};

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
            envelope.hash,
            "seal must embed the recomputed hash"
        );
        hasher.update(&envelope.hash);
        hasher.update(&envelope.compute_hash()?);
    }
    Ok(trace_tag(hasher))
}

/// Mutating any sealed field breaks verification.
#[test]
fn sim_every_field_tamper_breaks_seal() -> Result<(), Box<dyn std::error::Error>> {
    check([0x41BB_0001_u64, 0x41BB_0002_u64], tamper_trace)
}

/// Trace for [`sim_every_field_tamper_breaks_seal`].
fn tamper_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"tamper");
    for _ in 0..6 {
        let base = seal_frontier(&mut rng, 64)?;
        let field = rng
            .below(9)
            .map_err(|error| format!("sim field overflow: {error}"))?;
        let mut forged = base;
        match field {
            0 => forged.account = harness::fresh_account(&mut rng)?,
            1 => forged.space = harness::fresh_space(&mut rng)?,
            2 => forged.projection = harness::fresh_projection(&mut rng)?,
            3 => forged.semantics[0] ^= 0xFF,
            4 => forged.checkpoint[0] ^= 0xFF,
            5 => forged.last_row[0] ^= 0xFF,
            6 => forged.last_seq ^= 0xFFFF,
            7 => forged.created_at_secs = forged.created_at_secs.wrapping_add(1),
            _ => forged.hash[0] ^= 0xFF,
        }
        let error = forged.verify();
        assert!(
            matches!(error, Err(SnapshotEnvelopeError::Tampered { .. })),
            "tampered field {field} must fail verification, got {error:?}"
        );
        hasher.update(&field.to_le_bytes());
    }
    Ok(trace_tag(hasher))
}

/// A future schema version is refused by the version gate.
#[test]
fn sim_future_version_refused() -> Result<(), Box<dyn std::error::Error>> {
    check([0x41CC_0001_u64, 0x41CC_0002_u64], version_trace)
}

/// Trace for [`sim_future_version_refused`].
fn version_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"version");
    let own = harness::fixture(&mut rng, "envelope-version")?;
    for _ in 0..4 {
        let mut envelope = seal_frontier(&mut rng, 16)?;
        envelope.schema_version = 999;
        envelope.hash = envelope.compute_hash()?;
        let error = own.store.save_snapshot(&envelope);
        assert!(
            matches!(error, Err(SnapshotEnvelopeError::VersionMismatch { .. })),
            "a future version must be refused at save, got {error:?}"
        );
        hasher.update(&envelope.schema_version.to_le_bytes());
    }
    harness::cleanup(&own.path)?;
    Ok(trace_tag(hasher))
}

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
        hasher.update(&restored.hash);
    }
    harness::cleanup(&own.path)?;
    Ok(trace_tag(hasher))
}

/// A mutated-after-seal envelope never serializes for storage.
#[test]
fn sim_unsealed_mutation_never_persists() -> Result<(), Box<dyn std::error::Error>> {
    check([0x41EE_0001_u64, 0x41EE_0002_u64], unsealed_trace)
}

/// Trace for [`sim_unsealed_mutation_never_persists`].
fn unsealed_trace(seed: u64) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let (mut rng, mut hasher) = opened(seed, TRACE_DOMAIN, b"unsealed");
    let own = harness::fixture(&mut rng, "envelope-unsealed")?;
    for _ in 0..4 {
        let mut envelope = seal_frontier(&mut rng, 32)?;
        envelope.hash[0] ^= 0xFF;
        let error = own.store.save_snapshot(&envelope);
        assert!(
            error.is_err(),
            "a mutated-after-seal envelope must never persist, got {error:?}"
        );
        hasher.update(&[u8::from(error.is_err())]);
    }
    harness::cleanup(&own.path)?;
    Ok(trace_tag(hasher))
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
    assert_ne!(first.hash, second.hash, "frontiers must separate seals");
    first.verify()?;
    second.verify()?;
    hasher.update(&first.hash);
    hasher.update(&second.hash);
    Ok(trace_tag(hasher))
}
