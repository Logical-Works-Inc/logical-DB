//! Envelope forgery tests: each mutates a sealed field, which only this crate
//! may do, so the integration sims cannot hold them.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::snapshot_backend::RedbSnapshotStore;
use crate::snapshot_envelope::tests::sealed;
use crate::snapshot_envelope::{SnapshotEnvelope, SnapshotEnvelopeError, SnapshotStore};
use crate::test_support::test_path;
use logical_db_model::{AccountId, IndexId, SpaceId};
use std::{error::Error, fs};

/// Flips every byte of one sealed array field.
fn flip(bytes: [u8; 32]) -> [u8; 32] {
    bytes.map(|byte| byte ^ 0xFF)
}

#[test]
fn every_sealed_field_tamper_breaks_the_seal() -> Result<(), Box<dyn Error>> {
    let mutations: [fn(&mut SnapshotEnvelope); 9] = [
        |forged| forged.account = AccountId::new(),
        |forged| forged.space = SpaceId::new(),
        |forged| forged.projection = IndexId::new(),
        |forged| forged.semantics = flip(forged.semantics),
        |forged| forged.checkpoint = flip(forged.checkpoint),
        |forged| forged.last_row = flip(forged.last_row),
        |forged| forged.last_seq ^= 0xFFFF,
        |forged| forged.created_at_secs = forged.created_at_secs.wrapping_add(1),
        |forged| forged.hash = flip(forged.hash),
    ];
    for (field, mutate) in mutations.into_iter().enumerate() {
        let mut forged = sealed(vec![3, 4, 5])?;
        mutate(&mut forged);
        let error = forged.verify();
        assert!(
            matches!(error, Err(SnapshotEnvelopeError::Tampered { .. })),
            "tampered field {field} must fail verification, got {error:?}"
        );
    }
    Ok(())
}

#[test]
fn a_future_schema_version_is_refused_at_save() -> Result<(), Box<dyn Error>> {
    let path = test_path("envelope-version");
    let store = RedbSnapshotStore::open(&path)?;
    let mut envelope = sealed(vec![1, 2])?;
    envelope.schema_version = 999;
    envelope.hash = envelope.compute_hash()?;
    let error = store.save_snapshot(&envelope);
    assert!(
        matches!(error, Err(SnapshotEnvelopeError::VersionMismatch { .. })),
        "a future version must be refused at save, got {error:?}"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn a_mutated_after_seal_envelope_never_persists() -> Result<(), Box<dyn Error>> {
    let path = test_path("envelope-unsealed");
    let store = RedbSnapshotStore::open(&path)?;
    let mut envelope = sealed(vec![1, 2])?;
    envelope.hash = flip(envelope.hash);
    let error = store.save_snapshot(&envelope);
    assert!(
        error.is_err(),
        "a mutated-after-seal envelope must never persist, got {error:?}"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}
