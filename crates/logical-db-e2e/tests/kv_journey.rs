//! Key-value cache journey through the shipped public API.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! Thin black-box wrapper: the journey lives in [`logical_db_e2e::kv`], the
//! same code the `kv_journey` binary ships.

use logical_db_e2e::kv::{KvJourneyError, kv_cache_journey};

/// Cache, sync, chunked blob transfer, reclamation and migration all hold for
/// the user, with one receipt line per observable step.
#[test]
fn kv_cache_journey_holds_end_to_end() -> Result<(), KvJourneyError> {
    let dir = std::env::temp_dir().join(format!(
        "logical-db-kv-journey-{:?}",
        logical_db_kv::ReplicaId::new()
    ));
    std::fs::create_dir_all(&dir)?;
    let receipt = kv_cache_journey(&dir)?;
    std::fs::remove_dir_all(&dir)?;
    assert_eq!(receipt.len(), 7, "every journey step reports: {receipt:?}");
    Ok(())
}
