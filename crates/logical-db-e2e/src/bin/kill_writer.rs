//! Child process used to exercise kill-during-commit recovery.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use logical_db::{Database, DocumentId, Object, OperationDigest, OperationId, Scalar};
use logical_db_e2e::fixture::{FixtureError, FixtureReplica};

/// Writes one edit per iteration until the orchestrator kills this process.
fn main() -> Result<(), FixtureError> {
    let fixture = FixtureReplica::from_arguments(std::env::args().skip(1))?;
    let database = Database::open(fixture.path(), fixture.scope(), fixture.replica())?;
    for sequence in 1_u64..=u64::MAX {
        let mut digest = [0; 32];
        digest[..8].copy_from_slice(&sequence.to_be_bytes());
        database.edit(
            DocumentId::new(),
            OperationId::new(),
            OperationDigest::new(digest),
            |editor| editor.put(&Object::root(), "sequence", Scalar::Unsigned(sequence)),
        )?;
        if sequence == 1 {
            std::fs::write(fixture.last(), b"ready").map_err(FixtureError::Ready)?;
        }
    }
    Ok(())
}
