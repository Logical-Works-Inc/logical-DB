//! Subprocess fixture that exits without running database destructors.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{mem::ManuallyDrop, process::ExitCode};

use logical_db::{Database, DocumentId, Object, OperationDigest, OperationId, Scalar};
use logical_db_e2e::{
    fixture::{FixtureError, FixtureReplica},
    support::parse_identifier,
};

/// Exit status of a run that committed its edit and skipped every destructor.
const CRASHED_AFTER_COMMIT: u8 = 71;

/// Commits one edit, then ends the process without running the replica's
/// destructor: the handle is held in `ManuallyDrop`, so returning from `main`
/// leaves the database exactly as a killed process would.
fn main() -> Result<ExitCode, FixtureError> {
    let fixture = FixtureReplica::from_arguments(std::env::args().skip(1))?;
    let document = parse_identifier::<DocumentId>(fixture.last(), "document")
        .map_err(FixtureError::Identifier)?;
    let database = ManuallyDrop::new(Database::open(
        fixture.path(),
        fixture.scope(),
        fixture.replica(),
    )?);
    database.edit(
        document,
        OperationId::new(),
        OperationDigest::new([71; 32]),
        |editor| {
            editor.put(
                &Object::root(),
                "status",
                Scalar::String("committed-before-exit".into()),
            )
        },
    )?;
    if database.pending_changes(0, 1)?.is_empty() {
        return Err(FixtureError::NotCommitted);
    }
    Ok(ExitCode::from(CRASHED_AFTER_COMMIT))
}
