//! Subprocess fixture that exits without running database destructors.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]
#![expect(
    clippy::print_stderr,
    reason = "src/bin/ is the PRINTS-exempt binary surface per the execution contract"
)]
#![expect(
    clippy::exit,
    reason = "the crash fixture must exit without running destructors, and a failed run must exit nonzero; this fixture never prints to stdout"
)]

use logical_db::{
    AccountId, Database, DocumentId, Object, OperationDigest, OperationId, ReplicaId, Scalar,
    Scope, SpaceId,
};
use std::error::Error;

/// Reports a failed fixture run on stderr and exits nonzero.
fn main() {
    if let Err(error) = run() {
        eprintln!("crash_writer: {error}");
        std::process::exit(1);
    }
}

/// Commits one edit, then exits without running destructors.
fn run() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let [path, account, space, replica, document]: [String; 5] = arguments
        .try_into()
        .map_err(|_| "expected path, account, space, replica, and document arguments")?;
    let scope = Scope::new(
        AccountId::parse_canonical(&account)?,
        SpaceId::parse_canonical(&space)?,
    );
    let replica = ReplicaId::parse_canonical(&replica)?;
    let document = DocumentId::parse_canonical(&document)?;
    let database = Database::open(&path, scope, replica)?;
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
    std::process::exit(71);
}
