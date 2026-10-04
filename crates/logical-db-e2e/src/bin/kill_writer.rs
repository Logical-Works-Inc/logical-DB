//! Child process used to exercise kill-during-commit recovery.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

#![expect(
    clippy::print_stderr,
    reason = "src/bin/ is the PRINTS-exempt binary surface per the execution contract"
)]
#![expect(
    clippy::exit,
    reason = "a failed fixture run must report and exit nonzero; `fail` is the exit site because clippy::exit is a no-op inside fn main"
)]
use logical_db::{
    AccountId, Database, DocumentId, Object, OperationDigest, OperationId, ReplicaId, Scalar,
    Scope, SpaceId,
};
use std::error::Error;

/// Reports a failed fixture run on stderr and exits nonzero.
fn main() {
    if let Err(error) = run() {
        fail(error);
    }
}

/// Reports `error` on stderr and exits nonzero.
///
/// The `process::exit` lives here rather than in `main` because `clippy::exit`
/// does not fire inside `fn main` (verified 2026-09-22). Calling it from this
/// helper is what makes the crate-level exit suppression real rather than
/// unfulfilled.
fn fail(error: Box<dyn Error>) -> ! {
    eprintln!("kill_writer: {error}");
    std::process::exit(1);
}

/// Writes one edit per iteration until the orchestrator kills this process.
fn run() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let [path, account, space, replica, ready]: [String; 5] = arguments
        .try_into()
        .map_err(|_| "expected path, account, space, replica, and ready-file arguments")?;
    let scope = Scope::new(
        AccountId::parse_canonical(&account)?,
        SpaceId::parse_canonical(&space)?,
    );
    let replica = ReplicaId::parse_canonical(&replica)?;
    let database = Database::open(&path, scope, replica)?;
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
            std::fs::write(&ready, b"ready")?;
        }
    }
    Ok(())
}
