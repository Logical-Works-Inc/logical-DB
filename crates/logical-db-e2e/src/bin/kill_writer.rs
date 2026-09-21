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
    clippy::print_stdout,
    reason = "src/bin/ is the PRINTS-exempt binary surface per the execution contract"
)]
use logical_db::{
    AccountId, Database, DocumentId, Object, OperationDigest, OperationId, ReplicaId, Scalar,
    Scope, SpaceId,
};
use std::error::Error;
use uuid::Uuid;

/// Reports a failed fixture run on stderr and exits nonzero.
fn main() {
    if let Err(error) = run() {
        eprintln!("kill_writer: {error}");
        #[expect(
            clippy::exit,
            reason = "a failed fixture run must report and exit nonzero"
        )]
        {
            std::process::exit(1);
        }
    }
}

/// Writes one edit per iteration until the orchestrator kills this process.
fn run() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let &[ref path, ref account, ref space, ref replica, ref ready] = arguments.as_slice() else {
        return Err("expected path, account, space, replica, and ready-file arguments".into());
    };
    let scope = Scope::new(
        AccountId::from_uuid(Uuid::parse_str(account)?),
        SpaceId::from_uuid(Uuid::parse_str(space)?),
    );
    let replica = ReplicaId::from_uuid(Uuid::parse_str(replica)?);
    let database = Database::open(path, scope, replica)?;
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
            std::fs::write(ready, b"ready")?;
        }
    }
    Ok(())
}
