//! Subprocess fixture that exits without running database destructors.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use logical_db::{
    AccountId, Database, DocumentId, Object, OperationDigest, OperationId, ReplicaId, Scalar,
    Scope, SpaceId,
};
use std::error::Error;
use uuid::Uuid;

fn main() {
    if let Err(error) = run() {
        eprintln!("crash_writer: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let [path, account, space, replica, document] = arguments.as_slice() else {
        return Err("expected path, account, space, replica, and document arguments".into());
    };
    let scope = Scope::new(
        AccountId::from_uuid(Uuid::parse_str(account)?),
        SpaceId::from_uuid(Uuid::parse_str(space)?),
    );
    let replica = ReplicaId::from_uuid(Uuid::parse_str(replica)?);
    let document = DocumentId::from_uuid(Uuid::parse_str(document)?);
    let database = Database::open(path, scope, replica)?;
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
