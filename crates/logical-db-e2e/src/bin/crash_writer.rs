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
use uuid::Uuid;

fn main() {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    assert_eq!(arguments.len(), 5);
    let scope = Scope::new(
        AccountId::from_uuid(Uuid::parse_str(&arguments[1]).unwrap()),
        SpaceId::from_uuid(Uuid::parse_str(&arguments[2]).unwrap()),
    );
    let replica = ReplicaId::from_uuid(Uuid::parse_str(&arguments[3]).unwrap());
    let document = DocumentId::from_uuid(Uuid::parse_str(&arguments[4]).unwrap());
    let database = Database::open(&arguments[0], scope, replica).unwrap();
    database
        .edit(
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
        )
        .unwrap();
    std::process::exit(71);
}
