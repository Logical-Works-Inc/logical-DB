//! Child process used to exercise kill-during-commit recovery.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

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
    let database = Database::open(&arguments[0], scope, replica).unwrap();
    for sequence in 1_u64..=u64::MAX {
        let mut digest = [0; 32];
        digest[..8].copy_from_slice(&sequence.to_be_bytes());
        database
            .edit(
                DocumentId::new(),
                OperationId::new(),
                OperationDigest::new(digest),
                |editor| editor.put(&Object::root(), "sequence", Scalar::Unsigned(sequence)),
            )
            .unwrap();
        if sequence == 1 {
            std::fs::write(&arguments[4], b"ready").unwrap();
        }
    }
}
