//! Process-exit recovery journey for immediate embedded durability.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

use std::{fs, process::Command};

use logical_db::{
    AccountId, Database, DocumentId, Item, Object, ReplicaId, Scalar, Scope, SpaceId,
};

#[test]
fn committed_edit_survives_process_exit_without_destructors() {
    let path = std::env::temp_dir().join(format!(
        "logical-db-crash-recovery-{}.redb",
        uuid::Uuid::now_v7()
    ));
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let document = DocumentId::new();
    let status = Command::new(env!("CARGO_BIN_EXE_crash_writer"))
        .arg(&path)
        .arg(scope.account().to_string())
        .arg(scope.space().to_string())
        .arg(replica.to_string())
        .arg(document.to_string())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(71));

    let database = Database::open(&path, scope, replica).unwrap();
    assert_eq!(
        database
            .read(document, |reader| reader.get(&Object::root(), "status"))
            .unwrap(),
        Some(Some(Item::Scalar(Scalar::String(
            "committed-before-exit".into()
        ))))
    );
    drop(database);
    fs::remove_file(path).unwrap();
}
