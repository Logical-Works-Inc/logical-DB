//! Kill-during-write recovery journey for the embedded database.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::{fs, process::Command, thread, time::Duration};

use logical_db::{AccountId, Database, Item, Object, ReplicaId, Scalar, Scope, SpaceId};

#[test]
fn forced_termination_never_exposes_a_partial_embedded_transition() {
    let suffix = uuid::Uuid::now_v7();
    let path = std::env::temp_dir().join(format!("logical-db-kill-{suffix}.redb"));
    let ready = std::env::temp_dir().join(format!("logical-db-kill-{suffix}.ready"));
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let mut child = Command::new(env!("CARGO_BIN_EXE_kill_writer"))
        .arg(&path)
        .arg(scope.account().to_string())
        .arg(scope.space().to_string())
        .arg(replica.to_string())
        .arg(&ready)
        .spawn()
        .unwrap();
    for _ in 0..500 {
        if ready.exists() {
            break;
        }
        assert!(child.try_wait().unwrap().is_none());
        thread::sleep(Duration::from_millis(10));
    }
    assert!(ready.exists());
    thread::sleep(Duration::from_millis(20));
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(!status.success());

    let database = Database::open(&path, scope, replica).unwrap();
    let mut cursor = 0;
    let mut events = Vec::new();
    loop {
        let page = database.changes_since(cursor, 32).unwrap();
        if page.is_empty() {
            break;
        }
        cursor = page.last().unwrap().sequence();
        events.extend(page);
    }
    assert!(!events.is_empty());
    for (offset, event) in events.iter().enumerate() {
        assert_eq!(event.sequence(), u64::try_from(offset + 1).unwrap());
        assert!(matches!(
            database
                .read(event.document().unwrap(), |reader| reader
                    .get(&Object::root(), "sequence"))
                .unwrap(),
            Some(Some(Item::Scalar(Scalar::Unsigned(_))))
        ));
    }
    assert_eq!(
        database.pending_changes(0, 256).unwrap().len(),
        events.len()
    );
    drop(database);
    let reopened = Database::open(&path, scope, replica).unwrap();
    assert_eq!(reopened.changes_since(0, 256).unwrap(), events);
    drop(reopened);
    fs::remove_file(path).unwrap();
    fs::remove_file(ready).unwrap();
}
