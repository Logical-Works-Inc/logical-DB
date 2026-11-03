//! Store unit tests; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::error::StoreError;
use crate::keys::{DATA, key_operation};
use crate::store::EmbeddedStore;
use crate::test_support::{TestError, change, commit_live, test_path};
use crate::tests_sync::commit_secondary;
use crate::types::{CommitOutcome, LocalChange, LocalCommit, OutboxEntry, StoredSnapshot};
use logical_db_model::{
    AccountId, DocumentId, DocumentRevision, OperationId, ReplicaId, Scope, SpaceId,
};
use redb::{Database, TableDefinition};
use std::{error::Error, fs};

#[test]
fn commit_restart_and_outbox_are_one_atomic_state() -> Result<(), Box<dyn Error>> {
    let path = test_path("restart");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let operation = OperationId::new();
    let change = change(scope, document, operation, 7, 9)?;
    let replica = ReplicaId::new();
    {
        let store = EmbeddedStore::open(&path, replica)?;
        assert_eq!(
            store.commit_local(LocalCommit {
                change,
                expected_revision: None,
                snapshot: b"verified-snapshot",
                indexes: &[],
                tombstone: false,
            })?,
            CommitOutcome::Applied { sequence: 1 },
            "the first commit must apply at sequence one"
        );
    }
    {
        let store = EmbeddedStore::open(&path, replica)?;
        assert_eq!(
            store.load_snapshot(scope, document)?,
            Some(StoredSnapshot {
                bytes: b"verified-snapshot".to_vec(),
                revision: DocumentRevision::new(1),
            }),
            "the committed snapshot must survive restart"
        );
        assert_eq!(
            store.outbox_page(scope, 0, 10)?,
            vec![OutboxEntry {
                sequence: 1,
                document,
                operation,
                digest: change.digest,
            }],
            "restart must replay exactly the one outbox entry"
        );
        assert_eq!(
            store.commit_local(LocalCommit {
                change,
                expected_revision: Some(DocumentRevision::new(1)),
                snapshot: b"verified-snapshot",
                indexes: &[],
                tombstone: false,
            })?,
            CommitOutcome::Duplicate { sequence: 1 },
            "recommit at the same revision must be a duplicate"
        );
    }
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn operation_reuse_with_different_digest_is_rejected() -> Result<(), Box<dyn Error>> {
    let path = test_path("operation-conflict");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let operation = OperationId::new();
    let first = change(scope, document, operation, 7, 9)?;
    let second = change(scope, document, operation, 7, 8)?;
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    commit_live(&store, first, b"first")?;
    assert!(
        matches!(
            commit_secondary(&store, second, Some(DocumentRevision::new(1)), b"second"),
            Err(StoreError::OperationConflict)
        ),
        "reusing an operation with a different digest must be rejected"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn two_successors_of_one_snapshot_cannot_overwrite_materialized_state() -> Result<(), TestError> {
    let path = test_path("stale-successor");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let first = change(scope, document, OperationId::new(), 1, 9)?;
    let stale = change(scope, document, OperationId::new(), 2, 6)?;
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    commit_live(&store, first, b"first")?;
    assert!(
        matches!(
            store.commit_local(LocalCommit {
                change: stale,
                expected_revision: None,
                snapshot: b"stale",
                indexes: &[],
                tombstone: false,
            }),
            Err(StoreError::StaleDocument)
        ),
        "a second successor of the same snapshot must be refused as stale"
    );
    assert_eq!(
        store.load_snapshot(scope, document)?,
        Some(StoredSnapshot {
            bytes: b"first".to_vec(),
            revision: DocumentRevision::new(1),
        }),
        "the stale commit must leave the materialized snapshot untouched"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn replica_identity_is_durable_and_cannot_relabel_outbox_state() -> Result<(), Box<dyn Error>> {
    let path = test_path("replica-binding");
    let first = ReplicaId::new();
    let second = ReplicaId::new();
    drop(EmbeddedStore::open(&path, first)?);
    assert!(
        matches!(
            EmbeddedStore::open(&path, second),
            Err(StoreError::ReplicaMismatch {
                expected,
                actual
            }) if expected == first && actual == second
        ),
        "opening under a different replica identity must be refused"
    );
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn maximum_checkpoint_cannot_repeat_the_final_outbox_entry() -> Result<(), Box<dyn Error>> {
    let path = test_path("maximum-checkpoint");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    assert!(
        store.outbox_page(scope, u64::MAX, 1)?.is_empty(),
        "a checkpoint at the maximum sequence must page no entries"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn concurrent_commit_receipts_match_their_exact_outbox_entries() -> Result<(), Box<dyn Error>> {
    let path = test_path("concurrent-sequences");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let requests = (1..=16u8)
        .map(
            |byte| -> Result<(OperationId, LocalChange<'static>), StoreError> {
                let operation = OperationId::new();
                Ok((
                    operation,
                    change(scope, DocumentId::new(), operation, byte, byte)?,
                ))
            },
        )
        .collect::<Result<Vec<_>, StoreError>>()?;
    let receipts = std::thread::scope(|threads| {
        let store = &store;
        requests
            .iter()
            .map(|&(operation, change)| {
                threads.spawn(move || {
                    let outcome = store.commit_local(LocalCommit {
                        change,
                        expected_revision: None,
                        snapshot: b"concurrent-snapshot",
                        indexes: &[],
                        tombstone: false,
                    })?;
                    Ok::<_, StoreError>((operation, outcome))
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|thread| {
                thread
                    .join()
                    .map_err(|_| StoreError::CorruptInternalValue)?
            })
            .collect::<Result<Vec<_>, StoreError>>()
    })?;
    let outbox = store
        .outbox_page(scope, 0, requests.len())?
        .into_iter()
        .map(|entry| (entry.operation, entry.sequence))
        .collect::<std::collections::BTreeMap<_, _>>();
    for (operation, outcome) in receipts {
        let sequence = match outcome {
            CommitOutcome::Applied { sequence } => sequence,
            CommitOutcome::Duplicate { sequence } => {
                return Err(format!(
                    "unique operation {operation} reported as duplicate at {sequence}"
                )
                .into());
            }
        };
        assert_eq!(
            outbox.get(&operation),
            Some(&sequence),
            "every concurrent receipt must match its exact outbox entry"
        );
    }
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

/// Unique suffix for the unicode-path fixture, shared process-wide so parallel
/// test threads never share a directory.
static NEXT_UNICODE_DIR_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[test]
fn unicode_and_space_path_survives_restart() -> Result<(), Box<dyn Error>> {
    let directory = std::env::temp_dir().join(format!(
        "logical db 雲 {}-{}",
        std::process::id(),
        NEXT_UNICODE_DIR_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    fs::create_dir(&directory)?;
    let path = directory.join("replica state.redb");
    let replica = ReplicaId::new();
    drop(EmbeddedStore::open(&path, replica)?);
    let reopened = EmbeddedStore::open(&path, replica)?;
    assert_eq!(
        reopened.replica(),
        replica,
        "the replica identity must survive reopen"
    );
    drop(reopened);
    fs::remove_file(path)?;
    fs::remove_dir(directory)?;
    Ok(())
}

#[test]
fn corrupt_operation_record_fails_closed() -> Result<(), Box<dyn Error>> {
    let path = test_path("corrupt-operation");
    let replica = ReplicaId::new();
    let store = EmbeddedStore::open(&path, replica)?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let operation = OperationId::new();
    {
        let transaction = store.database.begin_write()?;
        {
            let mut table = transaction.open_table(DATA)?;
            table.insert(
                key_operation(scope, operation).as_slice(),
                b"truncated".as_slice(),
            )?;
        }
        transaction.commit()?;
    }
    assert!(
        matches!(
            store.operation(scope, operation),
            Err(StoreError::CorruptInternalValue)
        ),
        "a truncated operation record must fail closed as corrupt"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn existing_unknown_schema_is_never_opened_as_an_empty_v3_replica() -> Result<(), Box<dyn Error>> {
    /// A legacy table used only to prove schema refusal on open.
    const LEGACY: TableDefinition<u8, u8> = TableDefinition::new("legacy_data");

    let path = test_path("legacy-schema");
    {
        let database = Database::create(&path)?;
        let transaction = database.begin_write()?;
        transaction.open_table(LEGACY)?;
        transaction.commit()?;
    }
    assert!(
        matches!(
            EmbeddedStore::open(&path, ReplicaId::new()),
            Err(StoreError::UnsupportedSchema)
        ),
        "an unknown pre-existing schema must never open as an empty replica"
    );
    fs::remove_file(path)?;
    Ok(())
}
