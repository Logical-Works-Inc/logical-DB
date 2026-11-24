//! Index unit tests; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::decode_uuid;
use crate::error::StoreError;
use crate::keys::{DATA, key_change, key_index_reverse};
use crate::store::EmbeddedStore;
use crate::test_support::{change, test_path};
use crate::types::{IndexHit, LocalCommit};
use logical_db_model::{
    AccountId, DocumentId, DocumentRevision, IndexEntry, IndexId, IndexValue, OperationId,
    ReplicaId, Scope, SpaceId,
};
use std::{error::Error, fs};

#[test]
fn index_and_event_reads_reject_orphaned_internal_rows() -> Result<(), Box<dyn Error>> {
    let path = test_path("orphaned-rows");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let index = IndexId::new();
    let value = IndexValue::Text("kind".into());
    let local = change(scope, document, OperationId::new(), 3, 4)?;
    let digest = local.digest;
    store.commit_local(LocalCommit {
        change: local,
        expected_revision: None,
        snapshot: b"snapshot",
        indexes: &[IndexEntry::new(index, value.clone())],
        tombstone: false,
    })?;
    assert_eq!(
        store.query_index(scope, index, &value, None, 1)?.len(),
        1,
        "the committed index must hold one hit"
    );
    assert_eq!(
        store.changes_since(scope, 0, 1)?.len(),
        1,
        "the feed must hold one event"
    );
    {
        let transaction = store.database.begin_write()?;
        {
            let mut table = transaction.open_table(DATA)?;
            table.remove(key_index_reverse(scope, document, index).as_slice())?;
            table.remove(key_change(scope, document, digest).as_slice())?;
        }
        transaction.commit()?;
    }
    assert!(
        matches!(
            store.query_index(scope, index, &value, None, 1),
            Err(StoreError::CorruptInternalValue)
        ),
        "an orphaned index row must fail closed"
    );
    assert!(
        matches!(
            store.changes_since(scope, 0, 1),
            Err(StoreError::CorruptInternalValue)
        ),
        "an orphaned change row must fail closed"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn secondary_indexes_replace_rows_page_stably_and_encode_order() -> Result<(), Box<dyn Error>> {
    let path = test_path("direct-index");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let index = IndexId::new();
    let first_document = DocumentId::from_uuid(decode_uuid(&1_u128.to_be_bytes())?);
    let second_document = DocumentId::from_uuid(decode_uuid(&2_u128.to_be_bytes())?);
    for (document, byte) in [(first_document, 1), (second_document, 2)] {
        store.commit_local(LocalCommit {
            change: change(scope, document, OperationId::new(), byte, byte)?,
            expected_revision: None,
            snapshot: b"indexed",
            indexes: &[IndexEntry::new(index, IndexValue::Text("note".into()))],
            tombstone: false,
        })?;
    }
    assert_eq!(
        store.query_index(scope, index, &IndexValue::Text("note".into()), None, 1)?,
        vec![IndexHit {
            document: first_document
        }],
        "the first page must start at the first document"
    );
    assert_eq!(
        store.query_index(
            scope,
            index,
            &IndexValue::Text("note".into()),
            Some(first_document),
            10,
        )?,
        vec![IndexHit {
            document: second_document
        }],
        "the continuation must yield the second document"
    );
    assert!(
        matches!(
            store.query_index(scope, index, &IndexValue::Text("note".into()), None, 0),
            Err(StoreError::InvalidIndexLimit(0))
        ),
        "a zero index limit must be refused"
    );
    store.commit_local(LocalCommit {
        change: change(scope, first_document, OperationId::new(), 3, 3)?,
        expected_revision: Some(DocumentRevision::new(1)),
        snapshot: b"reindexed",
        indexes: &[IndexEntry::new(index, IndexValue::Text("task".into()))],
        tombstone: false,
    })?;
    assert_eq!(
        store.query_index(scope, index, &IndexValue::Text("note".into()), None, 10)?,
        vec![IndexHit {
            document: second_document
        }],
        "reindexing must remove the old value row"
    );
    assert_eq!(
        store.query_index(scope, index, &IndexValue::Text("task".into()), None, 10)?,
        vec![IndexHit {
            document: first_document
        }],
        "the new value row must be queryable"
    );

    drop(store);
    fs::remove_file(path)?;
    Ok(())
}
