//! Schema unit tests; split from the store root.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use crate::codec::decode_uuid;
use crate::error::StoreError;
use crate::keys::{DATA, key_revision, key_snapshot};
use crate::store::EmbeddedStore;
use crate::test_support::{apply_migration_documents, change, seed_migration_documents, test_path};
use crate::types::{LocalCommit, MigrationIndexes};
use logical_db_model::{
    AccountId, DocumentId, DocumentRevision, IndexId, IndexValue, OperationId, ReplicaId, Scope,
    SpaceId,
};
use std::{error::Error, fs};

#[test]
fn schema_binding_is_exact_nonzero_and_never_adopts_existing_state() -> Result<(), Box<dyn Error>> {
    let path = test_path("schema-binding");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let first = Scope::new(AccountId::new(), SpaceId::new());
    assert!(
        matches!(
            store.bind_schema(first, 0, [1; 32]),
            Err(StoreError::InvalidSchemaVersion)
        ),
        "schema version zero must be refused"
    );
    assert!(
        store.bind_schema(first, 7, [1; 32]).is_ok(),
        "the first binding must succeed"
    );
    assert!(
        store.bind_schema(first, 7, [1; 32]).is_ok(),
        "rebinding the identical schema must succeed"
    );
    assert!(
        matches!(
            store.bind_schema(first, 8, [1; 32]),
            Err(StoreError::SchemaMigrationRequired)
        ),
        "a different version must require migration"
    );
    assert!(
        matches!(
            store.bind_schema(first, 7, [2; 32]),
            Err(StoreError::SchemaMigrationRequired)
        ),
        "a different fingerprint must require migration"
    );

    let second = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    store.commit_local(LocalCommit {
        change: change(second, document, OperationId::new(), 1, 2)?,
        expected_revision: None,
        snapshot: b"state-before-binding",
        indexes: &[],
        tombstone: false,
    })?;
    assert!(
        matches!(
            store.bind_schema(second, 1, [3; 32]),
            Err(StoreError::SchemaMigrationRequired)
        ),
        "existing state must never be adopted by a late binding"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn schema_migration_is_exclusive_cursor_resumable_and_atomically_published()
-> Result<(), Box<dyn Error>> {
    let path = test_path("schema-migration");
    let replica = ReplicaId::new();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let old_index = IndexId::new();
    let new_index = IndexId::new();
    let documents = [
        DocumentId::from_uuid(decode_uuid(&1_u128.to_be_bytes())?),
        DocumentId::from_uuid(decode_uuid(&2_u128.to_be_bytes())?),
        DocumentId::from_uuid(decode_uuid(&3_u128.to_be_bytes())?),
    ];
    let store = EmbeddedStore::open(&path, replica)?;
    seed_migration_documents(&store, scope, old_index, documents)?;
    store.begin_schema_migration(scope, 2, [2; 32])?;
    assert!(
        matches!(
            store.bind_schema(scope, 1, [1; 32]),
            Err(StoreError::MigrationInProgress)
        ),
        "schema binding must be excluded while a migration is in progress"
    );
    assert!(
        matches!(
            store.query_index(scope, old_index, &IndexValue::Text("old".into()), None, 10),
            Err(StoreError::MigrationInProgress)
        ),
        "index reads must be excluded while a migration is in progress"
    );
    assert!(
        matches!(
            store.finish_schema_migration(scope, 2, [2; 32]),
            Err(StoreError::MigrationIncomplete)
        ),
        "finishing before the cursor completes must fail"
    );
    assert!(
        matches!(
            store.migration_page(scope, 2, [2; 32], 0),
            Err(StoreError::InvalidMigrationLimit(0))
        ),
        "a zero migration limit must be refused"
    );
    assert!(
        matches!(
            store.migration_page(scope, 3, [3; 32], 1),
            Err(StoreError::MigrationTargetMismatch)
        ),
        "a mismatched migration target must be refused"
    );
    let first = store.migration_page(scope, 2, [2; 32], 1)?;
    assert_eq!(
        first.after, None,
        "the first page starts at the cursor origin"
    );
    assert_eq!(
        first.documents[0].document, documents[0],
        "the first page must open with the first document"
    );
    apply_migration_documents(&store, scope, new_index, &first)?;
    drop(store);

    let store = EmbeddedStore::open(&path, replica)?;
    store.begin_schema_migration(scope, 2, [2; 32])?;
    assert!(
        matches!(
            store.begin_schema_migration(scope, 3, [3; 32]),
            Err(StoreError::MigrationTargetMismatch)
        ),
        "a second migration to another target must be refused"
    );
    let second = store.migration_page(scope, 2, [2; 32], 1)?;
    assert_eq!(
        second.after,
        Some(documents[0]),
        "the resumed cursor must point at the last applied document"
    );
    assert_eq!(
        second.documents[0].document, documents[1],
        "the resumed page must continue with the next document"
    );
    assert!(
        matches!(
            store.apply_migration_page(
                scope,
                2,
                [2; 32],
                None,
                &[MigrationIndexes {
                    document: second.documents[0].document,
                    expected_revision: second.documents[0].revision,
                    indexes: &[],
                }]
            ),
            Err(StoreError::StaleMigrationPage)
        ),
        "a stale page cursor must be refused"
    );

    let mut processed: usize = 1;
    loop {
        let page = store.migration_page(scope, 2, [2; 32], 1)?;
        if page.documents.is_empty() {
            break;
        }
        processed = processed.saturating_add(page.documents.len());
        apply_migration_documents(&store, scope, new_index, &page)?;
    }
    assert_eq!(
        processed,
        documents.len(),
        "every seeded document must be projected exactly once"
    );
    store.finish_schema_migration(scope, 2, [2; 32])?;
    store.bind_schema(scope, 2, [2; 32])?;
    assert!(
        matches!(
            store.bind_schema(scope, 1, [1; 32]),
            Err(StoreError::SchemaMigrationRequired)
        ),
        "the superseded schema must never bind again"
    );
    assert!(
        store
            .query_index(scope, old_index, &IndexValue::Text("old".into()), None, 10)?
            .is_empty(),
        "the old index must be dropped by the published migration"
    );
    assert_eq!(
        store
            .query_index(scope, new_index, &IndexValue::Text("new".into()), None, 10)?
            .len(),
        documents.len(),
        "the new index must hold every migrated document"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn migration_cursor_cannot_skip_an_unprojected_document() -> Result<(), Box<dyn Error>> {
    let path = test_path("migration-skip");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let documents = [
        DocumentId::from_uuid(decode_uuid(&1_u128.to_be_bytes())?),
        DocumentId::from_uuid(decode_uuid(&2_u128.to_be_bytes())?),
        DocumentId::from_uuid(decode_uuid(&3_u128.to_be_bytes())?),
    ];
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    seed_migration_documents(&store, scope, IndexId::new(), documents)?;
    store.begin_schema_migration(scope, 2, [2; 32])?;
    let all = store.migration_page(scope, 2, [2; 32], 3)?;
    assert!(
        matches!(
            store.apply_migration_page(
                scope,
                2,
                [2; 32],
                None,
                &[MigrationIndexes {
                    document: all.documents[2].document,
                    expected_revision: all.documents[2].revision,
                    indexes: &[],
                }]
            ),
            Err(StoreError::InvalidMigrationPage)
        ),
        "applying a page past an unprojected document must be refused"
    );
    assert_eq!(
        store.migration_page(scope, 2, [2; 32], 1)?.after,
        None,
        "a refused apply must leave the cursor unmoved"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn migration_pages_reject_corrupt_snapshots_and_batch_cardinality_edges()
-> Result<(), Box<dyn Error>> {
    let path = test_path("migration-corruption");
    let store = EmbeddedStore::open(&path, ReplicaId::new())?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    store.bind_schema(scope, 1, [1; 32])?;
    store.begin_schema_migration(scope, 2, [2; 32])?;
    {
        let transaction = store.database.begin_write()?;
        {
            let mut table = transaction.open_table(DATA)?;
            table.insert(key_snapshot(scope, document).as_slice(), b"".as_slice())?;
            table.insert(
                key_revision(scope, document).as_slice(),
                1_u64.to_be_bytes().as_slice(),
            )?;
        }
        transaction.commit()?;
    }
    assert!(
        matches!(
            store.migration_page(scope, 2, [2; 32], 1),
            Err(StoreError::CorruptInternalValue)
        ),
        "an empty corrupt snapshot must fail closed"
    );
    assert!(
        matches!(
            store.apply_migration_page(scope, 2, [2; 32], None, &[]),
            Err(StoreError::InvalidMigrationPage)
        ),
        "an empty apply must be refused"
    );
    let excessive = (0..=256_u16)
        .map(|value| {
            Ok::<MigrationIndexes, StoreError>(MigrationIndexes {
                document: DocumentId::from_uuid(decode_uuid(
                    &u128::from(value).saturating_add(1).to_be_bytes(),
                )?),
                expected_revision: DocumentRevision::new(1),
                indexes: &[],
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        matches!(
            store.apply_migration_page(scope, 2, [2; 32], None, &excessive),
            Err(StoreError::InvalidMigrationPage)
        ),
        "one row past the migration batch ceiling must be refused"
    );
    drop(store);
    fs::remove_file(path)?;
    Ok(())
}
