//! Schema binding and migration tests; split from the facade schema module.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use logical_db_crdt::{CrdtError, Document, Item, Object, Scalar};
use logical_db_model::{
    AccountId, DocumentId, IndexEntry, IndexId, IndexValue, OperationDigest, OperationId,
    ReplicaId, Scope, SpaceId,
};
use logical_db_store::{CommitOutcome, EmbeddedStore, LocalChange, LocalCommit, StoreError};

use super::{Database, DatabaseError, MigrationReport, Projection};
use crate::projection::ProjectionSemantics;
use crate::schema::projection_digest;
use crate::test_helpers::{kind_projection, path, read_kind};

/// Every failure a facade test reports, typed by the layer that broke.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TestError {
    /// The facade under test returned an error.
    #[error(transparent)]
    Database(#[from] DatabaseError),
    /// The embedded store below the facade returned an error.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A CRDT document fixture failed.
    #[error(transparent)]
    Crdt(#[from] CrdtError),
    /// A model value was refused.
    #[error(transparent)]
    Model(#[from] logical_db_model::ModelError),
    /// A fixture file could not be created, read or removed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A fixture length or index did not fit its integer width.
    #[error(transparent)]
    Width(#[from] std::num::TryFromIntError),
    /// A JSON fixture did not encode or decode.
    #[error(transparent)]
    Json(#[from] lgwks_std::json::Error),
    /// The facade answered, but not with what the test expected.
    #[error("{0}")]
    Expectation(&'static str),
}

impl From<&'static str> for TestError {
    /// Wraps a failed expectation.
    fn from(message: &'static str) -> Self {
        Self::Expectation(message)
    }
}

/// Opens one schema-case fixture with a fresh scope and index.
fn schema_case() -> (PathBuf, Scope, ReplicaId, DocumentId, IndexId) {
    (
        path(),
        Scope::new(AccountId::new(), SpaceId::new()),
        ReplicaId::new(),
        DocumentId::new(),
        IndexId::new(),
    )
}

/// Runs one failing migration and pins the in-progress gate behind it.
fn expect_failed_migration(path: &Path, scope: Scope, replica: ReplicaId, failing: Projection) {
    assert!(matches!(
        Database::migrate_schema(path, scope, replica, 2, vec![failing], 1),
        Err(DatabaseError::Crdt(CrdtError::InvalidProperty))
    ));
    assert!(matches!(
        Database::open_with_schema(path, scope, replica, 1, vec![]),
        Err(DatabaseError::Store(StoreError::MigrationInProgress))
    ));
}

#[test]
fn secondary_index_replaces_old_value_atomically_and_survives_restart() -> Result<(), TestError> {
    let (path, scope, replica, document, index) = schema_case();
    let projection = Projection::new(index, ProjectionSemantics::V1, |reader| {
        Ok(match reader.get(&Object::root(), "kind")? {
            Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
            _ => None,
        })
    });
    {
        let database =
            Database::open_with_projections(&path, scope, replica, vec![projection.clone()])?;
        database.edit(
            document,
            OperationId::new(),
            OperationDigest::new([10; 32]),
            |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
        )?;
        assert_eq!(
            database.query_index(index, &IndexValue::Text("note".into()), None, 10)?,
            vec![document]
        );
        database.edit(
            document,
            OperationId::new(),
            OperationDigest::new([11; 32]),
            |editor| editor.put(&Object::root(), "kind", Scalar::String("task".into())),
        )?;
        assert!(
            database
                .query_index(index, &IndexValue::Text("note".into()), None, 10)?
                .is_empty()
        );
    }
    assert!(matches!(
        Database::open(&path, scope, replica),
        Err(DatabaseError::Store(StoreError::SchemaMigrationRequired))
    ));
    let database = Database::open_with_projections(&path, scope, replica, vec![projection])?;
    assert_eq!(
        database.query_index(index, &IndexValue::Text("task".into()), None, 10)?,
        vec![document]
    );
    drop(database);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn changed_projection_semantics_requires_migration_and_rebuilds_rows() -> Result<(), TestError> {
    let (path, scope, replica, document, index) = schema_case();
    assert!(ProjectionSemantics::version(0).is_none());
    let changed_semantics = ProjectionSemantics::version(2).ok_or("expected Some")?;
    {
        let database = Database::open_with_schema(
            &path,
            scope,
            replica,
            1,
            vec![Projection::new(index, ProjectionSemantics::V1, |reader| {
                Ok(match reader.get(&Object::root(), "kind")? {
                    Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                    _ => None,
                })
            })],
        )?;
        database.edit(
            document,
            OperationId::new(),
            OperationDigest::new([1; 32]),
            |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
        )?;
        assert_eq!(
            database.query_index(index, &IndexValue::Text("note".into()), None, 10)?,
            vec![document]
        );
    }
    let changed = || {
        Projection::new(index, changed_semantics, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => {
                    Some(IndexValue::Text(value.to_uppercase()))
                }
                _ => None,
            })
        })
    };
    // Same index identity and schema version with different declared semantics:
    // the durable digest no longer matches and stale rows are refused.
    assert!(matches!(
        Database::open_with_schema(&path, scope, replica, 1, vec![changed()]),
        Err(DatabaseError::Store(StoreError::SchemaMigrationRequired))
    ));
    // One index identity cannot hold two semantics at once, so the
    // migration target stays unambiguous.
    assert!(matches!(
        Database::open_with_schema(
            &path,
            scope,
            replica,
            1,
            vec![
                Projection::new(index, ProjectionSemantics::V1, |_reader| Ok(None)),
                changed(),
            ],
        ),
        Err(DatabaseError::DuplicateProjection)
    ));
    let (database, report) =
        Database::migrate_schema(&path, scope, replica, 1, vec![changed()], 10)?;
    assert_eq!(report.documents, 1);
    assert!(
        database
            .query_index(index, &IndexValue::Text("note".into()), None, 10)?
            .is_empty()
    );
    assert_eq!(
        database.query_index(index, &IndexValue::Text("NOTE".into()), None, 10)?,
        vec![document]
    );
    drop(database);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn opaque_projection_digest_semantics_bind_and_migrate_like_versions() -> Result<(), TestError> {
    let (path, scope, replica, document, index) = schema_case();
    let first = ProjectionSemantics::digest([1; 32]);
    let second = ProjectionSemantics::digest([2; 32]);
    assert_ne!(first, second);
    {
        let database = Database::open_with_schema(
            &path,
            scope,
            replica,
            3,
            vec![Projection::new(index, first, |reader| {
                Ok(match reader.get(&Object::root(), "kind")? {
                    Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                    _ => None,
                })
            })],
        )?;
        database.edit(
            document,
            OperationId::new(),
            OperationDigest::new([3; 32]),
            |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
        )?;
    }
    // The same opaque digest reopens the schema that recorded it.
    drop(Database::open_with_schema(
        &path,
        scope,
        replica,
        3,
        vec![Projection::new(index, first, |_reader| Ok(None))],
    )?);
    // A different digest under the same index and schema version must
    // migrate rather than reuse the recorded rows.
    assert!(matches!(
        Database::open_with_schema(
            &path,
            scope,
            replica,
            3,
            vec![Projection::new(index, second, |_reader| Ok(None))],
        ),
        Err(DatabaseError::Store(StoreError::SchemaMigrationRequired))
    ));
    let (database, _) = Database::migrate_schema(
        &path,
        scope,
        replica,
        3,
        vec![Projection::new(index, second, |reader| {
            Ok(reader
                .get(&Object::root(), "kind")?
                .map(|_| IndexValue::Text("digest-v2".into())))
        })],
        10,
    )?;
    assert_eq!(
        database.query_index(index, &IndexValue::Text("digest-v2".into()), None, 10)?,
        vec![document]
    );
    drop(database);
    fs::remove_file(path)?;
    Ok(())
}

/// Literal durable schema digest a pre-semantics v1 build recorded for the
/// retained legacy fixture: version 1 over one index and no declared
/// projection semantics.
/// Historical binding recorded by the retired pre-semantics v1 build. The
/// v1 build derived this binding with SHA-2, which the estate no longer
/// provides; the bytes are retained as committed fixture data so the
/// migration test proves current code refuses a foreign binding rather
/// than accepting anything it did not compute itself.
const LEGACY_V1_SCHEMA_FIXTURE_DIGEST: [u8; 32] = [
    0x21, 0x78, 0xfe, 0x6e, 0x16, 0xe8, 0xc2, 0x04, 0x35, 0xc8, 0xb8, 0xef, 0xe9, 0xce, 0x2d, 0x6a,
    0x1f, 0xd4, 0xf2, 0x1b, 0x5c, 0x2c, 0xa6, 0x3a, 0x34, 0x21, 0xd1, 0xd4, 0x26, 0x0a, 0xe7, 0x75,
];

/// Fixed digest vector for a known versioned projection input. Pinned at
/// the 2026-09-16 estate hash swap, when the digest primitive moved from
/// the retired SHA-2 build to BLAKE3 via `lgwks_std::hash`.
const SCHEMA_SEMANTICS_VERSION_VECTOR: [u8; 32] = [
    78, 15, 162, 51, 125, 121, 109, 169, 86, 121, 45, 236, 160, 171, 169, 182, 6, 217, 79, 80, 23,
    10, 138, 220, 25, 13, 176, 92, 133, 10, 225, 213,
];

/// Fixed digest vector for a known opaque-digest projection input. Pinned
/// at the 2026-09-16 estate hash swap.
const SCHEMA_SEMANTICS_DIGEST_VECTOR: [u8; 32] = [
    141, 35, 230, 82, 229, 135, 28, 128, 175, 192, 239, 133, 29, 122, 168, 41, 214, 52, 100, 67,
    53, 7, 209, 61, 236, 7, 85, 162, 143, 213, 224, 18,
];

/// Writes a real on-disk database bound exactly as a v1 build left it: the
/// version-1 schema row carries [`LEGACY_V1_SCHEMA_FIXTURE_DIGEST`], and one
/// document's snapshot is stored beside a deliberately stale index row, as
/// if produced by older projection logic. Only the schema binding changed
/// across the upgrade boundary; the CRDT snapshot bytes do not.
fn write_legacy_v1_schema_fixture(
    path: &std::path::Path,
    scope: Scope,
    replica: ReplicaId,
    index: IndexId,
    document: DocumentId,
) -> Result<(), TestError> {
    let store = EmbeddedStore::open(path, replica)?;
    store.bind_schema(scope, 1, LEGACY_V1_SCHEMA_FIXTURE_DIGEST)?;
    let mut state = Document::new();
    let ((), changes) =
        state.edit(|editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())))?;
    assert_eq!(changes.len(), 1);
    let change = &changes[0];
    let indexes = [IndexEntry::new(index, IndexValue::Text("legacy-v1".into()))];
    let snapshot = state.save()?;
    let local_change = LocalChange::new(
        scope,
        document,
        OperationId::new(),
        OperationDigest::new([0x4c; 32]),
        change.digest,
        &change.bytes,
    )?;
    let outcome = store.commit_local(LocalCommit::new(
        local_change,
        None,
        &snapshot,
        &indexes,
        false,
    ))?;
    assert!(matches!(outcome, CommitOutcome::Applied { sequence: 1 }));
    drop(store);
    Ok(())
}

#[test]
fn legacy_v1_schema_fixture_is_refused_then_migrated_rebuilt_and_reopened() -> Result<(), TestError>
{
    let path = path();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let document = DocumentId::new();
    let index: IndexId = lgwks_std::json::from_str("\"00000000-0000-0000-0000-000000000001\"")?;
    write_legacy_v1_schema_fixture(&path, scope, replica, index, document)?;

    // The current v2 reader refuses the v1-bound rows, with or without the
    // matching projection set, until an explicit migration.
    for projections in [vec![kind_projection(index)], Vec::new()] {
        assert!(matches!(
            Database::open_with_schema(&path, scope, replica, 1, projections),
            Err(DatabaseError::Store(StoreError::SchemaMigrationRequired))
        ));
    }

    // Explicit migration reprojects the snapshot, discards the stale v1 row,
    // and swaps the binding.
    let (database, report) =
        Database::migrate_schema(&path, scope, replica, 1, vec![kind_projection(index)], 10)?;
    assert_eq!(
        report,
        MigrationReport {
            batches: 1,
            documents: 1
        }
    );
    assert!(
        database
            .query_index(index, &IndexValue::Text("legacy-v1".into()), None, 10)?
            .is_empty()
    );
    assert_eq!(
        database.query_index(index, &IndexValue::Text("note".into()), None, 10)?,
        vec![document]
    );
    drop(database);

    // The migrated v2 binding reopens cleanly and still reads the snapshot.
    let reopened =
        Database::open_with_schema(&path, scope, replica, 1, vec![kind_projection(index)])?;
    assert_eq!(
        reopened.query_index(index, &IndexValue::Text("note".into()), None, 10)?,
        vec![document]
    );
    assert_eq!(read_kind(&reopened, document).as_deref(), Some("note"));
    drop(reopened);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn projection_digest_is_invariant_to_declaration_order_and_reopens_durably() -> Result<(), TestError>
{
    let first: IndexId = lgwks_std::json::from_str("\"00000000-0000-0000-0000-000000000011\"")?;
    let second: IndexId = lgwks_std::json::from_str("\"00000000-0000-0000-0000-000000000022\"")?;
    let forward = vec![
        Projection::new(
            first,
            ProjectionSemantics::version(2).ok_or("expected Some")?,
            |_reader| Ok(None),
        ),
        Projection::new(second, ProjectionSemantics::digest([0x77; 32]), |_reader| {
            Ok(None)
        }),
    ];
    let reverse = vec![forward[1].clone(), forward[0].clone()];
    assert_eq!(
        projection_digest(4, &forward)?,
        projection_digest(4, &reverse)?
    );

    // The equality is durable, not only a hash: rows written under one
    // declaration order reopen under the other without a migration.
    let path = path();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let document = DocumentId::new();
    {
        let database = Database::open_with_schema(&path, scope, replica, 4, forward)?;
        database.edit(
            document,
            OperationId::new(),
            OperationDigest::new([0x4f; 32]),
            |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
        )?;
    }
    let database = Database::open_with_schema(&path, scope, replica, 4, reverse)?;
    assert_eq!(read_kind(&database, document).as_deref(), Some("note"));
    drop(database);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn projection_digest_separates_semantics_versions_from_opaque_digests() -> Result<(), TestError> {
    let index: IndexId = lgwks_std::json::from_str("\"00010203-0405-0607-0809-0a0b0c0d0e0f\"")?;
    let versioned = vec![Projection::new(
        index,
        ProjectionSemantics::version(3).ok_or("expected Some")?,
        |_reader| Ok(None),
    )];
    let mut opaque_bytes = [0_u8; 32];
    for (slot, slot_byte) in opaque_bytes.iter_mut().enumerate() {
        if let Ok(byte) = u8::try_from(slot) {
            *slot_byte = byte;
        }
    }
    let opaque = vec![Projection::new(
        index,
        ProjectionSemantics::digest(opaque_bytes),
        |_reader| Ok(None),
    )];

    // Fixed digests for a known input, so framing changes cannot silently
    // invalidate existing stores without breaking these literals.
    assert_eq!(
        projection_digest(7, &versioned)?,
        SCHEMA_SEMANTICS_VERSION_VECTOR
    );
    assert_eq!(
        projection_digest(7, &opaque)?,
        SCHEMA_SEMANTICS_DIGEST_VECTOR
    );

    // A version change and an opaque-digest change are never conflated:
    // they hash to different digests under the same index and version.
    assert_ne!(
        projection_digest(7, &versioned)?,
        projection_digest(7, &opaque)?
    );
    // A schema-version bump differs from either semantics change.
    assert_ne!(
        projection_digest(8, &versioned)?,
        projection_digest(7, &versioned)?
    );
    assert_ne!(
        projection_digest(8, &versioned)?,
        projection_digest(7, &opaque)?
    );
    // Tag separation: `Version(1)` cannot alias a `Digest` carrying the same
    // numeric bytes, so a version is never read back as a digest.
    let aliasing_digest = {
        let mut bytes = [0_u8; 32];
        bytes[..4].copy_from_slice(&1_u32.to_be_bytes());
        ProjectionSemantics::digest(bytes)
    };
    assert_ne!(
        projection_digest(
            7,
            &[Projection::new(
                index,
                ProjectionSemantics::version(1).ok_or("expected Some")?,
                |_reader| Ok(None)
            )],
        )?,
        projection_digest(
            7,
            &[Projection::new(index, aliasing_digest, |_reader| Ok(None))]
        )?
    );
    Ok(())
}

#[test]
fn facade_schema_migration_resumes_after_projection_failure_without_mixed_indexes()
-> Result<(), TestError> {
    let path = path();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let index = IndexId::new();
    let documents = [DocumentId::new(), DocumentId::new()];
    {
        let database = Database::open_with_schema(&path, scope, replica, 1, vec![])?;
        for (offset, document) in documents.into_iter().enumerate() {
            database.edit(
                document,
                OperationId::new(),
                OperationDigest::new([u8::try_from(offset + 1)?; 32]),
                |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
            )?;
        }
    }
    assert!(matches!(
        Database::migrate_schema(&path, scope, replica, 2, vec![], 0),
        Err(DatabaseError::InvalidMigrationBatchSize(0))
    ));
    drop(Database::open_with_schema(
        &path,
        scope,
        replica,
        1,
        vec![],
    )?);

    let fail_once = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let failing = Projection::new(index, ProjectionSemantics::V1, {
        let fail_once = Arc::clone(&fail_once);
        move |reader| {
            if fail_once.swap(false, std::sync::atomic::Ordering::SeqCst) {
                return Err(CrdtError::InvalidProperty);
            }
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        }
    });
    expect_failed_migration(&path, scope, replica, failing);

    let projection = Projection::new(index, ProjectionSemantics::V1, |reader| {
        Ok(match reader.get(&Object::root(), "kind")? {
            Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
            _ => None,
        })
    });
    let (database, report) =
        Database::migrate_schema(&path, scope, replica, 2, vec![projection], 1)?;
    assert_eq!(
        report,
        MigrationReport {
            batches: 2,
            documents: 2
        }
    );
    let hits = database.query_index(index, &IndexValue::Text("note".into()), None, 10)?;
    assert_eq!(hits.len(), documents.len());
    assert!(documents.iter().all(|document| hits.contains(document)));
    drop(database);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn facade_schema_migration_resumes_to_different_target_semantics_after_failure()
-> Result<(), TestError> {
    let path = path();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let index = IndexId::new();
    let documents = [DocumentId::new(), DocumentId::new(), DocumentId::new()];
    let target_semantics = ProjectionSemantics::version(2).ok_or("expected Some")?;
    {
        let database = Database::open_with_schema(
            &path,
            scope,
            replica,
            1,
            vec![Projection::new(index, ProjectionSemantics::V1, |reader| {
                Ok(match reader.get(&Object::root(), "kind")? {
                    Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                    _ => None,
                })
            })],
        )?;
        for (offset, document) in documents.into_iter().enumerate() {
            database.edit(
                document,
                OperationId::new(),
                OperationDigest::new([u8::try_from(offset + 1)?; 32]),
                |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
            )?;
        }
    }

    // Interrupt after two pages commit: the durable cursor advances while
    // the last document still carries source rows, then the target
    // projection fails once. The target semantics are bound throughout so
    // the exact-target retry can resume.
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failing = Projection::new(index, target_semantics, {
        let calls = Arc::clone(&calls);
        move |reader| {
            if calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 2 {
                return Err(CrdtError::InvalidProperty);
            }
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => {
                    Some(IndexValue::Text(value.to_uppercase()))
                }
                _ => None,
            })
        }
    });
    expect_failed_migration(&path, scope, replica, failing);

    let projection = Projection::new(index, target_semantics, |reader| {
        Ok(match reader.get(&Object::root(), "kind")? {
            Some(Item::Scalar(Scalar::String(value))) => {
                Some(IndexValue::Text(value.to_uppercase()))
            }
            _ => None,
        })
    });
    let (database, report) =
        Database::migrate_schema(&path, scope, replica, 2, vec![projection], 1)?;
    assert_eq!(
        report,
        MigrationReport {
            batches: 1,
            documents: 1
        }
    );
    // No mixed indexes survive: every source row was replaced by the bound
    // target semantics.
    assert!(
        database
            .query_index(index, &IndexValue::Text("note".into()), None, 10)?
            .is_empty()
    );
    let hits = database.query_index(index, &IndexValue::Text("NOTE".into()), None, 10)?;
    assert_eq!(hits.len(), documents.len());
    assert!(documents.iter().all(|document| hits.contains(document)));
    drop(database);
    fs::remove_file(path)?;
    Ok(())
}

#[test]
fn schema_migration_carries_tombstones_with_empty_indexes() -> Result<(), TestError> {
    let db_path = path();
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let document = DocumentId::new();
    let index = IndexId::new();
    let projection = Projection::new(index, ProjectionSemantics::V1, |reader| {
        Ok(match reader.get(&Object::root(), "kind")? {
            Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
            _ => None,
        })
    });
    {
        let database = Database::open_with_schema(&db_path, scope, replica, 1, vec![projection])?;
        database.edit(
            document,
            OperationId::new(),
            OperationDigest::new([1; 32]),
            |editor| editor.put(&Object::root(), "kind", Scalar::String("note".into())),
        )?;
        database.delete(document, OperationId::new(), OperationDigest::new([2; 32]))?;
    }
    let (database, report) = Database::migrate_schema(&db_path, scope, replica, 2, vec![], 10)?;
    assert_eq!(report.documents, 1);
    assert!(database.is_deleted(document)?);
    assert!(
        database
            .read(document, |reader| reader.get(&Object::root(), "kind"))?
            .is_none()
    );
    drop(database);
    fs::remove_file(db_path)?;
    Ok(())
}
