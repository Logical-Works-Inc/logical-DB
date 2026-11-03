//! Projection-view journeys over the public [`Database`](logical_db::Database) API.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.
//!
//! The shared composition harness drives what the black-box journey tests
//! assert: hybrid queries over a materialized text index, durable LWMX
//! snapshot builds queried through positional reads, tenant isolation on
//! both paths, and the sealed envelope round-trip with cross-engine
//! restore-or-replay. Every projection view the facade publishes is
//! exercised here through the shipped public API, never through internals.

use std::sync::atomic::{AtomicU64, Ordering};

use logical_db::{
    AccountId, Database, DocumentId, FileProjectionIndex, IndexId, IndexValue, Item, Object,
    OperationDigest, OperationId, Projection, ProjectionAction, ProjectionError,
    ProjectionSemantics, RedbSnapshotStore, RelationalProjection, ReplicaId, RestorableProjection,
    RestoreStats, Scalar, Scope, SnapshotEnvelope, SnapshotInput, SnapshotMetadata, SnapshotStore,
    SpaceId, SqliteRelationalProjection, create_snapshot, query_snapshot, restore_or_replay,
    snapshot_row_count, verify_snapshot_integrity,
};

/// Builds one unique temporary database path for a named fixture.
#[must_use]
pub(crate) fn db_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("logical-db-views-{name}-{}.redb", unique_suffix()))
}

/// One process-unique suffix shared by every temp path in this run.
#[must_use]
pub(crate) fn unique_suffix() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let sequence = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}-{sequence}", std::process::id())
}

/// Opens a replica whose `kind` index projects text rows.
#[must_use = "an unopened replica answers nothing"]
pub(crate) fn open_kind_replica(
    path: &std::path::Path,
    scope: Scope,
) -> Result<(Database, IndexId), Box<dyn std::error::Error>> {
    let kind = IndexId::new();
    let projection = Projection::new(kind, ProjectionSemantics::V1, |reader| {
        Ok(match reader.get(&Object::root(), "kind")? {
            Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
            _ => None,
        })
    });
    let database = Database::open_with_schema(path, scope, ReplicaId::new(), 1, vec![projection])?;
    Ok((database, kind))
}

/// Commits one document carrying a `kind` tag.
pub(crate) fn put_kind(
    database: &Database,
    document: DocumentId,
    kind: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let tag = String::from(kind);
    database.edit(
        document,
        OperationId::new(),
        OperationDigest::new([7_u8; 32]),
        |editor| {
            editor.put(&Object::root(), "kind", Scalar::String(tag))?;
            Ok(())
        },
    )?;
    Ok(())
}

/// Deterministic test row identity for sequence `i`: fixed bytes the
/// restore path treats opaquely.
#[must_use]
pub(crate) fn test_row(i: u64) -> [u8; 32] {
    let mut row = [0_u8; 32];
    row[..8].copy_from_slice(&i.to_le_bytes());
    row[8..16].copy_from_slice(&i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_le_bytes());
    row[16..24].copy_from_slice(&i.wrapping_mul(0xBF58_476D_1CE4_E5B9).to_le_bytes());
    row
}

/// Witness that the restore path abstracts over either relational engine.
pub(crate) fn assert_restorable<P: RestorableProjection>() {}

/// Hybrid queries, durable snapshots, and tenant isolation agree end to end.
pub fn projection_views_agree_across_live_and_snapshot_paths()
-> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let path = db_path("views");
    let (database, kind) = open_kind_replica(&path, scope)?;

    let alpha = DocumentId::new();
    let beta = DocumentId::new();
    let gamma = DocumentId::new();
    put_kind(&database, alpha, "alpha shared")?;
    put_kind(&database, beta, "beta shared")?;
    put_kind(&database, gamma, "gamma unrelated")?;

    let live = database.query_indexed_hybrid(kind, "shared", &[], 8)?;
    assert_eq!(live.len(), 2, "two documents carry the shared term");
    let mut live_docs: Vec<DocumentId> = live.iter().map(|hit| hit.document).collect();
    live_docs.sort_by_key(|document| document.as_uuid());
    let mut expected = vec![alpha, beta];
    expected.sort_by_key(|document| document.as_uuid());
    assert_eq!(
        live_docs, expected,
        "the live query must rank both carriers"
    );
    for hit in &live {
        assert!(hit.score > 0, "every returned hit must carry a live score");
    }

    let snapshot_path = std::env::temp_dir().join(format!(
        "logical-db-views-snapshot-{}.lwmx",
        unique_suffix()
    ));
    let report = database.build_indexed_snapshot(kind, &snapshot_path)?;
    assert_eq!(report.rows, 3, "all three text rows fold into the snapshot");
    assert!(
        report.bytes > 96,
        "a three-row snapshot exceeds its own header"
    );

    let frozen = Database::query_indexed_snapshot(&snapshot_path, "shared", &[], 8)?;
    assert_eq!(
        live.iter().map(|hit| hit.document).collect::<Vec<_>>(),
        frozen.iter().map(|hit| hit.document).collect::<Vec<_>>(),
        "snapshot order must match the live order"
    );
    assert_eq!(
        live.iter().map(|hit| hit.score).collect::<Vec<_>>(),
        frozen.iter().map(|hit| hit.score).collect::<Vec<_>>(),
        "snapshot scores must match the live scores"
    );

    let absent = Database::query_indexed_snapshot(&snapshot_path, "missing", &[], 8)?;
    assert!(
        absent.is_empty(),
        "an unknown term answers nothing on the snapshot path"
    );

    let direct = FileProjectionIndex::open(&snapshot_path)?;
    direct.verify_integrity()?;
    assert_eq!(direct.len(), 3, "the file index reports three rows");
    assert!(!direct.is_empty(), "a three-row snapshot is not empty");
    assert_eq!(
        direct.byte_len(),
        report.bytes,
        "the file index length matches the build report"
    );
    let raw_bytes = std::fs::read(&snapshot_path)?;
    verify_snapshot_integrity(&raw_bytes)?;
    assert_eq!(
        snapshot_row_count(&raw_bytes)?,
        3,
        "the byte reader reports three rows"
    );
    let raw_matches = query_snapshot(&raw_bytes, "shared", &[], 8)?;
    assert_eq!(raw_matches.len(), 2, "the byte reader ranks both carriers");

    let stranger = Scope::new(AccountId::new(), SpaceId::new());
    let stranger_path = db_path("stranger");
    let (stranger_db, stranger_kind) = open_kind_replica(&stranger_path, stranger)?;
    let other = DocumentId::new();
    put_kind(&stranger_db, other, "shared")?;
    let stranger_hits = stranger_db.query_indexed_hybrid(stranger_kind, "shared", &[], 8)?;
    assert_eq!(
        stranger_hits.len(),
        1,
        "the second tenant sees only its own row"
    );
    assert_eq!(
        stranger_hits[0].document, other,
        "isolation must hold on the hybrid path"
    );
    let home_hits = database.query_indexed_hybrid(kind, "shared", &[], 8)?;
    assert_eq!(
        home_hits.len(),
        2,
        "the first tenant must not see the second tenant's row"
    );

    drop(database);
    drop(stranger_db);
    std::fs::remove_file(&path)?;
    std::fs::remove_file(&stranger_path)?;
    std::fs::remove_file(&snapshot_path)?;
    Ok(())
}

/// Sealed envelopes survive a backend round-trip and restore plus tail
/// replay converges across engines; any refusal falls back to genesis.
pub fn snapshot_envelope_round_trip_restores_across_engines()
-> Result<(), Box<dyn std::error::Error>> {
    assert_restorable::<RelationalProjection>();
    assert_restorable::<SqliteRelationalProjection>();

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let projection = IndexId::new();
    let store_path =
        std::env::temp_dir().join(format!("logical-db-snapstore-{}.redb", unique_suffix()));
    let store = RedbSnapshotStore::open(&store_path)?;

    let mut mem = RelationalProjection::new();
    for i in 0..100_u64 {
        mem.apply(ProjectionAction::Apply(test_row(i)))?;
    }
    let last_row = *mem.state().last().ok_or("rows should exist")?;
    let sealed = create_snapshot(
        &store,
        scope,
        SnapshotInput::new(projection, [9_u8; 32], &mem, last_row, 99, 1_700_000_000),
    )?;
    sealed.verify()?;
    assert_eq!(
        sealed.compute_hash()?,
        sealed.hash,
        "seal must embed the recomputed hash"
    );

    let tamper_scope = Scope::new(AccountId::new(), SpaceId::new());
    let mut tamper_mem = RelationalProjection::new();
    for i in 0..10_u64 {
        tamper_mem.apply(ProjectionAction::Apply(test_row(i)))?;
    }
    let tamper_last = *tamper_mem.state().last().ok_or("rows should exist")?;
    let direct = create_snapshot(
        &store,
        tamper_scope,
        SnapshotInput::new(
            projection,
            [4_u8; 32],
            &tamper_mem,
            tamper_last,
            9,
            1_700_000_001,
        ),
    )?;
    direct.verify()?;
    assert_ne!(
        direct.hash, sealed.hash,
        "a distinct scope and frontier seals a distinct envelope"
    );
    assert!(
        matches!(
            RelationalProjection::restore_from_state(&[1, 2, 3]),
            Err(ProjectionError::Malformed(_))
        ),
        "a misaligned blob must be refused"
    );

    let listed: Vec<SnapshotMetadata> = store.list_snapshots(scope, projection)?;
    assert_eq!(listed.len(), 1, "one snapshot listed");
    assert_eq!(listed[0].hash, sealed.hash, "listing names the sealed hash");
    assert_eq!(
        listed[0].account,
        scope.account(),
        "listing is tenant-scoped"
    );

    let mut tail = RelationalProjection::new();
    for i in 0..200_u64 {
        tail.apply(ProjectionAction::Apply(test_row(i)))?;
    }
    let full_checkpoint = tail.checkpoint();

    let sqlite_seed = SqliteRelationalProjection::new()?;
    let replay = |restored: &mut SqliteRelationalProjection, after: u64| -> Result<u64, String> {
        let mut applied = 0_u64;
        for i in (after.saturating_add(1))..200 {
            restored
                .apply(ProjectionAction::Apply(test_row(i)))
                .map_err(|error| error.to_string())?;
            applied = applied.saturating_add(1);
        }
        Ok(applied)
    };
    let (restored, stats): (SqliteRelationalProjection, RestoreStats) =
        restore_or_replay(&store, scope, projection, sqlite_seed, replay)?;
    assert!(stats.from_snapshot, "must take the snapshot path");
    assert_eq!(
        restored.checkpoint(),
        full_checkpoint,
        "sqlite restore plus tail must match full replay"
    );

    let missing: Option<SnapshotEnvelope> =
        store.load_latest_snapshot(Scope::new(AccountId::new(), SpaceId::new()), IndexId::new())?;
    assert!(missing.is_none(), "cold start answers none");

    let deleted = store.delete_snapshot(scope, projection, &sealed.hash)?;
    assert!(deleted, "delete must report the removal");
    let after: Option<SnapshotEnvelope> = store.load_latest_snapshot(scope, projection)?;
    assert!(after.is_none(), "the snapshot must be gone after delete");

    std::fs::remove_file(&store_path)?;
    Ok(())
}
