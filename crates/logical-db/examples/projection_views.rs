//! Hybrid text-plus-vector projection views over a materialized text index.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28. Source URLs are in
//! `evidence/SOURCES.md`.
//!
//! A Database with one text projection answers a bounded hybrid query over
//! its materialized rows, folds those rows into a durable snapshot file, and
//! answers identically from the file through positional reads.

use std::sync::atomic::{AtomicU64, Ordering};

use logical_db::{
    AccountId, Database, DocumentId, IndexId, IndexValue, Item, Object, OperationDigest,
    OperationId, Projection, ProjectionSemantics, ReplicaId, Scalar, Scope, SpaceId,
};

/// One unique temporary database path for this run.
fn db_path() -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let sequence = COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "logical-db-example-{}-{sequence}.redb",
        std::process::id()
    ))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let kind = IndexId::new();
    let projection = Projection::new(kind, ProjectionSemantics::V1, |reader| {
        Ok(match reader.get(&Object::root(), "kind")? {
            Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
            _ => None,
        })
    });
    let path = db_path();
    let database =
        Database::open_with_schema(path.clone(), scope, ReplicaId::new(), 1, vec![projection])?;

    for (document, kind) in [
        (DocumentId::new(), "alpha shared"),
        (DocumentId::new(), "beta shared"),
    ] {
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
    }

    let live = database.query_indexed_hybrid(kind, "shared", &[], 8)?;
    assert_eq!(live.len(), 2, "both tagged documents must rank");

    let snapshot = std::env::temp_dir().join(format!(
        "logical-db-example-{}-snap.lwmx",
        std::process::id()
    ));
    let report = database.build_indexed_snapshot(kind, &snapshot)?;
    assert_eq!(report.rows, 2, "both text rows fold into the snapshot");
    let frozen = Database::query_indexed_snapshot(&snapshot, "shared", &[], 8)?;
    assert_eq!(
        live.iter().map(|hit| hit.document).collect::<Vec<_>>(),
        frozen.iter().map(|hit| hit.document).collect::<Vec<_>>(),
        "snapshot order must match the live order"
    );
    std::fs::remove_file(&path)?;
    std::fs::remove_file(&snapshot)?;
    Ok(())
}
