//! Convergence journeys over real HTTP against the in-memory service.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

use std::fs;

use logical_db::{
    AccountId, Database, DocumentId, IndexId, Object, OperationDigest, OperationId, PeerId,
    ReplicaId, Scalar, Scope, SpaceId, SyncLimits,
};

use super::harness::{
    JourneyError, assert_converged, path, projection, serve_test_router, test_runtime,
};
use crate::support::MemoryServiceStore;

#[test]
fn two_embedded_replicas_converge_through_real_http_and_keep_indexes_current()
-> Result<(), JourneyError> {
    test_runtime()?.block_on(two_embedded_replicas_body())
}

async fn two_embedded_replicas_body() -> Result<(), JourneyError> {
    use std::sync::Arc;

    let store = Arc::new(MemoryServiceStore::default());
    let (mut transport, mut server_tasks) = serve_test_router(Arc::clone(&store), [21; 32], 60_000)
        .await
        .map_err(JourneyError::Harness)?;

    let first_path = path("first");
    let second_path = path("second");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let document = DocumentId::new();
    let index = IndexId::new();
    let first = Database::open_with_schema(
        &first_path,
        scope,
        ReplicaId::new(),
        7,
        vec![projection(index)],
    )?;
    let second = Database::open_with_schema(
        &second_path,
        scope,
        ReplicaId::new(),
        7,
        vec![projection(index)],
    )?;
    let peer = PeerId::new();

    first.edit(
        document,
        OperationId::new(),
        OperationDigest::new([1; 32]),
        |editor| editor.put(&Object::root(), "kind", Scalar::String("record".into())),
    )?;
    first
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;

    first.edit(
        document,
        OperationId::new(),
        OperationDigest::new([2; 32]),
        |editor| editor.put(&Object::root(), "left", Scalar::Integer(11)),
    )?;
    second.edit(
        document,
        OperationId::new(),
        OperationDigest::new([3; 32]),
        |editor| editor.put(&Object::root(), "right", Scalar::Integer(22)),
    )?;

    first
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    second
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;
    first
        .synchronize(peer, &mut transport, SyncLimits::default(), || false)
        .await?;

    for database in [&first, &second] {
        assert_converged(database, document, index).map_err(JourneyError::Harness)?;
    }

    drop((first, second));
    fs::remove_file(first_path)?;
    fs::remove_file(second_path)?;
    server_tasks.abort_all();
    Ok(())
}

#[cfg(target_os = "windows")]
#[test]
fn windows_default_path_lock_unicode_restart_and_read_only_failure_are_real()
-> Result<(), JourneyError> {
    test_runtime()?
        .block_on(windows_default_path_lock_unicode_restart_and_read_only_failure_are_real_body())
}

#[cfg(target_os = "windows")]
async fn windows_default_path_lock_unicode_restart_and_read_only_failure_are_real_body()
-> Result<(), JourneyError> {
    let application = {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("com.logicaldb.windows-{}-{sequence}", std::process::id())
    };
    let local = std::env::var_os("LOCALAPPDATA").ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Windows runtime did not define LOCALAPPDATA",
        )
    })?;
    let directory = std::path::PathBuf::from(local)
        .join("LogicalDB")
        .join(&application);
    let path = directory.join("replica-v3.redb");
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let document = DocumentId::new();
    let database = Database::open_default(&application, scope, replica)?;
    database.edit(
        document,
        OperationId::new(),
        OperationDigest::new([0x77; 32]),
        |editor| {
            editor.put(
                &Object::root(),
                "windows",
                Scalar::String("Renée ∕ 状態".into()),
            )
        },
    )?;
    assert!(Database::open_default(&application, scope, replica).is_err());
    drop(database);
    let reopened = Database::open_default(&application, scope, replica)?;
    assert!(matches!(
        reopened
            .read(document, |reader| reader.get(&Object::root(), "windows"))
            ?,
        Some(Some(Item::Scalar(Scalar::String(value)))) if value == "Renée ∕ 状態"
    ));
    drop(reopened);

    let original_permissions = fs::metadata(&path)?.permissions();
    let mut permissions = original_permissions.clone();
    permissions.set_readonly(true);
    fs::set_permissions(&path, permissions)?;
    if let Ok(read_only) = Database::open(&path, scope, replica) {
        assert!(
            read_only
                .edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new([0x78; 32]),
                    |editor| editor.put(&Object::root(), "forbidden", Scalar::Boolean(true)),
                )
                .is_err()
        );
        drop(read_only);
    }
    fs::set_permissions(&path, original_permissions)?;
    fs::remove_file(path)?;
    fs::remove_dir(directory)?;
    Ok(())
}
