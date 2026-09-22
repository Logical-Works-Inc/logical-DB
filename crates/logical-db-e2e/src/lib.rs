//! Black-box process-boundary journeys for the complete client/service path.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

pub mod support;

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use logical_db::{
        AccountId, BatchEdit, ChangeEvent, Database, DatabaseError, DatabaseErrorKind, DocumentId,
        IndexId, IndexValue, Item, Object, OperationDigest, OperationId, PeerId, Projection,
        ProjectionSemantics, RemoteChange, ReplicaId, Scalar, Scope, SpaceId, SyncLimits,
    };
    use logical_db_http::{HttpTransport, StaticBearer};
    use logical_db_model::{Effect, MAX_BASELINE_PAGE_DOCUMENTS, SnapshotArtifact};
    use logical_db_server::router;
    use logical_db_service::{
        CheckpointCodec, ConsumedResult, PruneReceipt, PruneResult, RetentionStore, SealResult,
        ServiceStore,
    };

    pub use crate::support::{FixedClock, Grant, MemoryServiceStore, TestAuthentication};

    use logical_db_protocol::{mark_consumed_action, prune_action, seal_snapshot_action};

    fn path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "logical-db-network-{label}-{}.redb",
            uuid::Uuid::now_v7()
        ))
    }

    /// Builds the two-thread runtime every async journey runs on.
    fn test_runtime() -> Result<lgwks_bot::Runtime, Box<dyn std::error::Error>> {
        Ok(lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()?)
    }

    fn projection(index: IndexId) -> Projection {
        Projection::new(index, ProjectionSemantics::V1, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        })
    }

    fn assert_converged(
        database: &Database,
        document: DocumentId,
        index: IndexId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let values = database.read(document, |reader| {
            Ok((
                reader.get(&Object::root(), "left")?,
                reader.get(&Object::root(), "right")?,
            ))
        })?;
        assert_eq!(
            values,
            Some((
                Some(Item::Scalar(Scalar::Integer(11))),
                Some(Item::Scalar(Scalar::Integer(22))),
            ))
        );
        assert_eq!(
            database.query_index(index, &IndexValue::Text("record".into()), None, 10)?,
            vec![document]
        );
        let events = database.changes_since(0, 10)?;
        assert_eq!(events.len(), 3);
        assert!(
            events
                .iter()
                .all(|event| event.document() == Some(document))
        );
        assert!(
            database
                .changes_since(
                    events
                        .last()
                        .ok_or("the fixture must record at least one event")?
                        .sequence(),
                    10
                )?
                .is_empty()
        );
        Ok(())
    }

    #[test]
    fn two_embedded_replicas_converge_through_real_http_and_keep_indexes_current()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(two_embedded_replicas_body())
    }

    async fn two_embedded_replicas_body() -> Result<(), Box<dyn std::error::Error>> {
        let app = router(
            MemoryServiceStore::default(),
            TestAuthentication,
            CheckpointCodec::new([21; 32], 60_000)?,
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, app).await;
            drop(serve_result);
        });
        let endpoint = format!("http://{address}/").parse()?;
        let mut transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;

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
            assert_converged(database, document, index)?;
        }

        drop((first, second));
        fs::remove_file(first_path)?;
        fs::remove_file(second_path)?;
        server.abort();
        Ok(())
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_default_path_lock_unicode_restart_and_read_only_failure_are_real()
    -> Result<(), Box<dyn std::error::Error>> {
        let application = format!("com.logicaldb.windows.{}", uuid::Uuid::now_v7());
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

    async fn seal_consume_and_prune(
        store: &MemoryServiceStore,
        scope: Scope,
    ) -> Result<(), Box<dyn std::error::Error>> {
        seal_consume_and_prune_through(store, scope, 1).await
    }

    async fn seal_consume_and_prune_through(
        store: &MemoryServiceStore,
        scope: Scope,
        through: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let artifact = store.build_snapshot(scope, through).await?;
        let frontier = store.frontier(scope).await?;
        let seal_request = OperationDigest::new([0x52; 32]);
        assert!(matches!(
            store
                .seal_snapshot(
                    scope,
                    &artifact,
                    seal_request,
                    &Grant {
                        scope,
                        effect: Effect::Sync,
                        action: seal_snapshot_action(scope, seal_request, &artifact.baseline),
                        frontier,
                    },
                )
                .await
                ?,
            SealResult::Sealed { position } if position == through
        ));
        let consumed_request = OperationDigest::new([0x53; 32]);
        assert!(matches!(
            store
                .mark_consumed(
                    scope,
                    through,
                    consumed_request,
                    &Grant {
                        scope,
                        effect: Effect::Sync,
                        action: mark_consumed_action(scope, consumed_request, through),
                        frontier,
                    },
                )
                .await
                ?,
            ConsumedResult::Advanced { consumed_through } if consumed_through == through
        ));
        let prune_request = OperationDigest::new([0x54; 32]);
        assert!(matches!(
            store
                .prune(
                    scope,
                    through,
                    None,
                    prune_request,
                    &Grant {
                        scope,
                        effect: Effect::Sync,
                        action: prune_action(scope, prune_request, through, None),
                        frontier,
                    },
                )
                .await
                ?,
            PruneResult::Pruned(PruneReceipt { pruned_through, floor, .. })
                if pruned_through == through && floor == through
        ));
        Ok(())
    }

    #[test]
    fn fresh_replica_recovers_a_pruned_document_through_real_http()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(fresh_replica_recovers_a_pruned_document_body())
    }

    async fn fresh_replica_recovers_a_pruned_document_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([23; 32], 60_000)?,
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, app).await;
            drop(serve_result);
        });
        let endpoint = format!("http://{address}/").parse()?;
        let mut transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let first_path = path("recovery-first");
        let second_path = path("recovery-second");

        let first = Database::open(&first_path, scope, ReplicaId::new())?;
        first.edit(
            document,
            OperationId::new(),
            OperationDigest::new([0x51; 32]),
            |editor| editor.put(&Object::root(), "state", Scalar::String("sealed".into())),
        )?;
        first
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;
        assert_eq!(store.frontier(scope).await?, 1);

        seal_consume_and_prune(&store, scope).await?;

        let second = Database::open(&second_path, scope, ReplicaId::new())?;
        let report = second
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;
        assert!(report.recovered);
        assert_eq!(
            second.read(document, |reader| reader.get(&Object::root(), "state"))?,
            Some(Some(Item::Scalar(Scalar::String("sealed".into()))))
        );

        drop((first, second));
        fs::remove_file(first_path)?;
        fs::remove_file(second_path)?;
        server.abort();
        Ok(())
    }

    #[test]
    fn a_baseline_larger_than_one_page_recovers_through_real_http()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(a_baseline_larger_than_one_page_recovers_body())
    }

    async fn a_baseline_larger_than_one_page_recovers_body()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([25; 32], 60_000)?,
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, app).await;
            drop(serve_result);
        });
        let endpoint = format!("http://{address}/").parse()?;
        let mut transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let first_path = path("paged-recovery-first");
        let second_path = path("paged-recovery-second");
        let total = MAX_BASELINE_PAGE_DOCUMENTS + 44;
        let documents = (0..total).map(|_| DocumentId::new()).collect::<Vec<_>>();
        // The first document of page two is deleted on the seeding replica, so
        // its tombstone crosses the page boundary inside the sealed baseline.
        let tombstoned = documents[MAX_BASELINE_PAGE_DOCUMENTS];

        let first = Database::open(&first_path, scope, ReplicaId::new())?;
        for (batch, chunk) in documents.chunks(150).enumerate() {
            let edits = chunk
                .iter()
                .enumerate()
                .map(|(offset, document)| {
                    let index = batch.saturating_mul(150).saturating_add(offset);
                    let value = i64::try_from(index.checked_rem(1_000).unwrap_or(0)).unwrap_or(0);
                    BatchEdit::new(
                        *document,
                        OperationId::new(),
                        OperationDigest::new(
                            [u8::try_from(index.checked_rem(256).unwrap_or(0)).unwrap_or(0); 32],
                        ),
                        move |editor| editor.put(&Object::root(), "page", Scalar::Integer(value)),
                    )
                })
                .collect::<Vec<_>>();
            first.edit_many(edits)?;
        }
        first.delete(
            tombstoned,
            OperationId::new(),
            OperationDigest::new([0x7a; 32]),
        )?;
        let seeded = u64::try_from(total + 1)?;
        for _round in 0..8 {
            first
                .synchronize(peer, &mut transport, SyncLimits::default(), || false)
                .await?;
            if store.frontier(scope).await? == seeded {
                break;
            }
        }
        assert_eq!(store.frontier(scope).await?, seeded);

        seal_consume_and_prune_through(&store, scope, seeded).await?;

        let second = Database::open(&second_path, scope, ReplicaId::new())?;
        let report = second
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;
        assert!(report.recovered, "a >256-document baseline must recover");
        assert!(
            second.is_deleted(tombstoned)?,
            "a tombstone crossing the page boundary must survive recovery"
        );
        for (index, document) in documents.iter().enumerate() {
            let observed = second.read(*document, |reader| reader.get(&Object::root(), "page"))?;
            if *document == tombstoned {
                assert_eq!(observed, None, "the boundary tombstone must not resurrect");
            } else {
                let expected = i64::try_from(index.checked_rem(1_000).unwrap_or(0))?;
                assert_eq!(
                    observed,
                    Some(Some(Item::Scalar(Scalar::Integer(expected)))),
                    "document {index} must carry its seeded value after paged recovery"
                );
            }
        }

        drop((first, second));
        fs::remove_file(first_path)?;
        fs::remove_file(second_path)?;
        server.abort();
        Ok(())
    }

    #[test]
    fn a_local_edit_landing_between_recovery_pages_survives_the_install()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(local_edit_between_pages_body())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the local-edit-between-pages journey is one linear interleaving narrative; splitting it would hide the page-boundary interleaving that the test exists to prove"
    )]
    async fn local_edit_between_pages_body() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([27; 32], 60_000)?,
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, app).await;
            drop(serve_result);
        });
        let endpoint = format!("http://{address}/").parse()?;
        let mut transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let first_path = path("interleave-first");
        let second_path = path("interleave-second");
        let total = MAX_BASELINE_PAGE_DOCUMENTS + 1;
        let documents = (0..total).map(|_| DocumentId::new()).collect::<Vec<_>>();

        let first = Database::open(&first_path, scope, ReplicaId::new())?;
        for (batch, chunk) in documents.chunks(150).enumerate() {
            let edits = chunk
                .iter()
                .enumerate()
                .map(|(offset, document)| {
                    let index = batch.saturating_mul(150).saturating_add(offset);
                    BatchEdit::new(
                        *document,
                        OperationId::new(),
                        OperationDigest::new(
                            [u8::try_from(index.checked_rem(256).unwrap_or(0)).unwrap_or(0); 32],
                        ),
                        move |editor| {
                            editor.put(
                                &Object::root(),
                                "page",
                                Scalar::Integer(i64::try_from(index).unwrap_or(0)),
                            )
                        },
                    )
                })
                .collect::<Vec<_>>();
            first.edit_many(edits)?;
        }
        let seeded = u64::try_from(total)?;
        for _round in 0..8 {
            first
                .synchronize(peer, &mut transport, SyncLimits::default(), || false)
                .await?;
            if store.frontier(scope).await? == seeded {
                break;
            }
        }
        assert_eq!(store.frontier(scope).await?, seeded);
        seal_consume_and_prune_through(&store, scope, seeded).await?;

        let second = Database::open(&second_path, scope, ReplicaId::new())?;
        let local_document = DocumentId::new();
        let local_operation = OperationId::new();
        let mut calls = 0usize;
        let mut landed = false;
        let mut seam_error: Option<Box<dyn std::error::Error>> = None;
        let report = second
            .synchronize(peer, &mut transport, SyncLimits::default(), || {
                calls = calls.saturating_add(1);
                if calls == 2 {
                    match second.read(documents[0], |reader| reader.get(&Object::root(), "page")) {
                        Ok(Some(_)) => {
                            seam_error = Some(
                                "the edit must land after the first page and before any install"
                                    .into(),
                            );
                        }
                        Ok(None) => {
                            match second.edit(
                                local_document,
                                local_operation,
                                OperationDigest::new([0xb7; 32]),
                                |editor| editor.put(&Object::root(), "local", Scalar::Integer(7)),
                            ) {
                                Ok(_) => {
                                    landed = true;
                                }
                                Err(error) => {
                                    seam_error = Some(error.into());
                                }
                            }
                        }
                        Err(error) => {
                            seam_error = Some(error.into());
                        }
                    }
                }
                false
            })
            .await?;
        if let Some(error) = seam_error {
            return Err(error);
        }

        assert!(
            landed,
            "the recovery loop must reach its between-pages seam, saw {calls} calls"
        );
        assert!(report.recovered, "the replica must still take the baseline");
        assert_eq!(
            second.read(local_document, |reader| reader
                .get(&Object::root(), "local"))?,
            Some(Some(Item::Scalar(Scalar::Integer(7)))),
            "an edit that lands mid-recovery must survive the install"
        );
        for (index, document) in documents.iter().enumerate() {
            assert_eq!(
                second.read(*document, |reader| reader.get(&Object::root(), "page"))?,
                Some(Some(Item::Scalar(Scalar::Integer(i64::try_from(index)?)))),
                "baseline document {index} must still install around a concurrent edit"
            );
        }
        let pending = second.pending_changes(0, 8)?;
        assert!(
            pending
                .iter()
                .any(|change| change.operation == local_operation),
            "the concurrent edit must remain in the outbox for upload"
        );

        drop((first, second));
        fs::remove_file(first_path)?;
        fs::remove_file(second_path)?;
        server.abort();
        Ok(())
    }

    #[test]
    fn tombstone_survives_baseline_recovery_and_cannot_resurrect()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(tombstone_baseline_recovery_body())
    }

    async fn tombstone_baseline_recovery_body() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([24; 32], 60_000)?,
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, app).await;
            drop(serve_result);
        });
        let endpoint = format!("http://{address}/").parse()?;
        let mut transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let first_path = path("tombstone-first");
        let second_path = path("tombstone-second");

        let first = Database::open(&first_path, scope, ReplicaId::new())?;
        first.edit(
            document,
            OperationId::new(),
            OperationDigest::new([0x61; 32]),
            |editor| editor.put(&Object::root(), "state", Scalar::String("v1".into())),
        )?;
        // The edit below the delete is the resurrection candidate: once the
        // recovered baseline carries the tombstone it must never bring the
        // document back, even if the service re-delivers that older change.
        let pre_floor = first.pending_changes(0, 10)?;
        assert_eq!(pre_floor.len(), 1);
        first.delete(
            document,
            OperationId::new(),
            OperationDigest::new([0x62; 32]),
        )?;
        first
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;
        assert_eq!(store.frontier(scope).await?, 2);

        seal_consume_and_prune_through(&store, scope, 2).await?;

        let second = Database::open(&second_path, scope, ReplicaId::new())?;
        let report = second
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;
        assert!(report.recovered);
        assert!(second.is_deleted(document)?);
        assert!(
            second
                .read(document, |reader| reader.get(&Object::root(), "state"))?
                .is_none()
        );

        let resurrection = pre_floor
            .iter()
            .map(|change| RemoteChange::new(change.document, change.digest, change.bytes.clone()))
            .collect::<Vec<_>>();
        second.install_remote_page(peer, &resurrection, b"resurrection-attempt")?;
        assert!(second.is_deleted(document)?);
        assert!(
            second
                .read(document, |reader| reader.get(&Object::root(), "state"))?
                .is_none()
        );

        drop((first, second));
        fs::remove_file(first_path)?;
        fs::remove_file(second_path)?;
        server.abort();
        Ok(())
    }

    #[test]
    fn corrupt_or_incomplete_baseline_leaves_state_and_checkpoint_unchanged()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(corrupt_baseline_refusal_body())
    }

    async fn corrupt_baseline_refusal_body() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([25; 32], 60_000)?,
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, app).await;
            drop(serve_result);
        });
        let endpoint = format!("http://{address}/").parse()?;
        let mut transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let victim_path = path("corrupt-victim");

        let victim = Database::open(&victim_path, scope, ReplicaId::new())?;
        victim.edit(
            document,
            OperationId::new(),
            OperationDigest::new([0x71; 32]),
            |editor| editor.put(&Object::root(), "state", Scalar::String("keep".into())),
        )?;
        victim
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;
        let checkpoint_before = victim
            .checkpoint(peer)?
            .ok_or("a synchronized replica must hold a checkpoint")?;
        assert!(!victim.is_deleted(document)?);
        assert_eq!(
            victim.read(document, |reader| reader.get(&Object::root(), "state"))?,
            Some(Some(Item::Scalar(Scalar::String("keep".into()))))
        );

        let genuine = store.build_snapshot(scope, 1).await?;
        assert_eq!(genuine.documents.len(), 1);

        let mut substituted = genuine.clone();
        substituted.documents[0].snapshot = vec![0xEE; substituted.documents[0].snapshot.len()];

        let mut truncated = genuine.clone();
        truncated.documents.pop();

        let mut mismatched = genuine.clone();
        mismatched.documents[0].tombstone = !mismatched.documents[0].tombstone;
        // Recomputing the descriptor keeps the artifact internally consistent,
        // so only the tombstone-versus-document check can refuse it.
        let mismatched =
            SnapshotArtifact::new(scope, genuine.baseline.position(), mismatched.documents)?;

        for (label, artifact) in [
            ("substituted snapshot payload", substituted),
            ("truncated document list", truncated),
            ("mismatched tombstone flag", mismatched),
        ] {
            let Err(error) = victim.install_baseline(peer, &artifact, b"corrupt-checkpoint") else {
                return Err("a corrupt checkpoint must be refused".into());
            };
            assert!(
                matches!(&error, DatabaseError::InvalidBaseline),
                "{label} must be refused with a typed failure, got {error:?}"
            );
            assert_eq!(error.kind(), DatabaseErrorKind::Other, "{label} kind");
            assert_eq!(
                victim.checkpoint(peer)?.as_deref(),
                Some(checkpoint_before.as_slice()),
                "{label} must leave the checkpoint byte-for-byte unchanged"
            );
            assert!(
                !victim.is_deleted(document)?,
                "{label} must not tombstone the existing document"
            );
            assert_eq!(
                victim.read(document, |reader| reader.get(&Object::root(), "state"))?,
                Some(Some(Item::Scalar(Scalar::String("keep".into())))),
                "{label} must leave the readable state unchanged"
            );
        }

        drop(victim);
        fs::remove_file(victim_path)?;
        server.abort();
        Ok(())
    }

    #[test]
    fn recovery_merges_a_local_edit_the_baseline_predates() -> Result<(), Box<dyn std::error::Error>>
    {
        test_runtime()?.block_on(recovery_merges_a_local_edit_body())
    }

    async fn recovery_merges_a_local_edit_body() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([26; 32], 60_000)?,
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, app).await;
            drop(serve_result);
        });
        let endpoint = format!("http://{address}/").parse()?;
        let mut transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let victim_path = path("merge-victim");
        let replica = ReplicaId::new();
        let victim = Database::open(&victim_path, scope, replica)?;

        victim.edit(
            document,
            OperationId::new(),
            OperationDigest::new([0x81; 32]),
            |editor| editor.put(&Object::root(), "state", Scalar::Integer(1)),
        )?;
        victim
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;

        let baseline = store.build_snapshot(scope, 1).await?;
        assert_eq!(baseline.documents.len(), 1);

        let local_operation = OperationId::new();
        victim.edit(
            document,
            local_operation,
            OperationDigest::new([0x82; 32]),
            |editor| editor.put(&Object::root(), "state", Scalar::Integer(2)),
        )?;
        assert_eq!(victim.pending_changes(0, 10)?.len(), 1);

        victim.install_baseline(peer, &baseline, b"merge-checkpoint")?;

        assert_eq!(
            victim.read(document, |reader| reader.get(&Object::root(), "state"))?,
            Some(Some(Item::Scalar(Scalar::Integer(2)))),
            "the baseline predates the local edit, so the merged value must keep it"
        );
        let pending = victim.pending_changes(0, 10)?;
        assert_eq!(
            pending.len(),
            1,
            "recovery must not consume the local outbox"
        );
        assert_eq!(pending[0].operation, local_operation);
        drop(victim);

        let reopened = Database::open(&victim_path, scope, replica)?;
        assert_eq!(
            reopened.read(document, |reader| reader.get(&Object::root(), "state"))?,
            Some(Some(Item::Scalar(Scalar::Integer(2)))),
            "the merged local edit must survive reopen"
        );
        drop(reopened);
        fs::remove_file(victim_path)?;
        server.abort();
        Ok(())
    }

    #[test]
    fn recovery_preserves_a_local_backlog_larger_than_one_upload_batch()
    -> Result<(), Box<dyn std::error::Error>> {
        test_runtime()?.block_on(recovery_preserves_a_large_backlog_body())
    }

    // One linear fill/drain narrative: splitting it would hide the backlog depth
    // that the test exists to prove. Kept whole rather than suppressed, because
    // `clippy::too_many_lines` no longer fires here and an unfulfilled `expect`
    // is itself a defect.
    async fn recovery_preserves_a_large_backlog_body() -> Result<(), Box<dyn std::error::Error>> {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([26; 32], 60_000)?,
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = lgwks_bot::rt::task::spawn(async move {
            let serve_result = axum::serve(listener, app).await;
            drop(serve_result);
        });
        let endpoint = format!("http://{address}/").parse()?;
        let mut transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let replica = ReplicaId::new();
        let backlog_path = path("backlog-victim");
        let victim = Database::open(&backlog_path, scope, replica)?;

        let seeded = DocumentId::new();
        victim.edit(
            seeded,
            OperationId::new(),
            OperationDigest::new([0x81; 32]),
            |editor| editor.put(&Object::root(), "state", Scalar::Integer(1)),
        )?;
        victim
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await?;
        let baseline = store.build_snapshot(scope, 1).await?;

        let batch = usize::from(SyncLimits::default().changes_per_page);
        let backlog = batch + 2;
        let mut documents = Vec::with_capacity(backlog);
        let mut operations = Vec::with_capacity(backlog);
        for index in 0..backlog {
            let document = DocumentId::new();
            let operation = OperationId::new();
            let mut digest = [0u8; 32];
            digest[0] = 0x90;
            digest[1] = u8::try_from(index & 0xff)?;
            digest[2] = u8::try_from((index >> 8) & 0xff)?;
            let sequence_value = i64::try_from(index).unwrap_or(i64::MAX).saturating_add(10);
            victim.edit(
                document,
                operation,
                OperationDigest::new(digest),
                |editor| editor.put(&Object::root(), "state", Scalar::Integer(sequence_value)),
            )?;
            documents.push(document);
            operations.push(operation);
        }

        let before = victim.pending_changes(0, backlog.saturating_add(8))?;
        assert!(
            before.len() > batch,
            "the fixture backlog must exceed one upload batch, saw {} of {batch}",
            before.len()
        );

        victim.install_baseline(peer, &baseline, b"backlog-checkpoint")?;

        let after = victim.pending_changes(0, backlog.saturating_add(8))?;
        assert_eq!(
            after.len(),
            backlog,
            "recovery must keep every unsent change in a multi-batch backlog"
        );
        let kept: Vec<_> = after.iter().map(|change| change.operation).collect();
        assert_eq!(kept, operations, "the backlog must keep its exact order");
        for (index, document) in documents.iter().enumerate() {
            assert_eq!(
                victim.read(*document, |reader| reader.get(&Object::root(), "state"))?,
                Some(Some(Item::Scalar(Scalar::Integer(
                    i64::try_from(index)?.saturating_add(10)
                )))),
                "local change {index} of a multi-batch backlog must survive recovery"
            );
        }
        assert_eq!(
            victim.read(seeded, |reader| reader.get(&Object::root(), "state"))?,
            Some(Some(Item::Scalar(Scalar::Integer(1)))),
            "the acknowledged seed change must not regress"
        );
        drop(victim);
        let reopened = Database::open(&backlog_path, scope, replica)?;
        assert_eq!(
            reopened
                .pending_changes(0, backlog.saturating_add(8))?
                .len(),
            backlog,
            "the preserved backlog must be durable, not in-memory only"
        );
        drop(reopened);
        fs::remove_file(backlog_path)?;
        server.abort();
        Ok(())
    }

    #[test]
    fn recovery_publishes_a_durable_invalidation_for_a_held_cursor()
    -> Result<(), Box<dyn std::error::Error>> {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()?
            .block_on(async {
                let store = Arc::new(MemoryServiceStore::default());
                let app = router(
                    Arc::clone(&store),
                    TestAuthentication,
                    CheckpointCodec::new([27; 32], 60_000)?,
                    FixedClock,
                );
                let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                let server = lgwks_bot::rt::task::spawn(async move {
                    let serve_result = axum::serve(listener, app).await;
                    drop(serve_result);
                });
                let endpoint = format!("http://{address}/").parse()?;
                let mut transport = HttpTransport::new(endpoint, StaticBearer::new("black-box")?)?;

                let scope = Scope::new(AccountId::new(), SpaceId::new());
                let document = DocumentId::new();
                let peer = PeerId::new();
                let victim_path = path("invalidate-victim");
                let replica = ReplicaId::new();
                let victim = Database::open(&victim_path, scope, replica)?;

                victim.edit(
                    document,
                    OperationId::new(),
                    OperationDigest::new([0x91; 32]),
                    |editor| editor.put(&Object::root(), "state", Scalar::Integer(1)),
                )?;
                victim
                    .synchronize(peer, &mut transport, SyncLimits::default(), || false)
                    .await?;

                // A subscriber holds a cursor immediately before recovery.
                let held = victim.changes_since(0, 256)?;
                let cursor = held
                    .last()
                    .ok_or("the held cursor must observe at least one change")?
                    .sequence();
                assert_eq!(victim.baseline_generation()?, 0);

                let baseline = store.build_snapshot(scope, 1).await?;
                victim.install_baseline(peer, &baseline, b"invalidate-checkpoint")?;

                // The install carries no change of its own, so nothing in the
                // pre-recovery stream reveals it; the committed invalidation
                // must still be exposed to the held cursor and be durable.
                assert_eq!(victim.baseline_generation()?, 1);
                let page = victim.changes_since(cursor, 256)?;
                assert_eq!(page.len(), 1, "the baseline install must be visible");
                assert!(matches!(
                    page[0],
                    ChangeEvent::Baseline { generation: 1, .. }
                ));
                assert!(page[0].digest().is_none());
                drop(victim);

                let reopened = Database::open(&victim_path, scope, replica)?;
                assert_eq!(reopened.baseline_generation()?, 1);
                let replayed = reopened.changes_since(cursor, 256)?;
                assert!(
                    matches!(
                        replayed.as_slice(),
                        [ChangeEvent::Baseline { generation: 1, .. }]
                    ),
                    "the invalidation must survive reopen"
                );
                // The consumer rebuilds its view from the recovered state.
                assert_eq!(
                    reopened.read(document, |reader| reader.get(&Object::root(), "state"))?,
                    Some(Some(Item::Scalar(Scalar::Integer(1))))
                );
                // A recovered replica's backup must round-trip through restore
                // even though its baseline marker carries no change of its own.
                let exported = reopened.export()?;
                drop(reopened);
                fs::remove_file(victim_path)?;

                let restored_path = path("invalidate-restored");
                let restored = Database::open(&restored_path, scope, ReplicaId::new())?;
                restored.restore(&exported)?;
                assert_eq!(
                    restored.read(document, |reader| reader.get(&Object::root(), "state"))?,
                    Some(Some(Item::Scalar(Scalar::Integer(1))))
                );
                drop(restored);
                fs::remove_file(restored_path)?;
                server.abort();

                Ok::<(), Box<dyn std::error::Error>>(())
            })
    }
}
