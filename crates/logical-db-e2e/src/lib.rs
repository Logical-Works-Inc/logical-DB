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

    pub use crate::support::{
        FixedClock, Grant, MemoryServiceStore, TestAuthentication,
    };

    use logical_db_protocol::{mark_consumed_action, prune_action, seal_snapshot_action};

    fn path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "logical-db-network-{label}-{}.redb",
            uuid::Uuid::now_v7()
        ))
    }

    fn projection(index: IndexId) -> Projection {
        Projection::new(index, ProjectionSemantics::V1, |reader| {
            Ok(match reader.get(&Object::root(), "kind")? {
                Some(Item::Scalar(Scalar::String(value))) => Some(IndexValue::Text(value)),
                _ => None,
            })
        })
    }

    fn assert_converged(database: &Database, document: DocumentId, index: IndexId) {
        let values = database
            .read(document, |reader| {
                Ok((
                    reader.get(&Object::root(), "left")?,
                    reader.get(&Object::root(), "right")?,
                ))
            })
            .unwrap();
        assert_eq!(
            values,
            Some((
                Some(Item::Scalar(Scalar::Integer(11))),
                Some(Item::Scalar(Scalar::Integer(22))),
            ))
        );
        assert_eq!(
            database
                .query_index(index, &IndexValue::Text("record".into()), None, 10)
                .unwrap(),
            vec![document]
        );
        let events = database.changes_since(0, 10).unwrap();
        assert_eq!(events.len(), 3);
        assert!(
            events
                .iter()
                .all(|event| event.document() == Some(document))
        );
        assert!(
            database
                .changes_since(events.last().unwrap().sequence(), 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn two_embedded_replicas_converge_through_real_http_and_keep_indexes_current() {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
            .block_on(two_embedded_replicas_body());
    }

    async fn two_embedded_replicas_body() {
        let app = router(
            MemoryServiceStore::default(),
            TestAuthentication,
            CheckpointCodec::new([21; 32], 60_000).unwrap(),
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = lgwks_bot::rt::task::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let endpoint = format!("http://{address}/").parse().unwrap();
        let mut transport =
            HttpTransport::new(endpoint, StaticBearer::new("black-box").unwrap()).unwrap();

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
        )
        .unwrap();
        let second = Database::open_with_schema(
            &second_path,
            scope,
            ReplicaId::new(),
            7,
            vec![projection(index)],
        )
        .unwrap();
        let peer = PeerId::new();

        first
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([1; 32]),
                |editor| editor.put(&Object::root(), "kind", Scalar::String("record".into())),
            )
            .unwrap();
        first
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        second
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();

        first
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([2; 32]),
                |editor| editor.put(&Object::root(), "left", Scalar::Integer(11)),
            )
            .unwrap();
        second
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([3; 32]),
                |editor| editor.put(&Object::root(), "right", Scalar::Integer(22)),
            )
            .unwrap();

        first
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        second
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        first
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();

        for database in [&first, &second] {
            assert_converged(database, document, index);
        }

        drop((first, second));
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
        server.abort();
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn windows_default_path_lock_unicode_restart_and_read_only_failure_are_real() {
        let application = format!("com.logicaldb.windows.{}", uuid::Uuid::now_v7());
        let local = std::env::var_os("LOCALAPPDATA").expect("Windows defines LOCALAPPDATA");
        let directory = std::path::PathBuf::from(local)
            .join("LogicalDB")
            .join(&application);
        let path = directory.join("replica-v3.redb");
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = ReplicaId::new();
        let document = DocumentId::new();
        let database = Database::open_default(&application, scope, replica).unwrap();
        database
            .edit(
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
            )
            .unwrap();
        assert!(Database::open_default(&application, scope, replica).is_err());
        drop(database);
        let reopened = Database::open_default(&application, scope, replica).unwrap();
        assert!(matches!(
            reopened
                .read(document, |reader| reader.get(&Object::root(), "windows"))
                .unwrap(),
            Some(Some(Item::Scalar(Scalar::String(value)))) if value == "Renée ∕ 状態"
        ));
        drop(reopened);

        let original_permissions = fs::metadata(&path).unwrap().permissions();
        let mut permissions = original_permissions.clone();
        permissions.set_readonly(true);
        fs::set_permissions(&path, permissions).unwrap();
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
        fs::set_permissions(&path, original_permissions).unwrap();
        fs::remove_file(path).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    async fn seal_consume_and_prune(store: &MemoryServiceStore, scope: Scope) {
        seal_consume_and_prune_through(store, scope, 1).await;
    }

    async fn seal_consume_and_prune_through(
        store: &MemoryServiceStore,
        scope: Scope,
        through: u64,
    ) {
        let artifact = store.build_snapshot(scope, through).await.unwrap();
        let frontier = store.frontier(scope).await.unwrap();
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
                .unwrap(),
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
                .unwrap(),
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
                .unwrap(),
            PruneResult::Pruned(PruneReceipt { pruned_through, floor })
                if pruned_through == through && floor == through
        ));
    }

    #[test]
    fn fresh_replica_recovers_a_pruned_document_through_real_http() {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
            .block_on(fresh_replica_recovers_a_pruned_document_body());
    }

    async fn fresh_replica_recovers_a_pruned_document_body() {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([23; 32], 60_000).unwrap(),
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = lgwks_bot::rt::task::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let endpoint = format!("http://{address}/").parse().unwrap();
        let mut transport =
            HttpTransport::new(endpoint, StaticBearer::new("black-box").unwrap()).unwrap();

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let first_path = path("recovery-first");
        let second_path = path("recovery-second");

        let first = Database::open(&first_path, scope, ReplicaId::new()).unwrap();
        first
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([0x51; 32]),
                |editor| editor.put(&Object::root(), "state", Scalar::String("sealed".into())),
            )
            .unwrap();
        first
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        assert_eq!(store.frontier(scope).await.unwrap(), 1);

        seal_consume_and_prune(&store, scope).await;

        let second = Database::open(&second_path, scope, ReplicaId::new()).unwrap();
        let report = second
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        assert!(report.recovered);
        assert_eq!(
            second
                .read(document, |reader| reader.get(&Object::root(), "state"))
                .unwrap(),
            Some(Some(Item::Scalar(Scalar::String("sealed".into()))))
        );

        drop((first, second));
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
        server.abort();
    }

    #[test]
    fn a_baseline_larger_than_one_page_recovers_through_real_http() {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
            .block_on(a_baseline_larger_than_one_page_recovers_body());
    }

    async fn a_baseline_larger_than_one_page_recovers_body() {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([25; 32], 60_000).unwrap(),
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = lgwks_bot::rt::task::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let endpoint = format!("http://{address}/").parse().unwrap();
        let mut transport =
            HttpTransport::new(endpoint, StaticBearer::new("black-box").unwrap()).unwrap();

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let first_path = path("paged-recovery-first");
        let second_path = path("paged-recovery-second");
        let total = MAX_BASELINE_PAGE_DOCUMENTS + 44;
        let documents = (0..total).map(|_| DocumentId::new()).collect::<Vec<_>>();
        // The first document of page two is deleted on the seeding replica, so
        // its tombstone crosses the page boundary inside the sealed baseline.
        let tombstoned = documents[MAX_BASELINE_PAGE_DOCUMENTS];

        let first = Database::open(&first_path, scope, ReplicaId::new()).unwrap();
        for (batch, chunk) in documents.chunks(150).enumerate() {
            let edits = chunk
                .iter()
                .enumerate()
                .map(|(offset, document)| {
                    let index = batch * 150 + offset;
                    let value = i64::try_from(index % 1_000).unwrap();
                    BatchEdit::new(
                        *document,
                        OperationId::new(),
                        OperationDigest::new([u8::try_from(index % 256).unwrap(); 32]),
                        move |editor| editor.put(&Object::root(), "page", Scalar::Integer(value)),
                    )
                })
                .collect::<Vec<_>>();
            first.edit_many(edits).unwrap();
        }
        first
            .delete(
                tombstoned,
                OperationId::new(),
                OperationDigest::new([0x7a; 32]),
            )
            .unwrap();
        let seeded = u64::try_from(total + 1).unwrap();
        for _round in 0..8 {
            first
                .synchronize(peer, &mut transport, SyncLimits::default(), || false)
                .await
                .unwrap();
            if store.frontier(scope).await.unwrap() == seeded {
                break;
            }
        }
        assert_eq!(store.frontier(scope).await.unwrap(), seeded);

        seal_consume_and_prune_through(&store, scope, seeded).await;

        let second = Database::open(&second_path, scope, ReplicaId::new()).unwrap();
        let report = second
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        assert!(report.recovered, "a >256-document baseline must recover");
        assert!(
            second.is_deleted(tombstoned).unwrap(),
            "a tombstone crossing the page boundary must survive recovery"
        );
        for (index, document) in documents.iter().enumerate() {
            let observed = second
                .read(*document, |reader| reader.get(&Object::root(), "page"))
                .unwrap();
            if *document == tombstoned {
                assert_eq!(observed, None, "the boundary tombstone must not resurrect");
            } else {
                let expected = i64::try_from(index % 1_000).unwrap();
                assert_eq!(
                    observed,
                    Some(Some(Item::Scalar(Scalar::Integer(expected)))),
                    "document {index} must carry its seeded value after paged recovery"
                );
            }
        }

        drop((first, second));
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
        server.abort();
    }

    #[test]
    fn a_local_edit_landing_between_recovery_pages_survives_the_install() {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
            .block_on(local_edit_between_pages_body());
    }

    #[allow(clippy::too_many_lines)]
    async fn local_edit_between_pages_body() {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([27; 32], 60_000).unwrap(),
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = lgwks_bot::rt::task::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let endpoint = format!("http://{address}/").parse().unwrap();
        let mut transport =
            HttpTransport::new(endpoint, StaticBearer::new("black-box").unwrap()).unwrap();

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let first_path = path("interleave-first");
        let second_path = path("interleave-second");
        let total = MAX_BASELINE_PAGE_DOCUMENTS + 1;
        let documents = (0..total).map(|_| DocumentId::new()).collect::<Vec<_>>();

        let first = Database::open(&first_path, scope, ReplicaId::new()).unwrap();
        for (batch, chunk) in documents.chunks(150).enumerate() {
            let edits = chunk
                .iter()
                .enumerate()
                .map(|(offset, document)| {
                    let index = batch * 150 + offset;
                    BatchEdit::new(
                        *document,
                        OperationId::new(),
                        OperationDigest::new([u8::try_from(index % 256).unwrap(); 32]),
                        move |editor| {
                            editor.put(
                                &Object::root(),
                                "page",
                                Scalar::Integer(i64::try_from(index).unwrap()),
                            )
                        },
                    )
                })
                .collect::<Vec<_>>();
            first.edit_many(edits).unwrap();
        }
        let seeded = u64::try_from(total).unwrap();
        for _round in 0..8 {
            first
                .synchronize(peer, &mut transport, SyncLimits::default(), || false)
                .await
                .unwrap();
            if store.frontier(scope).await.unwrap() == seeded {
                break;
            }
        }
        assert_eq!(store.frontier(scope).await.unwrap(), seeded);
        seal_consume_and_prune_through(&store, scope, seeded).await;

        let second = Database::open(&second_path, scope, ReplicaId::new()).unwrap();
        let local_document = DocumentId::new();
        let local_operation = OperationId::new();
        let mut calls = 0usize;
        let mut landed = false;
        let report = second
            .synchronize(peer, &mut transport, SyncLimits::default(), || {
                calls += 1;
                if calls == 2 {
                    assert!(
                        second
                            .read(documents[0], |reader| reader.get(&Object::root(), "page"))
                            .unwrap()
                            .is_none(),
                        "the edit must land after the first page and before any install"
                    );
                    second
                        .edit(
                            local_document,
                            local_operation,
                            OperationDigest::new([0xb7; 32]),
                            |editor| editor.put(&Object::root(), "local", Scalar::Integer(7)),
                        )
                        .unwrap();
                    landed = true;
                }
                false
            })
            .await
            .unwrap();

        assert!(
            landed,
            "the recovery loop must reach its between-pages seam, saw {calls} calls"
        );
        assert!(report.recovered, "the replica must still take the baseline");
        assert_eq!(
            second
                .read(local_document, |reader| reader
                    .get(&Object::root(), "local"))
                .unwrap(),
            Some(Some(Item::Scalar(Scalar::Integer(7)))),
            "an edit that lands mid-recovery must survive the install"
        );
        for (index, document) in documents.iter().enumerate() {
            assert_eq!(
                second
                    .read(*document, |reader| reader.get(&Object::root(), "page"))
                    .unwrap(),
                Some(Some(Item::Scalar(Scalar::Integer(
                    i64::try_from(index).unwrap()
                )))),
                "baseline document {index} must still install around a concurrent edit"
            );
        }
        let pending = second.pending_changes(0, 8).unwrap();
        assert!(
            pending
                .iter()
                .any(|change| change.operation == local_operation),
            "the concurrent edit must remain in the outbox for upload"
        );

        drop((first, second));
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
        server.abort();
    }

    #[test]
    fn tombstone_survives_baseline_recovery_and_cannot_resurrect() {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
            .block_on(tombstone_baseline_recovery_body());
    }

    async fn tombstone_baseline_recovery_body() {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([24; 32], 60_000).unwrap(),
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = lgwks_bot::rt::task::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let endpoint = format!("http://{address}/").parse().unwrap();
        let mut transport =
            HttpTransport::new(endpoint, StaticBearer::new("black-box").unwrap()).unwrap();

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let first_path = path("tombstone-first");
        let second_path = path("tombstone-second");

        let first = Database::open(&first_path, scope, ReplicaId::new()).unwrap();
        first
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([0x61; 32]),
                |editor| editor.put(&Object::root(), "state", Scalar::String("v1".into())),
            )
            .unwrap();
        // The edit below the delete is the resurrection candidate: once the
        // recovered baseline carries the tombstone it must never bring the
        // document back, even if the service re-delivers that older change.
        let pre_floor = first.pending_changes(0, 10).unwrap();
        assert_eq!(pre_floor.len(), 1);
        first
            .delete(
                document,
                OperationId::new(),
                OperationDigest::new([0x62; 32]),
            )
            .unwrap();
        first
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        assert_eq!(store.frontier(scope).await.unwrap(), 2);

        seal_consume_and_prune_through(&store, scope, 2).await;

        let second = Database::open(&second_path, scope, ReplicaId::new()).unwrap();
        let report = second
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        assert!(report.recovered);
        assert!(second.is_deleted(document).unwrap());
        assert!(
            second
                .read(document, |reader| reader.get(&Object::root(), "state"))
                .unwrap()
                .is_none()
        );

        let resurrection = pre_floor
            .iter()
            .map(|change| RemoteChange {
                document: change.document,
                digest: change.digest,
                bytes: change.bytes.clone(),
            })
            .collect::<Vec<_>>();
        second
            .install_remote_page(peer, &resurrection, b"resurrection-attempt")
            .unwrap();
        assert!(second.is_deleted(document).unwrap());
        assert!(
            second
                .read(document, |reader| reader.get(&Object::root(), "state"))
                .unwrap()
                .is_none()
        );

        drop((first, second));
        fs::remove_file(first_path).unwrap();
        fs::remove_file(second_path).unwrap();
        server.abort();
    }

    #[test]
    fn corrupt_or_incomplete_baseline_leaves_state_and_checkpoint_unchanged() {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
            .block_on(corrupt_baseline_refusal_body());
    }

    async fn corrupt_baseline_refusal_body() {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([25; 32], 60_000).unwrap(),
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = lgwks_bot::rt::task::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let endpoint = format!("http://{address}/").parse().unwrap();
        let mut transport =
            HttpTransport::new(endpoint, StaticBearer::new("black-box").unwrap()).unwrap();

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let victim_path = path("corrupt-victim");

        let victim = Database::open(&victim_path, scope, ReplicaId::new()).unwrap();
        victim
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([0x71; 32]),
                |editor| editor.put(&Object::root(), "state", Scalar::String("keep".into())),
            )
            .unwrap();
        victim
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        let checkpoint_before = victim
            .checkpoint(peer)
            .unwrap()
            .expect("the replica accepted a durable checkpoint");
        assert!(!victim.is_deleted(document).unwrap());
        assert_eq!(
            victim
                .read(document, |reader| reader.get(&Object::root(), "state"))
                .unwrap(),
            Some(Some(Item::Scalar(Scalar::String("keep".into()))))
        );

        let genuine = store.build_snapshot(scope, 1).await.unwrap();
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
            SnapshotArtifact::new(scope, genuine.baseline.position(), mismatched.documents)
                .unwrap();

        for (label, artifact) in [
            ("substituted snapshot payload", substituted),
            ("truncated document list", truncated),
            ("mismatched tombstone flag", mismatched),
        ] {
            let error = victim
                .install_baseline(peer, &artifact, b"corrupt-checkpoint")
                .unwrap_err();
            assert!(
                matches!(&error, DatabaseError::InvalidBaseline),
                "{label} must be refused with a typed failure, got {error:?}"
            );
            assert_eq!(error.kind(), DatabaseErrorKind::Other, "{label} kind");
            assert_eq!(
                victim.checkpoint(peer).unwrap().as_deref(),
                Some(checkpoint_before.as_slice()),
                "{label} must leave the checkpoint byte-for-byte unchanged"
            );
            assert!(
                !victim.is_deleted(document).unwrap(),
                "{label} must not tombstone the existing document"
            );
            assert_eq!(
                victim
                    .read(document, |reader| reader.get(&Object::root(), "state"))
                    .unwrap(),
                Some(Some(Item::Scalar(Scalar::String("keep".into())))),
                "{label} must leave the readable state unchanged"
            );
        }

        drop(victim);
        fs::remove_file(victim_path).unwrap();
        server.abort();
    }

    #[test]
    fn recovery_merges_a_local_edit_the_baseline_predates() {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
            .block_on(recovery_merges_a_local_edit_body());
    }

    async fn recovery_merges_a_local_edit_body() {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([26; 32], 60_000).unwrap(),
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = lgwks_bot::rt::task::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let endpoint = format!("http://{address}/").parse().unwrap();
        let mut transport =
            HttpTransport::new(endpoint, StaticBearer::new("black-box").unwrap()).unwrap();

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let document = DocumentId::new();
        let peer = PeerId::new();
        let victim_path = path("merge-victim");
        let replica = ReplicaId::new();
        let victim = Database::open(&victim_path, scope, replica).unwrap();

        victim
            .edit(
                document,
                OperationId::new(),
                OperationDigest::new([0x81; 32]),
                |editor| editor.put(&Object::root(), "state", Scalar::Integer(1)),
            )
            .unwrap();
        victim
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();

        let baseline = store.build_snapshot(scope, 1).await.unwrap();
        assert_eq!(baseline.documents.len(), 1);

        let local_operation = OperationId::new();
        victim
            .edit(
                document,
                local_operation,
                OperationDigest::new([0x82; 32]),
                |editor| editor.put(&Object::root(), "state", Scalar::Integer(2)),
            )
            .unwrap();
        assert_eq!(victim.pending_changes(0, 10).unwrap().len(), 1);

        victim
            .install_baseline(peer, &baseline, b"merge-checkpoint")
            .unwrap();

        assert_eq!(
            victim
                .read(document, |reader| reader.get(&Object::root(), "state"))
                .unwrap(),
            Some(Some(Item::Scalar(Scalar::Integer(2)))),
            "the baseline predates the local edit, so the merged value must keep it"
        );
        let pending = victim.pending_changes(0, 10).unwrap();
        assert_eq!(
            pending.len(),
            1,
            "recovery must not consume the local outbox"
        );
        assert_eq!(pending[0].operation, local_operation);
        drop(victim);

        let reopened = Database::open(&victim_path, scope, replica).unwrap();
        assert_eq!(
            reopened
                .read(document, |reader| reader.get(&Object::root(), "state"))
                .unwrap(),
            Some(Some(Item::Scalar(Scalar::Integer(2)))),
            "the merged local edit must survive reopen"
        );
        drop(reopened);
        fs::remove_file(victim_path).unwrap();
        server.abort();
    }

    #[test]
    fn recovery_preserves_a_local_backlog_larger_than_one_upload_batch() {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .expect("test runtime")
            .block_on(recovery_preserves_a_large_backlog_body());
    }

    #[allow(clippy::too_many_lines)]
    async fn recovery_preserves_a_large_backlog_body() {
        let store = Arc::new(MemoryServiceStore::default());
        let app = router(
            Arc::clone(&store),
            TestAuthentication,
            CheckpointCodec::new([26; 32], 60_000).unwrap(),
            FixedClock,
        );
        let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let server = lgwks_bot::rt::task::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let endpoint = format!("http://{address}/").parse().unwrap();
        let mut transport =
            HttpTransport::new(endpoint, StaticBearer::new("black-box").unwrap()).unwrap();

        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let peer = PeerId::new();
        let replica = ReplicaId::new();
        let backlog_path = path("backlog-victim");
        let victim = Database::open(&backlog_path, scope, replica).unwrap();

        let seeded = DocumentId::new();
        victim
            .edit(
                seeded,
                OperationId::new(),
                OperationDigest::new([0x81; 32]),
                |editor| editor.put(&Object::root(), "state", Scalar::Integer(1)),
            )
            .unwrap();
        victim
            .synchronize(peer, &mut transport, SyncLimits::default(), || false)
            .await
            .unwrap();
        let baseline = store.build_snapshot(scope, 1).await.unwrap();

        let batch = usize::from(SyncLimits::default().changes_per_page);
        let backlog = batch + 2;
        let mut documents = Vec::with_capacity(backlog);
        let mut operations = Vec::with_capacity(backlog);
        for index in 0..backlog {
            let document = DocumentId::new();
            let operation = OperationId::new();
            let mut digest = [0u8; 32];
            digest[0] = 0x90;
            digest[1] = u8::try_from(index & 0xff).expect("fixture index fits");
            digest[2] = u8::try_from((index >> 8) & 0xff).expect("fixture index fits");
            victim
                .edit(
                    document,
                    operation,
                    OperationDigest::new(digest),
                    |editor| {
                        editor.put(
                            &Object::root(),
                            "state",
                            Scalar::Integer(i64::try_from(index).unwrap() + 10),
                        )
                    },
                )
                .unwrap();
            documents.push(document);
            operations.push(operation);
        }

        let before = victim.pending_changes(0, backlog + 8).unwrap();
        assert!(
            before.len() > batch,
            "the fixture backlog must exceed one upload batch, saw {} of {batch}",
            before.len()
        );

        victim
            .install_baseline(peer, &baseline, b"backlog-checkpoint")
            .unwrap();

        let after = victim.pending_changes(0, backlog + 8).unwrap();
        assert_eq!(
            after.len(),
            backlog,
            "recovery must keep every unsent change in a multi-batch backlog"
        );
        let kept: Vec<_> = after.iter().map(|change| change.operation).collect();
        assert_eq!(kept, operations, "the backlog must keep its exact order");
        for (index, document) in documents.iter().enumerate() {
            assert_eq!(
                victim
                    .read(*document, |reader| reader.get(&Object::root(), "state"))
                    .unwrap(),
                Some(Some(Item::Scalar(Scalar::Integer(
                    i64::try_from(index).unwrap() + 10
                )))),
                "local change {index} of a multi-batch backlog must survive recovery"
            );
        }
        assert_eq!(
            victim
                .read(seeded, |reader| reader.get(&Object::root(), "state"))
                .unwrap(),
            Some(Some(Item::Scalar(Scalar::Integer(1)))),
            "the acknowledged seed change must not regress"
        );
        drop(victim);
        let reopened = Database::open(&backlog_path, scope, replica).unwrap();
        assert_eq!(
            reopened.pending_changes(0, backlog + 8).unwrap().len(),
            backlog,
            "the preserved backlog must be durable, not in-memory only"
        );
        drop(reopened);
        fs::remove_file(backlog_path).unwrap();
        server.abort();
    }

    #[test]
    fn recovery_publishes_a_durable_invalidation_for_a_held_cursor() {
        lgwks_bot::Builder::new()
            .worker_threads(std::num::NonZeroUsize::new(2))
            .build()
            .unwrap()
            .block_on(async {
                let store = Arc::new(MemoryServiceStore::default());
                let app = router(
                    Arc::clone(&store),
                    TestAuthentication,
                    CheckpointCodec::new([27; 32], 60_000).unwrap(),
                    FixedClock,
                );
                let listener = lgwks_bot::rt::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .unwrap();
                let address = listener.local_addr().unwrap();
                let server = lgwks_bot::rt::task::spawn(async move {
                    axum::serve(listener, app).await.unwrap();
                });
                let endpoint = format!("http://{address}/").parse().unwrap();
                let mut transport =
                    HttpTransport::new(endpoint, StaticBearer::new("black-box").unwrap()).unwrap();

                let scope = Scope::new(AccountId::new(), SpaceId::new());
                let document = DocumentId::new();
                let peer = PeerId::new();
                let victim_path = path("invalidate-victim");
                let replica = ReplicaId::new();
                let victim = Database::open(&victim_path, scope, replica).unwrap();

                victim
                    .edit(
                        document,
                        OperationId::new(),
                        OperationDigest::new([0x91; 32]),
                        |editor| editor.put(&Object::root(), "state", Scalar::Integer(1)),
                    )
                    .unwrap();
                victim
                    .synchronize(peer, &mut transport, SyncLimits::default(), || false)
                    .await
                    .unwrap();

                // A subscriber holds a cursor immediately before recovery.
                let held = victim.changes_since(0, 256).unwrap();
                let cursor = held.last().unwrap().sequence();
                assert_eq!(victim.baseline_generation().unwrap(), 0);

                let baseline = store.build_snapshot(scope, 1).await.unwrap();
                victim
                    .install_baseline(peer, &baseline, b"invalidate-checkpoint")
                    .unwrap();

                // The install carries no change of its own, so nothing in the
                // pre-recovery stream reveals it; the committed invalidation
                // must still be exposed to the held cursor and be durable.
                assert_eq!(victim.baseline_generation().unwrap(), 1);
                let page = victim.changes_since(cursor, 256).unwrap();
                assert_eq!(page.len(), 1, "the baseline install must be visible");
                assert!(matches!(
                    page[0],
                    ChangeEvent::Baseline { generation: 1, .. }
                ));
                assert!(page[0].digest().is_none());
                drop(victim);

                let reopened = Database::open(&victim_path, scope, replica).unwrap();
                assert_eq!(reopened.baseline_generation().unwrap(), 1);
                let replayed = reopened.changes_since(cursor, 256).unwrap();
                assert!(
                    matches!(
                        replayed.as_slice(),
                        [ChangeEvent::Baseline { generation: 1, .. }]
                    ),
                    "the invalidation must survive reopen"
                );
                // The consumer rebuilds its view from the recovered state.
                assert_eq!(
                    reopened
                        .read(document, |reader| reader.get(&Object::root(), "state"))
                        .unwrap(),
                    Some(Some(Item::Scalar(Scalar::Integer(1))))
                );
                // A recovered replica's backup must round-trip through restore
                // even though its baseline marker carries no change of its own.
                let exported = reopened.export().unwrap();
                drop(reopened);
                fs::remove_file(victim_path).unwrap();

                let restored_path = path("invalidate-restored");
                let restored = Database::open(&restored_path, scope, ReplicaId::new()).unwrap();
                restored.restore(&exported).unwrap();
                assert_eq!(
                    restored
                        .read(document, |reader| reader.get(&Object::root(), "state"))
                        .unwrap(),
                    Some(Some(Item::Scalar(Scalar::Integer(1))))
                );
                drop(restored);
                fs::remove_file(restored_path).unwrap();
                server.abort();
            });
    }
}
