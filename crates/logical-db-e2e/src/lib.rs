//! Black-box process-boundary journeys for the complete client/service path.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

#![forbid(unsafe_code)]

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        convert::Infallible,
        fs,
        future::{Future, ready},
        sync::{Arc, Mutex},
    };

    use logical_db::{
        AccountId, Database, DatabaseError, DatabaseErrorKind, DocumentId, IndexId, IndexValue,
        Item, Object, OperationDigest, OperationId, PeerId, Projection, ProjectionSemantics,
        RemoteChange, ReplicaId, Scalar, Scope, SpaceId, SyncLimits,
    };
    use logical_db_http::{HttpTransport, StaticBearer};
    use logical_db_model::{ChangeDigest, Effect, SnapshotArtifact, SnapshotRecord};
    use logical_db_protocol::ChangeEnvelope;
    use logical_db_server::{AuthenticationError, Authenticator, Clock, router};
    use logical_db_service::{
        AcceptResult, Authority, CheckpointCodec, ConsumedResult, ExecutionGrant, PageResult,
        PruneReceipt, PruneRefusal, PruneResult, RetentionStore, SealResult, ServiceStore,
        StoredPage, materialize_snapshot, prune_decision,
    };

    use logical_db_model::ActionDigest;
    use logical_db_protocol::{
        PageAction, mark_consumed_action, prune_action, seal_snapshot_action, write_action,
    };

    struct Allow;
    struct Grant {
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
    }

    impl ExecutionGrant for Grant {
        fn scope(&self) -> Scope {
            self.scope
        }
        fn effect(&self) -> Effect {
            self.effect
        }
        fn action(&self) -> ActionDigest {
            self.action
        }
        fn frontier(&self) -> u64 {
            self.frontier
        }
        fn invocation_id(&self) -> [u8; 32] {
            self.action.into_bytes()
        }
    }

    impl Authority for Allow {
        type Grant = Grant;

        fn authorize(
            &self,
            scope: Scope,
            effect: Effect,
            action: ActionDigest,
            frontier: u64,
            _now_unix_ms: u64,
        ) -> Option<Self::Grant> {
            Some(Grant {
                scope,
                effect,
                action,
                frontier,
            })
        }
    }

    struct TestAuthentication;

    impl Authenticator for TestAuthentication {
        type Policy = Allow;

        fn authenticate(
            &self,
            authorization: &axum::http::HeaderValue,
            _now_unix_ms: u64,
        ) -> Result<Self::Policy, AuthenticationError> {
            if authorization.as_bytes() == b"Bearer black-box" {
                Ok(Allow)
            } else {
                Err(AuthenticationError::Unauthorized)
            }
        }
    }

    struct FixedClock;

    impl Clock for FixedClock {
        fn now_unix_ms(&self) -> Option<u64> {
            Some(10_000)
        }
    }

    #[derive(Clone, Default)]
    struct RetentionState {
        floor: u64,
        consumed: u64,
        baseline: Option<SnapshotArtifact>,
    }

    #[derive(Default)]
    struct MemoryServiceStore {
        changes: Mutex<Vec<ChangeEnvelope>>,
        operations: Mutex<HashMap<OperationId, (OperationDigest, ChangeDigest)>>,
        retention: Mutex<HashMap<Scope, RetentionState>>,
    }

    impl MemoryServiceStore {
        fn scope_frontier(&self, scope: Scope) -> u64 {
            let changes = self.changes.lock().unwrap();
            self.retention_state(scope).floor + Self::retained_count(&changes, scope)
        }

        fn retained_count(changes: &[ChangeEnvelope], scope: Scope) -> u64 {
            changes
                .iter()
                .filter(|change| change.scope == scope)
                .count() as u64
        }

        fn retention_state(&self, scope: Scope) -> RetentionState {
            self.retention
                .lock()
                .unwrap()
                .get(&scope)
                .cloned()
                .unwrap_or_default()
        }

        fn check_retention_grant(
            &self,
            scope: Scope,
            action: ActionDigest,
            grant: &impl ExecutionGrant,
        ) -> Option<u64> {
            let frontier = self.scope_frontier(scope);
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.action() != action
                || grant.frontier() != frontier
            {
                return None;
            }
            Some(frontier)
        }
    }

    impl ServiceStore for MemoryServiceStore {
        type Error = Infallible;

        fn accept(
            &self,
            change: &ChangeEnvelope,
            grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
            if grant.scope() != change.scope
                || grant.effect() != Effect::Write
                || grant.action() != write_action(change)
            {
                return ready(Ok(AcceptResult::StaleAuthority));
            }
            let mut operations = self.operations.lock().unwrap();
            let outcome = match operations.get(&change.operation) {
                Some(existing) if *existing == (change.request, change.digest) => {
                    AcceptResult::Duplicate
                }
                Some(_) => AcceptResult::Conflict,
                None => {
                    operations.insert(change.operation, (change.request, change.digest));
                    self.changes.lock().unwrap().push(change.clone());
                    AcceptResult::Accepted
                }
            };
            ready(Ok(outcome))
        }

        fn frontier(&self, scope: Scope) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            ready(Ok(self.scope_frontier(scope)))
        }

        fn retention_floor(
            &self,
            scope: Scope,
        ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            ready(Ok(self.retention_state(scope).floor))
        }

        fn page(
            &self,
            action: PageAction,
            grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
            let PageAction {
                scope,
                after,
                max_changes,
                max_bytes,
                ..
            } = action;
            let floor = self.retention_state(scope).floor;
            if after < floor {
                return ready(Ok(PageResult::BehindFloor));
            }
            let changes = self.changes.lock().unwrap();
            let scoped = changes
                .iter()
                .filter(|change| change.scope == scope)
                .collect::<Vec<_>>();
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.action() != action.digest()
                || grant.frontier() != floor + scoped.len() as u64
            {
                return ready(Ok(PageResult::StaleAuthority));
            }
            let mut bytes = 0usize;
            let page = scoped
                .iter()
                .skip(usize::try_from(after - floor).unwrap_or(usize::MAX))
                .take(usize::from(max_changes))
                .take_while(|change| {
                    bytes = bytes.saturating_add(change.bytes.len());
                    bytes <= max_bytes as usize
                })
                .map(|change| (*change).clone())
                .collect::<Vec<_>>();
            let position = after + page.len() as u64;
            ready(Ok(PageResult::Page(StoredPage {
                changes: page,
                position,
                more: floor + scoped.len() as u64 > position,
            })))
        }
    }

    impl RetentionStore for MemoryServiceStore {
        fn consumed_through(
            &self,
            scope: Scope,
        ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
            ready(Ok(self.retention_state(scope).consumed))
        }

        fn snapshot_baseline(
            &self,
            scope: Scope,
        ) -> impl Future<Output = Result<Option<SnapshotRecord>, Self::Error>> + Send {
            ready(Ok(self
                .retention_state(scope)
                .baseline
                .map(|artifact| SnapshotRecord::State(artifact.baseline))))
        }

        fn build_snapshot(
            &self,
            scope: Scope,
            through: u64,
        ) -> impl Future<Output = Result<SnapshotArtifact, Self::Error>> + Send {
            let state = self.retention_state(scope);
            let base = state
                .baseline
                .filter(|artifact| artifact.baseline.position() <= through);
            let base_position = base
                .as_ref()
                .map_or(state.floor, |artifact| artifact.baseline.position());
            let skip =
                usize::try_from(base_position.saturating_sub(state.floor)).unwrap_or(usize::MAX);
            let retained =
                usize::try_from(through.saturating_sub(base_position)).unwrap_or(usize::MAX);
            let changes = self.changes.lock().unwrap();
            let retained_changes = changes
                .iter()
                .filter(|change| change.scope == scope)
                .skip(skip)
                .take(retained)
                .cloned()
                .collect::<Vec<_>>();
            let artifact = materialize_snapshot(scope, through, base.as_ref(), &retained_changes)
                .expect("the journey store materializes bounded validated artifacts");
            ready(Ok(artifact))
        }

        fn seal_snapshot(
            &self,
            scope: Scope,
            artifact: &SnapshotArtifact,
            request: OperationDigest,
            grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<SealResult, Self::Error>> + Send {
            let baseline = &artifact.baseline;
            let Some(frontier) = self.check_retention_grant(
                scope,
                seal_snapshot_action(scope, request, baseline),
                grant,
            ) else {
                return ready(Ok(SealResult::StaleAuthority));
            };
            if baseline.position() > frontier {
                return ready(Ok(SealResult::BeyondFrontier { frontier }));
            }
            self.retention
                .lock()
                .unwrap()
                .entry(scope)
                .or_default()
                .baseline = Some(artifact.clone());
            ready(Ok(SealResult::Sealed {
                position: baseline.position(),
            }))
        }

        fn mark_consumed(
            &self,
            scope: Scope,
            through: u64,
            request: OperationDigest,
            grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<ConsumedResult, Self::Error>> + Send {
            let Some(frontier) = self.check_retention_grant(
                scope,
                mark_consumed_action(scope, request, through),
                grant,
            ) else {
                return ready(Ok(ConsumedResult::StaleAuthority));
            };
            if through > frontier {
                return ready(Ok(ConsumedResult::BeyondFrontier { frontier }));
            }
            let mut retention = self.retention.lock().unwrap();
            let state = retention.entry(scope).or_default();
            if through <= state.consumed {
                return ready(Ok(ConsumedResult::Unchanged {
                    consumed_through: state.consumed,
                }));
            }
            state.consumed = through;
            ready(Ok(ConsumedResult::Advanced {
                consumed_through: through,
            }))
        }

        fn prune(
            &self,
            scope: Scope,
            up_to: u64,
            min_live: Option<u64>,
            request: OperationDigest,
            grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<PruneResult, Self::Error>> + Send {
            let Some(frontier) = self.check_retention_grant(
                scope,
                prune_action(scope, request, up_to, min_live),
                grant,
            ) else {
                return ready(Ok(PruneResult::StaleAuthority));
            };
            let state = self.retention_state(scope);
            let through =
                match prune_decision(frontier, state.floor, up_to, min_live, state.consumed) {
                    Ok(through) => through,
                    Err(refusal) => return ready(Ok(PruneResult::Refused(refusal))),
                };
            if through > state.floor {
                let Some(artifact) = state.baseline.as_ref() else {
                    return ready(Ok(PruneResult::Refused(PruneRefusal::MissingBaseline)));
                };
                if artifact.baseline.position() < through {
                    return ready(Ok(PruneResult::Refused(PruneRefusal::BaselineBehind {
                        position: artifact.baseline.position(),
                    })));
                }
                let floor = state.floor;
                let mut position = floor;
                self.changes.lock().unwrap().retain(|change| {
                    if change.scope != scope {
                        return true;
                    }
                    position += 1;
                    position > through
                });
                self.retention
                    .lock()
                    .unwrap()
                    .entry(scope)
                    .or_default()
                    .floor = through;
            }
            ready(Ok(PruneResult::Pruned(PruneReceipt {
                pruned_through: through,
                floor: through.max(state.floor),
            })))
        }
    }

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
        assert!(events.iter().all(|event| event.document == document));
        assert!(
            database
                .changes_since(events.last().unwrap().sequence, 10)
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
}
