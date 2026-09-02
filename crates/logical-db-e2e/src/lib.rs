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
        sync::Mutex,
    };

    use logical_db::{
        AccountId, Database, DocumentId, IndexId, IndexValue, Item, Object, OperationDigest,
        OperationId, PeerId, Projection, ReplicaId, Scalar, Scope, SpaceId, SyncLimits,
    };
    use logical_db_http::{HttpTransport, StaticBearer};
    use logical_db_model::{ChangeDigest, Effect};
    use logical_db_protocol::ChangeEnvelope;
    use logical_db_server::{AuthenticationError, Authenticator, Clock, router};
    use logical_db_service::{
        AcceptResult, Authority, CheckpointCodec, ExecutionGrant, PageResult, ServiceStore,
        StoredPage,
    };

    struct Allow;
    struct Grant {
        scope: Scope,
        effect: Effect,
        request: OperationDigest,
        frontier: u64,
    }

    impl ExecutionGrant for Grant {
        fn scope(&self) -> Scope {
            self.scope
        }
        fn effect(&self) -> Effect {
            self.effect
        }
        fn request(&self) -> OperationDigest {
            self.request
        }
        fn frontier(&self) -> u64 {
            self.frontier
        }
        fn invocation_id(&self) -> [u8; 32] {
            self.request.into_bytes()
        }
    }

    impl Authority for Allow {
        type Grant = Grant;

        fn authorize(
            &self,
            scope: Scope,
            effect: Effect,
            request: OperationDigest,
            frontier: u64,
            _now_unix_ms: u64,
        ) -> Option<Self::Grant> {
            Some(Grant {
                scope,
                effect,
                request,
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

    #[derive(Default)]
    struct MemoryServiceStore {
        changes: Mutex<Vec<ChangeEnvelope>>,
        operations: Mutex<HashMap<OperationId, (OperationDigest, ChangeDigest)>>,
    }

    impl ServiceStore for MemoryServiceStore {
        type Error = Infallible;

        fn accept(
            &self,
            change: &ChangeEnvelope,
            _grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<AcceptResult, Self::Error>> + Send {
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
            let frontier = self
                .changes
                .lock()
                .unwrap()
                .iter()
                .filter(|change| change.scope == scope)
                .count() as u64;
            ready(Ok(frontier))
        }

        fn page(
            &self,
            scope: Scope,
            request: OperationDigest,
            after: u64,
            max_changes: u16,
            max_bytes: u32,
            grant: &impl ExecutionGrant,
        ) -> impl Future<Output = Result<PageResult, Self::Error>> + Send {
            let changes = self.changes.lock().unwrap();
            let scoped = changes
                .iter()
                .filter(|change| change.scope == scope)
                .collect::<Vec<_>>();
            if grant.scope() != scope
                || grant.effect() != Effect::Sync
                || grant.request() != request
                || grant.frontier() != scoped.len() as u64
            {
                return ready(Ok(PageResult::StaleAuthority));
            }
            let mut bytes = 0usize;
            let page = scoped
                .iter()
                .skip(usize::try_from(after).unwrap_or(usize::MAX))
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
                more: usize::try_from(position).is_ok_and(|position| position < scoped.len()),
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
        Projection::new(index, |reader| {
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_embedded_replicas_converge_through_real_http_and_keep_indexes_current() {
        let app = router(
            MemoryServiceStore::default(),
            TestAuthentication,
            CheckpointCodec::new([21; 32], 60_000).unwrap(),
            FixedClock,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
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

        let mut permissions = fs::metadata(&path).unwrap().permissions();
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
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        // This test is compiled only on Windows, where clearing the read-only
        // file attribute is the precise cleanup operation and does not alter
        // Unix mode bits.
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(false);
        fs::set_permissions(&path, permissions).unwrap();
        fs::remove_file(path).unwrap();
        fs::remove_dir(directory).unwrap();
    }
}
