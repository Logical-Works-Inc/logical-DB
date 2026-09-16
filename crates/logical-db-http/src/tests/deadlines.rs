//! Public-transport deadline regressions for issue #54.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39. Source URLs are in `evidence/SOURCES.md`.

use super::*;
use lgwks_bot::rt::time::{sleep, timeout};
use logical_db_model::{AccountId, OperationDigest, ReplicaId, Scope, SpaceId};
use logical_db_protocol::{Checkpoint, PROTOCOL_VERSION};
use std::{
    future::pending,
    net::TcpListener,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Clone, Copy, Debug)]
enum Operation {
    Push,
    Pull,
    Baseline,
}
const OPERATIONS: [Operation; 3] = [Operation::Push, Operation::Pull, Operation::Baseline];

async fn invoke<C: CredentialProvider>(
    transport: &mut HttpTransport<C>,
    operation: Operation,
) -> Result<(), HttpError> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let request = OperationDigest::new([1; 32]);
    match operation {
        Operation::Push => transport
            .push(PushRequest {
                protocol: PROTOCOL_VERSION,
                replica,
                changes: Vec::new(),
            })
            .await
            .map(|_| ()),
        Operation::Pull => transport
            .pull(PullRequest {
                protocol: PROTOCOL_VERSION,
                scope,
                replica,
                request,
                after: None,
                max_changes: 1,
                max_bytes: 1_024,
            })
            .await
            .map(|_| ()),
        Operation::Baseline => transport
            .baseline(BaselineRequest {
                protocol: PROTOCOL_VERSION,
                scope,
                replica,
                request,
                after: None,
                after_document: None,
            })
            .await
            .map(|_| ()),
    }
}

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Refresh {
    delay: Option<Duration>,
    fail: bool,
    drops: Arc<AtomicUsize>,
}
impl Refresh {
    fn new(delay: Option<Duration>) -> Self {
        Self {
            delay,
            fail: false,
            drops: Arc::new(AtomicUsize::new(0)),
        }
    }
}
impl CredentialProvider for Refresh {
    async fn authorization(&mut self) -> Result<HeaderValue, CredentialError> {
        let _guard = DropCount(Arc::clone(&self.drops));
        match self.delay {
            Some(delay) => sleep(delay).await,
            None => pending::<()>().await,
        }
        if self.fail {
            Err(CredentialError::Refresh)
        } else {
            Ok(HeaderValue::from_static("Bearer never-print-me"))
        }
    }
}

fn undispatched_endpoint() -> (TcpListener, Url) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/", listener.local_addr().unwrap());
    (listener, endpoint.parse().unwrap())
}

fn assert_undispatched(listener: &TcpListener) {
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

fn successful_endpoint(operation: Operation) -> Url {
    let (status, body) = match operation {
        Operation::Push => (
            "200 OK",
            encode(&PushResponse {
                protocol: PROTOCOL_VERSION,
                outcomes: Vec::new(),
            })
            .unwrap(),
        ),
        Operation::Pull => (
            "200 OK",
            encode(&PullPage {
                protocol: PROTOCOL_VERSION,
                changes: Vec::new(),
                checkpoint: Checkpoint::from_wire(vec![1]).unwrap(),
                more: false,
            })
            .unwrap(),
        ),
        Operation::Baseline => ("404 Not Found", Vec::new()),
    };
    spawn_raw_server(move |stream| {
        write_bytes(
            stream,
            format!(
                "HTTP/1.1 {status}\r\ncontent-type: {MEDIA_TYPE}\r\ncontent-length: {}\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        );
        write_bytes(stream, &body);
    })
}

#[test]
fn pending_credentials_timeout_without_dispatch_on_every_route() {
    test_runtime().block_on(async {
        for operation in OPERATIONS {
            let (listener, endpoint) = undispatched_endpoint();
            let mut transport = HttpTransport::new(endpoint, Refresh::new(None)).unwrap();
            assert_eq!(transport.operation_timeout, Duration::from_secs(30));
            transport.operation_timeout = Duration::from_millis(50);
            let started = Instant::now();
            let outcome = timeout(Duration::from_secs(1), invoke(&mut transport, operation))
                .await
                .expect("refresh must not hang");
            assert!(matches!(outcome, Err(HttpError::CredentialTimeout)));
            assert!(started.elapsed() >= transport.operation_timeout);
            assert_eq!(transport.credentials.drops.load(Ordering::SeqCst), 1);
            assert_undispatched(&listener);
        }
    });
}

#[test]
fn refresh_and_http_headers_or_body_share_one_budget() {
    test_runtime().block_on(async {
        for operation in OPERATIONS {
            for stall_body in [false, true] {
                let endpoint = spawn_raw_server(move |stream| {
                    if !stall_body {
                        std::thread::sleep(Duration::from_millis(300));
                    }
                    write_bytes(stream, &chunked_head());
                    if stall_body {
                        chunk(stream, &[0xa0]);
                        std::thread::sleep(Duration::from_millis(300));
                    } else {
                        chunk(stream, &[0xa0]);
                    }
                    write_bytes(stream, b"0\r\n\r\n");
                });
                let mut transport =
                    HttpTransport::new(endpoint, Refresh::new(Some(Duration::from_millis(300))))
                        .unwrap();
                transport.operation_timeout = Duration::from_millis(500);
                let started = Instant::now();
                let outcome = timeout(Duration::from_secs(2), invoke(&mut transport, operation))
                    .await
                    .expect("exchange must not hang");
                let elapsed = started.elapsed();
                assert!(
                    matches!(outcome, Err(HttpError::OperationTimeout)),
                    "{operation:?}, stall_body={stall_body}: {outcome:?}, elapsed={elapsed:?}"
                );
                assert!(elapsed >= transport.operation_timeout);
                assert!(elapsed < Duration::from_secs(1));
            }
        }
    });
}

#[test]
fn cancelling_refresh_drops_it_and_reuses_the_same_transport() {
    test_runtime().block_on(async {
        for operation in OPERATIONS {
            let (listener, endpoint) = undispatched_endpoint();
            let mut transport = HttpTransport::new(endpoint, Refresh::new(None)).unwrap();
            assert!(
                timeout(Duration::from_millis(50), invoke(&mut transport, operation))
                    .await
                    .is_err()
            );
            assert_eq!(transport.credentials.drops.load(Ordering::SeqCst), 1);
            assert_undispatched(&listener);
            transport.credentials.delay = Some(Duration::ZERO);
            transport.endpoint = successful_endpoint(operation);
            timeout(Duration::from_secs(2), invoke(&mut transport, operation))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(transport.credentials.drops.load(Ordering::SeqCst), 2);
        }
    });
}

#[test]
fn credential_failures_and_timeout_retry_policy_stay_distinct() {
    test_runtime().block_on(async {
        for operation in OPERATIONS {
            let (listener, endpoint) = undispatched_endpoint();
            let mut credentials = Refresh::new(Some(Duration::ZERO));
            credentials.fail = true;
            let mut transport = HttpTransport::new(endpoint, credentials).unwrap();
            let error = invoke(&mut transport, operation).await.unwrap_err();
            assert!(matches!(
                error,
                HttpError::Credential(CredentialError::Refresh)
            ));
            assert!(!error.is_retryable());
            assert!(!format!("{error:?} {error}").contains("never-print-me"));
            assert_undispatched(&listener);
        }
        for error in [HttpError::CredentialTimeout, HttpError::OperationTimeout] {
            assert!(error.is_retryable());
            assert_eq!(error.retry_after(), None);
            assert!(!format!("{error:?} {error}").contains("never-print-me"));
        }
    });
}

#[test]
fn static_and_refreshed_credentials_keep_all_routes_working() {
    test_runtime().block_on(async {
        for operation in OPERATIONS {
            let mut transport = HttpTransport::new(
                successful_endpoint(operation),
                StaticBearer::new("integration").unwrap(),
            )
            .unwrap();
            invoke(&mut transport, operation).await.unwrap();
            let mut transport = HttpTransport::new(
                successful_endpoint(operation),
                Refresh::new(Some(Duration::from_millis(10))),
            )
            .unwrap();
            invoke(&mut transport, operation).await.unwrap();
        }
    });
}

#[test]
fn expired_deadline_never_polls_the_next_stage() {
    test_runtime().block_on(async {
        let outcome = within_deadline(Instant::now(), HttpError::OperationTimeout, async {
            panic!("an expired stage must not start")
        })
        .await;
        assert!(matches!(outcome, Err(HttpError::OperationTimeout)));
    });
}

#[test]
fn late_ready_credentials_are_refused_before_dispatch() {
    struct Late;
    impl CredentialProvider for Late {
        fn authorization(
            &mut self,
        ) -> impl std::future::Future<Output = Result<HeaderValue, CredentialError>> {
            // Deliberately violate the cooperative-provider contract to prove
            // the post-poll check rejects a late ready value, not to promise preemption.
            std::thread::sleep(Duration::from_millis(50));
            std::future::ready(Ok(HeaderValue::from_static("Bearer never-print-me")))
        }
    }
    test_runtime().block_on(async {
        for operation in OPERATIONS {
            let (listener, endpoint) = undispatched_endpoint();
            let mut transport = HttpTransport::new(endpoint, Late).unwrap();
            transport.operation_timeout = Duration::from_millis(10);
            assert!(matches!(
                invoke(&mut transport, operation).await,
                Err(HttpError::CredentialTimeout)
            ));
            assert_undispatched(&listener);
        }
    });
}
