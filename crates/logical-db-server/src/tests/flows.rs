//! End-to-end router flows over the in-memory store doubles.

use super::*;

/// One wire field of a change envelope, carrying its replacement value.
enum Mutation {
    /// Replaces the owning scope.
    Scope(Scope),
    /// Replaces the document identity.
    Document(DocumentId),
    /// Replaces the replica identity.
    Replica(ReplicaId),
    /// Replaces the operation identity.
    Operation(OperationId),
    /// Replaces the request digest.
    Request(OperationDigest),
    /// Replaces the change digest alone.
    Digest(ChangeDigest),
    /// Replaces the payload bytes together with their digest.
    Payload(Vec<u8>, ChangeDigest),
}

/// Returns `original` with exactly one wire field replaced.
fn mutated(original: &ChangeEnvelope, mutation: Mutation) -> ChangeEnvelope {
    let mut parts = (
        original.scope(),
        original.document(),
        original.replica(),
        original.operation(),
        original.request(),
        original.digest(),
        original.bytes().to_vec(),
    );
    match mutation {
        Mutation::Scope(value) => parts.0 = value,
        Mutation::Document(value) => parts.1 = value,
        Mutation::Replica(value) => parts.2 = value,
        Mutation::Operation(value) => parts.3 = value,
        Mutation::Request(value) => parts.4 = value,
        Mutation::Digest(value) => parts.5 = value,
        Mutation::Payload(bytes, digest) => {
            parts.6 = bytes;
            parts.5 = digest;
        }
    }
    ChangeEnvelope::new(
        parts.0, parts.1, parts.2, parts.3, parts.4, parts.5, parts.6,
    )
}

#[test]
/// Fixed action rejects wire mutations and accepts exact retry
fn fixed_action_rejects_wire_mutations_and_accepts_exact_retry()
-> Result<(), Box<dyn std::error::Error>> {
    test_runtime()?.block_on(fixed_action_body())?;
    Ok(())
}

/// Drives every wire mutation, then the exact original twice.
async fn fixed_action_body() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let original = change(scope, ReplicaId::new())?;
    let action = ExactAction {
        scope,
        action: write_action(&original),
    };
    let app = router(
        Memory::default(),
        action,
        CheckpointCodec::new([9; 32], 60_000)?,
        FixedClock,
    );
    let replacement = change(scope, original.replica())?;
    let mutations = [
        Mutation::Scope(Scope::new(AccountId::new(), scope.space())),
        Mutation::Scope(Scope::new(scope.account(), SpaceId::new())),
        Mutation::Document(DocumentId::new()),
        Mutation::Operation(OperationId::new()),
        Mutation::Replica(ReplicaId::new()),
        Mutation::Request(OperationDigest::new([99; 32])),
        Mutation::Digest(ChangeDigest::new([99; 32])),
        Mutation::Payload(replacement.bytes().to_vec(), replacement.digest()),
    ]
    .map(|mutation| mutated(&original, mutation));
    for mutation in mutations {
        assert!(
            matches!(
                push_one(app.clone(), mutation).await?,
                PushOutcome::Rejected { .. }
            ),
            "a mutated change must be rejected"
        );
    }
    assert!(
        matches!(
            push_one(app.clone(), original.clone()).await?,
            PushOutcome::Accepted { .. }
        ),
        "the exact action must be accepted"
    );
    assert!(
        matches!(
            push_one(app, original).await?,
            PushOutcome::Duplicate { .. }
        ),
        "the exact retry must be a duplicate"
    );
    Ok(())
}

#[test]
/// Production clock and response encoder preserve exact resource edge
fn production_clock_and_response_encoder_preserve_exact_resource_edge()
-> Result<(), Box<dyn std::error::Error>> {
    /// Milliseconds since the epoch for 2023-11-14; any live clock is later.
    const PLAUSIBLE_FLOOR_UNIX_MS: u64 = 1_700_000_000_000;
    let observed = SystemClock.now_unix_ms().ok_or("expected Some")?;
    assert!(
        observed > PLAUSIBLE_FLOOR_UNIX_MS,
        "the production clock must report a current epoch time, got {observed}"
    );

    let exact_payload = vec![1; MAX_HTTP_BODY_BYTES - 5];
    assert!(
        encode_response(StatusCode::OK, &serde_bytes::Bytes::new(&exact_payload)).is_ok(),
        "a payload at the exact resource edge must encode"
    );
    let excessive_payload = vec![1; MAX_HTTP_BODY_BYTES - 4];
    assert!(
        matches!(
            encode_response(StatusCode::OK, &serde_bytes::Bytes::new(&excessive_payload)),
            Err(ApiError::Internal)
        ),
        "a payload one byte past the edge must be refused"
    );
    Ok(())
}

#[test]
/// Router pushes then pulls versioned cbor without leaking credentials
fn router_pushes_then_pulls_versioned_cbor_without_leaking_credentials()
-> Result<(), Box<dyn std::error::Error>> {
    test_runtime()?.block_on(router_pushes_then_pulls_body())?;
    Ok(())
}

/// Pushes one change, then pulls it back through the router.
async fn router_pushes_then_pulls_body() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let change = change(scope, replica)?;
    let app = router(
        Memory::default(),
        Bearer,
        CheckpointCodec::new([9; 32], 60_000)?,
        FixedClock,
    );
    let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change.clone()]);
    let response = app
        .clone()
        .oneshot(request("/v1/changes:push", encode(&push)?)?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK, "push must succeed");
    let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
    let response: PushResponse = ciborium::from_reader(body.as_ref())?;
    assert!(
        matches!(
            response.outcomes(),
            [PushOutcome::Accepted { operation }] if *operation == change.operation()
        ),
        "the push must accept exactly the pushed operation"
    );

    let pull = PullRequest::new(
        PROTOCOL_VERSION,
        scope,
        replica,
        OperationDigest::new([3; 32]),
        None,
        16,
        1_048_576,
    );
    let response = app
        .oneshot(request("/v1/changes:pull", encode(&pull)?)?)
        .await?;
    assert_eq!(response.status(), StatusCode::OK, "pull must succeed");
    let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
    let page: PullPage = ciborium::from_reader(body.as_ref())?;
    assert_eq!(
        page.changes(),
        std::slice::from_ref(&change),
        "the pull must return the pushed change"
    );
    Ok(())
}

#[test]
/// Ingress rejects ambiguous auth and wrong representations
fn ingress_rejects_ambiguous_auth_and_wrong_representations()
-> Result<(), Box<dyn std::error::Error>> {
    test_runtime()?.block_on(ingress_rejects_body())?;
    Ok(())
}

/// Sends each malformed request shape and checks its refusal status.
async fn ingress_rejects_body() -> Result<(), Box<dyn std::error::Error>> {
    let app = router(
        Memory::default(),
        Bearer,
        CheckpointCodec::new([9; 32], 60_000)?,
        FixedClock,
    );
    let mut ambiguous = request("/v1/changes:push", vec![0x80])?;
    ambiguous
        .headers_mut()
        .append(AUTHORIZATION, HeaderValue::from_static("Bearer second"));
    let response = app.clone().oneshot(ambiguous).await?;
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "two authorization headers are ambiguous"
    );

    let malformed = request_as("/v1/changes:push", vec![0x80], "Basic invalid")?;
    let response = app.clone().oneshot(malformed).await?;
    assert_eq!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "a non-bearer scheme is refused"
    );

    let response = app
        .oneshot(
            Request::post("/v1/changes:push")
                .header(CONTENT_TYPE, "application/json")
                .header(ACCEPT, MEDIA_TYPE)
                .header(AUTHORIZATION, "Bearer integration-test")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "a JSON body is not the supported media type"
    );

    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change(scope, replica)?]);
    let mut trailing = encode(&push)?;
    trailing.push(0);
    let app = router(
        Memory::default(),
        Bearer,
        CheckpointCodec::new([9; 32], 60_000)?,
        FixedClock,
    );
    let response = app.oneshot(request("/v1/changes:push", trailing)?).await?;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "trailing CBOR bytes are refused"
    );
    Ok(())
}

#[test]
/// Admission bounds complete requests and enforces deadline
fn admission_bounds_complete_requests_and_enforces_deadline()
-> Result<(), Box<dyn std::error::Error>> {
    test_runtime()?.block_on(admission_bounds_body())?;
    Ok(())
}

/// Saturates a one-slot router, then observes the deadline of the first call.
async fn admission_bounds_body() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change(scope, replica)?]);
    let entered = Arc::new(Notify::new());
    let app = router_with_limits(
        Stalled {
            entered: Arc::clone(&entered),
        },
        Bearer,
        CheckpointCodec::new([9; 32], 60_000)?,
        FixedClock,
        1,
        Duration::from_millis(50),
    );
    let mut first = lgwks_bot::rt::task::JoinSet::new();
    first.spawn(
        app.clone()
            .oneshot(request("/v1/changes:push", encode(&push)?)?),
    );
    entered.notified().await;
    let saturated = app
        .oneshot(request("/v1/changes:push", encode(&push)?)?)
        .await?;
    assert_eq!(
        saturated.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a saturated router sheds the second request"
    );
    assert_eq!(
        saturated
            .headers()
            .get(RETRY_AFTER)
            .ok_or("expected Some")?,
        "1",
        "a shed request tells the client to retry"
    );
    assert_eq!(
        first
            .join_next()
            .await
            .ok_or("expected the first push to finish")???
            .status(),
        StatusCode::GATEWAY_TIMEOUT,
        "the stalled request ends at its deadline"
    );
    Ok(())
}

#[test]
/// Default composition refuses writes and sync over http
fn default_composition_refuses_writes_and_sync_over_http() -> Result<(), Box<dyn std::error::Error>>
{
    test_runtime()?.block_on(default_composition_body())?;
    Ok(())
}

/// Pushes and pulls under the read-only default composition.
async fn default_composition_body() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let app = router(
        Memory::default(),
        DefaultAuthentication { scope },
        CheckpointCodec::new([9; 32], 60_000)?,
        FixedClock,
    );
    let change = change(scope, replica)?;
    let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change.clone()]);
    let response = app
        .clone()
        .oneshot(request_as(
            "/v1/changes:push",
            encode(&push)?,
            "Bearer default-composition",
        )?)
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the push envelope itself succeeds"
    );
    let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
    let response: PushResponse = ciborium::from_reader(body.as_ref())?;
    assert!(
        matches!(
            response.outcomes(),
            [PushOutcome::Rejected { operation, code: RejectionCode::Forbidden }]
                if *operation == change.operation()
        ),
        "the default composition forbids writes"
    );

    let pull = PullRequest::new(
        PROTOCOL_VERSION,
        scope,
        replica,
        OperationDigest::new([3; 32]),
        None,
        16,
        1_048_576,
    );
    let response = app
        .oneshot(request_as(
            "/v1/changes:pull",
            encode(&pull)?,
            "Bearer default-composition",
        )?)
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "the default composition forbids sync"
    );
    Ok(())
}

#[test]
/// Consumed invocation surfaces as typed rejection over http
fn consumed_invocation_surfaces_as_typed_rejection_over_http()
-> Result<(), Box<dyn std::error::Error>> {
    test_runtime()?.block_on(consumed_invocation_body())?;
    Ok(())
}

/// Pushes against a store that reports every invocation consumed.
async fn consumed_invocation_body() -> Result<(), Box<dyn std::error::Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let replica = ReplicaId::new();
    let app = router(
        ConsumedInvocations,
        Bearer,
        CheckpointCodec::new([9; 32], 60_000)?,
        FixedClock,
    );
    let change = change(scope, replica)?;
    let push = PushRequest::new(PROTOCOL_VERSION, replica, vec![change.clone()]);
    let response = app
        .oneshot(request("/v1/changes:push", encode(&push)?)?)
        .await?;
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the push envelope itself succeeds"
    );
    let body = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES).await?;
    let response: PushResponse = ciborium::from_reader(body.as_ref())?;
    assert!(
        matches!(
            response.outcomes(),
            [PushOutcome::Rejected {
                operation,
                code: RejectionCode::InvocationConsumed
            }] if *operation == change.operation()
        ),
        "a consumed invocation surfaces as a typed rejection"
    );
    Ok(())
}
