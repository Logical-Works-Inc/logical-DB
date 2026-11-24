//! The HTTP/service/store action seam over the live `FdbServiceStore`: one
//! canonical write is admitted once, its replay is a duplicate, and every
//! mutated field of the same envelope is refused without touching state.
//!
//! This proves the action binding only. It is not presented as runtime human
//! confirmation or independent authority admission.

use std::{
    error::Error,
    io,
    net::{Ipv4Addr, SocketAddr},
};

use logical_db::Transport as _;
use logical_db::http::{Endpoint, HttpTransport, StaticBearer};
use logical_db_e2e::support::{Allow, TestAuthentication};
use logical_db_model::{
    AccountId, DocumentId, Effect, OperationDigest, OperationId, ReplicaId, Scope, SpaceId,
};
use logical_db_protocol::{
    ChangeEnvelope, PROTOCOL_VERSION, PushOutcome, PushRequest, RejectionCode, write_action,
};
use logical_db_server::{SystemClock, router};
use logical_db_service::CheckpointCodec;
use logical_db_store_fdb::FdbServiceStore;

use super::{recovery::ambiguity_probe_change, retries::read_frontier_with_retry};

/// Exact pushes per change before a retryable transport failure is final.
const PUSH_BUDGET: usize = 32;

/// The bearer token the HTTP action-binding journey presents.
const ACTION_TOKEN: &str = "action-fixture";

/// Serves `store` admitting one write action, then proves the action binding
/// through the product HTTP transport. `observer` reads the frontier through
/// an independent database handle.
pub(super) async fn verify_http_action_binding(
    store: FdbServiceStore,
    observer: &FdbServiceStore,
    scope: Scope,
    replica: ReplicaId,
) -> Result<String, Box<dyn Error>> {
    let original = ambiguity_probe_change(scope, replica, 600)?;
    // Admit exactly the canonical write action, as a write, in this one scope.
    let canonical = write_action(&original);
    let authentication = TestAuthentication::new(
        ACTION_TOKEN,
        Allow::when(move |requested_scope, effect, action| {
            requested_scope == scope && effect == Effect::Write && action == canonical
        }),
    );
    let application = router(
        store,
        authentication,
        CheckpointCodec::new([0x27; 32], 60_000)?,
        SystemClock::new(),
    );
    // An ephemeral loopback port: this journey owns the only client.
    let listener =
        lgwks_bot::rt::net::TcpListener::bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).await?;
    let address = listener.local_addr()?;
    let mut server = lgwks_bot::rt::task::JoinSet::new();
    server.spawn(async move { axum::serve(listener, application).await });
    let bearer = StaticBearer::new(ACTION_TOKEN)?;
    let mut transport =
        HttpTransport::new(Endpoint::parse(&format!("http://{address}/"))?, bearer)?;

    let before = read_frontier_with_retry(observer, scope).await?;
    let first = push_over_http(&mut transport, &original).await?;
    if !matches!(
        first,
        PushOutcome::Accepted { .. } | PushOutcome::Duplicate { .. }
    ) {
        return Err(io::Error::other(format!("canonical HTTP write was {first:?}")).into());
    }
    let after_first = read_frontier_with_retry(observer, scope).await?;
    if after_first != before.saturating_add(1) {
        return Err(io::Error::other("canonical HTTP write did not advance once").into());
    }
    let replay = push_over_http(&mut transport, &original).await?;
    if !matches!(replay, PushOutcome::Duplicate { .. }) {
        return Err(io::Error::other(format!("exact HTTP replay was {replay:?}")).into());
    }

    let (variants, account_scope, space_scope) = mutation_variants(scope, replica, &original)?;
    for (variant, expected) in variants {
        let outcome = push_over_http(&mut transport, &variant).await?;
        if !matches!(outcome, PushOutcome::Rejected { code, .. } if code == expected) {
            return Err(io::Error::other(format!("mutated HTTP action was {outcome:?}")).into());
        }
    }
    if read_frontier_with_retry(observer, scope).await? != after_first {
        return Err(io::Error::other("rejected HTTP action advanced the frontier").into());
    }
    if read_frontier_with_retry(observer, account_scope).await? != 0
        || read_frontier_with_retry(observer, space_scope).await? != 0
    {
        return Err(io::Error::other("rejected HTTP scope mutation created state").into());
    }
    server.abort_all();
    Ok("PASS fdb-action-binding canonical=accepted retry=duplicate mutations=forbidden scope=isolated".to_owned())
}

/// Pushes one change through the product HTTP transport, retrying only the
/// failures the transport itself classifies as retryable.
async fn push_over_http(
    transport: &mut HttpTransport<StaticBearer>,
    change: &ChangeEnvelope,
) -> Result<PushOutcome, Box<dyn Error>> {
    let mut last_error = None;
    for _attempt in 0..PUSH_BUDGET {
        let request = PushRequest::new(PROTOCOL_VERSION, change.replica(), vec![change.clone()]);
        match transport.push(request).await {
            Ok(response) => {
                return response
                    .outcomes()
                    .first()
                    .cloned()
                    .ok_or_else(|| io::Error::other("HTTP push returned no outcome").into());
            }
            Err(error) if error.is_retryable() => last_error = Some(error),
            Err(error) => return Err(error.into()),
        }
    }
    Err(io::Error::other(format!(
        "HTTP push exhausted its retry budget: {last_error:?}"
    ))
    .into())
}

/// Every single-field mutation of a change, each with the refusal it must earn.
type Variants = [(ChangeEnvelope, RejectionCode); 8];

/// Builds every single-field mutation of `original` with the refusal each
/// must earn, plus the two foreign scopes the mutations reach into.
fn mutation_variants(
    scope: Scope,
    replica: ReplicaId,
    original: &ChangeEnvelope,
) -> Result<(Variants, Scope, Scope), Box<dyn Error>> {
    let replacement = ambiguity_probe_change(scope, replica, 601)?;
    let account_scope = Scope::new(AccountId::new(), scope.space());
    let space_scope = Scope::new(scope.account(), SpaceId::new());
    // One field of the original replaced per variant; the envelope's fields are
    // private, so each variant is rebuilt through its one constructor.
    let rebuilt = |scope: Scope,
                   document: DocumentId,
                   replica: ReplicaId,
                   operation: OperationId,
                   request: OperationDigest,
                   source: &ChangeEnvelope| {
        ChangeEnvelope::new(
            scope,
            document,
            replica,
            operation,
            request,
            source.digest(),
            source.bytes().to_vec(),
        )
    };
    let (document, operation, request) = (
        original.document(),
        original.operation(),
        original.request(),
    );
    let digest_only = ChangeEnvelope::new(
        scope,
        document,
        original.replica(),
        operation,
        request,
        replacement.digest(),
        original.bytes().to_vec(),
    );
    let variants = [
        (
            rebuilt(
                account_scope,
                document,
                replica,
                operation,
                request,
                original,
            ),
            RejectionCode::Forbidden,
        ),
        (
            rebuilt(space_scope, document, replica, operation, request, original),
            RejectionCode::Forbidden,
        ),
        (
            rebuilt(
                scope,
                DocumentId::new(),
                replica,
                operation,
                request,
                original,
            ),
            RejectionCode::Forbidden,
        ),
        (
            rebuilt(
                scope,
                document,
                replica,
                OperationId::new(),
                request,
                original,
            ),
            RejectionCode::Forbidden,
        ),
        (
            rebuilt(
                scope,
                document,
                ReplicaId::new(),
                operation,
                request,
                original,
            ),
            RejectionCode::Forbidden,
        ),
        (
            rebuilt(
                scope,
                document,
                replica,
                operation,
                OperationDigest::new([0x51; 32]),
                original,
            ),
            RejectionCode::Forbidden,
        ),
        (digest_only, RejectionCode::DigestMismatch),
        (
            rebuilt(scope, document, replica, operation, request, &replacement),
            RejectionCode::Forbidden,
        ),
    ];
    Ok((variants, account_scope, space_scope))
}
