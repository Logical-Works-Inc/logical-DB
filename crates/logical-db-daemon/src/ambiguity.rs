//! Retained live injection of FDB's post-commit `commit_unknown_result` path.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

use std::{collections::HashSet, error::Error, io};

use foundationdb::{Database, api::FdbApiBuilder, options::NetworkOption};
use foundationdb_tuple::Subspace;
use logical_db_crdt::{Document, Object, Scalar};
use logical_db_model::{
    AccountId, ActionDigest, DocumentId, Effect, MAX_PAGE_BYTES, OperationDigest, OperationId,
    ReplicaId, Scope, SpaceId,
};
use logical_db_protocol::{ChangeEnvelope, PageAction, write_action};
use logical_db_protocol::{
    PROTOCOL_VERSION, PushOutcome, PushRequest, PushResponse, RejectionCode,
};
use logical_db_server::{AuthenticationError, Authenticator, SystemClock, router};
use logical_db_service::{
    AcceptResult, Authority, CheckpointCodec, ExecutionGrant, PageResult, ServiceStore, StoredPage,
};
use logical_db_store_fdb::{FdbDataError, FdbServiceStore, FdbStoreError};
use sha2::{Digest, Sha256};

const FULL_FDB_CHUNK_PROBE_BYTES: usize = 65_536;

#[derive(Clone, Copy)]
struct ForgedGrant {
    scope: Scope,
    effect: Effect,
    action: ActionDigest,
    frontier: u64,
    invocation: [u8; 32],
}

#[derive(Clone, Copy)]
struct FixedActionAuthority {
    scope: Scope,
    action: ActionDigest,
}

impl Authority for FixedActionAuthority {
    type Grant = ForgedGrant;

    fn authorize(
        &self,
        scope: Scope,
        effect: Effect,
        action: ActionDigest,
        frontier: u64,
        _now_unix_ms: u64,
    ) -> Option<Self::Grant> {
        (scope == self.scope && effect == Effect::Write && action == self.action)
            .then(|| fixture_grant(scope, effect, action, frontier))
    }
}

#[derive(Clone, Copy)]
struct FixedActionAuthenticator(FixedActionAuthority);

impl Authenticator for FixedActionAuthenticator {
    type Policy = FixedActionAuthority;

    fn authenticate(
        &self,
        authorization: &axum::http::HeaderValue,
        _now_unix_ms: u64,
    ) -> Result<Self::Policy, AuthenticationError> {
        if authorization.as_bytes() == b"Bearer action-fixture" {
            Ok(self.0)
        } else {
            Err(AuthenticationError::Unauthorized)
        }
    }
}

impl ExecutionGrant for ForgedGrant {
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
        self.invocation
    }
}

fn fixture_grant(scope: Scope, effect: Effect, action: ActionDigest, frontier: u64) -> ForgedGrant {
    let mut digest = Sha256::new();
    digest.update(b"logical-db/fdb-ambiguity-fixture/v1\0");
    digest.update(scope.account().as_uuid().as_bytes());
    digest.update(scope.space().as_uuid().as_bytes());
    digest.update([match effect {
        Effect::Read => 1,
        Effect::Scan => 2,
        Effect::Write => 3,
        Effect::Sync => 4,
    }]);
    digest.update(action.into_bytes());
    digest.update(frontier.to_be_bytes());
    ForgedGrant {
        scope,
        effect,
        action,
        frontier,
        invocation: digest.finalize().into(),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    run().await
}

async fn run() -> Result<(), Box<dyn Error>> {
    let cluster_file = std::env::var("LOGICAL_DB_FDB_CLUSTER_FILE")?;
    let network_builder = FdbApiBuilder::default().build()?;
    let network_builder = network_builder.set_option(NetworkOption::ClientBuggifyEnable)?;
    let network_builder =
        network_builder.set_option(NetworkOption::ClientBuggifySectionActivatedProbability(100))?;
    let network_builder =
        network_builder.set_option(NetworkOption::ClientBuggifySectionFiredProbability(12))?;
    #[allow(unsafe_code)]
    // SAFETY: this dedicated test process boots exactly one FDB network and
    // retains the auto-stop guard beyond every database, store, and future.
    let network = unsafe { network_builder.boot()? };
    let database = Database::new_compat(Some(&cluster_file)).await?;
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let root = Subspace::from_bytes(
        format!(
            "logical-db/ambiguity/{}/{}/",
            scope.account(),
            scope.space()
        )
        .into_bytes(),
    );
    let audit_database = Database::new_compat(Some(&cluster_file)).await?;
    let http_database = Database::new_compat(Some(&cluster_file)).await?;
    let audit_store = FdbServiceStore::new(audit_database, root.clone())?;
    let http_store = FdbServiceStore::new(http_database, root.clone())?;
    let replica = ReplicaId::new();
    verify_http_action_binding(http_store, &audit_store, scope, replica).await?;
    let store = FdbServiceStore::new(database, root)?;

    for attempt in 0_u16..512 {
        let change = match ambiguity_probe_change(scope, replica, attempt) {
            Ok(change) => change,
            Err(error) => {
                eprintln!("fdb-ambiguity: could not build attempt {attempt}: {error}");
                return Err(error);
            }
        };
        let action = write_action(&change);
        let Ok(frontier) = store.frontier(scope).await else {
            continue;
        };
        let grant = fixture_grant(scope, Effect::Write, action, frontier);
        match store.accept(&change, &grant).await {
            Ok(AcceptResult::RecoveredUnknownCommit) => {
                let observed =
                    complete_recovery_path(&store, scope, frontier, &change, &grant).await?;
                println!(
                    "PASS fdb-commit-unknown-recovered attempt={attempt} frontier={observed} invocation=consumed page=bounded scope=isolated"
                );
                drop(store);
                drop(network);
                return Ok(());
            }
            Ok(AcceptResult::Accepted) | Err(_) => {}
            Ok(other) => {
                eprintln!("fdb-ambiguity: unexpected acceptance result: {other:?}");
                return Err(
                    io::Error::other(format!("unexpected acceptance result: {other:?}")).into(),
                );
            }
        }
    }
    Err(io::Error::other("FDB buggify did not exercise a post-commit unknown result").into())
}

async fn verify_http_action_binding(
    store: FdbServiceStore,
    observer: &FdbServiceStore,
    scope: Scope,
    replica: ReplicaId,
) -> Result<(), Box<dyn Error>> {
    let original = ambiguity_probe_change(scope, replica, 600)?;
    let authority = FixedActionAuthority {
        scope,
        action: write_action(&original),
    };
    let application = router(
        store,
        FixedActionAuthenticator(authority),
        CheckpointCodec::new([0x27; 32], 60_000)?,
        SystemClock,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move { axum::serve(listener, application).await });
    let endpoint = format!("http://{address}/v1/changes:push");
    let client = reqwest::Client::new();

    let before = observer.frontier(scope).await?;
    let first = push_over_http(&client, &endpoint, &original).await?;
    if !matches!(first, PushOutcome::Accepted { .. }) {
        eprintln!("fdb-ambiguity: canonical HTTP write was {first:?}");
        return Err(io::Error::other(format!("canonical HTTP write was {first:?}")).into());
    }
    let after_first = observer.frontier(scope).await?;
    if after_first != before + 1 {
        eprintln!("fdb-ambiguity: HTTP frontier moved from {before} to {after_first}");
        return Err(io::Error::other("canonical HTTP write did not advance once").into());
    }
    let replay = push_over_http(&client, &endpoint, &original).await?;
    if !matches!(replay, PushOutcome::Duplicate { .. }) {
        eprintln!("fdb-ambiguity: exact HTTP replay was {replay:?}");
        return Err(io::Error::other(format!("exact HTTP replay was {replay:?}")).into());
    }

    let replacement = ambiguity_probe_change(scope, replica, 601)?;
    // This fixture proves the HTTP/service/store action seam. It is deliberately
    // not presented as runtime human confirmation or independent Braid admission.
    let account_scope = Scope::new(AccountId::new(), scope.space());
    let space_scope = Scope::new(scope.account(), SpaceId::new());
    let mut variants = vec![original.clone(); 8];
    variants[0].scope = account_scope;
    variants[1].scope = space_scope;
    variants[2].document = DocumentId::new();
    variants[3].operation = OperationId::new();
    variants[4].replica = ReplicaId::new();
    variants[5].request = OperationDigest::new([0x51; 32]);
    variants[6].digest = replacement.digest;
    variants[7].digest = replacement.digest;
    variants[7].bytes = replacement.bytes;
    for (index, variant) in variants.into_iter().enumerate() {
        let outcome = push_over_http(&client, &endpoint, &variant).await?;
        let expected = if index == 6 {
            RejectionCode::DigestMismatch
        } else {
            RejectionCode::Forbidden
        };
        if !matches!(outcome, PushOutcome::Rejected { code, .. } if code == expected) {
            eprintln!("fdb-ambiguity: mutated HTTP action was {outcome:?}");
            return Err(io::Error::other(format!("mutated HTTP action was {outcome:?}")).into());
        }
    }
    if observer.frontier(scope).await? != after_first {
        eprintln!("fdb-ambiguity: rejected HTTP action advanced canonical scope");
        return Err(io::Error::other("rejected HTTP action advanced the frontier").into());
    }
    if observer.frontier(account_scope).await? != 0 || observer.frontier(space_scope).await? != 0 {
        eprintln!("fdb-ambiguity: rejected HTTP scope mutation created durable state");
        return Err(io::Error::other("rejected HTTP scope mutation created state").into());
    }
    server.abort();
    Ok(())
}

async fn push_over_http(
    client: &reqwest::Client,
    endpoint: &str,
    change: &ChangeEnvelope,
) -> Result<PushOutcome, Box<dyn Error>> {
    let request = PushRequest {
        protocol: PROTOCOL_VERSION,
        replica: change.replica,
        changes: vec![change.clone()],
    };
    let mut body = Vec::new();
    ciborium::into_writer(&request, &mut body)?;
    let response = client
        .post(endpoint)
        .header("authorization", "Bearer action-fixture")
        .header("content-type", "application/vnd.logical-db.v1+cbor")
        .header("accept", "application/vnd.logical-db.v1+cbor")
        .body(body)
        .send()
        .await?;
    if !response.status().is_success() {
        eprintln!("fdb-ambiguity: HTTP push status {}", response.status());
        return Err(io::Error::other(format!("HTTP push status {}", response.status())).into());
    }
    let bytes = response.bytes().await?;
    let decoded: PushResponse = ciborium::from_reader(bytes.as_ref())?;
    decoded
        .outcomes
        .into_iter()
        .next()
        .ok_or_else(|| io::Error::other("HTTP push returned no outcome").into())
}

async fn complete_recovery_path(
    store: &FdbServiceStore,
    scope: Scope,
    frontier: u64,
    change: &ChangeEnvelope,
    grant: &ForgedGrant,
) -> Result<u64, Box<dyn Error>> {
    let (observed, rebound) = verify_recovered_write(store, scope, frontier, change, grant).await?;
    let (observed, continuation) = append_continuation_record(store, scope, observed).await?;
    verify_page(store, scope, observed, change, &continuation, &rebound).await?;
    Ok(observed)
}

fn ambiguity_probe_change(
    scope: Scope,
    replica: ReplicaId,
    attempt: u16,
) -> Result<ChangeEnvelope, Box<dyn Error>> {
    let mut document = Document::new();
    let ((), mut encoded) = document.edit(|editor| {
        editor.put(
            &Object::root(),
            "attempt",
            Scalar::Integer(i64::from(attempt)),
        )?;
        editor.put(
            &Object::root(),
            "full-fdb-chunk-probe",
            Scalar::Bytes(vec![0xa5; FULL_FDB_CHUNK_PROBE_BYTES]),
        )
    })?;
    let encoded = encoded
        .pop()
        .ok_or_else(|| io::Error::other("missing CRDT change"))?;
    let request_byte = u8::try_from(attempt % 256).map_err(io::Error::other)?;
    let request = OperationDigest::new([request_byte; 32]);
    Ok(ChangeEnvelope {
        scope,
        document: DocumentId::new(),
        replica,
        operation: OperationId::new(),
        request,
        digest: encoded.digest,
        bytes: encoded.bytes,
    })
}

async fn append_continuation_record(
    store: &FdbServiceStore,
    scope: Scope,
    minimum_frontier: u64,
) -> Result<(u64, ChangeEnvelope), Box<dyn Error>> {
    for attempt in 0_u16..512 {
        let Ok(frontier) = store.frontier(scope).await else {
            continue;
        };
        if frontier < minimum_frontier {
            eprintln!("fdb-ambiguity: frontier regressed from {minimum_frontier} to {frontier}");
            return Err(io::Error::other("durable frontier regressed").into());
        }
        let mut document = Document::new();
        let ((), mut encoded) = document.edit(|editor| {
            editor.put(
                &Object::root(),
                "continuation-proof",
                Scalar::Integer(i64::from(attempt)),
            )
        })?;
        let encoded = encoded
            .pop()
            .ok_or_else(|| io::Error::other("missing continuation CRDT change"))?;
        let request = continuation_fixture_request(scope, frontier, attempt);
        let change = ChangeEnvelope {
            scope,
            document: DocumentId::new(),
            replica: ReplicaId::new(),
            operation: OperationId::new(),
            request,
            digest: encoded.digest,
            bytes: encoded.bytes,
        };
        let grant = fixture_grant(scope, Effect::Write, write_action(&change), frontier);
        if matches!(
            store.accept(&change, &grant).await,
            Ok(AcceptResult::Accepted | AcceptResult::RecoveredUnknownCommit)
        ) {
            let observed = frontier
                .checked_add(1)
                .ok_or_else(|| io::Error::other("continuation frontier overflowed"))?;
            return Ok((observed, change));
        }
    }
    Err(io::Error::other("could not append a known continuation record").into())
}

fn continuation_fixture_request(scope: Scope, frontier: u64, attempt: u16) -> OperationDigest {
    let mut digest = Sha256::new();
    digest.update(b"logical-db/fdb-continuation-fixture/v1\0");
    digest.update(scope.account().as_uuid().as_bytes());
    digest.update(scope.space().as_uuid().as_bytes());
    digest.update(frontier.to_be_bytes());
    digest.update(attempt.to_be_bytes());
    OperationDigest::new(digest.finalize().into())
}

async fn verify_recovered_write(
    store: &FdbServiceStore,
    scope: Scope,
    frontier: u64,
    change: &ChangeEnvelope,
    grant: &ForgedGrant,
) -> Result<(u64, ChangeEnvelope), Box<dyn Error>> {
    let mut observed = None;
    for _retry in 0..128 {
        if let Ok(value) = store.frontier(scope).await {
            observed = Some(value);
            break;
        }
    }
    let observed = observed.ok_or_else(|| io::Error::other("frontier retry budget exhausted"))?;
    if observed != frontier + 1 {
        eprintln!(
            "fdb-ambiguity: recovered frontier expected={} observed={observed}",
            frontier + 1
        );
        return Err(io::Error::other("unknown recovery advanced more than once").into());
    }
    let mut exact_retry = None;
    for _retry in 0..128 {
        if let Ok(value) = store.accept(change, grant).await {
            exact_retry = Some(value);
            break;
        }
    }
    if !matches!(
        exact_retry,
        Some(AcceptResult::Duplicate | AcceptResult::RecoveredUnknownCommit)
    ) {
        eprintln!("fdb-ambiguity: exact retry result was {exact_retry:?}");
        return Err(io::Error::other("exact retry was not a durable duplicate").into());
    }
    let mut collision = change.clone();
    let first = collision
        .bytes
        .first_mut()
        .ok_or_else(|| io::Error::other("change unexpectedly empty"))?;
    *first ^= 0xff;
    let collision_grant = fixture_grant(scope, Effect::Write, write_action(&collision), observed);
    let mut collision_result = None;
    for _retry in 0..128 {
        if let Ok(value) = store.accept(&collision, &collision_grant).await {
            collision_result = Some(value);
            break;
        }
    }
    if collision_result != Some(AcceptResult::Conflict) {
        eprintln!("fdb-ambiguity: collision result was {collision_result:?}");
        return Err(io::Error::other("operation collision was accepted as an exact retry").into());
    }
    let mut rebound = change.clone();
    rebound.operation = OperationId::new();
    let rebound_grant = ForgedGrant {
        action: write_action(&rebound),
        ..*grant
    };
    let mut reused = None;
    for _retry in 0..128 {
        if let Ok(value) = store.accept(&rebound, &rebound_grant).await {
            reused = Some(value);
            break;
        }
    }
    if reused != Some(AcceptResult::InvocationConsumed) {
        eprintln!("fdb-ambiguity: reused invocation result was {reused:?}");
        return Err(io::Error::other("runnable invocation was reusable").into());
    }
    Ok((observed, rebound))
}

async fn verify_page(
    store: &FdbServiceStore,
    scope: Scope,
    observed: u64,
    change: &ChangeEnvelope,
    continuation: &ChangeEnvelope,
    rebound: &ChangeEnvelope,
) -> Result<(), Box<dyn Error>> {
    // Force the proof through the continuation path even when buggify produces
    // the recovered commit early. A one-record page also makes every progress
    // and `more` assertion observable rather than merely permitted.
    let count = 1;
    let bytes = u32::try_from(MAX_PAGE_BYTES)?;
    let mut after = 0_u64;
    let mut returned = 0_usize;
    let mut pages = 0_usize;
    let mut first_page_had_more = false;
    let mut recovered_change_count = 0_usize;
    let mut continuation_change_count = 0_usize;
    let mut operations = HashSet::new();
    loop {
        let action = page_action(scope, change.replica, after, count, bytes);
        let grant = fixture_grant(scope, Effect::Sync, action.digest(), observed);
        let page = read_page_with_retry(store, action, &grant).await?;
        if page.position <= after
            || page.position > observed
            || page.changes.is_empty()
            || page.changes.len() > usize::from(count)
            || page.more != (page.position < observed)
        {
            eprintln!(
                "fdb-ambiguity: page bounds failed after={after} position={} observed={observed}",
                page.position
            );
            return Err(io::Error::other("ordered FDB page violated its bounds").into());
        }
        returned = returned
            .checked_add(page.changes.len())
            .ok_or_else(|| io::Error::other("page coverage overflowed"))?;
        pages = pages
            .checked_add(1)
            .ok_or_else(|| io::Error::other("page count overflowed"))?;
        if pages == 1 {
            first_page_had_more = page.more;
        }
        for returned_change in &page.changes {
            if !operations.insert(returned_change.operation) {
                eprintln!(
                    "fdb-ambiguity: repeated operation {}",
                    returned_change.operation
                );
                return Err(io::Error::other("ordered FDB pages repeated an operation").into());
            }
            recovered_change_count += usize::from(returned_change == change);
            continuation_change_count += usize::from(returned_change == continuation);
        }
        after = page.position;
        if !page.more {
            break;
        }
    }
    if pages < 2
        || !first_page_had_more
        || after != observed
        || returned != usize::try_from(observed)?
        || operations.len() != returned
        || recovered_change_count != 1
        || continuation_change_count != 1
    {
        eprintln!(
            "fdb-ambiguity: incomplete pages pages={pages} after={after} observed={observed} returned={returned}"
        );
        return Err(io::Error::other("ordered FDB pages lost their frontier").into());
    }
    let mut byte_ceiling_proven = false;
    let byte_action = page_action(scope, change.replica, 0, count, 1);
    let byte_grant = fixture_grant(scope, Effect::Sync, byte_action.digest(), observed);
    for _retry in 0..128 {
        match store.page(byte_action, &byte_grant).await {
            Err(FdbStoreError::Corruption(FdbDataError::ChangeExceedsPage)) => {
                byte_ceiling_proven = true;
                break;
            }
            Err(_) => {}
            Ok(_) => break,
        }
    }
    if !byte_ceiling_proven {
        eprintln!("fdb-ambiguity: one-byte page accepted an oversized change");
        return Err(io::Error::other("page byte ceiling was not enforced").into());
    }
    let rebound_action = page_action(scope, change.replica, 0, 1, bytes);
    verify_rebound_grants(store, observed, rebound_action, rebound).await
}

async fn read_page_with_retry(
    store: &FdbServiceStore,
    action: PageAction,
    grant: &impl ExecutionGrant,
) -> Result<StoredPage, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..128 {
        match store.page(action, grant).await {
            Ok(PageResult::Page(page)) => return Ok(page),
            Ok(PageResult::StaleAuthority) => {
                let durable = store.frontier(action.scope).await;
                eprintln!(
                    "fdb-ambiguity: stale page authority after={} durable={durable:?}",
                    action.after
                );
                return Err(io::Error::other(format!(
                    "page authority became stale after={} expected_frontier={} durable_frontier={durable:?}", action.after,
                    grant.frontier()
                ))
                .into());
            }
            Ok(PageResult::BehindFloor) => {
                eprintln!(
                    "fdb-ambiguity: page checkpoint expired after={}",
                    action.after
                );
                return Err(io::Error::other(format!(
                    "page checkpoint expired after={} expected_frontier={}",
                    action.after,
                    grant.frontier()
                ))
                .into());
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!(
        "page retry budget exhausted after={} expected_frontier={} last_error={last_error:?}",
        action.after,
        grant.frontier()
    ))
    .into())
}

fn page_action(
    scope: Scope,
    replica: ReplicaId,
    after: u64,
    max_changes: u16,
    max_bytes: u32,
) -> PageAction {
    PageAction {
        scope,
        replica,
        intent: OperationDigest::new([0x70; 32]),
        after,
        max_changes,
        max_bytes,
    }
}

async fn verify_rebound_grants(
    store: &FdbServiceStore,
    observed: u64,
    action: PageAction,
    rebound: &ChangeEnvelope,
) -> Result<(), Box<dyn Error>> {
    let forged = ForgedGrant {
        scope: action.scope,
        effect: Effect::Write,
        action: write_action(rebound),
        frontier: observed,
        invocation: [0x44; 32],
    };
    let wrong_scope = ForgedGrant {
        scope: Scope::new(AccountId::new(), SpaceId::new()),
        ..forged
    };
    let wrong_effect = ForgedGrant {
        effect: Effect::Sync,
        ..forged
    };
    let wrong_action = ForgedGrant {
        action: ActionDigest::new([0x55; 32]),
        ..forged
    };
    for invalid in [wrong_scope, wrong_effect, wrong_action] {
        if store.accept(rebound, &invalid).await? != AcceptResult::StaleAuthority {
            eprintln!("fdb-ambiguity: accept admitted an invalid rebound grant");
            return Err(io::Error::other("accept admitted a rebound grant").into());
        }
    }
    let mut foreign_frontier = None;
    for _retry in 0..128 {
        if let Ok(value) = store.frontier(wrong_scope.scope).await {
            foreign_frontier = Some(value);
            break;
        }
    }
    if foreign_frontier != Some(0) {
        eprintln!("fdb-ambiguity: foreign scope frontier was {foreign_frontier:?}");
        return Err(io::Error::other("scope tuple axis leaked a frontier").into());
    }
    let stale = ForgedGrant {
        scope: action.scope,
        effect: Effect::Sync,
        action: action.digest(),
        frontier: observed.saturating_sub(1),
        invocation: [0x66; 32],
    };
    let mut stale_result = None;
    for _retry in 0..128 {
        if let Ok(value) = store.page(action, &stale).await {
            stale_result = Some(value);
            break;
        }
    }
    if stale_result != Some(PageResult::StaleAuthority) {
        eprintln!("fdb-ambiguity: stale page grant result was {stale_result:?}");
        return Err(io::Error::other("page ignored its justified frontier").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use logical_db_model::{AccountId, Scope, SpaceId};

    use super::{page_action, run};

    #[test]
    fn page_fixture_identity_binds_scope_position_and_limits() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let replica = logical_db_model::ReplicaId::new();
        let baseline = page_action(scope, replica, 0, 1, 1024).digest();
        assert_ne!(
            baseline,
            page_action(scope, logical_db_model::ReplicaId::new(), 0, 1, 1024).digest()
        );
        assert_ne!(baseline, page_action(scope, replica, 1, 1, 1024).digest());
        assert_ne!(baseline, page_action(scope, replica, 0, 2, 1024).digest());
        assert_ne!(baseline, page_action(scope, replica, 0, 1, 1025).digest());
        assert_ne!(
            baseline,
            page_action(
                Scope::new(AccountId::new(), SpaceId::new()),
                replica,
                0,
                1,
                1024,
            )
            .digest()
        );
    }

    #[tokio::test]
    async fn live_fdb_recovers_unknown_commit_and_preserves_all_axes() {
        if std::env::var_os("LOGICAL_DB_FDB_CLUSTER_FILE").is_none() {
            return;
        }
        run().await.expect("live FDB ambiguity proof");
    }
}
