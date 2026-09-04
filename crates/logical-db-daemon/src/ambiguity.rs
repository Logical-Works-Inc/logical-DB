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
    AccountId, DocumentId, Effect, MAX_PAGE_BYTES, OperationDigest, OperationId, ReplicaId, Scope,
    SpaceId,
};
use logical_db_protocol::ChangeEnvelope;
use logical_db_service::{AcceptResult, ExecutionGrant, PageResult, ServiceStore, StoredPage};
use logical_db_store_fdb::{FdbDataError, FdbServiceStore, FdbStoreError};
use sha2::{Digest, Sha256};

const FULL_FDB_CHUNK_PROBE_BYTES: usize = 65_536;

#[derive(Clone, Copy)]
struct ForgedGrant {
    scope: Scope,
    effect: Effect,
    request: OperationDigest,
    frontier: u64,
    invocation: [u8; 32],
}

impl ExecutionGrant for ForgedGrant {
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
        self.invocation
    }
}

fn fixture_grant(
    scope: Scope,
    effect: Effect,
    request: OperationDigest,
    frontier: u64,
) -> ForgedGrant {
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
    digest.update(request.into_bytes());
    digest.update(frontier.to_be_bytes());
    ForgedGrant {
        scope,
        effect,
        request,
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
    let network_builder = FdbApiBuilder::default()
        .build()?
        .set_option(NetworkOption::ClientBuggifyEnable)?
        .set_option(NetworkOption::ClientBuggifySectionActivatedProbability(100))?
        .set_option(NetworkOption::ClientBuggifySectionFiredProbability(12))?;
    // SAFETY: this dedicated test process boots exactly one FDB network and
    // retains the auto-stop guard beyond every database, store, and future.
    #[allow(unsafe_code)]
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
    let store = FdbServiceStore::new(database, root)?;
    let replica = ReplicaId::new();

    for attempt in 0_u16..512 {
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
        let request_byte = u8::try_from(attempt % 256).expect("modulo 256 fits u8");
        let request = OperationDigest::new([request_byte; 32]);
        let change = ChangeEnvelope {
            scope,
            document: DocumentId::new(),
            replica,
            operation: OperationId::new(),
            request,
            digest: encoded.digest,
            bytes: encoded.bytes,
        };
        let Ok(frontier) = store.frontier(scope).await else {
            continue;
        };
        let grant = fixture_grant(scope, Effect::Write, request, frontier);
        match store.accept(&change, &grant).await {
            Ok(AcceptResult::RecoveredUnknownCommit) => {
                let (observed, rebound) =
                    verify_recovered_write(&store, scope, frontier, &change, &grant).await?;
                let (observed, continuation) =
                    append_continuation_record(&store, scope, observed).await?;
                verify_page(&store, scope, observed, &change, &continuation, &rebound).await?;
                println!(
                    "PASS fdb-commit-unknown-recovered attempt={attempt} frontier={observed} invocation=consumed page=bounded scope=isolated"
                );
                drop(store);
                drop(network);
                return Ok(());
            }
            Ok(AcceptResult::Accepted) | Err(_) => {}
            Ok(other) => {
                return Err(
                    io::Error::other(format!("unexpected acceptance result: {other:?}")).into(),
                );
            }
        }
    }
    Err(io::Error::other("FDB buggify did not exercise a post-commit unknown result").into())
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
        let grant = fixture_grant(scope, Effect::Write, request, frontier);
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
    grant: &impl ExecutionGrant,
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
        return Err(io::Error::other("exact retry was not a durable duplicate").into());
    }
    let mut collision = change.clone();
    let first = collision
        .bytes
        .first_mut()
        .ok_or_else(|| io::Error::other("change unexpectedly empty"))?;
    *first ^= 0xff;
    let collision_grant = fixture_grant(scope, Effect::Write, collision.request, observed);
    let mut collision_result = None;
    for _retry in 0..128 {
        if let Ok(value) = store.accept(&collision, &collision_grant).await {
            collision_result = Some(value);
            break;
        }
    }
    if collision_result != Some(AcceptResult::Conflict) {
        return Err(io::Error::other("operation collision was accepted as an exact retry").into());
    }
    let mut rebound = change.clone();
    rebound.operation = OperationId::new();
    let mut reused = None;
    for _retry in 0..128 {
        if let Ok(value) = store.accept(&rebound, grant).await {
            reused = Some(value);
            break;
        }
    }
    if reused != Some(AcceptResult::InvocationConsumed) {
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
        let request = page_fixture_request(scope, after, count, bytes);
        let grant = fixture_grant(scope, Effect::Sync, request, observed);
        let page = read_page_with_retry(store, scope, request, after, count, bytes, &grant).await?;
        if page.position <= after
            || page.position > observed
            || page.changes.is_empty()
            || page.changes.len() > usize::from(count)
            || page.more != (page.position < observed)
        {
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
        return Err(io::Error::other("ordered FDB pages lost their frontier").into());
    }
    let mut byte_ceiling_proven = false;
    let byte_request = page_fixture_request(scope, 0, count, 1);
    let byte_grant = fixture_grant(scope, Effect::Sync, byte_request, observed);
    for _retry in 0..128 {
        match store
            .page(scope, byte_request, 0, count, 1, &byte_grant)
            .await
        {
            Err(FdbStoreError::Corruption(FdbDataError::ChangeExceedsPage)) => {
                byte_ceiling_proven = true;
                break;
            }
            Err(_) => {}
            Ok(_) => break,
        }
    }
    if !byte_ceiling_proven {
        return Err(io::Error::other("page byte ceiling was not enforced").into());
    }
    let rebound_request = page_fixture_request(scope, 0, 1, bytes);
    verify_rebound_grants(store, scope, observed, rebound_request, rebound).await
}

async fn read_page_with_retry(
    store: &FdbServiceStore,
    scope: Scope,
    request: OperationDigest,
    after: u64,
    max_changes: u16,
    max_bytes: u32,
    grant: &impl ExecutionGrant,
) -> Result<StoredPage, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..128 {
        match store
            .page(scope, request, after, max_changes, max_bytes, grant)
            .await
        {
            Ok(PageResult::Page(page)) => return Ok(page),
            Ok(PageResult::StaleAuthority) => {
                let durable = store.frontier(scope).await;
                return Err(io::Error::other(format!(
                    "page authority became stale after={after} expected_frontier={} durable_frontier={durable:?}",
                    grant.frontier()
                ))
                .into());
            }
            Ok(PageResult::BehindFloor) => {
                return Err(io::Error::other(format!(
                    "page checkpoint expired after={after} expected_frontier={}",
                    grant.frontier()
                ))
                .into());
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!(
        "page retry budget exhausted after={after} expected_frontier={} last_error={last_error:?}",
        grant.frontier()
    ))
    .into())
}

fn page_fixture_request(
    scope: Scope,
    after: u64,
    max_changes: u16,
    max_bytes: u32,
) -> OperationDigest {
    let mut digest = Sha256::new();
    digest.update(b"logical-db/fdb-page-fixture/v1\0");
    digest.update(scope.account().as_uuid().as_bytes());
    digest.update(scope.space().as_uuid().as_bytes());
    digest.update(after.to_be_bytes());
    digest.update(max_changes.to_be_bytes());
    digest.update(max_bytes.to_be_bytes());
    OperationDigest::new(digest.finalize().into())
}

async fn verify_rebound_grants(
    store: &FdbServiceStore,
    scope: Scope,
    observed: u64,
    request: OperationDigest,
    rebound: &ChangeEnvelope,
) -> Result<(), Box<dyn Error>> {
    let forged = ForgedGrant {
        scope,
        effect: Effect::Write,
        request: rebound.request,
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
    let wrong_request = ForgedGrant {
        request: OperationDigest::new([0x55; 32]),
        ..forged
    };
    for invalid in [wrong_scope, wrong_effect, wrong_request] {
        if store.accept(rebound, &invalid).await? != AcceptResult::StaleAuthority {
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
        return Err(io::Error::other("scope tuple axis leaked a frontier").into());
    }
    let stale = ForgedGrant {
        scope,
        effect: Effect::Sync,
        request,
        frontier: observed.saturating_sub(1),
        invocation: [0x66; 32],
    };
    let mut stale_result = None;
    for _retry in 0..128 {
        if let Ok(value) = store
            .page(scope, request, 0, 1, u32::try_from(MAX_PAGE_BYTES)?, &stale)
            .await
        {
            stale_result = Some(value);
            break;
        }
    }
    if stale_result != Some(PageResult::StaleAuthority) {
        return Err(io::Error::other("page ignored its justified frontier").into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use logical_db_model::{AccountId, Scope, SpaceId};

    use super::{page_fixture_request, run};

    #[test]
    fn page_fixture_identity_binds_scope_position_and_limits() {
        let scope = Scope::new(AccountId::new(), SpaceId::new());
        let baseline = page_fixture_request(scope, 0, 1, 1024);
        assert_ne!(baseline, page_fixture_request(scope, 1, 1, 1024));
        assert_ne!(baseline, page_fixture_request(scope, 0, 2, 1024));
        assert_ne!(baseline, page_fixture_request(scope, 0, 1, 1025));
        assert_ne!(
            baseline,
            page_fixture_request(Scope::new(AccountId::new(), SpaceId::new()), 0, 1, 1024)
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
