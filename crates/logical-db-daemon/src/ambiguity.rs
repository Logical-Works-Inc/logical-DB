//! Retained live injection of FDB's post-commit `commit_unknown_result` path.
//!
//! Evidence: S01 S02 S03 S04 S05 S06 S07 S08 S09 S10 S11 S12 S13 S14 S15
//! S16 S17 S18 S19 S20 S21 S22 S23 S24 S25 S26 S27 S28 S29 S30 S31 S32
//! S33 S34 S35 S36 S37 S38 S39 S42 S43. Source URLs are in `evidence/SOURCES.md`.

use std::{error::Error, io};

use foundationdb::{Database, api::FdbApiBuilder, options::NetworkOption};
use foundationdb_tuple::Subspace;
use logical_db_braid::prepare_local_request;
use logical_db_crdt::{Document, Object, Scalar};
use logical_db_model::{
    AccountId, DocumentId, Effect, MAX_CHANGES_PER_PAGE, MAX_PAGE_BYTES, OperationDigest,
    OperationId, ReplicaId, Scope, SpaceId,
};
use logical_db_protocol::ChangeEnvelope;
use logical_db_service::{AcceptResult, ExecutionGrant, PageResult, ServiceStore};
use logical_db_store_fdb::{FdbDataError, FdbServiceStore, FdbStoreError};

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
        let grant = prepare_local_request(scope, Effect::Write, request, frontier, u64::MAX)?;
        match store.accept(&change, &grant).await {
            Ok(AcceptResult::RecoveredUnknownCommit) => {
                let (observed, rebound) =
                    verify_recovered_write(&store, scope, frontier, &change, &grant).await?;
                verify_page(&store, scope, observed, &change, &rebound).await?;
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
    let collision_grant =
        prepare_local_request(scope, Effect::Write, collision.request, observed, u64::MAX)?;
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
    rebound: &ChangeEnvelope,
) -> Result<(), Box<dyn Error>> {
    let request = OperationDigest::new([0x77; 32]);
    let grant = prepare_local_request(scope, Effect::Sync, request, observed, u64::MAX)?;
    let count = u16::try_from(MAX_CHANGES_PER_PAGE)?;
    let bytes = u32::try_from(MAX_PAGE_BYTES)?;
    let mut page_result = None;
    for _retry in 0..128 {
        if let Ok(value) = store.page(scope, request, 0, count, bytes, &grant).await {
            page_result = Some(value);
            break;
        }
    }
    let Some(PageResult::Page(page)) = page_result else {
        return Err(io::Error::other("stable page proof became stale").into());
    };
    if page.position != observed
        || page.more
        || page.changes.len() != usize::try_from(observed)?
        || !page.changes.contains(change)
    {
        return Err(io::Error::other("ordered FDB page lost its frontier").into());
    }
    let mut byte_ceiling_proven = false;
    for _retry in 0..128 {
        match store.page(scope, request, 0, count, 1, &grant).await {
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
    verify_rebound_grants(store, scope, observed, request, rebound).await
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
    use super::run;

    #[tokio::test]
    async fn live_fdb_recovers_unknown_commit_and_preserves_all_axes() {
        if std::env::var_os("LOGICAL_DB_FDB_CLUSTER_FILE").is_none() {
            return;
        }
        run().await.expect("live FDB ambiguity proof");
    }
}
