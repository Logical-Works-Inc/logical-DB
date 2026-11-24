//! The post-commit `commit_unknown_result` recovery path: the recovered write
//! is exactly-once, its invocation is single-use, ordered pages cover it and a
//! later continuation record, and every rebound grant is refused.

use std::{collections::HashSet, error::Error, io};

use lgwks_std::hash::Hasher;
use logical_db_crdt::{Document, EncodedChange, Object, Scalar};
use logical_db_e2e::support::Grant;
use logical_db_model::{
    AccountId, ActionDigest, DocumentId, Effect, MAX_PAGE_BYTES, OperationDigest, OperationId,
    ReplicaId, Scope, SpaceId,
};
use logical_db_protocol::{ChangeEnvelope, PageAction, write_action};
use logical_db_service::{AcceptResult, PageResult, ServiceStore};
use logical_db_store_fdb::{FdbDataError, FdbServiceStore, FdbStoreError};

use super::{
    page_action,
    retries::{
        RETRY_BUDGET, accept_with_retry, page_outcome_with_retry, read_frontier_with_retry,
        read_page_with_retry,
    },
};

/// Bytes in the probe value: one full FDB value chunk.
const FULL_FDB_CHUNK_PROBE_BYTES: usize = 65_536;
/// Upper bound on one-record pages the page verifier walks.
const MAX_VERIFIED_PAGES: usize = 1_024;

/// Proves the whole recovery path after a `RecoveredUnknownCommit` accept.
pub(super) async fn complete_recovery_path(
    store: &FdbServiceStore,
    scope: Scope,
    frontier: u64,
    change: &ChangeEnvelope,
    grant: &Grant,
) -> Result<u64, Box<dyn Error>> {
    let (observed, rebound) = verify_recovered_write(store, scope, frontier, change, grant).await?;
    let (observed, continuation) = append_continuation_record(store, scope, observed).await?;
    verify_page(store, scope, observed, change, &continuation, &rebound).await?;
    Ok(observed)
}

/// The request byte of one probe attempt: the attempt's low byte.
const fn request_byte(attempt: u16) -> u8 {
    let [_, low] = attempt.to_be_bytes();
    low
}

/// Builds one probe change carrying a full FDB value chunk.
pub(super) fn ambiguity_probe_change(
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
    Ok(ChangeEnvelope::new(
        scope,
        DocumentId::new(),
        replica,
        OperationId::new(),
        OperationDigest::new([request_byte(attempt); 32]),
        encoded.digest(),
        encoded.as_bytes().to_vec(),
    ))
}

/// Builds the tombstone that deletes the document `previous` created.
pub(super) fn tombstone_probe_change(
    scope: Scope,
    replica: ReplicaId,
    previous: &ChangeEnvelope,
    attempt: u16,
) -> Result<ChangeEnvelope, Box<dyn Error>> {
    let mut document = Document::new();
    document.apply(&[EncodedChange::new(
        previous.digest(),
        previous.bytes().to_vec(),
    )])?;
    let encoded = document
        .delete()?
        .pop()
        .ok_or_else(|| io::Error::other("missing CRDT tombstone change"))?;
    Ok(ChangeEnvelope::new(
        scope,
        previous.document(),
        replica,
        OperationId::new(),
        OperationDigest::new([request_byte(attempt); 32]),
        encoded.digest(),
        encoded.as_bytes().to_vec(),
    ))
}

/// Appends one known record after the recovered write, so paging must cross it.
async fn append_continuation_record(
    store: &FdbServiceStore,
    scope: Scope,
    minimum_frontier: u64,
) -> Result<(u64, ChangeEnvelope), Box<dyn Error>> {
    let mut last_outcome = String::from("no attempt ran");
    for attempt in 0_u16..512 {
        let frontier = read_frontier_with_retry(store, scope).await?;
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
        let change = ChangeEnvelope::new(
            scope,
            DocumentId::new(),
            ReplicaId::new(),
            OperationId::new(),
            continuation_fixture_request(scope, frontier, attempt),
            encoded.digest(),
            encoded.as_bytes().to_vec(),
        );
        let grant = Grant::new(scope, Effect::Write, write_action(&change), frontier);
        match store.accept(&change, &grant).await {
            Ok(AcceptResult::Accepted | AcceptResult::RecoveredUnknownCommit) => {
                let observed = frontier
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("continuation frontier overflowed"))?;
                return Ok((observed, change));
            }
            outcome => last_outcome = format!("{outcome:?}"),
        }
    }
    Err(io::Error::other(format!(
        "could not append a known continuation record; last outcome {last_outcome}"
    ))
    .into())
}

/// A request digest unique to one scope, frontier and attempt.
fn continuation_fixture_request(scope: Scope, frontier: u64, attempt: u16) -> OperationDigest {
    let mut digest = Hasher::new();
    digest.update(b"logical-db/fdb-continuation-fixture/v1\0");
    digest.update(scope.account().as_uuid().as_bytes());
    digest.update(scope.space().as_uuid().as_bytes());
    digest.update(&frontier.to_be_bytes());
    digest.update(&attempt.to_be_bytes());
    OperationDigest::new(*digest.finalize().as_bytes())
}

/// Proves the recovered write advanced once, retries as a duplicate, refuses
/// an operation collision, and refuses its invocation rebound to a new change.
async fn verify_recovered_write(
    store: &FdbServiceStore,
    scope: Scope,
    frontier: u64,
    change: &ChangeEnvelope,
    grant: &Grant,
) -> Result<(u64, ChangeEnvelope), Box<dyn Error>> {
    let observed = read_frontier_with_retry(store, scope).await?;
    if observed != frontier.saturating_add(1) {
        return Err(io::Error::other("unknown recovery advanced more than once").into());
    }
    let exact_retry = accept_with_retry(store, change, grant).await?;
    if !matches!(
        exact_retry,
        AcceptResult::Duplicate | AcceptResult::RecoveredUnknownCommit
    ) {
        return Err(io::Error::other(format!(
            "exact retry was not a durable duplicate: {exact_retry:?}"
        ))
        .into());
    }
    let mut collided_bytes = change.bytes().to_vec();
    let first = collided_bytes
        .first_mut()
        .ok_or_else(|| io::Error::other("change unexpectedly empty"))?;
    *first ^= 0xff;
    let collision = ChangeEnvelope::new(
        change.scope(),
        change.document(),
        change.replica(),
        change.operation(),
        change.request(),
        change.digest(),
        collided_bytes,
    );
    let collision_grant = Grant::new(scope, Effect::Write, write_action(&collision), observed);
    let collision_result = accept_with_retry(store, &collision, &collision_grant).await?;
    if collision_result != AcceptResult::Conflict {
        return Err(io::Error::other(format!(
            "operation collision was accepted as an exact retry: {collision_result:?}"
        ))
        .into());
    }
    let rebound = ChangeEnvelope::new(
        change.scope(),
        change.document(),
        change.replica(),
        OperationId::new(),
        change.request(),
        change.digest(),
        change.bytes().to_vec(),
    );
    // The same grant and invocation identity, rebound to another action: the
    // store must refuse it once that invocation is consumed.
    let mut rebound_grant = *grant;
    rebound_grant.action = write_action(&rebound);
    let reused = accept_with_retry(store, &rebound, &rebound_grant).await?;
    if reused != AcceptResult::InvocationConsumed {
        return Err(
            io::Error::other(format!("runnable invocation was reusable: {reused:?}")).into(),
        );
    }
    Ok((observed, rebound))
}

/// Walks one-record pages from zero to `observed` and proves exact coverage,
/// then proves the byte ceiling and the rebound-grant refusals.
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
    while pages < MAX_VERIFIED_PAGES {
        let action = page_action(scope, change.replica(), after, count, bytes);
        let grant = Grant::new(scope, Effect::Sync, action.digest(), observed);
        let page = read_page_with_retry(store, action, &grant).await?;
        if page.position() <= after
            || page.position() > observed
            || page.changes().is_empty()
            || page.changes().len() > usize::from(count)
            || page.more() != (page.position() < observed)
        {
            return Err(io::Error::other("ordered FDB page violated its bounds").into());
        }
        returned = returned
            .checked_add(page.changes().len())
            .ok_or_else(|| io::Error::other("page coverage overflowed"))?;
        pages = pages
            .checked_add(1)
            .ok_or_else(|| io::Error::other("page count overflowed"))?;
        if pages == 1 {
            first_page_had_more = page.more();
        }
        for returned_change in page.changes() {
            if !operations.insert(returned_change.operation()) {
                return Err(io::Error::other("ordered FDB pages repeated an operation").into());
            }
            recovered_change_count =
                recovered_change_count.saturating_add(usize::from(returned_change == change));
            continuation_change_count = continuation_change_count
                .saturating_add(usize::from(returned_change == continuation));
        }
        after = page.position();
        if !page.more() {
            break;
        }
    }
    if pages == MAX_VERIFIED_PAGES && after != observed {
        return Err(io::Error::other("ordered FDB page verification exceeded its bound").into());
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
    verify_byte_ceiling(store, scope, observed, change.replica()).await?;
    let rebound_action = page_action(scope, change.replica(), 0, 1, bytes);
    verify_rebound_grants(store, observed, rebound_action, rebound).await
}

/// A one-byte page budget must refuse the first change with a typed error.
async fn verify_byte_ceiling(
    store: &FdbServiceStore,
    scope: Scope,
    observed: u64,
    replica: ReplicaId,
) -> Result<(), Box<dyn Error>> {
    let action = page_action(scope, replica, 0, 1, 1);
    let grant = Grant::new(scope, Effect::Sync, action.digest(), observed);
    let mut last_outcome = String::from("no attempt ran");
    for _retry in 0..RETRY_BUDGET {
        match store.page(action, &grant).await {
            Err(FdbStoreError::Corruption(FdbDataError::ChangeExceedsPage)) => return Ok(()),
            Err(error) => last_outcome = format!("{error:?}"),
            Ok(outcome) => {
                last_outcome = format!("{outcome:?}");
                break;
            }
        }
    }
    Err(io::Error::other(format!(
        "page byte ceiling was not enforced: {last_outcome}"
    ))
    .into())
}

/// Every grant bound to the wrong scope, effect, action or frontier is refused.
async fn verify_rebound_grants(
    store: &FdbServiceStore,
    observed: u64,
    action: PageAction,
    rebound: &ChangeEnvelope,
) -> Result<(), Box<dyn Error>> {
    let foreign = Scope::new(AccountId::new(), SpaceId::new());
    let rebound_action = write_action(rebound);
    let invalid = [
        Grant::new(foreign, Effect::Write, rebound_action, observed),
        Grant::new(action.scope, Effect::Sync, rebound_action, observed),
        Grant::new(
            action.scope,
            Effect::Write,
            ActionDigest::new([0x55; 32]),
            observed,
        ),
    ];
    for mut grant in invalid {
        grant.invocation = [0x44; 32];
        let outcome = accept_with_retry(store, rebound, &grant).await?;
        if outcome != AcceptResult::StaleAuthority {
            return Err(
                io::Error::other(format!("accept admitted a rebound grant: {outcome:?}")).into(),
            );
        }
    }
    if read_frontier_with_retry(store, foreign).await? != 0 {
        return Err(io::Error::other("scope tuple axis leaked a frontier").into());
    }
    let mut stale = Grant::new(
        action.scope,
        Effect::Sync,
        action.digest(),
        observed.saturating_sub(1),
    );
    stale.invocation = [0x66; 32];
    let stale_result = page_outcome_with_retry(store, action, &stale).await?;
    if stale_result != PageResult::StaleAuthority {
        return Err(io::Error::other(format!(
            "page ignored its justified frontier: {stale_result:?}"
        ))
        .into());
    }
    Ok(())
}
