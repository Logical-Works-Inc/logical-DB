//! Live baseline retention: seed, consume, refuse, seal, prune and page behind
//! the floor, all against the real `FdbServiceStore`.

use std::{error::Error, io};

use logical_db_crdt::Document;
use logical_db_e2e::support::Grant;
use logical_db_model::{
    AccountId, Effect, MAX_PAGE_BYTES, OperationDigest, ReplicaId, Scope, SnapshotArtifact,
    SnapshotDocument, SnapshotRecord, SpaceId,
};
use logical_db_protocol::{mark_consumed_action, prune_action, seal_snapshot_action};
use logical_db_service::{
    ConsumedResult, PageResult, PruneReceipt, PruneRefusal, PruneResult, RetentionStore, SealResult,
};
use logical_db_store_fdb::{FdbDataError, FdbServiceStore, FdbStoreError};

use super::{
    page_action,
    recovery::{ambiguity_probe_change, tombstone_probe_change},
    retries::{
        accept_seed_with_retry, build_snapshot_with_retry, mark_consumed_with_retry,
        page_outcome_with_retry, prune_with_retry, read_consumed_with_retry, read_floor_with_retry,
        read_frontier_with_retry, read_snapshot_with_retry, seal_snapshot_with_retry,
    },
};

/// Runs the whole retention journey in a fresh scope and returns its receipt.
pub(super) async fn verify_live_baseline_retention(
    store: &FdbServiceStore,
    replica: ReplicaId,
) -> Result<String, Box<dyn Error>> {
    let scope = Scope::new(AccountId::new(), SpaceId::new());
    let artifact_one = seed_and_build_first(store, scope, replica).await?;
    seal_first_baseline(store, scope, &artifact_one).await?;
    finish_baseline_retention(store, scope, replica, &artifact_one).await
}

/// Seeds a live change and its tombstone, consumes both, refuses a prune with
/// no baseline, and builds the first state-bearing artifact.
async fn seed_and_build_first(
    store: &FdbServiceStore,
    scope: Scope,
    replica: ReplicaId,
) -> Result<SnapshotArtifact, Box<dyn Error>> {
    let initial_frontier = read_frontier_with_retry(store, scope).await?;
    if initial_frontier != 0 {
        return Err(io::Error::other(format!(
            "baseline journey scope was not fresh: frontier={initial_frontier}"
        ))
        .into());
    }
    let first = ambiguity_probe_change(scope, replica, 700)?;
    let first_frontier = accept_seed_with_retry(store, &first, 0).await?;
    if first_frontier != 1 {
        return Err(io::Error::other(format!(
            "baseline journey first seed frontier={first_frontier}"
        ))
        .into());
    }
    let second = tombstone_probe_change(scope, replica, &first, 701)?;
    let second_frontier = accept_seed_with_retry(store, &second, 1).await?;
    if second_frontier != 2 || read_frontier_with_retry(store, scope).await? != 2 {
        return Err(io::Error::other(format!(
            "baseline journey seed frontier did not reach two: {second_frontier}"
        ))
        .into());
    }

    let consume_request = OperationDigest::new([0x91; 32]);
    let consume_action = mark_consumed_action(scope, consume_request, 2);
    let consume_grant = Grant::new(scope, Effect::Sync, consume_action, 2);
    let consumed = mark_consumed_with_retry(store, scope, consume_request, &consume_grant).await?;
    if !matches!(
        consumed,
        ConsumedResult::Advanced {
            consumed_through: 2
        } | ConsumedResult::Unchanged {
            consumed_through: 2
        }
    ) {
        return Err(
            io::Error::other(format!("baseline journey consumed result was {consumed:?}")).into(),
        );
    }
    if read_consumed_with_retry(store, scope).await? != 2 {
        return Err(io::Error::other("baseline journey consumption did not reach two").into());
    }

    let missing_request = OperationDigest::new([0x92; 32]);
    let missing_action = prune_action(scope, missing_request, 2, None);
    let missing_grant = Grant::new(scope, Effect::Sync, missing_action, 2);
    let missing = prune_with_retry(store, scope, missing_request, &missing_grant).await?;
    if missing != PruneResult::Refused(PruneRefusal::MissingBaseline) {
        return Err(io::Error::other(format!(
            "baseline journey missing-baseline result was {missing:?}"
        ))
        .into());
    }
    if read_frontier_with_retry(store, scope).await? != 2
        || read_floor_with_retry(store, scope).await? != 0
    {
        return Err(io::Error::other("missing baseline prune changed frontier or floor").into());
    }

    let artifact_one = build_snapshot_with_retry(store, scope, 1).await?;
    let [ref only] = *artifact_one.documents() else {
        return Err(io::Error::other("first state-bearing artifact was not one document").into());
    };
    if artifact_one.baseline().position() != 1
        || only.document() != first.document()
        || only.is_tombstone()
    {
        return Err(io::Error::other("first state-bearing artifact was incomplete").into());
    }
    if Document::load(only.snapshot())?.is_deleted()? {
        return Err(io::Error::other("first state-bearing artifact lost live state").into());
    }
    Ok(artifact_one)
}

/// Refuses a corrupt baseline seal with a typed error, then seals the first.
async fn seal_first_baseline(
    store: &FdbServiceStore,
    scope: Scope,
    artifact_one: &SnapshotArtifact,
) -> Result<(), Box<dyn Error>> {
    // The descriptor is recomputed by the only public constructor, so the
    // corruption is a tampered document payload: the artifact is internally
    // consistent but disagrees with the durable log the store materializes.
    let tampered = artifact_one
        .documents()
        .iter()
        .map(|document| {
            let mut snapshot = document.snapshot().to_vec();
            let last = snapshot
                .last_mut()
                .ok_or_else(|| io::Error::other("baseline document unexpectedly empty"))?;
            *last ^= 1;
            Ok(SnapshotDocument::new(
                document.document(),
                snapshot,
                document.is_tombstone(),
            ))
        })
        .collect::<Result<Vec<_>, io::Error>>()?;
    let corrupt_artifact =
        SnapshotArtifact::new(scope, artifact_one.baseline().position(), tampered)?;
    let corrupt_request = OperationDigest::new([0x93; 32]);
    let corrupt_action = seal_snapshot_action(scope, corrupt_request, corrupt_artifact.baseline());
    let corrupt_grant = Grant::new(scope, Effect::Sync, corrupt_action, 2);
    let corrupt_result = store
        .seal_snapshot(scope, &corrupt_artifact, corrupt_request, &corrupt_grant)
        .await;
    if !matches!(
        corrupt_result,
        Err(FdbStoreError::Corruption(FdbDataError::BaselineCorrupt))
    ) {
        return Err(io::Error::other(format!(
            "corrupt baseline seal result was {corrupt_result:?}"
        ))
        .into());
    }
    if read_snapshot_with_retry(store, scope).await?.is_some()
        || read_floor_with_retry(store, scope).await? != 0
    {
        return Err(io::Error::other("corrupt baseline seal changed pointer or floor").into());
    }

    let request = OperationDigest::new([0x94; 32]);
    let action = seal_snapshot_action(scope, request, artifact_one.baseline());
    let grant = Grant::new(scope, Effect::Sync, action, 2);
    if seal_snapshot_with_retry(store, scope, artifact_one, request, &grant).await?
        != (SealResult::Sealed { position: 1 })
    {
        return Err(io::Error::other("first state-bearing baseline did not seal").into());
    }
    if !matches!(
        read_snapshot_with_retry(store, scope).await?,
        Some(SnapshotRecord::State(ref baseline)) if baseline == artifact_one.baseline()
    ) {
        return Err(io::Error::other("first state-bearing baseline pointer was wrong").into());
    }
    Ok(())
}

/// Refuses a prune behind the baseline, seals the tombstone baseline, prunes,
/// and proves a pruned checkpoint pages behind the floor.
async fn finish_baseline_retention(
    store: &FdbServiceStore,
    scope: Scope,
    replica: ReplicaId,
    artifact_one: &SnapshotArtifact,
) -> Result<String, Box<dyn Error>> {
    let behind_request = OperationDigest::new([0x95; 32]);
    let behind_action = prune_action(scope, behind_request, 2, None);
    let behind_grant = Grant::new(scope, Effect::Sync, behind_action, 2);
    let behind = prune_with_retry(store, scope, behind_request, &behind_grant).await?;
    if behind != PruneResult::Refused(PruneRefusal::BaselineBehind { position: 1 }) {
        return Err(
            io::Error::other(format!("behind baseline prune result was {behind:?}")).into(),
        );
    }
    if read_frontier_with_retry(store, scope).await? != 2
        || read_floor_with_retry(store, scope).await? != 0
    {
        return Err(io::Error::other("behind baseline prune changed frontier or floor").into());
    }

    let artifact_two = build_snapshot_with_retry(store, scope, 2).await?;
    let not_single = "state-bearing artifacts were not one document each";
    let [ref deleted] = *artifact_two.documents() else {
        return Err(io::Error::other(not_single).into());
    };
    let [ref live] = *artifact_one.documents() else {
        return Err(io::Error::other(not_single).into());
    };
    if artifact_two.baseline().position() != 2
        || deleted.document() != live.document()
        || !deleted.is_tombstone()
    {
        return Err(io::Error::other("second state-bearing artifact was incomplete").into());
    }
    if !Document::load(deleted.snapshot())?.is_deleted()? {
        return Err(io::Error::other("second state-bearing artifact lost its tombstone").into());
    }
    let seal_request = OperationDigest::new([0x96; 32]);
    let seal_action = seal_snapshot_action(scope, seal_request, artifact_two.baseline());
    let seal_grant = Grant::new(scope, Effect::Sync, seal_action, 2);
    if seal_snapshot_with_retry(store, scope, &artifact_two, seal_request, &seal_grant).await?
        != (SealResult::Sealed { position: 2 })
    {
        return Err(io::Error::other("second state-bearing baseline did not seal").into());
    }

    let prune_request = OperationDigest::new([0x97; 32]);
    let prune_grant = Grant::new(
        scope,
        Effect::Sync,
        prune_action(scope, prune_request, 2, None),
        2,
    );
    let pruned = prune_with_retry(store, scope, prune_request, &prune_grant).await?;
    if pruned != PruneResult::Pruned(PruneReceipt::new(2, 2)) {
        return Err(io::Error::other(format!("state-bearing prune result was {pruned:?}")).into());
    }
    let final_frontier = read_frontier_with_retry(store, scope).await?;
    let final_floor = read_floor_with_retry(store, scope).await?;
    if final_frontier != 2 || final_floor != 2 {
        return Err(io::Error::other(format!(
            "state-bearing prune ended at frontier={final_frontier} floor={final_floor}"
        ))
        .into());
    }
    let page = page_action(scope, replica, 0, 1, u32::try_from(MAX_PAGE_BYTES)?);
    let page_grant = Grant::new(scope, Effect::Sync, page.digest(), final_frontier);
    if page_outcome_with_retry(store, page, &page_grant).await? != PageResult::BehindFloor {
        return Err(io::Error::other("pruned checkpoint was not behind the floor").into());
    }
    Ok(format!(
        "PASS fdb-live-baseline-retention seeded=2 consumed=2 missing=refused corrupt=typed-baseline-corrupt baseline1=sealed baseline2=sealed prune=2 frontier={final_frontier} floor={final_floor} page_after=0=behind-floor scope=isolated"
    ))
}
