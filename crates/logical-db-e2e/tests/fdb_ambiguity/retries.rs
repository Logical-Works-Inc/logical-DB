//! Bounded exact retries over the live `FdbServiceStore`.
//!
//! Client buggify injects transient failures into every FDB transaction, so
//! each read and write the journey depends on retries the same request up to
//! [`RETRY_BUDGET`] times and reports the last error when the budget runs out.

use std::{error::Error, io};

use logical_db_e2e::support::Grant;
use logical_db_model::{Effect, OperationDigest, Scope, SnapshotArtifact, SnapshotRecord};
use logical_db_protocol::{ChangeEnvelope, PageAction, write_action};
use logical_db_service::{
    AcceptResult, ConsumedResult, ExecutionGrant, PageResult, PruneResult, RetentionStore,
    SealResult, ServiceStore, StoredPage,
};
use logical_db_store_fdb::FdbServiceStore;

/// Exact retries per request before the journey reports the last failure.
pub(super) const RETRY_BUDGET: usize = 128;

/// Runs one store read until it succeeds or the retry budget is spent.
macro_rules! read_with_retry {
    ($name:ident, $call:ident, $output:ty, $what:literal) => {
        /// Reads the store value, retrying transient FDB failures.
        pub(super) async fn $name(
            store: &FdbServiceStore,
            scope: Scope,
        ) -> Result<$output, Box<dyn Error>> {
            let mut last_error = None;
            for _retry in 0..RETRY_BUDGET {
                match store.$call(scope).await {
                    Ok(value) => return Ok(value),
                    Err(error) => last_error = Some(error),
                }
            }
            Err(io::Error::other(format!(
                concat!($what, " retry budget exhausted: {:?}"),
                last_error
            ))
            .into())
        }
    };
}

read_with_retry!(read_frontier_with_retry, frontier, u64, "frontier");
read_with_retry!(
    read_consumed_with_retry,
    consumed_through,
    u64,
    "consumed-through"
);
read_with_retry!(
    read_floor_with_retry,
    retention_floor,
    u64,
    "retention-floor"
);
read_with_retry!(
    read_snapshot_with_retry,
    snapshot_baseline,
    Option<SnapshotRecord>,
    "snapshot-baseline"
);

/// Accepts one change, retrying transient FDB failures; any outcome is final.
pub(super) async fn accept_with_retry(
    store: &FdbServiceStore,
    change: &ChangeEnvelope,
    grant: &impl ExecutionGrant,
) -> Result<AcceptResult, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..RETRY_BUDGET {
        match store.accept(change, grant).await {
            Ok(outcome) => return Ok(outcome),
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!("accept retry budget exhausted: {last_error:?}")).into())
}

/// Builds the snapshot through `through`, retrying transient FDB failures.
pub(super) async fn build_snapshot_with_retry(
    store: &FdbServiceStore,
    scope: Scope,
    through: u64,
) -> Result<SnapshotArtifact, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..RETRY_BUDGET {
        match store.build_snapshot(scope, through).await {
            Ok(snapshot) => return Ok(snapshot),
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!(
        "build-snapshot retry budget exhausted: {last_error:?}"
    ))
    .into())
}

/// Reads one page outcome of any kind, retrying transient FDB failures.
pub(super) async fn page_outcome_with_retry(
    store: &FdbServiceStore,
    action: PageAction,
    grant: &impl ExecutionGrant,
) -> Result<PageResult, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..RETRY_BUDGET {
        match store.page(action, grant).await {
            Ok(result) => return Ok(result),
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!("page retry budget exhausted: {last_error:?}")).into())
}

/// Reads one served page, failing on stale authority or an expired checkpoint.
pub(super) async fn read_page_with_retry(
    store: &FdbServiceStore,
    action: PageAction,
    grant: &impl ExecutionGrant,
) -> Result<StoredPage, Box<dyn Error>> {
    match page_outcome_with_retry(store, action, grant).await? {
        PageResult::Page(page) => Ok(page),
        PageResult::StaleAuthority => {
            let durable = store.frontier(action.scope).await;
            Err(io::Error::other(format!(
                "page authority became stale after={} expected_frontier={} durable_frontier={durable:?}",
                action.after,
                grant.frontier()
            ))
            .into())
        }
        other => Err(io::Error::other(format!(
            "page after={} expected_frontier={} ended as {other:?}",
            action.after,
            grant.frontier()
        ))
        .into()),
    }
}

/// Accepts one seed change and waits until the frontier moves exactly one step.
pub(super) async fn accept_seed_with_retry(
    store: &FdbServiceStore,
    change: &ChangeEnvelope,
    expected_frontier: u64,
) -> Result<u64, Box<dyn Error>> {
    let grant = Grant::new(
        change.scope(),
        Effect::Write,
        write_action(change),
        expected_frontier,
    );
    let target = expected_frontier.saturating_add(1);
    let mut last_error: Option<String> = None;
    for _retry in 0..RETRY_BUDGET {
        match store.accept(change, &grant).await {
            Ok(
                AcceptResult::Accepted
                | AcceptResult::Duplicate
                | AcceptResult::RecoveredUnknownCommit,
            ) => match store.frontier(change.scope()).await {
                Ok(frontier) if frontier == target => return Ok(frontier),
                Ok(frontier) if frontier > target => {
                    return Err(io::Error::other(format!(
                        "seed frontier advanced past expected position: {frontier}"
                    ))
                    .into());
                }
                Ok(frontier) => last_error = Some(format!("frontier still {frontier}")),
                Err(error) => last_error = Some(format!("{error:?}")),
            },
            Ok(other) => {
                return Err(io::Error::other(format!(
                    "seed change returned unexpected outcome: {other:?}"
                ))
                .into());
            }
            Err(error) => last_error = Some(format!("{error:?}")),
        }
    }
    Err(io::Error::other(format!("seed exact retry budget exhausted: {last_error:?}")).into())
}

/// Marks the scope consumed through two, refusing stale authority.
pub(super) async fn mark_consumed_with_retry(
    store: &FdbServiceStore,
    scope: Scope,
    request: OperationDigest,
    grant: &impl ExecutionGrant,
) -> Result<ConsumedResult, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..RETRY_BUDGET {
        match store.mark_consumed(scope, 2, request, grant).await {
            Ok(
                result @ ConsumedResult::Advanced {
                    consumed_through: 2,
                },
            )
            | Ok(
                result @ ConsumedResult::Unchanged {
                    consumed_through: 2,
                },
            ) => return Ok(result),
            Ok(other) => {
                return Err(io::Error::other(format!(
                    "mark-consumed returned unexpected outcome: {other:?}"
                ))
                .into());
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!(
        "mark-consumed retry budget exhausted: {last_error:?}"
    ))
    .into())
}

/// Seals one snapshot artifact, refusing stale authority.
pub(super) async fn seal_snapshot_with_retry(
    store: &FdbServiceStore,
    scope: Scope,
    artifact: &SnapshotArtifact,
    request: OperationDigest,
    grant: &impl ExecutionGrant,
) -> Result<SealResult, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..RETRY_BUDGET {
        match store.seal_snapshot(scope, artifact, request, grant).await {
            Ok(SealResult::StaleAuthority) => {
                return Err(io::Error::other("seal snapshot authority became stale").into());
            }
            Ok(result) => return Ok(result),
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!(
        "seal snapshot retry budget exhausted: {last_error:?}"
    ))
    .into())
}

/// Prunes through two, returning only a terminal refusal or receipt.
pub(super) async fn prune_with_retry(
    store: &FdbServiceStore,
    scope: Scope,
    request: OperationDigest,
    grant: &impl ExecutionGrant,
) -> Result<PruneResult, Box<dyn Error>> {
    let mut last_error = None;
    for _retry in 0..RETRY_BUDGET {
        match store.prune(scope, 2, None, request, grant).await {
            Ok(result @ (PruneResult::Refused(_) | PruneResult::Pruned(_))) => return Ok(result),
            Ok(other) => {
                return Err(io::Error::other(format!("prune ended as {other:?}")).into());
            }
            Err(error) => last_error = Some(error),
        }
    }
    Err(io::Error::other(format!("prune retry budget exhausted: {last_error:?}")).into())
}
